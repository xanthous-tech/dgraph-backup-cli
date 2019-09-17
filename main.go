package main

import (
  "compress/flate"
  "fmt"
  "io/ioutil"
  "log"
  "net/http"
  "os"
  "strings"
  "time"

  "github.com/aws/aws-sdk-go/aws"
  "github.com/aws/aws-sdk-go/aws/credentials"
  "github.com/aws/aws-sdk-go/aws/session"
  "github.com/aws/aws-sdk-go/service/s3/s3manager"

  "github.com/fatih/color"
  "github.com/jasonlvhit/gocron"
  "github.com/jpillora/backoff"
  "github.com/mholt/archiver"
  "github.com/urfave/cli"
)

var (
  Format     string = "format"
  AwsBucket  string = "aws-bucket"
  AwsRegion  string = "aws-region"
  DgraphHost string = "dgraph-host"
  FilePrefix string = "file-prefix"
  AwsKey     string = "aws-key"
  AwsSecret  string = "aws-secret"
  ExportPath string = "export-path"
)

func requestExport(c *cli.Context) (success bool) {
  var requestUri = c.String(DgraphHost) + "/admin/export?format=" + c.String(Format)
  yellow := color.New(color.FgYellow).SprintFunc()
  fmt.Printf("Requesting Export from %s \n", yellow(requestUri))
  req, _ := http.NewRequest("GET", requestUri, nil)

  client := &http.Client{}
  resp, err := client.Do(req)
  if err != nil {
    log.Panic("Cannot access to dgraph server", err)
  }
  defer resp.Body.Close()
  if http.StatusOK == resp.StatusCode {
    bodyBytes, err := ioutil.ReadAll(resp.Body)
    if err != nil {
      log.Fatal(err)
    }
    bodyString := string(bodyBytes)
    isSuccess := strings.Contains(bodyString, "Success")
    if isSuccess != true {
      log.Fatal("Export Failed", bodyString)
      return false
    }
    color.Blue("Data export successful")
    return true
  }
  return false
}

func zipIt(c *cli.Context) (filePath string, err error) {
  z := archiver.Zip{
    CompressionLevel: flate.DefaultCompression,
  }
  fileName := "./" + c.String(FilePrefix) + "-" + time.Now().Format(time.RFC3339) + ".zip"
  err = z.Archive([]string{"./export"}, fileName)
  if err != nil {
    log.Fatal("err Zipping", err)
    return "", err
  }
  return fileName, nil
}

func shipIt(c *cli.Context, filename string) error {
  // The session the S3 Uploader will use
  sess := session.Must(session.NewSession(&aws.Config{
    Region:      aws.String(c.String(AwsRegion)),
    Credentials: credentials.NewStaticCredentials(c.String(AwsKey), c.String(AwsSecret), ""),
  }))

  // Create an uploader with the session and default options
  uploader := s3manager.NewUploader(sess)

  f, err := os.Open(filename)
  if err != nil {
    return fmt.Errorf("failed to open file %q, %v", filename, err)
  }

  // Upload the file to S3.
  result, err := uploader.Upload(&s3manager.UploadInput{
    Bucket: aws.String(c.String(AwsBucket)),
    Key:    aws.String(filename),
    Body:   f,
  })
  if err != nil {
    return fmt.Errorf("failed to upload file, %v", err)
  }
  info := color.New(color.FgWhite, color.BgGreen).SprintFunc()
  fmt.Printf("%s Uploaded to %s \n", info("[DONE]"), aws.StringValue(&result.Location))
  return nil
}

func cleanUp(c *cli.Context, filePath string) {
  err := os.Remove(filePath)
  err = os.Remove(c.String(ExportPath))
  if err != nil {
    fmt.Println("Error while deleting side effects.", err)
  }
}

func Export(c *cli.Context) {
  success := requestExport(c)
  if success {
    b := &backoff.Backoff{
      Max: 5 * time.Minute,
    }
    for {
      _, err := os.Stat(c.String(ExportPath))
      if os.IsNotExist(err) {
        d := b.Duration()
        log.Printf("Export is not ready yet, retrying in  %s", d)
        time.Sleep(d)
        if b.Attempt() == 10 {
          log.Fatal("NO SUCCESS  after 10 try")
        }
        continue
      }
      b.Reset()
      filePath, _ := zipIt(c)
      err = shipIt(c, filePath)
      if err != nil {
        color.Red("Failed to upload", err)
      }
      cleanUp(c, filePath)
      color.Green("SUCCESS")
      break
    }
  }
}

func cronjob(c *cli.Context) {
  gocron.Every(1).Day().At("01:00").Do(Export, c)
  <-gocron.Start()
}

func main() {
  app := cli.NewApp()
  app.Name = "dgraph-backup"
  Flags := []cli.Flag{
    cli.StringFlag{
      Name:   "format",
      Value:  "json",
      EnvVar: "EXPORT_FORMAT",
      Usage:  "you can set rdf or json",
    },
    cli.StringFlag{
      Name:   AwsBucket,
      Value:  "dgraph-backup",
      EnvVar: "AWS_BUCKET",
      Usage:  "AWS Bucket",
    },
    cli.StringFlag{
      Name:   AwsRegion,
      Value:  "ap-northeast-2",
      EnvVar: "AWS_REGION",
    },
    cli.StringFlag{
      Name:   DgraphHost,
      Value:  "http://localhost:8080",
      EnvVar: "DGRAPH_HOST",
      Usage:  "Exp: http://localhost:8080",
    },
    cli.StringFlag{
      Name:   FilePrefix,
      Value:  "dgraph-backup",
      EnvVar: "FILE_PREFIX",
      Usage:  "Backup file prefix <prefix>-<timestamp>.zip",
    },
    cli.StringFlag{
      Name:     AwsKey,
      EnvVar:   "AWS_ACCESS_KEY",
      Required: true,
    },
    cli.StringFlag{
      Name:     AwsSecret,
      EnvVar:   "AWS_ACCESS_SECRET",
      Required: true,
    },
    cli.StringFlag{
      Name:     ExportPath,
      EnvVar:   "EXPORT_PATH",
      Value:    "./export",
    },
  }
  app.Commands = []cli.Command{
    {
      Name:   "backup-now",
      Action: Export,
      Flags:  Flags,
    },
    {
      Name:   "backup-cron",
      Action: cronjob,
      Flags:  Flags,
    },
    {
      Name:  "restore",
      Flags: Flags,
    },
  }

  err := app.Run(os.Args)
  if err != nil {
    log.Fatal(err)
  }
}
