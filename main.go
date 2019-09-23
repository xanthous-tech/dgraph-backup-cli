package main

import (
  "compress/flate"
  "fmt"
  "github.com/aws/aws-sdk-go/service/s3"
  "io/ioutil"
  "log"
  "net/http"
  "os"
  "os/exec"
  "regexp"
  "strings"
  "time"

  "github.com/aws/aws-sdk-go/aws"
  "github.com/aws/aws-sdk-go/aws/credentials"
  "github.com/aws/aws-sdk-go/aws/session"
  "github.com/aws/aws-sdk-go/service/s3/s3manager"

  "github.com/fatih/color"
  "github.com/jasonlvhit/gocron"
  "github.com/jpillora/backoff"
  "github.com/manifoldco/promptui"
  "github.com/mholt/archiver"
  "github.com/urfave/cli"
)

var (
  Format          string = "format"
  AwsBucket       string = "aws-bucket"
  AwsRegion       string = "aws-region"
  DgraphHost      string = "dgraph-host"
  FilePrefix      string = "file-prefix"
  AwsKey          string = "aws-key"
  AwsSecret       string = "aws-secret"
  ExportPath      string = "export-path"
  CronEveryMinute string = "cron-every-minute"
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
  err = z.Archive([]string{c.String(ExportPath)}, fileName)
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
  err = os.RemoveAll(c.String(ExportPath))
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
        if b.Attempt() == 1 {
          log.Println("NO SUCCESS  after 10 try")
          os.Exit(0)
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
  gocron.Every(c.Uint64(CronEveryMinute)).Minutes().Do(Export, c)
  <-gocron.Start()
}

func getBackUpFile(sess *session.Session, c *cli.Context) (key string) {

  // Create S3 service client
  svc := s3.New(sess)
  resp, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(c.String(AwsBucket))})
  if err != nil {
    log.Println("ERROR getting bucket")
  }

  templates := &promptui.SelectTemplates{
    Label:    "{{ . }}?",
    Active:   "\U0001F336 {{ .Key }} ({{ .Size }})",
    Inactive: "  {{ .Key }} ({{ .Size }})",
    Selected: "\U0001F336 {{ .Key }}",
    Details: `
--------- BACKUP FILE ----------
{{ "Name:" | faint }}	{{ .Key }}
{{ "Size:" | faint }}	{{ .Size }}
{{ "Last Modified:" | faint }}	{{ .LastModified }}`,
  }

  prompt := promptui.Select{
    Label:     "Select BackupTo Restore",
    Items:     resp.Contents,
    Templates: templates,
  }
  _, result, err := prompt.Run()
  r := regexp.MustCompile(`Key[:]\s\"(.*)\"`)
  key = r.FindStringSubmatch(result)[1]
  return key
}

func DownloadFile(c *cli.Context, sess *session.Session, fileName string) (filepath string) {
  downloader := s3manager.NewDownloader(sess)
  file, err := os.Create(fileName)
  if err != nil {
    log.Fatal("Unable to open file %q, %v", err)
  }
  defer file.Close()
  numBytes, err := downloader.Download(file,
    &s3.GetObjectInput{
      Bucket: aws.String(c.String(AwsBucket)),
      Key:    aws.String(fileName),
    })
  if err != nil {
    log.Fatal("Unable to download item %q, %v", fileName, err)
  }
  fmt.Println("Downloaded", file.Name(), numBytes, "bytes")
  err = archiver.Unarchive("./"+file.Name(), "data")
  return "./data/export/"
}

func Restore(c *cli.Context) {
  sess := session.Must(session.NewSession(&aws.Config{
    Region:      aws.String(c.String(AwsRegion)),
    Credentials: credentials.NewStaticCredentials(c.String(AwsKey), c.String(AwsSecret), ""),
  }))
  file := getBackUpFile(sess, c)
  filePath := DownloadFile(c, sess, file)
  cmd := exec.Command("dgraph", "live", "-f", filePath)
  stdoutStderr, err := cmd.CombinedOutput()
  if err != nil {
    log.Fatal(err)
  }
  fmt.Printf("%s\n", stdoutStderr)
  if err != nil {
    log.Fatal("Problem with Running", err)
  }

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
      Name:   ExportPath,
      EnvVar: "EXPORT_PATH",
      Value:  "./export",
    },

    cli.Uint64Flag{
      Name:   CronEveryMinute,
      EnvVar: "CRON_EVERY_MINUTE",
      Value:  1,
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
      Name:   "restore",
      Action: Restore,
      Flags:  Flags,
    },
  }

  err := app.Run(os.Args)
  if err != nil {
    log.Fatal(err)
  }
}
