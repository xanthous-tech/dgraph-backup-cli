FROM dgraph/dgraph:latest

RUN mkdir /backup
COPY ./dgraph-backup /backup/dgraph-backup
WORKDIR /backup
#RUN apk update && apk add ca-certificates && rm -rf /var/cache/apk/*
CMD ["./dgraph-backup","backup-cron"]
