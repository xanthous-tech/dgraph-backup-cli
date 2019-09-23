#!/bin/bash
# GOOS=linux GOARCH=amd64 go build -ldflags="-w -s" -o ./dgraph-backup
CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o dgraph-backup .

