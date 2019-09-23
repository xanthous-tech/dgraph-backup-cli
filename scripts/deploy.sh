#!/bin/bash
docker build -t necmettn/dgraph-backup -f Dockerfile .

docker push necmettn/dgraph-backup:latest

docker pull necmettn/dgraph-backup:latest

