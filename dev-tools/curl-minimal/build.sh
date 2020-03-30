#!/bin/bash

set -exo pipefail

IMAGE=curl-minimal:latest

docker build -t $IMAGE .

docker run -it --rm $IMAGE /curl https://www.google.com/
