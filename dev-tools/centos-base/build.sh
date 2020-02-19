#!/bin/bash

# This script builds a tiny base image for running Elasticsearch.

set -ex

# 1. Build the builder image
docker build -t centos-builder:latest .

# 2. Run the builder image, giving it access to the local Docker daemon so
# that the tiny base image can be created
docker run -v /var/run/docker.sock:/var/run/docker.sock centos-builder:latest

# 3. Check that the tiny image is vaguely functional
docker run -i -t --rm elasticsearch/centos-base:7 /bin/bash -c 'echo success'
