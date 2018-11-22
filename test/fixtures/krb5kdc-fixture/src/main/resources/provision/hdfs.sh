#!/bin/bash

set -e

addprinc.sh "elasticsearch"
addprinc.sh "hdfs/hdfs.build.elastic.co"

sleep infinity