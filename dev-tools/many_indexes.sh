#!/usr/bin/env bash

# Script used to swap elasticsearch with too many indices and cause it to crash.
# Run it like this to fill heap with allocated indexes:
# ./dev-tools/many_indexes.sh -y 100000
#
# Run it like this to fill heap with cluster state:
# ./dev-tools/many_indexes.sh -n -s 1000 -r 10

# Licensed to Elasticsearch under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


set -e
set -o pipefail

WORK=target/stress
BATCH_SIZE=25
DIAGS=target/stress/diags
AWAIT_YELLOW=0
AWAIT_YELLOW_DEFAULT=10
CLOSE_INDICES=false
NEVER_ALLOCATE=false
SHARDS=10
REPLICAS=5

while getopts "b:y:cnps:r:" opt; do
  case $opt in
    b)
      BATCH_SIZE=$OPTARG
      ;;
    y)
      AWAIT_YELLOW=$OPTARG
      ;;
    c)
      CLOSE_INDICES=true
      if [ $AWAIT_YELLOW -eq 0 ]; then
        AWAIT_YELLOW=$AWAIT_YELLOW_DEFAULT
      fi
      ;;
    n)
      NEVER_ALLOCATE=true
      ;;
    s)
      SHARDS=$OPTARG
      ;;
    r)
      REPLICAS=$OPTARG
      ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      ;;
  esac
done

function find_es_tar() {
  echo -n "Searching for elasticsearch tar..."
  export ES_TAR=elasticsearch*.tar.gz
  if [ ! -f $ES_TAR ]; then
    export ES_TAR=distribution/tar/target/releases/elasticsearch*.tar.gz
    if [ ! -f $ES_TAR ]; then
      echo "not found!"
      exit 1
    fi
  fi
  echo $ES_TAR
}

function reset() {
  echo -n "Cleaning and recreating $WORK..."
  rm -rf $WORK
  mkdir -p $WORK
  echo "done"
  echo -n "Untarring elasticsearch..."
  tar xf $ES_TAR -C $WORK
  export ES_ROOT=target/stress/elasticsearch-*
  echo "into" $ES_ROOT
}

function start_elasticsearch() {
  echo -n "Starting Elasticsearch..."
  ES_HEAP_SIZE=256m $ES_ROOT/bin/elasticsearch > $WORK/out 2>&1 &
  export ES_PID=$!
  echo $ES_PID
}

function start_gc_monitoring() {
  echo -n "Starting jstat..."
  jstat -gcutil $ES_PID 500ms > $WORK/gc &
  export JSTAT_PID=$!
  echo $JSTAT_PID
}

function wait_for_elasticsearch() {
  echo -n "Waiting for Elasticsearch to accept http requests..."
  until curl -s localhost:9200 > $WORK/root 2> /dev/null; do
    if [ $(kill -0 $ES_PID &> /dev/null) ]; then
      echo "the Elasticsearch process died!"
      exit 1
    fi
    sleep .2
  done
  grep tagline $WORK/root | cut -d'"' -f4
}

function cleanup() {
  trap - EXIT
  if [ ! -z ${ES_PID+x} ]; then
    stop_subprocess Elasticsearch $ES_PID
  fi
  if [ ! -z ${JSTAT_PID+x} ]; then
    stop_subprocess jstat $JSTAT_PID
  fi
}

function stop_subprocess() {
  local name=$1
  local pid=$2
  echo -n "Stopping $name ($pid)..."
  kill -9 $pid &> /dev/null # no need to be gentle
  wait $pid &> /dev/null || true
  echo "dead"
}

function index_settings() {
  if $NEVER_ALLOCATE; then
    echo '        "routing.allocation.include.tag": "never_set",'
  fi
  echo '        "number_of_replicas": '$REPLICAS','
  echo '        "number_of_shards": '$SHARDS
}

function create_index() {
  local name=$1
  curl -s -XPUT -m 10 localhost:9200/$name -d '{
    "settings": {
      "index": {
'"$(index_settings)"'
      }
    },
    "mappings": {
      "test": {
        "properties": {
          "a": {"type": "string"},
          "b": {"type": "string"},
          "c": {"type": "string"},
          "d": {"type": "string"},
          "e": {"type": "string"},
          "f": {"type": "string"},
          "g": {"type": "string"},
          "i": {"type": "string"},
          "j": {"type": "string"},
          "k": {"type": "string"}
        }
      }
    }
  }' &> /dev/null
}

function format_index_name() {
  printf %06d $1
}

function swamp_elasticsearch() {
  echo "Trying to crash elasticsearch with too many shards. This will take a few minutes..."
  local count=0
  local pretty_count=$(format_index_name $count)
  until false; do
    echo -n "  Creating index [$pretty_count, "
    local batch_start=$count
    local pids=''
    for i in $(seq 1 $BATCH_SIZE); do
      create_index $pretty_count &
      pids="$pids $!"
      ((count+=1))
      pretty_count=$(format_index_name $count)
    done
    if wait $pids; then
      echo -n "$pretty_count)..."
    else
      echo -n "$pretty_count)...some index creations timed out..."
    fi
    local batch_end=$((count-1))
    if ! await_yellow; then
      # We assume that failing to get a yellow status is as good as filling up memory
      echo "failed to get yellow state after $AWAIT_YELLOW seconds!"
      break
    fi
    if $CLOSE_INDICES; then
      echo -n "closing..."
      for i in $(seq $batch_start $batch_end); do
        curl -s -XPOST 'localhost:9200/'$(format_index_name $i)'/_close' &> /dev/null
      done
    fi
    echo -n "checking gc..."
    if tail -n 10000 $WORK/gc | egrep '100.00\s+[0-9\.]+\s+100.00\s+100.00|[0-9\.]+\s+100.00\s+100.00\s+100.00' | head -n1; then
      echo "Successfully filled elasticsearch's heap!"
      return
    else
      tail -n1 $WORK/gc
    fi
  done
}

function await_yellow() {
  if [ $AWAIT_YELLOW -gt 0 ]; then
    echo -n "waiting for yellow..."
    curl -s -m$AWAIT_YELLOW 'http://localhost:9200/_cluster/health?wait_for_status=yellow&timeout='$AWAIT_YELLOW's' &> /dev/null
  fi
}

function dump_diags() {
  echo "Dumping diagnostics to $DIAGS..."
  echo -n "  $DIAGS/heap..."
  mkdir -p $DIAGS
  jmap -dump:format=b,file=$DIAGS/heap.hprof $ES_PID > /dev/null
  echo "done"
  echo -n "  $DIAGS/histo..."
  jmap -histo $ES_PID > $DIAGS/histo
  echo "done"
  head -n 10 $DIAGS/histo
}

trap cleanup EXIT
find_es_tar
reset
start_elasticsearch
start_gc_monitoring
wait_for_elasticsearch
swamp_elasticsearch
dump_diags
