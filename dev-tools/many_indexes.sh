#!/bin/bash

set -e
set -o pipefail

WORK=target/stress
CHECK_EVERY=25
DIAGS=target/stress/diags

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
  echo "into " $ES_ROOT
}

function start_elasticsearch() {
  echo -n "Starting Elasticsearch..."
  ES_HEAP_SIZE=256m $ES_ROOT/bin/elasticsearch > $WORK/out 2>&1 &
  export ES_PID=$!
  trap stop_elasticsearch EXIT
  echo "as $ES_PID"
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

function stop_elasticsearch() {
  echo -n "Stopping Elasticsearch..."
  kill -9 $ES_PID &> /dev/null # no need to be gentle
  wait &> /dev/null
  echo "dead"
  trap - EXIT
}

function create_index() {
  local name=$1
  curl -s -XPUT localhost:9200/$name -d '{
    "settings": {
      "index": {
        "number_of_replicas": 5,
        "number_of_shards": 10
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
  }'&> /dev/null
}

function swamp_elasticsearch() {
  echo "Trying to crash elasticsearch with too many shards. This should take about a minute..."
  local count=0
  local pretty_count=$(printf %04d $count)
  until false; do
    for i in $(seq 1 $CHECK_EVERY); do
      create_index $pretty_count && ((count+=1)) || true
      pretty_count=$(printf %010d $count)
    done
    local JSTAT=$(jstat -gcutil $ES_PID | tail -n 1)
    echo "  Created $pretty_count indices...$JSTAT"
    if echo $JSTAT | egrep '100.0.+100.00\s+100.00' > /dev/null; then
      echo "Successfully filled elasticsearch's heap!"
      break
    fi
  done
}

function dump_diags() {
  echo "Dumping diagnostics to $DIAGS..."
  echo -n "  $DIAGS/heap..."
  mkdir -p $DIAGS
  jmap -dump:format=b,file=$DIAGS/heap.hprof $ES_PID
  echo "done"
  echo -n "  $DIAGS/histo..."
  jmap -histo $ES_PID > $DIAGS/histo
  echo "done"
  head -n 10 $DIAGS/histo
}

find_es_tar
reset
start_elasticsearch
wait_for_elasticsearch
swamp_elasticsearch
dump_diags
