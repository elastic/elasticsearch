#!/bin/bash

set -e
set -o pipefail

WORK=target/stress
BATCH_SIZE=25
DIAGS=target/stress/diags
AWAIT_YELLOW=0
CLOSE_INDICES=false

while getopts ":b:y:c" opt; do
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
        AWAIT_YELLOW=30
      fi
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
  echo "into " $ES_ROOT
}

function start_elasticsearch() {
  echo -n "Starting Elasticsearch..."
  ES_HEAP_SIZE=256m $ES_ROOT/bin/elasticsearch > $WORK/out 2>&1 &
  export ES_PID=$!
  trap stop_elasticsearch EXIT
  echo $ES_PID
}

function start_gc_monitoring() {
  echo -n "Starting gc monitoring..."
  jstat -gcutil $ES_PID 500ms > $WORK/gc &
  # There is no need to explicitly kill jstat because it'll die when Elasticsearch does
  echo $WORK/gc
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

function format_index_name() {
  printf %06d $1
}

function swamp_elasticsearch() {
  echo "Trying to crash elasticsearch with too many shards. This should take about a minute..."
  local count=0
  until false; do
    echo -n "  Creating index [$(format_index_name $count), "
    local batch_start=$count
    for i in $(seq 1 $BATCH_SIZE); do
      create_index $pretty_count && ((count+=1)) || true
    done
    echo -n "$(format_index_name $count))..."
    # Explicitly save batch_end because we're ok with some of the index creations failing.
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
    if tail -n 10000 $WORK/gc | egrep '100.0.+100.00\s+100.00' | head; then
      echo "Successfully filled elasticsearch's heap!"
      break
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

find_es_tar
reset
start_elasticsearch
start_gc_monitoring
wait_for_elasticsearch
swamp_elasticsearch
dump_diags
