#!/bin/bash

rm -Rfv ~/.gradle/init.d
mkdir -p ~/.gradle/init.d && cp -v $WORKSPACE/.ci/init.gradle ~/.gradle/init.d

MAX_WORKERS=4

if [ -z "$MAX_WORKER_ENV" ]; then
    # Don't run this stuff on Windows
    if ! uname -a | grep -q MING; then
      # drop page cache and kernel slab objects on linux
      [[ -x /usr/local/sbin/drop-caches ]] && sudo /usr/local/sbin/drop-caches

      if [ "$(uname -m)" = "arm64" ] || [ "$(uname -m)" = "aarch64" ]; then
        MAX_WORKERS=16
      elif [ -f /proc/cpuinfo ]; then
        MAX_WORKERS=`grep '^cpu\scores' /proc/cpuinfo  | uniq | sed 's/\s\+//g' |  cut -d':' -f 2`
      else
        if [[ "$OSTYPE" == "darwin"* ]]; then
            MAX_WORKERS=`sysctl -n hw.physicalcpu | sed 's/\s\+//g'`
        else
            echo "Unsupported OS Type: $OSTYPE"
            exit 1
        fi
      fi
      if pwd | grep -v -q ^/dev/shm ; then
        echo "Not running on a ramdisk, reducing number of workers"
        MAX_WORKERS=$(($MAX_WORKERS*2/3))
      fi

      # Export glibc version as environment variable since some BWC tests are incompatible with later versions
      export GLIBC_VERSION=$(ldd --version | grep '^ldd' | sed 's/.* \([1-9]\.[0-9]*\).*/\1/')
    fi

    # Running on 2-core machines without ramdisk can make this value be 0
    if [[ "$MAX_WORKERS" == "0" ]]; then
      MAX_WORKERS=1
    fi
else
    MAX_WORKERS=$MAX_WORKER_ENV
fi

set -e
$GRADLEW -S --max-workers=$MAX_WORKERS $@
