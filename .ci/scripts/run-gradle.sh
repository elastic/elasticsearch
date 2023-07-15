#!/bin/bash

if [[ -z "${WORKSPACE:-}" && "$BUILDKITE" ]]; then
  source .buildkite/hooks/pre-command
fi

GRADLE_USER_HOME="$HOME/.gradle"
export GRADLE_USER_HOME

rm -Rfv ~/.gradle/init.d
mkdir -p ~/.gradle/init.d && cp -v $WORKSPACE/.ci/init.gradle ~/.gradle/init.d

MAX_WORKERS=4

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

set -e
$GRADLEW -S --max-workers=$MAX_WORKERS $@

exitCode=$?
echo "From bash: $exitCode"
exit $exitCode
