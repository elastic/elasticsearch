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

# Pass TESTS_SEED as Java system property if available
TESTS_SEED_PARAM=""
if [[ -n "${TESTS_SEED:-}" ]]; then
  TESTS_SEED_PARAM="-Dtests.seed=$TESTS_SEED"
  echo "Using test seed: $TESTS_SEED"
fi

# Disable errexit so we can capture Gradle's exit code and check for preemption.
# In Gradle 9 the daemon and client are always separate JVMs; halt() in the daemon
# does not propagate the preemption exit code (default 42) to the client. The
# Gradle build writes the desired code to a temp file when preempted, and we
# re-exit with it here so the Buildkite retry rules (exit_status: 42) fire correctly.
PREEMPTION_FILE="/tmp/gradle-preemption-exit-${BUILDKITE_JOB_ID:-local}"
set +e
$GRADLEW -S --no-daemon --max-workers=$MAX_WORKERS $TESTS_SEED_PARAM ${EXTRA_GRADLE_ARGS:-} "$@"
GRADLE_EXIT=$?
set -e

if [[ -f "$PREEMPTION_FILE" ]]; then
  PREEMPTION_EXIT=$(cat "$PREEMPTION_FILE")
  rm -f "$PREEMPTION_FILE"
  echo "[gcp-preemption-watchdog] exiting with preemption exit code $PREEMPTION_EXIT"
  exit "$PREEMPTION_EXIT"
fi

exit "$GRADLE_EXIT"
