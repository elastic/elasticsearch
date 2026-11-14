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

# Installs the Gradle wrapper distribution on its own, so a failed download or checksum verification can be
# retried without ever retrying a real build. Agent images pre-cache the distribution, so this only does work
# when the wrapper version was bumped after the image was built.
ensure_gradle_distribution() {
  local distZip distDir attempt
  distZip=$(grep '^distributionUrl=' "$WORKSPACE/gradle/wrapper/gradle-wrapper.properties" | sed 's|.*/||' | tr -d '\r')
  distDir="${GRADLE_USER_HOME:-$HOME/.gradle}/wrapper/dists/${distZip%.zip}"

  # The wrapper writes <zip>.ok once the distribution is fully unpacked
  if compgen -G "$distDir/*/$distZip.ok" > /dev/null; then
    return 0
  fi

  echo "--- Installing Gradle wrapper distribution $distZip"
  for attempt in 1 2 3; do
    if (cd "$WORKSPACE" && ./gradlew --no-daemon --console=plain --version); then
      return 0
    fi
    if [[ $attempt -lt 3 ]]; then
      echo "Gradle wrapper distribution install failed (attempt $attempt of 3), removing partial download and retrying"
      for hashDir in "$distDir"/*/; do
        if [[ -d "$hashDir" && ! -f "$hashDir$distZip.ok" ]]; then
          rm -rf "$hashDir"
        fi
      done
      sleep $((attempt * 15))
    fi
  done
  echo "Gradle wrapper distribution install failed after 3 attempts"
  return 1
}

ensure_gradle_distribution

RUNNER_JAR="$WORKSPACE/build-tools/gradle-runner/build/libs/gradle-runner.jar"
if [[ ! -f "$RUNNER_JAR" ]]; then
  echo "--- Building gradle-runner"
  cd "$WORKSPACE"
  # Build from the standalone build-tools build: going through the root build would configure every project just to
  # produce this jar
  ./gradlew --no-scan --no-daemon --console=plain -p build-tools :gradle-runner:jar
  cd -
fi

# Pass TESTS_SEED as Java system property if available
TESTS_SEED_PARAM=""
if [[ -n "${TESTS_SEED:-}" ]]; then
  TESTS_SEED_PARAM="-Dtests.seed=$TESTS_SEED"
  echo "Using test seed: $TESTS_SEED"
fi

# Strip "./gradlew" from GRADLEW to get the default flags
GRADLEW_ARGS="${GRADLEW#./gradlew }"

echo "--- Running gradle tasks"

if [[ -n "${JAVA_HOME:-}" && -x "$JAVA_HOME/bin/java" ]]; then
  "$JAVA_HOME/bin/java" -jar "$RUNNER_JAR" -- $GRADLEW_ARGS -S --max-workers=$MAX_WORKERS $TESTS_SEED_PARAM ${EXTRA_GRADLE_ARGS:-} "$@"
elif command -v java > /dev/null; then
  java -jar "$RUNNER_JAR" -- $GRADLEW_ARGS -S --max-workers=$MAX_WORKERS $TESTS_SEED_PARAM ${EXTRA_GRADLE_ARGS:-} "$@"
else
  "$GRADLEW" -S --max-workers=$MAX_WORKERS $TESTS_SEED_PARAM ${EXTRA_GRADLE_ARGS:-} "$@"
fi
