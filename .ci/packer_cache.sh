#!/bin/bash

SCRIPT="$0"

# SCRIPT might be an arbitrarily deep series of symbolic links; loop until we
# have the concrete path
while [ -h "$SCRIPT" ]; do
  ls=$(ls -ld "$SCRIPT")
  # Drop everything prior to ->
  link=$(expr "$ls" : '.*-> \(.*\)$')
  if expr "$link" : '/.*' >/dev/null; then
    SCRIPT="$link"
  else
    SCRIPT=$(dirname "$SCRIPT")/"$link"
  fi
done

# Windows-2012 base images have been broken for a long time, and they have a very old version of runbld
# So, we're using the packer_cache script to update runbld to the latest version
OS=$(uname -s)
if [[ "$OS" == *"MINGW"* || "$OS" == *"MSYS"* ]]; then
  RUNBLD_DIR='/c/Program Files/runbld/src/runbld-7.0.3'
  RUNBLD="$RUNBLD_DIR/runbld"

  # We only need to do this if 7.0.3 doesn't already exist
  if [[ ! -f "$RUNBLD" ]]; then
    mkdir -p "$RUNBLD_DIR"
    curl -L -o "$RUNBLD" https://packages.elasticsearch.org.s3.amazonaws.com/infra/runbld-7.0.3

    RUNBLD_HARDLINK_DIR='/c/Program Files/infra/bin'
    RUNBLD_HARDLINK="$RUNBLD_HARDLINK_DIR/runbld"

    rm -f "$RUNBLD_HARDLINK"
    mkdir -p "$RUNBLD_HARDLINK_DIR"

    fsutil hardlink create "$RUNBLD_HARDLINK" "$RUNBLD"
  fi
fi

exit 0 # TODO remove this

if [ "$(uname -m)" = "arm64" ] || [ "$(uname -m)" = "aarch64" ]; then
  ## On ARM we use a different properties file for setting java home
  ## Also, we don't bother attempting to resolve dependencies for the 6.8 branch
  source $(dirname "${SCRIPT}")/java-versions-aarch64.properties
  export JAVA16_HOME="${HOME}"/.java/jdk16
else
  source $(dirname "${SCRIPT}")/java-versions.properties
  ## We are caching BWC versions too, need these so we can build those
  export JAVA8_HOME="${HOME}"/.java/java8
  export JAVA11_HOME="${HOME}"/.java/java11
  export JAVA12_HOME="${HOME}"/.java/openjdk12
  export JAVA13_HOME="${HOME}"/.java/openjdk13
  export JAVA14_HOME="${HOME}"/.java/openjdk14
  export JAVA15_HOME="${HOME}"/.java/openjdk15
  export JAVA16_HOME="${HOME}"/.java/openjdk16

  ## 6.8 branch is not referenced from any bwc project in main so we need to
  ## resolve its dependencies explicitly
  rm -rf checkout/6.8
  git clone --reference $(dirname "${SCRIPT}")/../.git https://github.com/elastic/elasticsearch.git --branch 6.8 --single-branch checkout/6.8
  export JAVA_HOME="${JAVA11_HOME}"
  ./checkout/6.8/gradlew --project-dir ./checkout/6.8 --parallel clean --stacktrace resolveAllDependencies
  rm -rf ./checkout/6.8
fi

## Gradle is able to resolve dependencies resolved with earlier gradle versions
## therefore we run main _AFTER_ we run 6.8 which uses an earlier gradle version
branches=($(cat .ci/snapshotBwcVersions | sed '1d;$d' | grep -E -o "[0-9]+\.[0-9]+"))
branches+=("main")
for branch in "${branches[@]}"; do
  echo "Resolving dependencies for ${branch} branch"
  rm -rf checkout/$branch
  git clone --reference $(dirname "${SCRIPT}")/../.git https://github.com/elastic/elasticsearch.git --branch ${branch} --single-branch checkout/${branch}
  export JAVA_HOME="${HOME}"/.java/${ES_BUILD_JAVA}
  ./checkout/${branch}/gradlew --project-dir ./checkout/${branch} --parallel clean -s resolveAllDependencies -Dorg.gradle.warning.mode=none
  rm -rf ./checkout/${branch}
done
