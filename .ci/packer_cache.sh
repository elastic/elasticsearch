#!/bin/bash

SCRIPT="$0"

# SCRIPT might be an arbitrarily deep series of symbolic links; loop until we
# have the concrete path
while [ -h "$SCRIPT" ] ; do
  ls=$(ls -ld "$SCRIPT")
  # Drop everything prior to ->
  link=$(expr "$ls" : '.*-> \(.*\)$')
  if expr "$link" : '/.*' > /dev/null; then
    SCRIPT="$link"
  else
    SCRIPT=$(dirname "$SCRIPT")/"$link"
  fi
done

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
  ./checkout/6.8/gradlew --project-dir ./checkout/6.8 --parallel clean --scan -Porg.elasticsearch.acceptScanTOS=true --stacktrace resolveAllDependencies
  rm -rf ./checkout/6.8
fi

## Gradle is able to resolve dependencies resolved with earlier gradle versions
## therefore we run main _AFTER_ we run 6.8 which uses an earlier gradle version
export JAVA_HOME="${HOME}"/.java/${ES_BUILD_JAVA}
./gradlew --parallel clean -s resolveAllDependencies -Dorg.gradle.warning.mode=none
