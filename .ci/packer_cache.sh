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

source $(dirname "${SCRIPT}")/java-versions.properties
export JAVA_HOME="${HOME}"/.java/${ES_BUILD_JAVA}
# We are caching BWC versions too, need these so we can build those
export JAVA8_HOME="${HOME}"/.java/java8
export JAVA11_HOME="${HOME}"/.java/java11
export JAVA12_HOME="${HOME}"/.java/openjdk12
export JAVA13_HOME="${HOME}"/.java/openjdk13
./gradlew --parallel clean -s resolveAllDependencies

