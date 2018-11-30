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
JAVA_HOME="${HOME}"/.java/${ES_BUILD_JAVA} ./gradlew --parallel  resolveAllDependencies composePull
