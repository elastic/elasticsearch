#!/bin/bash

set -euo pipefail

# Older versions of openjdk are incompatible to newer kernel of ubuntu. Use adoptopenjdk17 instead
resolve_java_home() {
  if [[ "$ES_BUILD_JAVA" == *"openjdk17"* ]]; then
    if [ -f "/etc/os-release" ]; then
        . /etc/os-release
        if [[ "$ID" == "ubuntu" && "$VERSION_ID" == "24.04" ]]; then
          echo "$HOME/.java/adoptopenjdk17"
          return
        fi
      fi
  fi

  echo "$JAVA_HOME"
}

cd $WORKSPACE/plugins/examples

JAVA_HOME=$(resolve_java_home) \
  $WORKSPACE/.ci/scripts/run-gradle.sh $@
