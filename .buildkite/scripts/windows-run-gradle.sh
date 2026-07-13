#!/bin/bash

set -euo pipefail

WORKSPACE="$(pwd)"
export WORKSPACE

export $(cat .ci/java-versions.properties | grep '=' | xargs)

JAVA_HOME="$HOME/.java/$ES_BUILD_JAVA"
export JAVA_HOME

PATH="$JAVA_HOME/bin:$PATH"
export PATH

.ci/scripts/run-gradle.sh -Dbwc.checkout.align=true ${GRADLE_PARAMS:-} $GRADLE_TASK
