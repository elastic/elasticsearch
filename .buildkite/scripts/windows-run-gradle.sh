#!/bin/bash

set -euo pipefail

WORKSPACE="$(pwd)"
export WORKSPACE

export $(cat .ci/java-versions.properties | grep '=' | xargs)

JAVA_HOME="$HOME/.java/$ES_BUILD_JAVA"
export JAVA_HOME

PATH="$JAVA_HOME/bin:$PATH"
export PATH

# eval is required so that shell quoting inside GRADLE_TASK and GRADLE_PARAMS is
# honoured. Without it, $GRADLE_TASK is unquoted and bash word-splits on every
# space — including spaces inside --tests "method name with spaces" — turning
# "jar hell configuration defaults can be overridden" into bare task names.
eval ".ci/scripts/run-gradle.sh -Dbwc.checkout.align=true ${GRADLE_PARAMS:-} $GRADLE_TASK"
