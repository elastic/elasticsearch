#!/bin/bash

set -euo pipefail

# TODO remove this
.buildkite/scripts/java8-windows.sh
.buildkite/scripts/java11-windows.sh

.ci/scripts/run-gradle.sh -Dbwc.checkout.align=true $GRADLE_TASK
