#!/bin/bash

set -euo pipefail

.ci/scripts/run-gradle.sh -Dbwc.checkout.align=true $GRADLE_TASK
