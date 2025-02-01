#!/bin/bash

set -euo pipefail

.ci/scripts/run-gradle.sh -Dbwc.checkout.align=true -Dtests.jvm.argline="-Des.entitlements.enabled=false" $GRADLE_TASK
