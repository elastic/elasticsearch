#!/bin/bash

set -euo pipefail

# This is a workaround for https://github.com/gradle/gradle/issues/28159
.ci/scripts/run-gradle.sh --no-daemon $@

.ci/scripts/run-gradle.sh --configuration-cache -Dorg.gradle.configuration-cache.inputs.unsafe.ignore.file-system-checks=build/*.tar.bz2 $@

# Create a temporary file
tmpOutputFile=$(mktemp)
trap "rm $tmpOutputFile" EXIT

echo "2nd run"
.ci/scripts/run-gradle.sh --configuration-cache -Dorg.gradle.configuration-cache.inputs.unsafe.ignore.file-system-checks=build/*.tar.bz2 $@ | tee $tmpOutputFile

# Check if the command was successful
if grep -q "Configuration cache entry reused." $tmpOutputFile; then
    echo "Gradle configuration cache reused"
    exit 0
else
    echo "Failed to reuse Gradle configuration cache."
    exit 1
fi


