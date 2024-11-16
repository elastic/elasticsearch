#!/bin/bash

set -euo pipefail

# TODO/ FIXIT without a full resolved gradle home, we see issues configuration cache reuse
.ci/scripts/run-gradle.sh --no-daemon precommit

.ci/scripts/run-gradle.sh --configuration-cache precommit -Dorg.gradle.configuration-cache.inputs.unsafe.ignore.file-system-checks=build/*.tar.bz2

# Create a temporary file
tmpOutputFile=$(mktemp)
trap "rm $tmpOutputFile" EXIT

echo "2nd run"
.ci/scripts/run-gradle.sh --configuration-cache precommit -Dorg.gradle.configuration-cache.inputs.unsafe.ignore.file-system-checks=build/*.tar.bz2 | tee $tmpOutputFile

# Check if the command was successful
if grep -q "Configuration cache entry reused." $tmpOutputFile; then
    echo "Gradle configuration cache reused"
    exit 0
else
    echo "Failed to reuse Gradle configuration cache."
    exit 1
fi


