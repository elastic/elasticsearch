#!/bin/bash

set -euo pipefail

GRADLEW="./gradlew --parallel --scan --build-cache --no-watch-fs"
export GRADLEW

.ci/scripts/run-gradle.sh precommit --configuration-cache

echo "2nd run"

# Create a temporary file
tmpOutputFile=$(mktemp)
trap "rm $tmpOutputFile" EXIT

.ci/scripts/run-gradle.sh precommit --configuration-cache | tee $tmpOutputFile

# Check if the command was successful
if grep -q "Configuration cache entry reused." $tmpOutputFile; then
    echo "Gradle configuration cache reused"
    exit 0
else
    echo "Failed to reuse Gradle configuration cache."
    exit 1
fi


