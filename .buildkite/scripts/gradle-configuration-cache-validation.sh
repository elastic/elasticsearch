#!/bin/bash

set -euo pipefail

./gradlew --max-workers=8 --parallel --scan --configuration-cache precommit

# Create a temporary file
tmpOutputFile=$(mktemp)
trap "rm $tmpOutputFile" EXIT

echo "2nd run"
# TODO run-gradle.sh script causes issues because of init script handling
./gradlew --max-workers=8 --parallel --scan --configuration-cache precommit | tee $tmpOutputFile

# Check if the command was successful
if grep -q "Configuration cache entry reused." $tmpOutputFile; then
    echo "Gradle configuration cache reused"
    exit 0
else
    echo "Failed to reuse Gradle configuration cache."
    exit 1
fi


