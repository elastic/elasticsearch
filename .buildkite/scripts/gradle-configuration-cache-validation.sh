#!/bin/bash

set -euo pipefail

# TODO/ FIXIT without a full resolved gradle home, we see issues configuration cache reuse
./gradlew --max-workers=8 --parallel --scan --no-daemon precommit

./gradlew --max-workers=8 --parallel --scan --configuration-cache precommit -Dorg.gradle.configuration-cache.inputs.unsafe.ignore.file-system-checks=build/*.tar.bz2

# Create a temporary file
tmpOutputFile=$(mktemp)
trap "rm $tmpOutputFile" EXIT

echo "2nd run"
# TODO run-gradle.sh script causes issues because of init script handling
./gradlew --max-workers=8 --parallel --scan --configuration-cache precommit -Dorg.gradle.configuration-cache.inputs.unsafe.ignore.file-system-checks=build/*.tar.bz2 | tee $tmpOutputFile

# Check if the command was successful
if grep -q "Configuration cache entry reused." $tmpOutputFile; then
    echo "Gradle configuration cache reused"
    exit 0
else
    echo "Failed to reuse Gradle configuration cache."
    exit 1
fi


