#!/bin/bash

set -euo pipefail


.ci/scripts/run-gradle.sh precommit --configuration-cache

echo "2nd run"

# Create a temporary file
tmpOutputFile=$(mktemp)
trap "rm $tmpOutputFile" EXIT

.ci/scripts/run-gradle.sh precommit --configuration-cache | tee $tmpOutputFile

grep -q "Configuration cache entry reused." $tmpOutputFile

# Capture the return value
retval=$?

# Check if the command was successful
if [ $retval -eq 0 ]; then
    echo "Gradle configuration cache reused"
else
    echo "Failed to reuse Gradle configuration cache."
fi

exit $retval


