#!/bin/bash

set -euo pipefail

STATUS=$(curl -s "https://api.github.com/repos/elastic/elasticsearch/branches/$BUILDKITE_BRANCH" | jq '.protected')
echo "Branch $BUILDKITE_BRANCH protection status is: $STATUS"
if [[ "$STATUS" == "false" ]]; then
  echo "Development branch $BUILDKITE_BRANCH is not set as protected in GitHub but should be."
  exit 1
fi
