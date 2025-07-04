#!/bin/bash

#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the "Elastic License
# 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
# Public License v 1"; you may not use this file except in compliance with, at
# your election, the "Elastic License 2.0", the "GNU Affero General Public
# License v3.0 only", or the "Server Side Public License, v 1".
#

set -euo pipefail

if [[ -z "$BUILDKITE_PULL_REQUEST_BASE_BRANCH" ]]; then
   echo "Not a pull request, skipping PR upgrade tests."
   exit 0
fi

# Identify the merge base of the current commit (branch) and the base branch of the pull request.
# PR upgrade tests are run from the merge base to the current commit.
git fetch origin $BUILDKITE_PULL_REQUEST_BASE_BRANCH
BASE_COMMIT=$(git merge-base origin/$BUILDKITE_PULL_REQUEST_BASE_BRANCH $BUILDKITE_COMMIT)

VERSION=$(sed -n 's/^elasticsearch[[:space:]]*=[[:space:]]*\(.*\)/\1/p' build-tools-internal/version.properties)

echo "Running PR upgrade tests from $BUILDKITE_PULL_REQUEST_BASE_BRANCH [$BASE_COMMIT] to $BUILDKITE_BRANCH [$BUILDKITE_COMMIT]."

cat << EOF | buildkite-agent pipeline upload
steps:
    - group: "pr-upgrade $BUILDKITE_PULL_REQUEST_BASE_BRANCH -> $BUILDKITE_BRANCH"
      steps:
        - label: "pr-upgrade-part-{{matrix.PART}}"
          command: .ci/scripts/run-gradle.sh -Dbwc.checkout.align=true -Dorg.elasticsearch.build.cache.push=true -Dignore.tests.seed -Dscan.capture-file-fingerprints -Dtests.bwc.main.version=${VERSION}-SNAPSHOT -Dtests.bwc.refspec.main=${BASE_COMMIT} bcUpgradeTestPart{{matrix.PART}}
          timeout_in_minutes: 300
          agents:
            provider: gcp
            image: family/elasticsearch-ubuntu-2004
            machineType: n1-standard-32
            buildDirectory: /dev/shm/bk
          matrix:
            setup:
              PART: ["1", "2", "3", "4", "5", "6"]
EOF
