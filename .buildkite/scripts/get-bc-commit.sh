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

# Select the most recent build from the current branch.
# We collect snapshots, order by date, then collect BCs, order by date, and concat them; then we select the last.
# So if we have one (or more) BC, we will always prefer to use that. Otherwise we will use the latest snapshot.
MANIFEST_URL="$(curl -s https://artifacts.elastic.co/releases/TfEVhiaBGqR64ie0g0r0uUwNAbEQMu1Z/future-releases/stack.json |
jq ".releases[] |
select(.branch == \"$BUILDKITE_BRANCH\") |
select(.active_release == true) |
((.snapshots | to_entries | sort_by(.value.completed_at)) +
(.build_candidates | to_entries | sort_by(.value.completed_at))) |
last | .value.manifest_url")"

if [[ -z "$MANIFEST_URL" ]]; then
   echo "No snapshots or build candidates for branch [$BUILDKITE_BRANCH]"
   exit 0
fi

MANIFEST="$(eval "curl -s $MANIFEST_URL")"
if [[ -z "$MANIFEST" ]]; then
   echo "Cannot get the build manifest from [$MANIFEST_URL]"
   exit 1
fi

TARGET_COMMIT_HASH=$(echo "$MANIFEST" | jq .projects.elasticsearch.commit_hash)
TARGET_VERSION=$(echo "$MANIFEST" | jq .version)

echo "Running bc-bwc tests on [$TARGET_VERSION] commit [$TARGET_COMMIT_HASH]"

cat <<EOF | buildkite-agent pipeline upload
steps:
    - label: $BUILDKITE_BRANCH / bc-bwc
      command: .ci/scripts/run-gradle.sh -Dbwc.checkout.align=true -Dorg.elasticsearch.build.cache.push=true -Dignore.tests.seed -Dscan.capture-file-fingerprints -Dtests.bwc.main.version=${TARGET_VERSION} -Dtests.bwc.refspec.main=${TARGET_COMMIT_HASH} -Dtests.jvm.argline=\"-Des.serverless_transport=true\"
      timeout_in_minutes: 300
      agents:
        provider: gcp
        image: family/elasticsearch-ubuntu-2004
        machineType: n1-standard-32
        buildDirectory: /dev/shm/bk
        preemptible: true
      retry:
        automatic:
          - exit_status: \"-1\"
            limit: 3
            signal_reason: none
          - signal_reason: agent_stop
            limit: 3
EOF
