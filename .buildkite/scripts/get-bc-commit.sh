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

#TODO: all current branches? Only the "last" one? (9.0 in this case?)
#source .buildkite/scripts/branches.sh
#for BRANCH in "${BRANCHES[@]}"; do

BRANCH=9.0

MANIFEST="$(curl -s https://artifacts.elastic.co/releases/TfEVhiaBGqR64ie0g0r0uUwNAbEQMu1Z/future-releases/stack.json |
jq ".releases[] |
select(.branch == \"$BRANCH\") |
select(.active_release == true) |
.build_candidates |
to_entries |
sort_by(.value.completed_at) |
last |
.value.manifest_url")"

BC_COMMIT_HASH="$(eval "curl -s $MANIFEST" | jq .projects.elasticsearch.commit_hash)"

echo "steps:
  - group: bc
    steps:
      - label: $BRANCH / bc-bwc
        command: .ci/scripts/run-gradle.sh -Dbwc.checkout.align=true -Dorg.elasticsearch.build.cache.push=true -Dignore.tests.seed -Dscan.capture-file-fingerprints -Dtests.bwc.main.version=9.1.0 -Dtests.bwc.refspec.main=$BC_COMMIT_HASH -Dtests.jvm.argline=\"-Des.serverless_transport=true\"
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
"
