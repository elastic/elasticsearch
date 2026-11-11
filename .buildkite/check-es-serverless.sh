#!/bin/bash

set -euo pipefail

# Pipeline file for the elasticsearch-check-serverless-submodule pipeline, which
# runs once per elasticsearch main commit to validate that commit against
# elasticsearch-serverless. Buildkite consumes this script's stdout as the
# pipeline definition, so only the pipeline YAML is allowed there.
#
# elasticsearch main can move past this commit before the serverless build gets
# to check anything out: a linked stateful + serverless merge lands the
# serverless half with a submodule pointer newer than this commit. Triggering
# then checks the submodule out backwards and runs the serverless tests against
# stale elasticsearch code, failing tests that are fine on current code and
# getting them muted on serverless main. Skip the trigger in that case; there is
# nothing to learn from a build we know is looking at the wrong tree.

if [[ "$(.buildkite/scripts/serverless-submodule-advance-decision.sh "${BUILDKITE_COMMIT}")" == "skip" ]]; then
  cat <<EOF
steps:
  - label: ":fast_forward: Skipped (not ahead of the serverless submodule)"
    command: 'echo "Skipping serverless validation: ${BUILDKITE_COMMIT} is not ahead of the commit elasticsearch-serverless main points at."'
EOF
  exit 0
fi

cat <<EOF
steps:
  - trigger: elasticsearch-serverless-validate-submodule
    label: ":elasticsearch: Check elasticsearch changes against serverless"
    build:
      message: "Validate latest elasticsearch changes"
      env:
        ELASTICSEARCH_SUBMODULE_COMMIT: "${BUILDKITE_COMMIT}"
        UPDATE_SUBMODULE: "false"
EOF
