#!/bin/bash

set -euo pipefail

env_id_baseline=$(python3 -c 'import uuid; print(uuid.uuid4())')
env_id_contender=$(python3 -c 'import uuid; print(uuid.uuid4())')
merge_base=$(git merge-base "${GITHUB_PR_TARGET_BRANCH}" HEAD)

buildkite-agent meta-data set pr_comment:custom-body:body \
  "This build attempted two ${GITHUB_PR_COMMENT_VAR_BENCHMARK} benchmarks to evaluate performance impact of this PR."
buildkite-agent meta-data set pr_comment:custom-baseline:head \
  "* Baseline: ${merge_base} (env ID ${env_id_baseline})"
buildkite-agent meta-data set pr_comment:custom-contender:head \
  "* Contender: ${GITHUB_PR_TRIGGERED_SHA} (env ID ${env_id_contender})"

cat << _EOF_
steps:
  - label: Trigger baseline benchmark
    trigger: elasticsearch-performance-esbench-pr
    build:
      message: Baseline benchmark for PR${GITHUB_PR_NUMBER}
      branch: master
      env:
        CONFIGURATION_NAME: ${GITHUB_PR_COMMENT_VAR_BENCHMARK}
        ENV_ID: ${env_id_baseline}
        REVISION: ${merge_base}
  - label: Trigger contender benchmark
    trigger: elasticsearch-performance-esbench-pr
    build:
      message: Contender benchmark for PR${GITHUB_PR_NUMBER}
      branch: master
      env:
        CONFIGURATION_NAME: ${GITHUB_PR_COMMENT_VAR_BENCHMARK}
        ENV_ID: ${env_id_contender}
        ES_REPO_URL: https://github.com/${GITHUB_PR_OWNER}/${GITHUB_PR_REPO}.git
        REVISION: ${GITHUB_PR_TRIGGERED_SHA}
  - wait: ~
  - label: Modify PR comment
    command: buildkite-agent meta-data set pr_comment:custom-comparison:head "* [Benchmark results](<https://esbench-metrics.kb.us-east-2.aws.elastic-cloud.com:9243/app/dashboards#/view/d9079962-5866-49ef-b9f5-145f2141cd31?_a=(query:(language:kuery,query:'user-tags.env-id:${env_id_baseline} or user-tags.env-id:${env_id_contender}'))>)"
_EOF_
