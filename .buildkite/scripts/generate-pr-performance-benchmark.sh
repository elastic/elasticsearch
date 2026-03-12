#!/bin/bash

set -euo pipefail

# uncomment for tests
#function buildkite-agent {
#  local command=$1
#  echo "$@"
#  if [ "$command" == "annotate" ]; then
#    while read -r line; do
#      echo "  read: $line";
#    done
#  fi
#}

env_id_baseline=$(python3 -c 'import uuid; print(uuid.uuid4())')
env_id_contender=$(python3 -c 'import uuid; print(uuid.uuid4())')
merge_base=$(git merge-base "origin/${GITHUB_PR_TARGET_BRANCH}" HEAD)

# PR comment
buildkite-agent meta-data set pr_comment:early_comment_job_id "$BUILDKITE_JOB_ID"
buildkite-agent meta-data set pr_comment:custom-body:body \
  "This build attempts two ${GITHUB_PR_COMMENT_VAR_BENCHMARK} benchmarks to evaluate performance impact of this PR. \
To estimate benchmark completion time inspect previous nightly runs [here](https://buildkite.com/elastic/elasticsearch-performance-esbench-nightly/builds?branch=master)."
buildkite-agent meta-data set pr_comment:custom-baseline:head \
  "* Baseline: ${merge_base} (env ID ${env_id_baseline})"
buildkite-agent meta-data set pr_comment:custom-contender:head \
  "* Contender: ${GITHUB_PR_TRIGGERED_SHA} (env ID ${env_id_contender})"

# Buildkite annotation
cat << _EOF1_ | buildkite-agent annotate --context "pr-benchmark-notification"
  This build attempts two ${GITHUB_PR_COMMENT_VAR_BENCHMARK} benchmarks to evaluate performance impact of PR [${GITHUB_PR_NUMBER}](https://github.com/elastic/elasticsearch/pull/${GITHUB_PR_NUMBER}).
  To estimate benchmark completion time inspect previous nightly runs [here](https://buildkite.com/elastic/elasticsearch-performance-esbench-nightly/builds?branch=master).
  * Baseline: [${merge_base:0:7}](https://github.com/elastic/elasticsearch/commit/${merge_base}) (env ID ${env_id_baseline})
  * Contender: [${GITHUB_PR_TRIGGERED_SHA:0:7}](https://github.com/elastic/elasticsearch/commit/${GITHUB_PR_TRIGGERED_SHA}) (env ID ${env_id_contender})
_EOF1_

cat << _EOF2_
steps:
  - label: Trigger baseline benchmark with ${merge_base:0:7}
    trigger: elasticsearch-performance-esbench-pr
    build:
      message: Baseline benchmark for PR ${GITHUB_PR_NUMBER} with ${merge_base:0:7}
      branch: master
      env:
        CONFIGURATION_NAME: ${GITHUB_PR_COMMENT_VAR_BENCHMARK}
        ENV_ID: ${env_id_baseline}
        REVISION: ${merge_base}
        BENCHMARK_TYPE: baseline
  - label: Trigger contender benchmark with ${GITHUB_PR_TRIGGERED_SHA:0:7}
    trigger: elasticsearch-performance-esbench-pr
    build:
      message: Contender benchmark for PR ${GITHUB_PR_NUMBER} with ${GITHUB_PR_TRIGGERED_SHA:0:7}
      branch: master
      env:
        CONFIGURATION_NAME: ${GITHUB_PR_COMMENT_VAR_BENCHMARK}
        ENV_ID: ${env_id_contender}
        ES_REPO_URL: https://github.com/${GITHUB_PR_OWNER}/${GITHUB_PR_REPO}.git
        REVISION: ${GITHUB_PR_TRIGGERED_SHA}
        BENCHMARK_TYPE: contender
  - wait: ~
  - label: Update PR comment and Buildkite annotation
    command: |
      buildkite-agent meta-data set pr_comment:custom-body:body "This build ran two ${GITHUB_PR_COMMENT_VAR_BENCHMARK} benchmarks to evaluate performance impact of this PR."
      buildkite-agent meta-data set pr_comment:custom-comparison:head "* [Benchmark results](<https://esbench-metrics.kb.us-east-2.aws.elastic-cloud.com:9243/app/dashboards#/view/d9079962-5866-49ef-b9f5-145f2141cd31?_a=(query:(language:kuery,query:'user-tags.env-id:${env_id_baseline} or user-tags.env-id:${env_id_contender}'))>)"
      cat << _EOF3_ | buildkite-agent annotate --context "pr-benchmark-notification"
        This build ran two ${GITHUB_PR_COMMENT_VAR_BENCHMARK} benchmarks to evaluate performance impact of PR [${GITHUB_PR_NUMBER}](https://github.com/elastic/elasticsearch/pull/${GITHUB_PR_NUMBER}).
        * Baseline: [${merge_base:0:7}](https://github.com/elastic/elasticsearch/commit/${merge_base}) (env ID ${env_id_baseline})
        * Contender: [${GITHUB_PR_TRIGGERED_SHA:0:7}](https://github.com/elastic/elasticsearch/commit/${GITHUB_PR_TRIGGERED_SHA}) (env ID ${env_id_contender})
        * [Benchmark results](<https://esbench-metrics.kb.us-east-2.aws.elastic-cloud.com:9243/app/dashboards#/view/d9079962-5866-49ef-b9f5-145f2141cd31?_a=(query:(language:kuery,query:'user-tags.env-id:${env_id_baseline} or user-tags.env-id:${env_id_contender}'))>)
      _EOF3_
_EOF2_
