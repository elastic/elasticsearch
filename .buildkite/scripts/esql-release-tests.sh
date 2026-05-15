#!/bin/bash

set -euo pipefail

task_set="${1:-}"

if [[ -z "${task_set}" ]]; then
  echo "Usage: $0 <esql-engine|esql-qa|esql-datasource-core|esql-datasource-qa>"
  exit 64
fi

declare -a check_tasks

case "${task_set}" in
esql-engine)
  check_tasks=(
    :x-pack:plugin:esql:checkPart3
    :x-pack:plugin:esql:arrow:checkPart3
    :x-pack:plugin:esql:compute:checkPart3
    :x-pack:plugin:esql:compute:ann:checkPart3
    :x-pack:plugin:esql:compute:gen:checkPart3
    :x-pack:plugin:esql:compute:test:checkPart3
    :x-pack:plugin:esql:qa:testFixtures:checkPart3
    :x-pack:plugin:esql:tools:checkPart3
    :x-pack:plugin:esql-core:checkPart5
  )
  ;;
esql-qa)
  check_tasks=(
    :x-pack:plugin:esql:qa:action:checkPart3
    :x-pack:plugin:esql:qa:server:checkPart3
    :x-pack:plugin:esql:qa:server:mixed-cluster:checkPart3
    :x-pack:plugin:esql:qa:server:multi-clusters:checkPart3
    :x-pack:plugin:esql:qa:server:single-node:checkPart3
    :x-pack:plugin:esql:qa:server:multi-node:checkPart5
    :x-pack:plugin:esql:qa:security:checkPart4
  )
  ;;
esql-datasource-core)
  check_tasks=(
    :x-pack:plugin:esql-datasource-azure:checkPart5
    :x-pack:plugin:esql-datasource-brotli:checkPart5
    :x-pack:plugin:esql-datasource-bzip2:checkPart5
    :x-pack:plugin:esql-datasource-compression-libs:checkPart5
    :x-pack:plugin:esql-datasource-csv:checkPart5
    :x-pack:plugin:esql-datasource-gcs:checkPart5
    :x-pack:plugin:esql-datasource-grpc:checkPart5
    :x-pack:plugin:esql-datasource-gzip:checkPart5
    :x-pack:plugin:esql-datasource-http:checkPart5
    :x-pack:plugin:esql-datasource-iceberg:checkPart5
    :x-pack:plugin:esql-datasource-lz4:checkPart5
    :x-pack:plugin:esql-datasource-ndjson:checkPart5
    :x-pack:plugin:esql-datasource-netty-commons:checkPart5
    :x-pack:plugin:esql-datasource-orc:checkPart5
  )
  ;;
esql-datasource-qa)
  check_tasks=(
    :x-pack:plugin:esql-datasource-csv:qa:checkPart5
    :x-pack:plugin:esql-datasource-gcs:qa:checkPart5
    :x-pack:plugin:esql-datasource-grpc:qa:checkPart5
    :x-pack:plugin:esql-datasource-iceberg:qa:checkPart5
    :x-pack:plugin:esql-datasource-ndjson:qa:checkPart5
    :x-pack:plugin:esql-datasource-orc:qa:checkPart5
    :x-pack:plugin:esql-datasource-parquet:checkPart5
    :x-pack:plugin:esql-datasource-parquet-rs:checkPart5
    :x-pack:plugin:esql-datasource-parquet-rs:qa:checkPart5
    :x-pack:plugin:esql-datasource-parquet:qa:checkPart5
    :x-pack:plugin:esql-datasource-s3:checkPart5
    :x-pack:plugin:esql-datasource-snappy:checkPart5
    :x-pack:plugin:esql-datasource-zstd:checkPart5
  )
  ;;
*)
  echo "Unknown ESQL release task set [${task_set}]"
  echo "Expected one of: esql-engine, esql-qa, esql-datasource-core, esql-datasource-qa"
  exit 64
  ;;
esac

echo "Running ESQL release task set [${task_set}] with ${#check_tasks[@]} tasks"
printf ' - %s\n' "${check_tasks[@]}"

.buildkite/scripts/release-tests.sh "${check_tasks[@]}"
