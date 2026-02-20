#!/usr/bin/env bash

set -euo pipefail

ARTIFACT_DIR="promql-query-files"
OUTPUT_DIR="/tmp/promql-cov-output"

cmd_setup() {
  local repo="${GIT_QUERY_BENCH_REPO:-elastic/grafana-dashboards-analysis}"
  local query_bench_dir="/tmp/${repo}"

  echo "--- Cloning ${repo}"
  rm -rf "${query_bench_dir}"
  git clone --depth 1 --single-branch \
    "https://${GH_TOKEN}@github.com/${repo}.git" \
    "${query_bench_dir}"

  mapfile -t query_files < <(find "${query_bench_dir}/results" -name "*.csv" -type f | sort)
  if (( ${#query_files[@]} == 0 )); then
    echo "No CSV files found in ${query_bench_dir}/results"
    exit 1
  fi
  echo "Found ${#query_files[@]} query file(s)"

  echo "--- Uploading query files"
  rm -rf "${ARTIFACT_DIR}"
  mkdir -p "${ARTIFACT_DIR}"
  for f in "${query_files[@]}"; do
    cp "${f}" "${ARTIFACT_DIR}/"
  done
  buildkite-agent artifact upload "${ARTIFACT_DIR}/*.csv"

  echo "--- Generating dynamic pipeline"
  local pipeline_file="/tmp/promql-cov-pipeline.yml"
  echo "steps:" > "${pipeline_file}"

  for qf in "${query_files[@]}"; do
    local name
    name="$(basename "${qf}" .csv)"
    cat >> "${pipeline_file}" <<EOF
  - label: "promql-cov / ${name}"
    command: .buildkite/scripts/promql-cov.sh run "${name}"
    depends_on: promql-cov-setup
    timeout_in_minutes: 120
    retry:
      automatic:
        - exit_status: -1
          limit: 2
        - exit_status: 137
          limit: 2
    agents:
      provider: gcp
      image: family/elasticsearch-ubuntu-2404
      machineType: n1-standard-8
      buildDirectory: /dev/shm/bk
    artifact_paths:
      - "${OUTPUT_DIR}/*"
EOF
  done

  cat "${pipeline_file}"
  buildkite-agent pipeline upload "${pipeline_file}"
}

cmd_run() {
  local filename="$1"
  local base_worktree=""

  cleanup() {
    if [[ -n "${base_worktree}" && -d "${base_worktree}" ]]; then
      git worktree remove --force "${base_worktree}" 2>/dev/null || true
      rm -rf "${base_worktree}" || true
    fi
  }
  trap cleanup EXIT

  mkdir -p "${OUTPUT_DIR}"

  echo "--- Downloading query file"
  buildkite-agent artifact download "${ARTIFACT_DIR}/${filename}.csv" /tmp/
  local queries_file="/tmp/${ARTIFACT_DIR}/${filename}.csv"

  echo "--- Fetching base branch ${BUILDKITE_PULL_REQUEST_BASE_BRANCH}"
  git fetch origin "${BUILDKITE_PULL_REQUEST_BASE_BRANCH}" --depth=1

  base_worktree="$(mktemp -d)"
  git worktree add --detach "${base_worktree}" "origin/${BUILDKITE_PULL_REQUEST_BASE_BRANCH}"

  local base_report="${OUTPUT_DIR}/${filename}_base.json"
  local current_report="${OUTPUT_DIR}/${filename}_current.json"

  run_analyzer() {
    (
      cd "$1"
      WORKSPACE="$1" GRADLEW="./gradlew" \
        .ci/scripts/run-gradle.sh :x-pack:plugin:esql:analyzePromqlQueries \
          -PqueriesFile="${queries_file}" \
          -PoutputFile="$2" \
          -Pverbose=false \
          --quiet
    )
  }

  echo "--- Analyzing on base branch"
  if ! run_analyzer "${base_worktree}" "${base_report}"; then
    echo "Analyzer unavailable on base branch, skipping"
    buildkite-agent annotate --style "info" --context "promql-cov-${filename}" \
      "**${filename}**: :white_circle: Skipped (analyzer unavailable on base branch)"
    exit 0
  fi

  echo "--- Analyzing on PR branch"
  run_analyzer "$(pwd)" "${current_report}"

  echo "--- Comparing results"
  local base_success current_success base_total current_total diff
  base_success="$(jq -r '.successful' "${base_report}")"
  current_success="$(jq -r '.successful' "${current_report}")"
  base_total="$(jq -r '.total' "${base_report}")"
  current_total="$(jq -r '.total' "${current_report}")"
  diff=$(( current_success - base_success ))

  local style="success" icon=":white_check_mark:" status="OK"
  if (( diff < 0 )); then
    style="error"; icon=":x:"; status="REGRESSION (${diff})"
  elif (( diff > 0 )); then
    status="IMPROVED (+${diff})"
  fi

  echo "${status}: ${base_success}/${base_total} -> ${current_success}/${current_total}"

  buildkite-agent annotate --style "${style}" --context "promql-cov-${filename}" <<EOF
**${filename}**: ${icon} ${status} — ${base_success}/${base_total} → ${current_success}/${current_total}
EOF

  (( diff < 0 )) && exit 1
  true
}

case "${1:-}" in
  setup) cmd_setup ;;
  run)   cmd_run "$2" ;;
  *)     echo "Usage: $0 {setup|run <name>}" >&2; exit 2 ;;
esac
