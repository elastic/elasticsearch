#!/usr/bin/env bash

set -Eeuo pipefail

readonly JOB_NAME="${JOB_NAME:-promql-compliance-bench}"
readonly SCRIPT_PATH="${SCRIPT_PATH:-.buildkite/scripts/${JOB_NAME}.sh}"
readonly SETUP_STEP_KEY="${SETUP_STEP_KEY:-promql-compliance-bench-setup}"

readonly INPUT_DIR="${INPUT_DIR:-${JOB_NAME}-input}"
readonly OUTPUT_DIR="${OUTPUT_DIR:-/tmp/${JOB_NAME}-output}"
readonly PIPELINE_FILE="${PIPELINE_FILE:-/tmp/${JOB_NAME}-pipeline.yml}"

readonly REPO="${BENCH_REPO:-elastic/grafana-dashboards-analysis}"
readonly REPO_DIR="${REPO_DIR:-/tmp/${JOB_NAME}-repo}"
readonly INPUT_SUBDIR="${INPUT_SUBDIR:-results}"
readonly INPUT_GLOB="${INPUT_GLOB:-*_raw_queries_simple.csv}"

readonly GRADLE_TASK="${GRADLE_TASK:-:x-pack:plugin:esql:analyzePromqlQueries}"

readonly STEP_TIMEOUT_MINUTES="${STEP_TIMEOUT_MINUTES:-120}"
readonly RETRY_LIMIT="${RETRY_LIMIT:-2}"

readonly AGENT_PROVIDER="${AGENT_PROVIDER:-gcp}"
readonly AGENT_IMAGE="${AGENT_IMAGE:-family/elasticsearch-ubuntu-2404}"
readonly AGENT_MACHINE_TYPE="${AGENT_MACHINE_TYPE:-n1-standard-8}"
readonly AGENT_BUILD_DIRECTORY="${AGENT_BUILD_DIRECTORY:-/dev/shm/bk}"

log() {
  echo "--- $*"
}

die() {
  echo "ERROR: $*" >&2
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"
}

cleanup_dir() {
  local dir="$1"
  [[ -n "${dir}" && -d "${dir}" ]] || return 0
  rm -rf "${dir}"
}

setup_requirements() {
  require_cmd git
  require_cmd find
  require_cmd sort
  require_cmd jq
  require_cmd buildkite-agent
}

clone_repo() {
  [[ -n "${GH_TOKEN:-}" ]] || die "GH_TOKEN is required"

  log "Cloning ${REPO}"
  cleanup_dir "${REPO_DIR}"

  git clone \
    --depth 1 \
    --single-branch \
    "https://${GH_TOKEN}@github.com/${REPO}.git" \
    "${REPO_DIR}"
}

find_input_files() {
  local root="${REPO_DIR}/${INPUT_SUBDIR}"

  [[ -d "${root}" ]] || die "input directory does not exist: ${root}"

  # The analyzer expects one query per line in the form "<dashboardId>;<query>".
  # The dashboards-analysis repo exports that shape as *_raw_queries_simple.csv;
  # the other CSVs are summaries and spreadsheet-oriented reports.
  mapfile -d '' -t INPUT_FILES < <(find "${root}" -type f -name "${INPUT_GLOB}" -print0 | sort -z)

  (( ${#INPUT_FILES[@]} > 0 )) || die "no CSV files matching ${INPUT_GLOB} found in ${root}"

  log "found ${#INPUT_FILES[@]} input files"
}

upload_artifacts() {
  log "uploading input files"

  cleanup_dir "${INPUT_DIR}"
  mkdir -p "${INPUT_DIR}"

  local f
  for f in "${INPUT_FILES[@]}"; do
    cp "${f}" "${INPUT_DIR}/"
  done

  buildkite-agent artifact upload "${INPUT_DIR}/*.csv"
}

step_key_for_dataset() {
  local dataset="$1"
  local normalized

  normalized="$(echo "${dataset}" | tr '[:upper:]' '[:lower:]' | tr -cs 'a-z0-9' '-')"
  normalized="${normalized#-}"
  normalized="${normalized%-}"

  [[ -n "${normalized}" ]] || normalized="dataset"

  echo "${JOB_NAME}-${normalized}"
}

generate_pipeline() {
  log "generating dynamic pipeline"

  {
    echo "steps:"

    local f name step_key
    for f in "${INPUT_FILES[@]}"; do
      name="$(basename "${f}" .csv)"
      step_key="$(step_key_for_dataset "${name}")"

      cat <<EOF
  - label: "${JOB_NAME} / ${name}"
    key: "${step_key}"
    command: ${SCRIPT_PATH} run "${name}"
    depends_on: "${SETUP_STEP_KEY}"
    timeout_in_minutes: ${STEP_TIMEOUT_MINUTES}
    retry:
      automatic:
        - exit_status: -1
          limit: ${RETRY_LIMIT}
        - exit_status: 137
          limit: ${RETRY_LIMIT}
    agents:
      provider: ${AGENT_PROVIDER}
      image: ${AGENT_IMAGE}
      machineType: ${AGENT_MACHINE_TYPE}
      buildDirectory: ${AGENT_BUILD_DIRECTORY}
    artifact_paths:
      - "${OUTPUT_DIR}/*"
EOF
    done
  } > "${PIPELINE_FILE}"

  cat "${PIPELINE_FILE}"
  buildkite-agent pipeline upload "${PIPELINE_FILE}"
}

cmd_setup() {
  setup_requirements
  clone_repo
  find_input_files
  upload_artifacts
  generate_pipeline
}

download_input_file() {
  local filename="$1"
  local local_root="/tmp/${INPUT_DIR}"

  mkdir -p "${local_root}"

  log "downloading input file ${filename}.csv"

  buildkite-agent artifact download "${INPUT_DIR}/${filename}.csv" /tmp/

  DOWNLOADED_INPUT_FILE="${local_root}/${filename}.csv"

  [[ -f "${DOWNLOADED_INPUT_FILE}" ]] || die "file not found: ${DOWNLOADED_INPUT_FILE}"
}

fetch_control_branch() {
  local branch="${BUILDKITE_PULL_REQUEST_BASE_BRANCH:-}"

  [[ -n "${branch}" ]] || die "BUILDKITE_PULL_REQUEST_BASE_BRANCH is required"

  log "fetching control branch ${branch}"

  git fetch --no-tags --depth=1 origin "${branch}"
  CONTROL_REF="origin/${branch}"
}

create_control_worktree() {
  log "creating control worktree"

  CONTROL_WORKTREE="$(mktemp -d "/tmp/${JOB_NAME}-control-XXXXXX")"

  git worktree add --detach "${CONTROL_WORKTREE}" "${CONTROL_REF}"
}

cleanup_worktree() {
  if [[ -n "${CONTROL_WORKTREE:-}" && -d "${CONTROL_WORKTREE}" ]]; then
    git worktree remove --force "${CONTROL_WORKTREE}" >/dev/null 2>&1 || true
    rm -rf "${CONTROL_WORKTREE}" || true
  fi
}

run_analyzer() {
  local workspace="$1"
  local input_file="$2"
  local output_file="$3"

  (
    cd "${workspace}"

    WORKSPACE="${workspace}" GRADLEW="./gradlew" \
      .ci/scripts/run-gradle.sh "${GRADLE_TASK}" \
        -PqueriesFile="${input_file}" \
        -PoutputFile="${output_file}" \
        -PoutputFormat=json \
        -Pverbose=false \
        --quiet
  )
}

cmd_run() {
  local filename="${1:?missing filename}"

  setup_requirements

  mkdir -p "${OUTPUT_DIR}"

  trap cleanup_worktree EXIT

  download_input_file "${filename}"

  fetch_control_branch
  create_control_worktree

  local control_report="${OUTPUT_DIR}/${filename}_control.json"
  local test_report="${OUTPUT_DIR}/${filename}_test.json"

  log "running analyzer on control branch"

  run_analyzer "${CONTROL_WORKTREE}" "${DOWNLOADED_INPUT_FILE}" "${control_report}" || die "control analyzer failed"

  log "running analyzer on test branch"

  run_analyzer "$(pwd)" "${DOWNLOADED_INPUT_FILE}" "${test_report}" || die "test analyzer failed"

  log "comparing results"

  local control_success test_success control_total test_total diff

  control_success="$(jq -r '.successful' "${control_report}")"
  test_success="$(jq -r '.successful' "${test_report}")"
  control_total="$(jq -r '.total' "${control_report}")"
  test_total="$(jq -r '.total' "${test_report}")"

  diff=$(( test_success - control_success ))

  if (( diff < 0 )); then
    log "regression: ${control_success}/${control_total} -> ${test_success}/${test_total} delta=${diff}"
    exit 1
  elif (( diff > 0 )); then
    log "improvement: ${control_success}/${control_total} -> ${test_success}/${test_total} delta=+${diff}"
  else
    log "stable: ${control_success}/${control_total} -> ${test_success}/${test_total} delta=0"
  fi
}

case "${1:-}" in
  setup)
    cmd_setup
    ;;
  run)
    cmd_run "${2:-}"
    ;;
  *)
    echo "Usage: $0 {setup | run <dataset>}" >&2
    exit 2
    ;;
esac
