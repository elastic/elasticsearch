#!/usr/bin/env bash

set -Eeuo pipefail

readonly JOB_NAME="${JOB_NAME:-promql-compliance-bench}"
readonly SETUP_STEP_KEY="${SETUP_STEP_KEY:-promql-compliance-bench-setup}"
readonly INPUT_DIR="${INPUT_DIR:-${JOB_NAME}-input}"
readonly OUTPUT_DIR="${OUTPUT_DIR:-/tmp/${JOB_NAME}-output}"
readonly PIPELINE_FILE="${PIPELINE_FILE:-/tmp/${JOB_NAME}-pipeline.yml}"
readonly REPO="${BENCH_REPO:-elastic/grafana-dashboards-analysis}"
readonly REPO_DIR="${REPO_DIR:-/tmp/${JOB_NAME}-repo}"
readonly INPUT_SUBDIR="${INPUT_SUBDIR:-results}"
readonly GRADLE_TASK="${GRADLE_TASK:-:x-pack:plugin:esql:analyzePromqlQueries}"
readonly ANNOTATE_CONTEXT_PREFIX="${ANNOTATE_CONTEXT_PREFIX:-${JOB_NAME}}"

log() {
  echo "$*"
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
  [[ -n "${dir}" ]] || return 0
  [[ -d "${dir}" ]] || return 0
  rm -rf "${dir}"
}

setup_requirements() {
  require_cmd git
  require_cmd find
  require_cmd sort
  require_cmd buildkite-agent
  require_cmd jq
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

  mapfile -d '' -t INPUT_FILES < <(find "${root}" -type f -name '*.csv' -print0 | sort -z)

  (( ${#INPUT_FILES[@]} > 0 )) || die "no CSV files found in ${root}"

  log "Found ${#INPUT_FILES[@]} input file(s)"
}

upload_artifacts() {
  log "Uploading input files"
  cleanup_dir "${INPUT_DIR}"
  mkdir -p "${INPUT_DIR}"

  local f
  for f in "${INPUT_FILES[@]}"; do
    cp "${f}" "${INPUT_DIR}/"
  done

  buildkite-agent artifact upload "${INPUT_DIR}/*.csv"
}

generate_pipeline() {
  log "Generating dynamic pipeline"

  {
    echo "steps:"
    local f name
    for f in "${INPUT_FILES[@]}"; do
      name="$(basename "${f}" .csv)"
      cat <<EOF
  - label: "${JOB_NAME} / ${name}"
    command: .buildkite/scripts/${JOB_NAME}.sh run "${name}"
    depends_on: "${SETUP_STEP_KEY}"
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

  buildkite-agent artifact download "${INPUT_DIR}/${filename}.csv" /tmp/

  INPUT_FILE="${local_root}/${filename}.csv"
  [[ -f "${INPUT_FILE}" ]] || die "file not found: ${INPUT_FILE}"
}

fetch_control_branch() {
  local branch="${BUILDKITE_PULL_REQUEST_BASE_BRANCH:-}"

  [[ -n "${branch}" ]] || die "BUILDKITE_PULL_REQUEST_BASE_BRANCH is required"

  git fetch --no-tags --depth=1 origin "${branch}"
  CONTROL_REF="origin/${branch}"
}

create_control_worktree() {
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
  local output_file="$2"

  (
    cd "${workspace}"
    WORKSPACE="${workspace}" GRADLEW="./gradlew" \
      .ci/scripts/run-gradle.sh "${GRADLE_TASK}" \
        -PqueriesFile="${INPUT_FILE}" \
        -PoutputFile="${output_file}" \
        -PoutputFormat=json \
        -Pverbose=false \
        --quiet
  )
}

annotate() {
  local style="$1"
  local context="$2"
  local message="$3"

  buildkite-agent annotate \
    --style "${style}" \
    --context "${context}" \
    "${message}"
}


check() {
  local name="$1"
  local control_report="$2"
  local test_report="$3"

  local control_success test_success control_total test_total diff
  control_success="$(jq -r '.successful' "${control_report}")"
  test_success="$(jq -r '.successful' "${test_report}")"
  control_total="$(jq -r '.total' "${control_report}")"
  test_total="$(jq -r '.total' "${test_report}")"
  diff=$(( test_success - control_success ))

  local style="success"
  local icon=":white_check_mark:"
  local status="OK"

  if (( diff < 0 )); then
    style="error"
    icon=":x:"
    status="REGRESSION (${diff})"
  elif (( diff > 0 )); then
    status="IMPROVED (+${diff})"
  fi

  log "${status}: ${control_success}/${control_total} -> ${test_success}/${test_total}"

  annotate \
    "${style}" \
    "${ANNOTATE_CONTEXT_PREFIX}-${name}" \
    "**${name}**: ${icon} ${status} - ${control_success}/${control_total} -> ${test_success}/${test_total}"

  (( diff < 0 )) && return 1
  return 0
}

cmd_run() {
  local filename="${1:?missing filename}"
  local control_report test_report

  setup_requirements
  mkdir -p "${OUTPUT_DIR}"

  trap cleanup_worktree EXIT

  download_input_file "${filename}"
  fetch_control_branch
  create_control_worktree

  control_report="${OUTPUT_DIR}/${filename}_control.json"
  test_report="${OUTPUT_DIR}/${filename}_test.json"

  log "--- Analyzing on control branch"
  if ! run_analyzer "${CONTROL_WORKTREE}" "${control_report}"; then
    log "Analyzer unavailable on control branch, skipping"
    annotate \
      "info" \
      "${ANNOTATE_CONTEXT_PREFIX}-${filename}" \
      "**${filename}**: :white_circle: Skipped (analyzer unavailable on control branch)"
    exit 0
  fi

  if ! jq -e 'has("successful") and has("total")' "${control_report}" >/dev/null 2>&1; then
    log "Control branch analyzer produced incompatible output format, skipping"
    annotate \
      "info" \
      "${ANNOTATE_CONTEXT_PREFIX}-${filename}" \
      "**${filename}**: :white_circle: Skipped (control branch output format incompatible)"
    exit 0
  fi

  log "Analyzing on test branch"
  run_analyzer "$(pwd)" "${test_report}"

  log "Comparing results"
  check "${filename}" "${control_report}" "${test_report}"
}

case "${1:-}" in
  setup)
    cmd_setup
    ;;
  run)
    cmd_run "${2:-}"
    ;;
  *)
    echo "Usage: $0 {setup|run <name>}" >&2
    exit 2
    ;;
esac
