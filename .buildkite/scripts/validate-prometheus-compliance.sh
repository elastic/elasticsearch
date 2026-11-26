#!/bin/bash

# Modes: oneshot compares a PR with its merge base; nightly indexes a single run in Overview.
# Detects mismatches between Prometheus and Elasticsearch with promcheck
# (https://github.com/elastic/promcheck) on the PR and its merge base, then reports
# regressions, improvements, and flaky cases on the build and the PR.
# Regressions fail the step; comparisons that cannot run are reported as warnings.
# The grafanalabs-top100.tar.gz release asset supplies the dataset and queries.
#
# Required environment for oneshot:
#   GITHUB_PR_TARGET_BRANCH          PR target branch used to find the merge base.
#   GH_TOKEN or GITHUB_TOKEN         GitHub token for release asset downloads.
#   GRADLEW                          CI Gradle command from the pre-command hook; its options are reused.
# Shared release, workload, and startup timeout are supplied by .buildkite/pipelines/pull-request-validate-prometheus-compliance.yml.
# Nightly also requires BUILDKITE_BUILD_ID and the results namespace, URL, and Vault secret
# configured by that YAML (or PROMCHECK_RESULTS_API_KEY in place of the Vault secret).
# Optional environment:
#   PROMCHECK_CONCURRENCY            Cases and seeding writes promcheck keeps in flight (default: the CPU count).
#   PROMETHEUS_COMPLIANCE_TMPDIR     Scratch/artifact parent directory (default /tmp).
#   BUILDKITE_BUILD_URL              Build link included in the report.
#   BUILDKITE_JOB_ID                 Job anchor added to the build link.
# When buildkite-agent is available, the report is annotated and saved as PR-comment
# metadata for the build bot (the pipeline enables ELASTIC_PR_COMMENTS_ENABLED).
# With oneshot notify, it only posts the running note as an early PR comment and exits;
# that needs only PROMCHECK_VER from the shared YAML configuration.

# Requires Bash 4.4+ (the Buildkite agent runs Linux).
set -euo pipefail
# Keep credentials out of traced commands in either mode.
set +x
shopt -s inherit_errexit lastpipe

promcheck_error() {
  printf '%s\n' "$*" >&2
  return 1
}

# Reuse the CI Gradle command and size both Elasticsearch and promcheck for the agent.
promcheck_configure() {
  local var cmd cpus pages page_size
  for var in PROMCHECK_VER PROMCHECK_TEST_INSTANCE_TIMEOUT PROMCHECK_WORKLOAD GRADLEW; do
    [[ -n "${!var:-}" ]] || { promcheck_error "$var must be set"; return 1; }
  done
  for cmd in curl getconf jq tee mkfifo; do
    command -v "$cmd" >/dev/null 2>&1 || { promcheck_error "$cmd must be on the PATH"; return 1; }
  done
  [[ "$PROMCHECK_TEST_INSTANCE_TIMEOUT" =~ ^[1-9][0-9]*$ ]] \
    || { promcheck_error "PROMCHECK_TEST_INSTANCE_TIMEOUT must be a positive integer: $PROMCHECK_TEST_INSTANCE_TIMEOUT"; return 1; }
  GITHUB_API_TOKEN="${GH_TOKEN:-${GITHUB_TOKEN:-}}"
  [[ -n "$GITHUB_API_TOKEN" ]] || { promcheck_error "GH_TOKEN or GITHUB_TOKEN must be set"; return 1; }

  # Size the node for the agent: every CPU (test clusters default to node.processors=2, which also caps the JVM at
  # two) and a quarter of the memory as heap; the rest is for the Gradle daemon, promcheck's in-process Prometheus,
  # the page cache, and the checkouts on the ramdisk. promcheck keeps one case or seeding write in flight per CPU by
  # default, which gives the same results as a sequential run, only sooner; 100 was no faster on n1-standard-8, since
  # the node is CPU-bound.
  cpus=$(getconf _NPROCESSORS_ONLN) || return
  pages=$(getconf _PHYS_PAGES) || return
  page_size=$(getconf PAGESIZE) || return
  AGENT_CPUS="$cpus"
  ES_HEAP="$(( pages * page_size / 4 / 1024 / 1024 ))m"
  PROMCHECK_CONCURRENCY="${PROMCHECK_CONCURRENCY:-$cpus}"
  [[ "$PROMCHECK_CONCURRENCY" =~ ^[1-9][0-9]*$ ]] \
    || { promcheck_error "PROMCHECK_CONCURRENCY must be a positive integer: $PROMCHECK_CONCURRENCY"; return 1; }
  # Reuse the pre-command hook options (remote build cache and scans), without ./gradlew itself.
  local -a gradlew_words
  read -ra gradlew_words <<< "$GRADLEW"
  GRADLEW_OPTIONS=("${gradlew_words[@]:1}")
}

# Keep the authorization header out of curl's process arguments.
promcheck_github_curl() {
  printf 'Authorization: Bearer %s\n' "$GITHUB_API_TOKEN" |
    curl -fsSL --retry 3 --retry-delay 2 --connect-timeout 30 --max-time 300 -H @- "$@"
}

promcheck_download_asset() {
  local release="$1" name="$2" target="$3" url
  url=$(jq -er --arg name "$name" '.assets[] | select(.name == $name) | .url' <<< "$release") \
    || { promcheck_error "The promcheck v$PROMCHECK_VER release has no $name"; return 1; }
  promcheck_github_curl -H 'Accept: application/octet-stream' -o "$target" -- "$url" \
    || { promcheck_error "Could not download $name"; return 1; }
}

# Download the binary and corpus from the same pinned release into caller-owned paths.
promcheck_download() {
  local binary="$1" bundle="$2" release arch
  release=$(promcheck_github_curl -H 'Accept: application/vnd.github+json' \
    -- "https://api.github.com/repos/elastic/promcheck/releases/tags/v$PROMCHECK_VER") \
    || { promcheck_error "Could not fetch the promcheck v$PROMCHECK_VER release"; return 1; }
  case "$(uname -m)" in
    x86_64) arch=amd64 ;;
    aarch64 | arm64) arch=arm64 ;;
    *) promcheck_error "Unsupported architecture $(uname -m)"; return 1 ;;
  esac
  promcheck_download_asset "$release" "promcheck-linux-$arch" "$binary" || return
  promcheck_download_asset "$release" "$PROMCHECK_WORKLOAD.tar.gz" "$bundle" || return
  chmod -- +x "$binary"
}

# Runs promcheck against Elasticsearch built from the given checkout. promcheck exits 1 when queries fail, which
# is a result; any other exit code means the run itself failed.
# Arguments after -- go to promcheck's ./gradlew run: the CI Gradle options from GRADLEW, which pull task outputs from
# the remote build cache that main CI pushes to and publish a build scan for each run, and .ci/init.gradle, which
# configures that cache. promcheck fixes only what every run needs; the node's size and settings are this job's.
# JSONL stdout and diagnostic stderr have separate artifacts.
promcheck_run() {
  local workspace="$1" binary="$2" bundle="$3" log_file="$4"
  # Keep the FIFO beside the temporary binary, outside the artifact glob.
  local stderr_log="${log_file%.log}.stderr.log" fifo="$binary.stderr.fifo"
  local stderr_pid stderr_status=0
  local -a statuses gradle_args=(
    "${GRADLEW_OPTIONS[@]}"
    --configuration-cache
    -I "$workspace/.ci/init.gradle"
    "-Dtests.es.node.processors=$AGENT_CPUS"
    "-Dtests.heap.size=$ES_HEAP"
    -Drun.license_type=trial
    -Dtests.es.xpack.ml.enabled=false
  )
  mkfifo -- "$fifo" || return
  tee -- "$stderr_log" < "$fifo" >&2 &
  stderr_pid=$!
  NO_COLOR=1 "$binary" \
    --elasticsearch "$workspace" \
    --elasticsearch-timeout "${PROMCHECK_TEST_INSTANCE_TIMEOUT}s" \
    --output json \
    --concurrency "$PROMCHECK_CONCURRENCY" \
    -- "${gradle_args[@]}" \
    < "$bundle" 2> "$fifo" | tee -- "$log_file"
  statuses=("${PIPESTATUS[@]}")
  wait "$stderr_pid" || stderr_status=$?
  rm -f -- "$fifo"
  # The node log times the build, startup, and seeding of each run; the merge-base worktree is gone after the job.
  cp -- "$workspace/build/testclusters/runTask-0/logs/runTask.log" "${log_file%.log}.elasticsearch.log" 2>/dev/null || true
  (( (statuses[0] == 0 || statuses[0] == 1) && statuses[1] == 0 && stderr_status == 0 ))
}

# Reject truncated/malformed logs and inconsistent counts before either caller reports a result.
promcheck_summary() {
  jq -enc '
    reduce inputs as $row ({count: 0};
      if $row.type == "summary" then {count: (.count + 1), summary: $row} else . end)
    | if .count == 1 then .summary else error("Expected one complete promcheck summary") end
    | if all([.total, .inspected, .match, .mismatch, .es_4xx, .es_5xx, .error,
              .request_filtered, .response_filtered, .flaky][]; type == "number" and . >= 0 and . == floor)
      then . else error("Invalid promcheck counts") end
    | . + {errors: (.es_4xx + .es_5xx + .error)}
    | if .total > 0 and .total == (.inspected + .request_filtered)
         and .inspected == (.match + .mismatch + .errors + .response_filtered + .flaky)
      then . else error("Inconsistent promcheck counts") end
  ' -- "$1"
}

log() {
  printf '%s\n' "$*" >&2
}

die() {
  log "$*"
  exit 1
}

# What reporting needs: the annotation and PR comment, and the links in them. The build link points at this job when
# the argument is true.
configure_report() {
  local link_job="$1" build_url
  [[ -n "${PROMCHECK_VER:-}" ]] || die "PROMCHECK_VER must be set"
  declare -gr RELEASE_URL="https://github.com/elastic/promcheck/releases/tag/v$PROMCHECK_VER"
  declare -gr ANNOTATION_CONTEXT="ctx-validate-prometheus-compliance"
  declare -gr PR_COMMENT_KEY="pr_comment:validate-prometheus-compliance:body"
  build_url="${BUILDKITE_BUILD_URL:-}"
  if [[ "$link_job" == true && -n "$build_url" && -n "${BUILDKITE_JOB_ID:-}" ]]; then
    build_url+="#$BUILDKITE_JOB_ID"
  fi
  declare -gr BUILD_URL="$build_url"
}

configure() {
  configure_report true
  declare -gr WORKLOAD="$PROMCHECK_WORKLOAD"
  [[ -n "${GITHUB_PR_TARGET_BRANCH:-}" ]] || die "GITHUB_PR_TARGET_BRANCH must be set"
  promcheck_configure || return
  local cmd
  for cmd in git sort; do
    command -v "$cmd" >/dev/null 2>&1 || die "$cmd must be on the PATH"
  done
  # Changed queries listed per section; the full list is the changes artifact.
  declare -gr MAX_ROWS=50
  declare -gr MAX_QUERY_CHARS=200

  # The pipeline uploads OUTPUT_DIR as artifacts. Tests point TMP_ROOT at a scratch directory.
  declare -gr TMP_ROOT="${PROMETHEUS_COMPLIANCE_TMPDIR:-/tmp}"
  declare -gr WORK_DIR="$TMP_ROOT/validate-prometheus-compliance"
  declare -gr OUTPUT_DIR="$TMP_ROOT/validate-prometheus-compliance-output"
  declare -gr PROMCHECK_BIN="$WORK_DIR/promcheck"
  declare -gr BUNDLE="$WORK_DIR/$WORKLOAD.tar.gz"
  declare -gr CONTROL_LOG="$OUTPUT_DIR/@$WORKLOAD-control.log"
  declare -gr TEST_LOG="$OUTPUT_DIR/@$WORKLOAD-test.log"
  declare -gr CHANGES_FILE="$OUTPUT_DIR/@$WORKLOAD-changes.json"
  declare -gr REPORT_FILE="$OUTPUT_DIR/@$WORKLOAD-report.md"
  # Artifacts keep their absolute path without the leading slash.
  declare -gr CHANGES_LINK="<a href=\"artifact://${CHANGES_FILE#/}\">${CHANGES_FILE##*/}</a>"
}

# Shows the report on the build and sets it as pr_comment meta-data, which the Buildkite build bot posts in its PR
# comment for pipelines that enable it (ELASTIC_PR_COMMENTS_ENABLED).
annotate() {
  local style="$1"
  local annotation="$2"
  if command -v buildkite-agent >/dev/null 2>&1; then
    buildkite-agent annotate --context "$ANNOTATION_CONTEXT" --style "$style" <<< "$annotation" \
      || log "Failed to annotate the build"
    buildkite-agent meta-data set "$PR_COMMENT_KEY" <<< "$annotation" \
      || log "Failed to set the PR comment"
  fi
}

# Wraps a report body read from stdin: first the marker that lets the PR comment be found and replaced, last the
# links to the run and the promcheck release.
report() {
  printf '<!-- promcheck-pr-report -->\n## Promcheck\n\n'
  cat
  if [[ -n "$BUILD_URL" ]]; then
    printf '[Buildkite run](%s) | ' "$BUILD_URL"
  fi
  printf '[promcheck v%s](%s)\n' "$PROMCHECK_VER" "$RELEASE_URL"
}

# The note shown while the comparison runs. The final report reuses the same annotation context and comment key, so
# it replaces this note instead of adding a second one.
announce_running() {
  annotate info "$(printf '**Promcheck running**\n\nChecking Prometheus/Elasticsearch mismatches on the merge base and this PR with promcheck v%s. The verdict replaces this note when both runs finish.\n\n' "$PROMCHECK_VER" | report)"
}

# The pipeline's first step: posts the running note as the PR comment right away. The build bot comments early once
# the job named by pr_comment:early_comment_job_id finishes, as the performance benchmark pipeline does, instead of
# when the whole build ends.
notify() {
  configure_report false
  announce_running
  if command -v buildkite-agent >/dev/null 2>&1; then
    buildkite-agent meta-data set pr_comment:early_comment_job_id "${BUILDKITE_JOB_ID:-}" \
      || log "Failed to request an early PR comment"
  fi
}

# The comparison could not run. It is reported like a result rather than failing the PR.
not_compared() {
  local reason="$1"
  log "Not compared: $reason"
  annotate warning "$(printf '**Not compared**\n\n%s.\n\n' "$reason" | report)"
  exit 0
}

# Called indirectly by the EXIT trap in main.
# shellcheck disable=SC2329
cleanup() {
  local control_dir="$1"
  if [[ -n "$control_dir" ]]; then
    git worktree remove --force -- "$control_dir" >/dev/null 2>&1 || true
    rm -rf -- "$control_dir"
  fi
  git worktree prune || log "Failed to prune worktrees"
}

# NUL-delimited fields preserve tabs, newlines, and backslashes in query text.
# lastpipe fills the caller's array; pipefail propagates jq failures.
read_queries() {
  jq -jn 'inputs | select(.type == "query") |
    [.name, .status, (.es_status // 0 | tostring), (.skip_reason // ""), (.query // "")] |
    join("\u0000") + "\u0000"' -- "$1" | mapfile -d '' -t "$2"
}

# Return values through namerefs to avoid a subshell for every case.
# shellcheck disable=SC2034
query_status() {
  local -n status_ref="$1"
  case "$2" in
    match|mismatch) status_ref="$2" ;;
    skipped)
      case "$4" in
        flaky) status_ref=flaky ;;
        response_filtered) status_ref=filtered ;;
        *) log "Unknown promcheck skip reason: $4"; return 1 ;;
      esac ;;
    error)
      if (( $3 >= 500 )); then status_ref=error5xx
      elif (( $3 >= 400 )); then status_ref=error4xx
      else status_ref=error; fi ;;
    *) log "Unknown promcheck status: $2"; return 1 ;;
  esac
}

# shellcheck disable=SC2034
classify() {
  local -n category_ref="$1"
  local -Ar severity=([match]=0 [mismatch]=1 [error4xx]=2 [error5xx]=3 [error]=3)
  case "$2:$3" in
    flaky:*|*:flaky) category_ref=flaky ;;
    filtered:*|*:filtered) category_ref=stable ;;
    *)
      if (( severity[$2] < severity[$3] )); then category_ref=regression
      elif (( severity[$2] > severity[$3] )); then category_ref=improvement
      else category_ref=stable; fi ;;
  esac
}

# Each result has five fields: case ID, previous status, current status, category, query.
compare_results() {
  local -n results_ref="$3"
  local -a control_records tested_records
  local -A previous=() seen=()
  local i id old new category
  read_queries "$1" control_records || return
  read_queries "$2" tested_records || return
  if (( ${#control_records[@]} != ${#tested_records[@]} )); then
    log "Promcheck runs returned different case counts"
    return 1
  fi
  for ((i = 0; i < ${#control_records[@]}; i += 5)); do
    id=${control_records[i]}
    if [[ -n "${previous[$id]+present}" ]]; then
      log "Duplicate control case: $id"
      return 1
    fi
    query_status old "${control_records[i+1]}" "${control_records[i+2]}" "${control_records[i+3]}" || return
    previous[$id]=$old
  done
  for ((i = 0; i < ${#tested_records[@]}; i += 5)); do
    id=${tested_records[i]}
    if [[ -z "${previous[$id]+present}" || -n "${seen[$id]+present}" ]]; then
      log "Unmatched or duplicate test case: $id"
      return 1
    fi
    seen[$id]=1
    old=${previous[$id]}
    query_status new "${tested_records[i+1]}" "${tested_records[i+2]}" "${tested_records[i+3]}" || return
    classify category "$old" "$new"
    [[ "$category" == stable ]] || results_ref+=("$id" "$old" "$new" "$category" "${tested_records[i+4]}")
  done
}

# Serialize the Bash comparison records in one jq invocation.
write_changes() {
  local -n rows_ref="$1"
  {
    if (( ${#rows_ref[@]} > 0 )); then printf '%s\0' "${rows_ref[@]}"; fi
  } | jq -Rs 'split("\u0000") | .[:-1] |
    [range(0; length; 5) as $i |
      {name: .[$i], before: .[$i+1], after: .[$i+2], category: .[$i+3], query: .[$i+4]}]'
}

count_category() {
  local -n rows_ref="$1"
  local category="$2" i count=0
  for ((i = 3; i < ${#rows_ref[@]}; i += 5)); do
    if [[ "${rows_ref[i]}" == "$category" ]]; then ((count += 1)); fi
  done
  printf '%d' "$count"
}

# shellcheck disable=SC2034
status_label() {
  local -n label_ref="$1"
  case "$2" in
    match) label_ref=PASS ;;
    mismatch) label_ref=FAIL ;;
    error4xx) label_ref='ERROR 4xx' ;;
    error5xx) label_ref='ERROR 5xx' ;;
    flaky) label_ref=FLAKY ;;
    *) label_ref=ERROR ;;
  esac
}

# Backticks below are literal Markdown delimiters.
# shellcheck disable=SC2016
print_code() {
  local text="$1"
  text=${text//$'\n'/ }
  text=${text//$'\r'/ }
  text=${text//$'\t'/ }
  text=${text//|/\\|}
  if (( ${#text} > MAX_QUERY_CHARS )); then text="${text:0:MAX_QUERY_CHARS-3}..."; fi
  if [[ "$text" == *'`'* ]]; then printf '`` %s ``' "$text"; else printf '`%s`' "$text"; fi
}

print_count() {
  if (( $1 == 1 )); then printf '%d %s' "$1" "$2"; else printf '%d %s' "$1" "$3"; fi
}

# shellcheck disable=SC2016
render_section() {
  local -n rows_ref="$1"
  local category="$2" title="$3" total="$4" i id sorted_ids before_label after_label shown=0
  local -A positions=()
  local -a ids=()
  (( total > 0 )) || return 0
  # Sort only case IDs; query strings never pass through a line-oriented format.
  for ((i = 0; i < ${#rows_ref[@]}; i += 5)); do
    if [[ "${rows_ref[i+3]}" == "$category" ]]; then
      id=${rows_ref[i]}
      positions[$id]=$i
      ids+=("$id")
    fi
  done
  printf '### %s\n\n| Case | Result | Query |\n|---:|:---:|---|\n' "$title"
  sorted_ids=$(printf '%s\n' "${ids[@]}" | sort -n)
  while IFS= read -r id; do
    i=${positions[$id]}
    status_label before_label "${rows_ref[i+1]}"
    status_label after_label "${rows_ref[i+2]}"
    printf '| %s | `%s -> %s` | ' "$id" "$before_label" "$after_label"
    print_code "${rows_ref[i+4]}"
    printf ' |\n'
    ((shown += 1))
    (( shown < MAX_ROWS )) || break
  done <<< "$sorted_ids"
  if (( total > MAX_ROWS )); then printf '\n_%d more in %s._\n' "$((total - MAX_ROWS))" "$CHANGES_LINK"; fi
  printf '\n'
}

# shellcheck disable=SC2016
render_changes() {
  local rows_name="$1" base="$2" revision="$3" regressions improvements flaky
  regressions=$(count_category "$rows_name" regression)
  improvements=$(count_category "$rows_name" improvement)
  flaky=$(count_category "$rows_name" flaky)
  if (( regressions > 0 )); then printf '**Regression detected**\n\n'; else printf '**No regressions**\n\n'; fi
  if (( regressions + improvements + flaky == 0 )); then
    printf 'No query compatibility changes detected.\n\n'
  else
    print_count "$regressions" regression regressions
    printf ', '
    print_count "$improvements" improvement improvements
    printf ', '
    print_count "$flaky" 'flaky case' 'flaky cases'
    printf '\n\n<details>\n<summary><strong>Show changed queries</strong></summary>\n\n'
    printf 'Queries are shortened to %d characters; full text is in %s.\n\n' "$MAX_QUERY_CHARS" "$CHANGES_LINK"
    render_section "$rows_name" regression Regressions "$regressions"
    render_section "$rows_name" improvement Improvements "$improvements"
    render_section "$rows_name" flaky Flaky "$flaky"
    printf '</details>\n\n'
  fi
  printf '**Compared** `%s` -> `%s`  \n' "${base:0:10}" "${revision:0:10}"
}

# Preserve the failure exit code while replacing any unfinished running note.
# shellcheck disable=SC2329
on_exit() {
  local status="$1" control_dir="$2" final_report="$3"
  if (( status != 0 )) && [[ "$final_report" != true ]]; then
    annotate warning "$(printf '**Not compared**\n\nReport generation or setup failed (exit %d). See the build log and artifacts.\n\n' "$status" | report)"
  fi
  cleanup "$control_dir"
}

# A subshell keeps the mode's locals available to the EXIT trap on errexit as well as explicit exit.
run_oneshot() (
  local control_dir="" base revision control_summary test_summary regressions
  local style=success status=0 final_report=false
  # Populated and consumed through namerefs in the helpers.
  # shellcheck disable=SC2034
  local -a changes=()
  configure || exit $?
  trap 'on_exit "$?" "$control_dir" "$final_report"' EXIT

  rm -rf -- "$WORK_DIR" "$OUTPUT_DIR"
  mkdir -p -- "$WORK_DIR" "$OUTPUT_DIR"

  # Show up on the build as soon as the comparison starts.
  announce_running

  log "--- Downloading promcheck v$PROMCHECK_VER"
  promcheck_download "$PROMCHECK_BIN" "$BUNDLE" || not_compared "Could not prepare promcheck"

  log "--- Checking out the merge base with $GITHUB_PR_TARGET_BRANCH"
  git fetch --no-tags -- origin "+refs/heads/$GITHUB_PR_TARGET_BRANCH:refs/remotes/origin/$GITHUB_PR_TARGET_BRANCH" \
    || not_compared "Could not fetch $GITHUB_PR_TARGET_BRANCH"
  # The control is where this PR branched off, not the tip of the target branch: a PromQL change that landed there
  # since would otherwise count as this PR's regression or fix.
  base=$(git merge-base "origin/$GITHUB_PR_TARGET_BRANCH" HEAD) \
    || not_compared "This PR has no merge base with $GITHUB_PR_TARGET_BRANCH"
  revision=$(git rev-parse HEAD)
  control_dir=$(mktemp -d "$TMP_ROOT/validate-prometheus-compliance-control-XXXXXX")
  git worktree add --detach -- "$control_dir" "$base" || not_compared "Could not check out the merge base $base"

  log "--- Running promcheck on the merge base $base"
  promcheck_run "$control_dir" "$PROMCHECK_BIN" "$BUNDLE" "$CONTROL_LOG" || not_compared "The control run failed"
  control_summary=$(promcheck_summary "$CONTROL_LOG") || not_compared "The control run printed no promcheck summary"

  log "--- Running promcheck on the PR $revision"
  promcheck_run "$PWD" "$PROMCHECK_BIN" "$BUNDLE" "$TEST_LOG" || not_compared "The test run failed"
  test_summary=$(promcheck_summary "$TEST_LOG") || not_compared "The test run printed no promcheck summary"

  log "--- Reporting"
  compare_results "$CONTROL_LOG" "$TEST_LOG" changes || not_compared "Could not compare promcheck results"
  write_changes changes > "$CHANGES_FILE"
  render_changes changes "$base" "$revision" | report > "$REPORT_FILE"

  regressions=$(count_category changes regression)
  if (( regressions > 0 )); then
    style=error
    status=1
  fi
  annotate "$style" "$(< "$REPORT_FILE")"
  final_report=true

  log "Merge base: $control_summary"
  log "PR: $test_summary"
  log "Regressions: $regressions"
  log "Report: $REPORT_FILE"
  exit "$status"
)

# The scheduled mode records one real run without the PR comparison or comment workflow.
# A subshell keeps the temporary directory available to its EXIT trap on failure.
run_nightly() (
  for var in PROMCHECK_RESULTS_NAMESPACE BUILDKITE_BUILD_ID; do
    [[ -n "${!var:-}" ]] || { echo "$var must be set" >&2; exit 1; }
  done
  promcheck_configure
  [[ "$PROMCHECK_RESULTS_NAMESPACE" =~ ^[a-z0-9_]+$ && ${#PROMCHECK_RESULTS_NAMESPACE} -le 100 ]] \
    || { echo "Invalid results namespace" >&2; exit 1; }
  # The namespace selects both the destination and its dashboard filter, keeping experimental runs isolated.
  readonly PROMCHECK_RESULTS_INDEX="synthetics-promql.support-$PROMCHECK_RESULTS_NAMESPACE"
  # CI provides Vault access. Environment overrides also allow an explicitly configured destination.
  if [[ -z "${PROMCHECK_RESULTS_URL:-}" || -z "${PROMCHECK_RESULTS_API_KEY:-}" ]]; then
    : "${PROMCHECK_RESULTS_SECRET:?Set the results URL and API key, or a Vault secret path}"
    secret=$(vault read -format=json "$PROMCHECK_RESULTS_SECRET")
    PROMCHECK_RESULTS_URL=${PROMCHECK_RESULTS_URL:-$(jq -er '.data.url' <<< "$secret")}
    PROMCHECK_RESULTS_API_KEY=${PROMCHECK_RESULTS_API_KEY:-$(jq -er '.data.api_key' <<< "$secret")}
    unset secret
  fi
  [[ "$PROMCHECK_RESULTS_URL" == https://* ]] || { echo "Results URL must use HTTPS" >&2; exit 1; }

  readonly OUTPUT_DIR="${PROMCHECK_OUTPUT_DIR:-/tmp/prometheus-nightly-output}"
  mkdir -p "$OUTPUT_DIR"
  work_dir=$(mktemp -d)
  trap 'rm -rf "$work_dir"' EXIT
  echo "--- Downloading promcheck v$PROMCHECK_VER"
  promcheck_download "$work_dir/promcheck" "$work_dir/$PROMCHECK_WORKLOAD.tar.gz"

  revision=$(git rev-parse HEAD)
  started_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  echo "--- Running promcheck"
  promcheck_run "$PWD" "$work_dir/promcheck" "$work_dir/$PROMCHECK_WORKLOAD.tar.gz" "$OUTPUT_DIR/promcheck.log" \
    || { echo "promcheck failed to complete; see run artifacts" >&2; exit 1; }
  promcheck_summary "$OUTPUT_DIR/promcheck.log" > "$OUTPUT_DIR/counts.json"
  jq -n --slurpfile counts "$OUTPUT_DIR/counts.json" \
    --arg timestamp "$started_at" --arg commit "$revision" --arg branch "${BUILDKITE_BRANCH:-}" \
    --arg version "$PROMCHECK_VER" --arg corpus "$PROMCHECK_WORKLOAD" --arg namespace "$PROMCHECK_RESULTS_NAMESPACE" \
    --arg build_id "$BUILDKITE_BUILD_ID" --arg build_url "${BUILDKITE_BUILD_URL:-}" \
    --arg pipeline "${BUILDKITE_PIPELINE_SLUG:-elasticsearch-pull-request-validate-prometheus-compliance}" \
    --argjson build_number "${BUILDKITE_BUILD_NUMBER:-0}" \
    '$counts[0] as $c | {
      "@timestamp": $timestamp, status: "ok", dataset: $corpus,
      data_stream: {type: "synthetics", dataset: "promql.support", namespace: $namespace},
      git: {commit: $commit, branch: $branch}, promcheck: {version: $version},
      buildkite: {build_id: $build_id, build_number: $build_number, build_url: $build_url, pipeline: $pipeline},
      compliance: {
        input_total: $c.total, executed_total: $c.inspected, executed_success: $c.match,
        executed_failure: $c.mismatch, executed_error: $c.errors,
        executed_skip: ($c.response_filtered + $c.flaky), success_pct: ((1000 * $c.match / $c.total | round) / 10)
      }}' > "$OUTPUT_DIR/result.json"

  echo "--- Indexing promcheck results"
  # Overview stores compliance history in a data stream, which requires create operations.
  # A stable build/corpus ID lets an upload retry recognize an already-created result.
  doc_id=$(jq -rn --arg id "$BUILDKITE_BUILD_ID-$PROMCHECK_WORKLOAD" '$id | @uri')
  http_status=$(curl -sS --retry 3 --connect-timeout 30 --max-time 120 -X PUT \
    -H "Authorization: ApiKey $PROMCHECK_RESULTS_API_KEY" -H 'Content-Type: application/json' \
    --data-binary "@$OUTPUT_DIR/result.json" -o "$OUTPUT_DIR/index-response.json" --write-out '%{http_code}' \
    "${PROMCHECK_RESULTS_URL%/}/$PROMCHECK_RESULTS_INDEX/_create/$doc_id")
  case "$http_status" in
    201) jq -e '.result == "created"' "$OUTPUT_DIR/index-response.json" >/dev/null ;;
    409) jq -e '.error.type == "version_conflict_engine_exception"' "$OUTPUT_DIR/index-response.json" >/dev/null ;;
    *) echo "Results upload failed (HTTP $http_status); see index-response.json" >&2; exit 1 ;;
  esac
  echo "Indexed $PROMCHECK_WORKLOAD results for $revision"
)

case "${1:-}" in
  oneshot)
    case "${2:-run}" in
      notify) notify ;;
      run) run_oneshot ;;
      *) die "Unknown oneshot action: $2 (expected run or notify)" ;;
    esac ;;
  nightly) run_nightly ;;
  *) die "Unknown promcheck mode: ${1:-<unset>} (expected oneshot or nightly)" ;;
esac
