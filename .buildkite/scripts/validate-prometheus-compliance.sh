#!/bin/bash

# Detects mismatches between Prometheus and Elasticsearch with promcheck
# (https://github.com/elastic/promcheck) on the PR and its merge base, then reports
# regressions, improvements, and flaky cases on the build and the PR.
# Regressions fail the step; comparisons that cannot run are reported as warnings.
# The grafanalabs-top100.tar.gz release asset supplies the dataset and queries.
#
# Required environment:
#   PROMCHECK_VER                    promcheck release version without the v prefix.
#   PROMCHECK_TEST_INSTANCE_TIMEOUT  Elasticsearch startup timeout in seconds.
#   GITHUB_PR_TARGET_BRANCH          PR target branch used to find the merge base.
#   GH_TOKEN or GITHUB_TOKEN         GitHub token for release asset downloads.
#   GRADLEW                          CI Gradle command from the pre-command hook; its options are reused.
# Optional environment:
#   PROMCHECK_CONCURRENCY            Cases and seeding writes promcheck keeps in flight (default: the CPU count).
#   PROMETHEUS_COMPLIANCE_TMPDIR     Scratch/artifact parent directory (default /tmp).
#   BUILDKITE_BUILD_URL              Build link included in the report.
#   BUILDKITE_JOB_ID                 Job anchor added to the build link.
# When buildkite-agent is available, the report is annotated and saved as PR-comment
# metadata for the build bot (the pipeline enables ELASTIC_PR_COMMENTS_ENABLED).
# With the argument notify, it only posts the running note as an early PR comment and exits;
# that needs PROMCHECK_VER alone.

# Requires Bash 4.4+ (the Buildkite agent runs Linux).
set -euo pipefail
shopt -s inherit_errexit lastpipe

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
  declare -gr CORPUS=grafanalabs-top100

  local var cmd
  for var in PROMCHECK_TEST_INSTANCE_TIMEOUT GITHUB_PR_TARGET_BRANCH GRADLEW; do
    [[ -n "${!var:-}" ]] || die "$var must be set"
  done
  for cmd in curl getconf git jq tee sort mkfifo; do
    command -v "$cmd" >/dev/null 2>&1 || die "$cmd must be on the PATH"
  done
  if [[ ! "$PROMCHECK_TEST_INSTANCE_TIMEOUT" =~ ^[1-9][0-9]*$ ]]; then
    die "PROMCHECK_TEST_INSTANCE_TIMEOUT must be a positive integer: $PROMCHECK_TEST_INSTANCE_TIMEOUT"
  fi

  if [[ -z "${GH_TOKEN:-${GITHUB_TOKEN:-}}" ]]; then
    die "GH_TOKEN or GITHUB_TOKEN must be set"
  fi

  declare -gr GITHUB_API_TOKEN="${GH_TOKEN:-${GITHUB_TOKEN:-}}"
  # Changed queries listed per section; the full list is the changes artifact.
  declare -gr MAX_ROWS=50
  declare -gr MAX_QUERY_CHARS=200
  # Size the node for the agent: every CPU (test clusters default to node.processors=2, which also caps the JVM at
  # two) and a quarter of the memory as heap; the rest is for the Gradle daemon, promcheck's in-process Prometheus,
  # the page cache, and the checkouts on the ramdisk. promcheck keeps one case or seeding write in flight per CPU by
  # default, which gives the same results as a sequential run, only sooner; 100 was no faster on n1-standard-8, since
  # the node is CPU-bound.
  local cpus pages page_size
  cpus=$(getconf _NPROCESSORS_ONLN)
  pages=$(getconf _PHYS_PAGES)
  page_size=$(getconf PAGESIZE)
  declare -gr AGENT_CPUS="$cpus"
  declare -gr ES_HEAP="$(( pages * page_size / 4 / 1024 / 1024 ))m"
  declare -gr PROMCHECK_CONCURRENCY="${PROMCHECK_CONCURRENCY:-$cpus}"
  if [[ ! "$PROMCHECK_CONCURRENCY" =~ ^[1-9][0-9]*$ ]]; then
    die "PROMCHECK_CONCURRENCY must be a positive integer: $PROMCHECK_CONCURRENCY"
  fi
  # The options of the Gradle command every CI job uses (build cache, its URL, build scans), without ./gradlew itself.
  local -a gradlew_words
  read -ra gradlew_words <<< "$GRADLEW"
  declare -gra GRADLEW_OPTIONS=("${gradlew_words[@]:1}")

  # The pipeline uploads OUTPUT_DIR as artifacts. Tests point TMP_ROOT at a scratch directory.
  declare -gr TMP_ROOT="${PROMETHEUS_COMPLIANCE_TMPDIR:-/tmp}"
  declare -gr WORK_DIR="$TMP_ROOT/validate-prometheus-compliance"
  declare -gr OUTPUT_DIR="$TMP_ROOT/validate-prometheus-compliance-output"
  declare -gr PROMCHECK_BIN="$WORK_DIR/promcheck"
  declare -gr BUNDLE="$WORK_DIR/$CORPUS.tar.gz"
  declare -gr CONTROL_LOG="$OUTPUT_DIR/@$CORPUS-control.log"
  declare -gr TEST_LOG="$OUTPUT_DIR/@$CORPUS-test.log"
  declare -gr CHANGES_FILE="$OUTPUT_DIR/@$CORPUS-changes.json"
  declare -gr REPORT_FILE="$OUTPUT_DIR/@$CORPUS-report.md"
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

# Keep the authorization header out of curl's process arguments.
github_curl() {
  printf 'Authorization: Bearer %s\n' "$GITHUB_API_TOKEN" |
    curl -fsSL --retry 3 --retry-delay 2 -H @- "$@"
}

# Downloads a promcheck release asset by name.
download_asset() {
  local release="$1"
  local name="$2"
  local target="$3"
  local url
  url=$(jq -er --arg name "$name" '.assets[] | select(.name == $name) | .url' <<< "$release") \
    || not_compared "The promcheck v$PROMCHECK_VER release has no $name"
  github_curl -H "Accept: application/octet-stream" -o "$target" -- "$url" \
    || not_compared "Could not download $name"
}

# Runs promcheck against Elasticsearch built from the given checkout. promcheck exits 1 when queries fail, which
# is a result; any other exit code means the run itself failed.
# Arguments after -- go to promcheck's ./gradlew run: the CI Gradle options from GRADLEW, which pull task outputs from
# the remote build cache that main CI pushes to and publish a build scan for each run, and .ci/init.gradle, which
# configures that cache. promcheck fixes only what every run needs; the node's size and settings are this job's.
run_promcheck() {
  local workspace="$1" log_file="$2"
  local stderr_log="${2%.log}.stderr.log" fifo="$WORK_DIR/promcheck-stderr.fifo"
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
  NO_COLOR=1 "$PROMCHECK_BIN" \
    --elasticsearch "$workspace" \
    --elasticsearch-timeout "${PROMCHECK_TEST_INSTANCE_TIMEOUT}s" \
    --output json \
    --concurrency "$PROMCHECK_CONCURRENCY" \
    -- "${gradle_args[@]}" \
    < "$BUNDLE" 2> "$fifo" | tee -- "$log_file"
  statuses=("${PIPESTATUS[@]}")
  wait "$stderr_pid" || stderr_status=$?
  rm -f -- "$fifo"
  # The node log times the build, startup, and seeding of each run; the merge-base worktree is gone after the job.
  cp -- "$workspace/build/testclusters/runTask-0/logs/runTask.log" "${log_file%.log}.elasticsearch.log" 2>/dev/null || true
  (( (statuses[0] == 0 || statuses[0] == 1) && statuses[1] == 0 && stderr_status == 0 ))
}

# Trust promcheck's summary; combine the error buckets for the console output.
summary() {
  jq -ec 'select(.type == "summary") | . + {errors: (.es_4xx + .es_5xx + .error)}' -- "$1"
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

# A subshell keeps main's locals available to the EXIT trap on errexit as well as explicit exit.
main() (
  local control_dir="" release arch base revision control_summary test_summary regressions
  local style=success status=0 final_report=false
  # Populated and consumed through namerefs in the helpers.
  # shellcheck disable=SC2034
  local -a changes=()
  configure
  trap 'on_exit "$?" "$control_dir" "$final_report"' EXIT

  rm -rf -- "$WORK_DIR" "$OUTPUT_DIR"
  mkdir -p -- "$WORK_DIR" "$OUTPUT_DIR"

  # Show up on the build as soon as the comparison starts.
  announce_running

  log "--- Downloading promcheck v$PROMCHECK_VER"
  release=$(
    github_curl -H "Accept: application/vnd.github+json" \
      -- "https://api.github.com/repos/elastic/promcheck/releases/tags/v$PROMCHECK_VER"
  ) || not_compared "Could not fetch the promcheck v$PROMCHECK_VER release"
  case "$(uname -m)" in
    x86_64) arch=amd64 ;;
    aarch64 | arm64) arch=arm64 ;;
    *) not_compared "Unsupported architecture $(uname -m)" ;;
  esac
  download_asset "$release" "promcheck-linux-$arch" "$PROMCHECK_BIN"
  download_asset "$release" "$CORPUS.tar.gz" "$BUNDLE"
  chmod -- +x "$PROMCHECK_BIN"

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
  run_promcheck "$control_dir" "$CONTROL_LOG" || not_compared "The control run failed"
  control_summary=$(summary "$CONTROL_LOG") || not_compared "The control run printed no promcheck summary"

  log "--- Running promcheck on the PR $revision"
  run_promcheck "$PWD" "$TEST_LOG" || not_compared "The test run failed"
  test_summary=$(summary "$TEST_LOG") || not_compared "The test run printed no promcheck summary"

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

if [[ "${1:-}" == notify ]]; then
  notify
else
  main
fi
