#!/bin/bash

# Execution helpers for PR comparisons and nightly metrics.
# Pipeline configuration lives in .buildkite/pipelines/pull-request-validate-prometheus-compliance.yml.
# Callers own their reporting policy: these functions return failures rather than exiting the job.

promcheck_error() {
  printf '%s\n' "$*" >&2
  return 1
}

# Reuse the CI Gradle command and size both Elasticsearch and promcheck for the agent.
promcheck_configure() {
  local var cmd cpus pages page_size
  for var in PROMCHECK_VER PROMCHECK_TEST_INSTANCE_TIMEOUT PROMCHECK_CORPUS GRADLEW; do
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
  promcheck_download_asset "$release" "$PROMCHECK_CORPUS.tar.gz" "$bundle" || return
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
