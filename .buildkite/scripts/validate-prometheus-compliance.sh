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
# Optional environment:
#   PROMETHEUS_COMPLIANCE_TMPDIR     Scratch/artifact parent directory (default /tmp).
#   BUILDKITE_BUILD_URL              Build link included in the report.
#   BUILDKITE_JOB_ID                 Job anchor added to the build link.
# When buildkite-agent is available, the report is annotated and saved as PR-comment
# metadata for the build bot (the pipeline enables ELASTIC_PR_COMMENTS_ENABLED).

# Requires Bash 4.4+ (the Buildkite agent runs Linux).
set -euo pipefail
shopt -s inherit_errexit

log() {
  printf '%s\n' "$*" >&2
}

die() {
  log "$*"
  exit 1
}

configure() {
  declare -gr CORPUS=grafanalabs-top100

  local var cmd build_url
  for var in PROMCHECK_VER PROMCHECK_TEST_INSTANCE_TIMEOUT GITHUB_PR_TARGET_BRANCH; do
    [[ -n "${!var:-}" ]] || die "$var must be set"
  done
  for cmd in awk curl git jq tee; do
    command -v "$cmd" >/dev/null 2>&1 || die "$cmd must be on the PATH"
  done
  if [[ ! "$PROMCHECK_TEST_INSTANCE_TIMEOUT" =~ ^[1-9][0-9]*$ ]]; then
    die "PROMCHECK_TEST_INSTANCE_TIMEOUT must be a positive integer: $PROMCHECK_TEST_INSTANCE_TIMEOUT"
  fi

  if [[ -z "${GH_TOKEN:-${GITHUB_TOKEN:-}}" ]]; then
    die "GH_TOKEN or GITHUB_TOKEN must be set"
  fi

  declare -gr GITHUB_API_TOKEN="${GH_TOKEN:-${GITHUB_TOKEN:-}}"
  declare -gr RELEASE_URL="https://github.com/elastic/promcheck/releases/tag/v$PROMCHECK_VER"
  declare -gr ANNOTATION_CONTEXT="ctx-validate-prometheus-compliance"
  declare -gr PR_COMMENT_KEY="pr_comment:validate-prometheus-compliance:body"
  # Changed queries listed per section; the full list is the changes artifact.
  declare -gr MAX_ROWS=50

  # The pipeline uploads OUTPUT_DIR as artifacts. Tests point TMP_ROOT at a scratch directory.
  declare -gr TMP_ROOT="${PROMETHEUS_COMPLIANCE_TMPDIR:-/tmp}"
  declare -gr WORK_DIR="$TMP_ROOT/validate-prometheus-compliance"
  declare -gr OUTPUT_DIR="$TMP_ROOT/validate-prometheus-compliance-output"
  declare -gr PROMCHECK_BIN="$WORK_DIR/promcheck"
  declare -gr BUNDLE="$WORK_DIR/$CORPUS.tar.gz"
  declare -gr CONTROL_LOG="$OUTPUT_DIR/@$CORPUS-control.log"
  declare -gr CONTROL_OUTCOMES="$OUTPUT_DIR/@$CORPUS-control.tsv"
  declare -gr TEST_OUTCOMES="$OUTPUT_DIR/@$CORPUS-test.tsv"
  declare -gr TEST_LOG="$OUTPUT_DIR/@$CORPUS-test.log"
  declare -gr CHANGES_FILE="$OUTPUT_DIR/@$CORPUS-changes.tsv"
  declare -gr REPORT_FILE="$OUTPUT_DIR/@$CORPUS-report.md"
  # Artifacts keep their absolute path without the leading slash.
  declare -gr CHANGES_LINK="<a href=\"artifact://${CHANGES_FILE#/}\">${CHANGES_FILE##*/}</a>"

  # The exact job this comparison ran in.
  build_url="${BUILDKITE_BUILD_URL:-}"
  if [[ -n "$build_url" && -n "${BUILDKITE_JOB_ID:-}" ]]; then
    build_url+="#$BUILDKITE_JOB_ID"
  fi
  declare -gr BUILD_URL="$build_url"
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
run_promcheck() {
  local workspace="$1"
  local log_file="$2"
  local -a statuses
  NO_COLOR=1 "$PROMCHECK_BIN" \
    --elasticsearch "$workspace" \
    --elasticsearch-timeout "${PROMCHECK_TEST_INSTANCE_TIMEOUT}s" \
    --output json < "$BUNDLE" | tee -- "$log_file"
  statuses=("${PIPESTATUS[@]}")
  (( (statuses[0] == 0 || statuses[0] == 1) && statuses[1] == 0 ))
}

# Trust promcheck's summary; combine the error buckets for the console output.
summary() {
  jq -ec 'select(.type == "summary") | . + {errors: (.es_4xx + .es_5xx + .error)}' -- "$1"
}

# Every executed query as "<case>\t<status>\t<expression>". Flatten whitespace
# for the Markdown table while preserving literal backslashes in PromQL.
outcomes() {
  jq -nr '
    inputs | select(.type == "query") |
    (if .status == "skipped" then
       if .skip_reason == "flaky" then "flaky" else "filtered" end
     elif .status == "error" and .es_status >= 500 then "error5xx"
     elif .status == "error" and .es_status >= 400 then "error4xx"
     else .status end) as $status |
    [.name, $status, ((.query // "") | gsub("[\t\r\n]"; " "))] | join("\t")
  ' -- "$1"
}

# Compare by case ID. Flaky on either side takes precedence; filtered cases
# do not participate. The last column records the comparison category.
changes() {
  awk -F $'\t' '
    function severity(s) {
      return s == "match" ? 0 : s == "mismatch" ? 1 : s == "error4xx" ? 2 : 3
    }
    FILENAME == ARGV[1] { before[$1] = $2; next }
    $1 in before {
      old = before[$1]
      if (old == "flaky" || $2 == "flaky") category = "flaky"
      else if (old == "filtered" || $2 == "filtered") next
      else if (severity(old) < severity($2)) category = "regression"
      else if (severity(old) > severity($2)) category = "improvement"
      else next
      printf "%s\t%s\t%s\t%s\t%s\n", $1, old, $2, $3, category
    }
  ' "$1" "$2" | sort -t $'\t' -k1,1n
}

# The report body, in GitHub markdown: the result first, the changed queries collapsed, the comparison last.
# Stable cases are omitted. Flaky cases are shown separately, even when flaky in both runs.
render_changes() {
  local changes_file="$1"
  local base="$2"
  local revision="$3"
  awk -F $'\t' -v base="$base" -v revision="$revision" -v max="$MAX_ROWS" -v full="$CHANGES_LINK" '
    function label(s) { return s == "match" ? "PASS" : s == "mismatch" ? "FAIL" : s == "error4xx" ? "ERROR 4xx" : s == "error5xx" ? "ERROR 5xx" : s == "flaky" ? "FLAKY" : "ERROR" }
    # n + 0: a section with no rows never set its counter, which awk would print as "".
    function count(n, one, many) { return (n + 0) " " (n == 1 ? one : many) }
    # Inline code: pipes escaped for the table, a backtick in the query widens the fence.
    function code(s) {
      gsub(/\|/, "\\|", s)
      return index(s, "`") ? "`` " s " ``" : "`" s "`"
    }
    function section(title, rows, n,   i, f) {
      if (!n) return
      printf "### %s\n\n| Case | Result | Query |\n|---:|:---:|---|\n", title
      for (i = 1; i <= n && i <= max; i++) {
        split(rows[i], f, "\t")
        printf "| %s | `%s -> %s` | %s |\n", f[1], label(f[2]), label(f[3]), code(f[4])
      }
      if (n > max) printf "\n_%d more in %s._\n", n - max, full
      printf "\n"
    }
    $5 == "regression" { regressions[++nr] = $0 }
    $5 == "improvement" { improvements[++ni] = $0 }
    $5 == "flaky" { flaky[++nk] = $0 }
    END {
      printf "**%s**\n\n", nr ? "Regression detected" : "No regressions"
      if (nr + ni + nk == 0) {
        printf "No query compatibility changes detected.\n\n"
      } else {
        printf "%s, %s, %s\n\n", count(nr, "regression", "regressions"), count(ni, "improvement", "improvements"),
          count(nk, "flaky case", "flaky cases")
        printf "<details>\n<summary><strong>Show changed queries</strong></summary>\n\n"
        section("Regressions", regressions, nr)
        section("Improvements", improvements, ni)
        section("Flaky", flaky, nk)
        printf "</details>\n\n"
      }
      printf "**Compared** `%s` -> `%s`  \n", substr(base, 1, 10), substr(revision, 1, 10)
    }
  ' "$changes_file"
}

count_category() {
  local category="$1"
  local changes_file="$2"
  awk -F $'\t' -v category="$category" '$5 == category { n++ } END { print n + 0 }' "$changes_file"
}

# A subshell keeps main's locals available to the EXIT trap on errexit as well as explicit exit.
main() (
  local control_dir="" release arch base revision control_summary test_summary regressions
  local style=success status=0
  configure
  trap 'cleanup "$control_dir"' EXIT

  rm -rf -- "$WORK_DIR" "$OUTPUT_DIR"
  mkdir -p -- "$WORK_DIR" "$OUTPUT_DIR"

  # Show up on the build as soon as the comparison starts. The final
  # report reuses the same annotation context and comment key, so it
  # replaces this note instead of adding a second one.
  annotate info "$(printf '**Promcheck running**\n\nChecking Prometheus/Elasticsearch mismatches on the merge base and this PR with promcheck v%s. The verdict replaces this note when both runs finish.\n\n' "$PROMCHECK_VER" | report)"

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
  outcomes "$CONTROL_LOG" > "$CONTROL_OUTCOMES" || not_compared "Could not parse control outcomes"
  outcomes "$TEST_LOG" > "$TEST_OUTCOMES" || not_compared "Could not parse test outcomes"
  changes "$CONTROL_OUTCOMES" "$TEST_OUTCOMES" > "$CHANGES_FILE"
  render_changes "$CHANGES_FILE" "$base" "$revision" | report > "$REPORT_FILE"

  regressions=$(count_category regression "$CHANGES_FILE")
  if (( regressions > 0 )); then
    style=error
    status=1
  fi
  annotate "$style" "$(< "$REPORT_FILE")"

  log "Merge base: $control_summary"
  log "PR: $test_summary"
  log "Regressions: $regressions"
  log "Report: $REPORT_FILE"
  exit "$status"
)

main "$@"
