#!/bin/bash
#
# Validates that the Elasticsearch build is fully cacheable when it is run from two
# different checkout locations, using Gradle's "Develocity Build Validation Scripts"
# (experiment 03, https://github.com/gradle/develocity-build-validation-scripts).
#
# The experiment runs `clean <tasks>` twice, from two copies of the repository in
# different directories, sharing one empty local build cache. Every cacheable task
# executed by the second build is a cache miss that every developer and CI agent pays
# for, usually because a task input contains an absolute path, a timestamp or other
# non-deterministic content.
#
# The outcome is published as a Buildkite annotation that explains what was checked,
# whether a failure is a cacheability regression or an infrastructure problem, and
# which tasks to fix.

set -euo pipefail

readonly VALIDATION_SCRIPTS_VERSION=2.9.0
readonly VALIDATION_SCRIPTS_DIR="develocity-gradle-build-validation"
readonly DEVELOCITY_SERVER="https://gradle-enterprise.elastic.co"
readonly GIT_REPO="https://github.com/elastic/elasticsearch.git"
readonly GRADLE_TASKS="${BUILD_CACHE_VALIDATION_TASKS:-precommit javadoc}"
# Each build runs in a fresh JVM. Reusing the daemon for the second build keeps the
# state of the first build around and, combined with the file fingerprint capturing
# the experiment enables, exhausted the heap and made the job hang until it timed out.
readonly GRADLE_ARGS="${BUILD_CACHE_VALIDATION_GRADLE_ARGS:---no-daemon}"
# Stop the experiment ourselves before the Buildkite step timeout so we can still
# publish a meaningful annotation. Keep this below `timeout_in_minutes` of the step.
readonly EXPERIMENT_TIMEOUT_MINUTES="${BUILD_CACHE_VALIDATION_TIMEOUT_MINUTES:-105}"
readonly ANNOTATION_CONTEXT="ctx-validation-summary"
readonly RAW_OUTPUT_FILE="build-cache-validation.raw.log"
readonly OUTPUT_FILE="build-cache-validation.log"
readonly COMMIT="${BUILDKITE_COMMIT:-}"
readonly BRANCH="${BUILDKITE_BRANCH:-main}"

# Exit codes of the build validation scripts, see lib/scripts/exit-code.sh
readonly EXIT_SUCCESS=0
readonly EXIT_INVALID_INPUT=1
readonly EXIT_BUILD_FAILED=2
readonly EXIT_NOT_FULLY_CACHEABLE=3
readonly EXIT_UNEXPECTED_ERROR=100
# Exit code of coreutils `timeout`
readonly EXIT_TIMEOUT=124
# 128 + SIGTERM, used when Buildkite cancels the job or the step timeout is reached
readonly EXIT_CANCELLED=143

experiment_exit_code=""
annotation_published=false

log() {
  echo "--- $*"
}

strip_ansi() {
  sed 's/\x1b\[[0-9;]*m//g'
}

trim() {
  local value="$1"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s' "$value"
}

# Prints the value of a "Label:   value" line of the experiment summary.
summary_value() {
  local label="$1"
  local line
  line=$(grep -m1 "^${label}:" "$OUTPUT_FILE" 2>/dev/null || true)
  trim "${line#*:}"
}

# Prints the lines of a section of the experiment summary, e.g. "Performance Characteristics".
summary_section() {
  local title="$1"
  awk -v title="$title" '
    $0 == title { in_section = 1; getline; next }   # skip the "-----" underline
    in_section && /^[[:space:]]*$/ { exit }
    in_section { print }
  ' "$OUTPUT_FILE" 2>/dev/null || true
}

scan_id() {
  local url="$1"
  printf '%s' "${url##*/}"
}

reproduce_command() {
  local commit_arg="-b ${BRANCH}"
  if [[ -n "$COMMIT" ]]; then
    commit_arg="-c ${COMMIT}"
  fi
  printf '%s' "./${VALIDATION_SCRIPTS_DIR}/03-validate-local-build-caching-different-locations.sh -r ${GIT_REPO} ${commit_arg} -s ${DEVELOCITY_SERVER} -t \"${GRADLE_TASKS}\" -a \"${GRADLE_ARGS}\""
}

# Lists the cacheable tasks the second build executed instead of loading them from the
# local build cache, together with a hint whether the cache key changed between both
# builds (unstable inputs) or not (outputs were not stored/reused for another reason).
# Requires the Develocity API; prints nothing if the data cannot be fetched.
executed_cacheable_tasks_markdown() {
  local first_scan="$1"
  local second_scan="$2"
  local api_key="${DEVELOCITY_API_ACCESS_KEY:-${GRADLE_ENTERPRISE_ACCESS_KEY:-}}"
  api_key="${api_key#*=}"
  if [[ -z "$api_key" || -z "$first_scan" || -z "$second_scan" ]] || ! command -v jq > /dev/null 2>&1; then
    return 0
  fi

  local tmp_dir
  tmp_dir=$(mktemp -d)
  local scan
  for scan in "$first_scan" "$second_scan"; do
    if ! curl -fsSL --retry 3 --retry-delay 5 --max-time 120 \
      -H "Authorization: Bearer ${api_key}" \
      "${DEVELOCITY_SERVER}/api/builds/${scan}/gradle-build-cache-performance" > "${tmp_dir}/${scan}.json"; then
      echo "Unable to fetch build cache performance data for build scan ${scan}" >&2
      rm -rf "$tmp_dir"
      return 0
    fi
  done

  jq -r --slurpfile first_build "${tmp_dir}/${first_scan}.json" '
    ($first_build[0].taskExecution // [] | map({ key: .taskPath, value: .cacheKey }) | from_entries) as $first_keys
    | [.taskExecution[]? | select(.avoidanceOutcome == "executed_cacheable")]
    | sort_by(-(.duration // 0))
    | if length == 0 then empty else
        "| Task | Type | Duration | Likely cause |",
        "| --- | --- | ---: | --- |",
        (.[] |
          ($first_keys[.taskPath]) as $first_key
          | (if $first_key == null then "Task did not run in the first build"
             elif $first_key != .cacheKey then "**Cache key changed** between the two builds: an input is location dependent or not deterministic. Compare the task inputs."
             elif .cacheArtifactRejectedReason != null then "Cache entry was rejected (`\(.cacheArtifactRejectedReason)`)"
             else "Same cache key, but the output was not reused from the cache (e.g. not stored by the first build)"
             end) as $cause
          | "| `\(.taskPath)` | `\(.taskType // "?" | split(".") | last)` | \(((.duration // 0) / 1000 * 10 | floor) / 10)s | \($cause) |"
        )
      end
  ' "${tmp_dir}/${second_scan}.json" || true
  rm -rf "$tmp_dir"
}

publish_annotation() {
  if [[ "$annotation_published" == "true" ]]; then
    return 0
  fi
  annotation_published=true
  # Rendering the annotation is best effort and must never mask the experiment result.
  set +e

  if [[ -f "$RAW_OUTPUT_FILE" ]]; then
    strip_ansi < "$RAW_OUTPUT_FILE" > "$OUTPUT_FILE"
  fi

  local exit_code="${experiment_exit_code:-$EXIT_UNEXPECTED_ERROR}"
  local style headline explanation
  case "$exit_code" in
    "$EXIT_SUCCESS")
      style="success"
      headline=":white_check_mark: The build is fully cacheable when run from different locations."
      explanation="Every cacheable task of the second build was loaded from the local build cache. Nothing to do."
      ;;
    "$EXIT_NOT_FULLY_CACHEABLE")
      style="error"
      headline=":x: Cacheability regression: the second build re-executed cacheable tasks."
      explanation="The cacheable tasks listed below (and in the task inputs comparison) should have been loaded from the build cache populated by the first build. Each of them is a cache miss for every developer and CI run. Open the task inputs comparison, find the input that differs between both builds (typically an absolute path, a timestamp, or non-deterministic file content) and normalize it, e.g. with \`@PathSensitive(PathSensitivity.RELATIVE)\`, \`@Classpath\`/\`@CompileClasspath\`, or by excluding volatile values from the task inputs."
      ;;
    "$EXIT_BUILD_FAILED")
      style="error"
      headline=":x: One of the two Gradle builds failed, so cacheability could not be validated."
      explanation="This is usually a genuine build failure on \`${BRANCH}\` (check the build scans below). Fix the build first; the cacheability result is meaningless until both builds succeed."
      ;;
    "$EXIT_TIMEOUT")
      style="warning"
      headline=":hourglass: Inconclusive: the experiment did not finish within ${EXPERIMENT_TIMEOUT_MINUTES} minutes."
      explanation="This is an infrastructure/performance problem, not a cacheability regression. Check the job log for \`OutOfMemoryError\` or long pauses between tasks."
      ;;
    "$EXIT_CANCELLED")
      style="warning"
      headline=":hourglass: Inconclusive: the job was cancelled or hit the Buildkite step timeout."
      explanation="This is not a cacheability regression. If the step timed out, check the job log for \`OutOfMemoryError\` or long pauses between tasks and consider lowering \`BUILD_CACHE_VALIDATION_TIMEOUT_MINUTES\` below the step timeout."
      ;;
    "$EXIT_INVALID_INPUT")
      style="warning"
      headline=":warning: Inconclusive: the experiment was invoked with invalid arguments."
      explanation="Check \`.buildkite/scripts/gradle-build-cache-validation.sh\`; the validation scripts version may have changed its command line interface."
      ;;
    *)
      style="warning"
      headline=":warning: Inconclusive: the experiment aborted with an unexpected error (exit code ${exit_code})."
      explanation="This is an infrastructure problem (e.g. build scan publishing or Develocity API access failed), not a cacheability regression. See the job log for details."
      ;;
  esac

  local oom_hint=""
  if grep -q "java.lang.OutOfMemoryError" "$OUTPUT_FILE" 2>/dev/null; then
    oom_hint=$'\n\n'":boom: A Gradle JVM ran out of heap (\`java.lang.OutOfMemoryError\`). Once the heap is exhausted the build slows to a crawl, which typically ends in a timeout."
    if [[ "$style" == "success" ]]; then
      style="warning"
    fi
  fi

  local first_scan_url second_scan_url
  first_scan_url=$(summary_value "Build scan first build")
  second_scan_url=$(summary_value "Build scan second build")
  if [[ -z "$first_scan_url" ]]; then
    first_scan_url=$(grep -m1 -o "${DEVELOCITY_SERVER}/s/[a-z0-9]*" "$OUTPUT_FILE" 2>/dev/null || true)
  fi

  local executed_tasks=""
  if [[ "$exit_code" == "$EXIT_NOT_FULLY_CACHEABLE" && -n "$first_scan_url" && -n "$second_scan_url" ]]; then
    executed_tasks=$(executed_cacheable_tasks_markdown "$(scan_id "$first_scan_url")" "$(scan_id "$second_scan_url")")
  fi

  local performance quick_links
  performance=$(summary_section "Performance Characteristics" | grep ':' | while IFS= read -r line; do
    printf '| %s | %s |\n' "$(trim "${line%%:*}")" "$(trim "${line#*:}")"
  done)
  quick_links=$(summary_section "Investigation Quick Links" | grep -E 'https?://' | while IFS= read -r line; do
    printf -- '- [%s](%s)\n' "$(trim "${line%%:*}")" "$(trim "${line#*:}")"
  done)

  {
    echo "#### Gradle build cache validation (local cache, different locations)"
    echo
    echo "**${headline}**"
    echo
    echo "$explanation$oom_hint"
    echo
    if [[ -n "$executed_tasks" ]]; then
      echo "**Cacheable tasks executed by the second build**"
      echo
      echo "$executed_tasks"
      echo
    fi
    if [[ -n "$first_scan_url" && -n "$second_scan_url" ]]; then
      echo "Investigate: [task inputs comparison](${DEVELOCITY_SERVER}/c/$(scan_id "$first_scan_url")/$(scan_id "$second_scan_url")/task-inputs?cacheability=cacheable,overlapping-outputs,validation-failure) · [first build scan](${first_scan_url}) · [second build scan](${second_scan_url})"
      echo
    elif [[ -n "$first_scan_url" ]]; then
      echo "First build scan: ${first_scan_url} (the second build did not publish a build scan)"
      echo
    fi
    echo "<details><summary>What does this job check?</summary>"
    echo
    echo "It runs \`./gradlew clean ${GRADLE_TASKS}\` twice on commit \`${COMMIT:-$BRANCH}\`, from two copies of the repository in different directories, sharing one initially empty local build cache. A build is *fully cacheable* if the second build loads every cacheable task from the cache. Tasks that are re-executed have inputs that depend on the checkout location or change between runs, which defeats the remote build cache for developers and CI."
    echo
    echo "Reproduce locally (requires a Develocity access key) after downloading the [build validation scripts ${VALIDATION_SCRIPTS_VERSION}](https://github.com/gradle/develocity-build-validation-scripts/releases/tag/v${VALIDATION_SCRIPTS_VERSION}):"
    echo
    echo '```'
    reproduce_command
    echo
    echo '```'
    echo
    echo "Documentation: [Gradle build validation scripts](https://github.com/gradle/develocity-build-validation-scripts/blob/main/Gradle.md)"
    echo "</details>"
    if [[ -n "$performance" ]]; then
      echo
      echo "<details><summary>Performance characteristics</summary>"
      echo
      echo "| Metric | Value |"
      echo "| --- | --- |"
      echo "$performance"
      echo "</details>"
    fi
    if [[ -n "$quick_links" ]]; then
      echo
      echo "<details><summary>Investigation quick links</summary>"
      echo
      echo "$quick_links"
      echo "</details>"
    fi
  } > build-cache-validation-annotation.md

  if command -v buildkite-agent > /dev/null 2>&1; then
    buildkite-agent annotate --context "$ANNOTATION_CONTEXT" --style "$style" < build-cache-validation-annotation.md \
      || echo "Failed to publish the build cache validation annotation"
    buildkite-agent artifact upload "$OUTPUT_FILE" || echo "Failed to upload ${OUTPUT_FILE}"
  else
    cat build-cache-validation-annotation.md
  fi
}

# Publish an annotation whatever happens, including the step being cancelled.
on_exit() {
  publish_annotation || echo "Failed to render the build cache validation annotation"
}
trap on_exit EXIT
trap 'experiment_exit_code=${experiment_exit_code:-$EXIT_CANCELLED}; exit "$EXIT_CANCELLED"' TERM INT

log "Downloading Develocity build validation scripts ${VALIDATION_SCRIPTS_VERSION}"
GRADLE_ENTERPRISE_ACCESS_KEY=$(vault kv get -field=value secret/ci/elastic-elasticsearch/gradle-enterprise-api-key)
export GRADLE_ENTERPRISE_ACCESS_KEY
validation_scripts_zip="develocity-gradle-build-validation-${VALIDATION_SCRIPTS_VERSION}.zip"
curl -fsSL --retry 3 --retry-delay 10 -o "$validation_scripts_zip" \
  "https://github.com/gradle/develocity-build-validation-scripts/releases/download/v${VALIDATION_SCRIPTS_VERSION}/${validation_scripts_zip}"
rm -rf "$VALIDATION_SCRIPTS_DIR"
unzip -q -o "$validation_scripts_zip"
# Be more patient than the default when waiting for build scans of this large build
# to become available through the Develocity API.
echo "max.wait.time=PT10M" >> "${VALIDATION_SCRIPTS_DIR}/network.settings"

commit_args=(-b "$BRANCH")
if [[ -n "$COMMIT" ]]; then
  # Validate exactly the commit this Buildkite build was created for.
  commit_args=(-c "$COMMIT")
fi

log "Running build cache validation experiment for '${GRADLE_TASKS}'"
set +e
timeout --signal=TERM --kill-after=2m "${EXPERIMENT_TIMEOUT_MINUTES}m" \
  "./${VALIDATION_SCRIPTS_DIR}/03-validate-local-build-caching-different-locations.sh" \
    -r "$GIT_REPO" \
    "${commit_args[@]}" \
    --develocity-server "$DEVELOCITY_SERVER" \
    -t "$GRADLE_TASKS" \
    -a "$GRADLE_ARGS" \
    --fail-if-not-fully-cacheable 2>&1 | tee "$RAW_OUTPUT_FILE"
experiment_exit_code=${PIPESTATUS[0]}
set -e
# `timeout` reports 137 if the experiment had to be killed after ignoring SIGTERM
if [[ "$experiment_exit_code" == "137" ]]; then
  experiment_exit_code=$EXIT_TIMEOUT
fi

case "$experiment_exit_code" in
  "$EXIT_SUCCESS") echo "Experiment completed successfully: the build is fully cacheable" ;;
  "$EXIT_INVALID_INPUT") echo "An invalid input was provided while attempting to run the experiment" ;;
  "$EXIT_BUILD_FAILED") echo "One of the builds that is part of the experiment failed" ;;
  "$EXIT_NOT_FULLY_CACHEABLE") echo "The build was not fully cacheable for the given task graph" ;;
  "$EXIT_TIMEOUT") echo "The experiment did not finish within ${EXPERIMENT_TIMEOUT_MINUTES} minutes" ;;
  "$EXIT_CANCELLED") echo "The experiment was cancelled" ;;
  "$EXIT_UNEXPECTED_ERROR") echo "An unclassified, fatal error happened while running the experiment" ;;
  *) echo "The experiment failed with unexpected exit code ${experiment_exit_code}" ;;
esac

exit "$experiment_exit_code"
