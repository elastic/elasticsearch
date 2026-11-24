#!/bin/bash

# Regression tests for dra-maven-publish.sh retry and javadoc upload behavior.
# Runs the real script against a stub `aws` command; no AWS access required.
# Also run by dra-maven-publish.test.ts as part of the `.buildkite` Vitest task.
#
#   .buildkite/scripts/dra-maven-publish.test.sh

set -uo pipefail

SCRIPT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/dra-maven-publish.sh"
TMP="$(mktemp -d -t dra-maven-publish-test.XXXXXX)"
trap 'chmod -R u+w "$TMP" 2>/dev/null; rm -rf "$TMP"' EXIT

failures=0

fail() { echo "FAIL: $*" >&2; failures=$((failures + 1)); }

# Stub behaviour is driven by env vars:
#   STUB_CP_FAILS       number of leading `s3 cp` calls that fail (exit 7)
#   STUB_CP_FAILS_ALL   if set, every `s3 cp` fails with exit 9
#   STUB_JAVADOC_FAILS  if set, every `s3 cp` to a javadoc/ destination fails with exit 11
#   STUB_CONFIGURE_FAIL if set, `configure set` fails
# Every invocation is appended to $STUB_LOG. `configure set` appends
# "<key>=<value>" to the file named by AWS_CONFIG_FILE (like the real CLI would),
# and every `s3 cp` records the config contents it would have seen.
setup_case() {
  CASE="$TMP/$1"
  mkdir -p "$CASE/bin" "$CASE/maven/org/example/1.0" "$CASE/home"
  echo jar > "$CASE/maven/org/example/1.0/example-1.0.jar"
  (cd "$CASE" && mkdir -p jd && echo '<html/>' > jd/index-all.html && zip -q -r maven/org/example/1.0/example-1.0-javadoc.jar jd)
  export STUB_LOG="$CASE/aws.log" STUB_STATE="$CASE/state"
  : > "$STUB_LOG"; echo 0 > "$STUB_STATE"
  cat > "$CASE/bin/aws" <<'STUB'
#!/bin/bash
echo "aws $* [AWS_CONFIG_FILE=${AWS_CONFIG_FILE:-}]" >> "$STUB_LOG"
if [[ "$1 $2" == "configure set" ]]; then
  [[ -n "${STUB_CONFIGURE_FAIL:-}" ]] && exit 5
  echo "$3=$4" >> "$AWS_CONFIG_FILE" || exit 6
  exit 0
fi
if [[ "$1 $2" == "s3 cp" ]]; then
  dest=""
  for a in "$@"; do [[ "$a" == s3://* ]] && dest="$a"; done
  echo "config-at-cp[$dest]: $(tr '\n' ' ' < "${AWS_CONFIG_FILE:-/dev/null}" 2>/dev/null)" >> "$STUB_LOG"
  [[ -n "${STUB_CP_FAILS_ALL:-}" ]] && exit 9
  [[ -n "${STUB_JAVADOC_FAILS:-}" && "$dest" == */javadoc/ ]] && exit 11
  n=$(( $(cat "$STUB_STATE") + 1 )); echo "$n" > "$STUB_STATE"
  (( n <= ${STUB_CP_FAILS:-0} )) && exit 7
  exit 0
fi
exit 0
STUB
  chmod +x "$CASE/bin/aws"
}

run_publish() {
  PATH="$CASE/bin:$PATH" DRA_PUBLISH_RETRY_DELAYS="${DRA_PUBLISH_RETRY_DELAYS:-0 0 0}" \
    MAVEN_AGGREGATION_DIR="$CASE/maven" HOME="$CASE/home" "$SCRIPT" > "$CASE/out" 2> "$CASE/err"
}

count_cp() { grep -c '^aws s3 cp.* s3://[^ ]*'"$1" "$STUB_LOG"; }

# 1. Transient failure is retried and the script succeeds.
setup_case transient
STUB_CP_FAILS=2 run_publish; rc=$?
(( rc == 0 )) || fail "transient: expected success, got $rc"
(( $(count_cp maven/) == 3 )) || fail "transient: expected 3 maven attempts, got $(count_cp maven/)"
(( $(count_cp javadoc/) == 1 )) || fail "transient: expected 1 javadoc attempt, got $(count_cp javadoc/)"

# 2. Four maven failures preserve the last exit code and the script does not report success.
setup_case exhausted
STUB_CP_FAILS_ALL=1 run_publish; rc=$?
(( rc == 9 )) || fail "exhausted: expected exit 9, got $rc"
(( $(count_cp maven/) == 4 )) || fail "exhausted: expected 4 maven attempts, got $(count_cp maven/)"
(( $(count_cp javadoc/) == 0 )) || fail "exhausted: javadoc upload must not start"
grep -q 'Published to:' "$CASE/out" && fail "exhausted: must not print 'Published to:'"

# 3. Maven succeeds and only the javadoc upload fails: four attempts, last exit code, no success message.
setup_case javadoc-exhausted
STUB_JAVADOC_FAILS=1 run_publish; rc=$?
(( rc == 11 )) || fail "javadoc-exhausted: expected exit 11, got $rc"
(( $(count_cp maven/) == 1 )) || fail "javadoc-exhausted: expected 1 maven attempt, got $(count_cp maven/)"
(( $(count_cp javadoc/) == 4 )) || fail "javadoc-exhausted: expected 4 javadoc attempts, got $(count_cp javadoc/)"
grep -q 'Published to:' "$CASE/out" && fail "javadoc-exhausted: must not print 'Published to:'"

# 4. A failed config write must not start the javadoc upload.
setup_case configure-fails
STUB_CONFIGURE_FAIL=1 run_publish; rc=$?
(( rc != 0 )) || fail "configure-fails: expected failure"
(( $(count_cp maven/) == 1 )) || fail "configure-fails: expected 1 maven attempt, got $(count_cp maven/)"
(( $(count_cp javadoc/) == 0 )) || fail "configure-fails: javadoc upload must not start"

# 5. The javadoc upload sees the caller's profile settings plus the single-part
#    settings, using a scratch copy; the caller's (read-only) config is untouched.
setup_case profile
CONFIG="$CASE/caller-config"
printf '[profile pub]\nrole_arn=arn:aws:iam::1:role/x\n' > "$CONFIG"
chmod 0444 "$CONFIG"
cp "$CONFIG" "$CASE/config.before"
AWS_CONFIG_FILE="$CONFIG" AWS_PROFILE=pub run_publish; rc=$?
(( rc == 0 )) || fail "profile: expected success, got $rc ($(cat "$CASE/err"))"
grep -q 'configure set s3.multipart_threshold 5GB --profile pub' "$STUB_LOG" || fail "profile: threshold not set for profile 'pub'"
grep -q 'configure set s3.preferred_transfer_client classic --profile pub' "$STUB_LOG" || fail "profile: classic transfer client not selected"
grep -q 'config-at-cp\[s3://[^]]*javadoc/\].*role_arn=arn:aws:iam::1:role/x.*s3.multipart_threshold=5GB' "$STUB_LOG" \
  || fail "profile: javadoc upload did not see original settings plus threshold"
grep -q 'cp .*javadoc/ --profile pub' "$STUB_LOG" || fail "profile: javadoc upload not run with --profile pub"
grep 'javadoc/' "$STUB_LOG" | grep -q "AWS_CONFIG_FILE=$CONFIG\]" && fail "profile: caller config must not be used by the javadoc upload"
cmp -s "$CONFIG" "$CASE/config.before" || fail "profile: caller config was modified"

# 6. AWS_DEFAULT_PROFILE takes precedence over AWS_PROFILE, as in the AWS CLI.
setup_case default-profile
AWS_DEFAULT_PROFILE=default AWS_PROFILE=pub run_publish; rc=$?
(( rc == 0 )) || fail "default-profile: expected success, got $rc"
grep -q 'configure set s3.multipart_threshold 5GB --profile default' "$STUB_LOG" || fail "default-profile: threshold not set for AWS_DEFAULT_PROFILE"
grep -q -- '--profile pub' "$STUB_LOG" && fail "default-profile: AWS_PROFILE must not be used"

# 7. No profile selected: no --profile is passed, so environment credentials stay in effect.
setup_case no-profile
run_publish; rc=$?
(( rc == 0 )) || fail "no-profile: expected success, got $rc"
grep -q -- '--profile' "$STUB_LOG" && fail "no-profile: --profile must not be passed"

# 8. A short retry-delay override must not abort the script or cut the exit code short.
setup_case short-delays
DRA_PUBLISH_RETRY_DELAYS="0" STUB_CP_FAILS_ALL=1 run_publish; rc=$?
(( rc == 9 )) || fail "short-delays: expected exit 9, got $rc"
(( $(count_cp maven/) == 2 )) || fail "short-delays: expected 2 maven attempts, got $(count_cp maven/)"

if (( failures > 0 )); then
  echo "$failures failure(s)" >&2
  exit 1
fi
echo "All dra-maven-publish tests passed"
