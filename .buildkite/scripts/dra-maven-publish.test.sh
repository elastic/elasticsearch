#!/bin/bash

# Regression tests for dra-maven-publish.sh retry and javadoc upload behavior.
# Runs the real script against a stub `aws` command; no AWS access required.
#
#   .buildkite/scripts/dra-maven-publish.test.sh

set -uo pipefail

SCRIPT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/dra-maven-publish.sh"
TMP="$(mktemp -d -t dra-maven-publish-test.XXXXXX)"
trap 'rm -rf "$TMP"' EXIT

failures=0

fail() { echo "FAIL: $*" >&2; failures=$((failures + 1)); }

# Stub behaviour is driven by env vars:
#   STUB_CP_FAILS       number of leading `s3 cp` calls that fail (exit 7)
#   STUB_CP_FAILS_ALL   if set, every `s3 cp` fails with exit 9
#   STUB_CONFIGURE_FAIL if set, `configure set` fails
# Every invocation is appended to $STUB_LOG.
setup_case() {
  CASE="$TMP/$1"
  mkdir -p "$CASE/bin" "$CASE/maven/org/example/1.0"
  echo jar > "$CASE/maven/org/example/1.0/example-1.0.jar"
  (cd "$CASE" && mkdir -p jd && echo '<html/>' > jd/index-all.html && zip -q -r maven/org/example/1.0/example-1.0-javadoc.jar jd)
  export STUB_LOG="$CASE/aws.log" STUB_STATE="$CASE/state"
  : > "$STUB_LOG"; echo 0 > "$STUB_STATE"
  cat > "$CASE/bin/aws" <<'STUB'
#!/bin/bash
echo "aws $* [AWS_CONFIG_FILE=${AWS_CONFIG_FILE:-}]" >> "$STUB_LOG"
if [[ "$1 $2" == "configure set" ]]; then
  [[ -n "${STUB_CONFIGURE_FAIL:-}" ]] && exit 5
  echo "$3=$4 profile=$6" >> "$AWS_CONFIG_FILE"
  exit 0
fi
if [[ "$1 $2" == "s3 cp" ]]; then
  [[ -n "${STUB_CP_FAILS_ALL:-}" ]] && exit 9
  n=$(( $(cat "$STUB_STATE") + 1 )); echo "$n" > "$STUB_STATE"
  (( n <= ${STUB_CP_FAILS:-0} )) && exit 7
  exit 0
fi
exit 0
STUB
  chmod +x "$CASE/bin/aws"
}

run_publish() {
  PATH="$CASE/bin:$PATH" DRA_PUBLISH_RETRY_DELAYS="0 0 0" MAVEN_AGGREGATION_DIR="$CASE/maven" \
    HOME="$CASE/home" "$SCRIPT" > "$CASE/out" 2> "$CASE/err"
}

count_cp() { grep -c '^aws s3 cp' "$STUB_LOG"; }

# 1. Transient failure is retried and the script succeeds.
setup_case transient
STUB_CP_FAILS=2 run_publish; rc=$?
(( rc == 0 )) || fail "transient: expected success, got $rc"
(( $(count_cp) == 4 )) || fail "transient: expected 4 cp calls (2 failed maven attempts+retry, then javadoc), got $(count_cp)"

# 2. Four failures preserve the last exit code and the script does not report success.
setup_case exhausted
STUB_CP_FAILS_ALL=1 run_publish; rc=$?
(( rc == 9 )) || fail "exhausted: expected exit 9, got $rc"
(( $(count_cp) == 4 )) || fail "exhausted: expected 4 cp attempts, got $(count_cp)"
grep -q 'Published to:' "$CASE/out" && fail "exhausted: must not print 'Published to:'"

# 3. A failed config write must not start the javadoc upload.
setup_case configure-fails
STUB_CONFIGURE_FAIL=1 run_publish; rc=$?
(( rc != 0 )) || fail "configure-fails: expected failure"
(( $(count_cp) == 1 )) || fail "configure-fails: only the maven upload should run, got $(count_cp) cp calls"

# 4. Selected profile receives the threshold and the caller's config is preserved.
setup_case profile
mkdir -p "$CASE/home/.aws"
printf '[profile pub]\nrole_arn=arn:aws:iam::1:role/x\n' > "$CASE/home/.aws/config"
cp "$CASE/home/.aws/config" "$CASE/config.before"
AWS_PROFILE=pub run_publish; rc=$?
(( rc == 0 )) || fail "profile: expected success, got $rc"
grep -q 'configure set s3.multipart_threshold 5GB --profile pub' "$STUB_LOG" || fail "profile: threshold not set for profile 'pub'"
cmp -s "$CASE/home/.aws/config" "$CASE/config.before" || fail "profile: caller config was modified"

if (( failures > 0 )); then
  echo "$failures failure(s)" >&2
  exit 1
fi
echo "All dra-maven-publish tests passed"
