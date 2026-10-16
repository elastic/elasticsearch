// Outcome taxonomy for a single flakiness-detection batch job. Shared by the
// analyze step (which classifies every batch job from its rc + JUnit XML) and
// its unit tests.

export type FlakinessOutcome = "clean_pass" | "flaky_detected" | "timeout" | "hang" | "infra_fail" | "not_applicable" | "build_failed";

export interface OutcomeInput {
  // Wrapped command's return code (124 = our SIGTERM timeout, 137 = SIGKILL).
  rc: number;
  // Wall-clock seconds the wrapped command ran (captured by the wrapper).
  durationSec: number;
  // Real (non-suite-timeout) failing test cases recorded in this job's XML.
  realFailures: number;
  // Total <testcase> elements recorded in this job's XML.
  totalCases: number;
  // rc 137 at/after this many seconds is treated as our own `timeout`
  // kill-after; earlier is the kernel OOM-killer. The caller derives this from
  // the configured pipeline timeout (inner `timeout` deadline = batch timeout
  // minus the never-fail grace) so it tracks config changes; see
  // entrypoints/analyze.ts.
  timeoutThresholdSec: number;
  // True when the never-fail wrapper found a heap dump (`*/build/heapdump/*.hprof`) after
  // the run, meaning a test JVM ran out of heap. That JVM either fails the Gradle run
  // (rc 1, unlike the rc 137 of a kernel OOM-kill) or gets stuck until the wrapper's timeout.
  // Without the dump, both look like a plain failure or a slow run, because the analyze
  // step does not read the job log. Only test JVMs write dumps there; a testclusters
  // node writes to its logs dir, so its OOMs are not detected.
  oomDetected?: boolean;
  // True when every Test task this batch asked for came back SKIPPED in gradle-runner's
  // task-status.json. Gradle reports a task rejected by `onlyIf` (bwc's `bwc_tests_enabled`,
  // the distro architecture check) and a task with no source that way: zero tests, exit 0.
  // Without this signal that is indistinguishable from a hang, so a target the resolver
  // could not have known was unrunnable reads as a flakiness-pipeline defect.
  taskSkipped?: boolean;
}

export interface DerivedOutcome {
  outcome: FlakinessOutcome;
  // True when the job hit the wrapper's wall-clock timeout (rc 124, or rc 137 from the kill-after).
  // Set independently of `outcome`, which can be:
  // - `timeout`: no test failed, so this is a false positive.
  // - `flaky_detected`: a test failed before the timeout, so flakiness is already proven.
  // - `infra_fail`/`oom`: a test JVM ran out of heap and got stuck until the timeout.
  timedOut: boolean;
  // "oom_killed": the kernel OOM-killer sent SIGKILL (rc 137 on a short run).
  // "oom": a heap dump shows a test JVM ran out of heap; the run either failed
  // (rc != 0) or got stuck until the wrapper's timeout (then `timedOut` is true).
  // Other infra causes would need the job log, which we do not read, so they get no subtype.
  infraSubtype?: "oom_killed" | "oom";
}

/**
 * Classify a batch job's outcome from its return code, duration and JUnit
 * counts, in priority order (see README "Outcome taxonomy"). A proven flaky
 * failure outranks everything, including a concurrent timeout, because that is
 * what matters for the false-positive metric.
 */
export function deriveOutcome({
  rc,
  durationSec,
  realFailures,
  totalCases,
  timeoutThresholdSec,
  oomDetected,
  taskSkipped,
}: OutcomeInput): DerivedOutcome {
  const threshold = timeoutThresholdSec;
  const timedOut = rc === 124 || (rc === 137 && durationSec >= threshold);

  if (realFailures > 0) {
    return { outcome: "flaky_detected", timedOut };
  }
  if (timedOut && oomDetected) {
    // A test JVM that runs out of heap often gets stuck instead of exiting, so the job only ends at the
    // wrapper's timeout. The heap dump shows the OOM is the root cause; `timedOut` keeps the timeout visible.
    return { outcome: "infra_fail", timedOut: true, infraSubtype: "oom" };
  }
  if (rc === 124) {
    return { outcome: "timeout", timedOut: true };
  }
  if (rc === 137) {
    return durationSec >= threshold
      ? { outcome: "timeout", timedOut: true }
      : { outcome: "infra_fail", timedOut: false, infraSubtype: "oom_killed" };
  }
  if (rc !== 0) {
    // A JVM-heap OutOfMemoryError exits here (rc=1, not SIGKILL); the heap-dump
    // signal lets us subtype it as `oom` instead of a generic infra_fail.
    return oomDetected
      ? { outcome: "infra_fail", timedOut: false, infraSubtype: "oom" }
      : { outcome: "infra_fail", timedOut: false };
  }
  if (totalCases !== 0) {
    return { outcome: "clean_pass", timedOut: false };
  }
  // Zero tests on a clean exit. If Gradle itself reports that every task we asked for was SKIPPED, the
  // target was never runnable in this environment - an `onlyIf` we cannot introspect at resolve time - so
  // it is `not_applicable`, not a pipeline defect. Anything else with zero tests stays a hang.
  return taskSkipped
    ? { outcome: "not_applicable", timedOut: false }
    : { outcome: "hang", timedOut: false };
}
