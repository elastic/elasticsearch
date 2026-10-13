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
  // True when the never-fail wrapper found a JVM heap dump (`*/build/heapdump/*.hprof`)
  // after the run - the signal for a JVM-heap `OutOfMemoryError` in a test worker,
  // which exits via Gradle with rc=1 (not the SIGKILL rc=137 the kernel OOM-killer
  // produces). Without this the two OOM shapes are indistinguishable from a plain
  // build failure, since the analyze step does not read the job log (a choice we
  // may revisit).
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
  // True when the step hit a wall-clock timeout (rc 124, or rc 137 kill-after).
  // Orthogonal to `outcome`: a job can be `flaky_detected` AND `timedOut`, which
  // distinguishes "timed out but flakiness already proven" from a clean
  // `timeout` where no run failed.
  timedOut: boolean;
  // "oom_killed" = kernel OOM-killer SIGKILL (rc 137 + short run). "oom" =
  // JVM-heap OutOfMemoryError detected from a heap-dump file (rc != 0,
  // non-SIGKILL). Every other infra subtype would need the job log, which we
  // currently choose not to read, so it is left unset here.
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
