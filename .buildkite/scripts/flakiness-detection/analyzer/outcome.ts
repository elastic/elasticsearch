// Outcome taxonomy for a single flakiness-detection batch job. Shared by the
// analyze step (which classifies every batch job from its rc + JUnit XML) and
// its unit tests.

export type FlakinessOutcome = "clean_pass" | "flaky_detected" | "timeout" | "hang" | "infra_fail";

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
}

export interface DerivedOutcome {
  outcome: FlakinessOutcome;
  // True when the step hit a wall-clock timeout (rc 124, or rc 137 kill-after).
  // Orthogonal to `outcome`: a job can be `flaky_detected` AND `timedOut`, which
  // distinguishes "timed out but flakiness already proven" from a clean
  // `timeout` where no run failed.
  timedOut: boolean;
  // Only ever "oom_killed" (rc 137 + short run). Every other infra subtype would
  // need the job log, which CI cannot read, so it is left unset here.
  infraSubtype?: "oom_killed";
}

/**
 * Classify a batch job's outcome from its return code, duration and JUnit
 * counts, in priority order (see README "Outcome taxonomy"). A proven flaky
 * failure outranks everything, including a concurrent timeout, because that is
 * what matters for the false-positive metric.
 */
export function deriveOutcome({ rc, durationSec, realFailures, totalCases, timeoutThresholdSec }: OutcomeInput): DerivedOutcome {
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
    return { outcome: "infra_fail", timedOut: false };
  }
  return totalCases === 0 ? { outcome: "hang", timedOut: false } : { outcome: "clean_pass", timedOut: false };
}
