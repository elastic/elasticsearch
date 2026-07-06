import { describe, expect, test } from "vitest";

import { deriveOutcome } from "./outcome.ts";

// Inner `timeout` kill-after for the default 60m batch step minus the 2m
// never-fail grace = 58m = 3480s. Production derives this from config (see
// entrypoints/analyze.ts); the tests pin an explicit value.
const THRESHOLD = 3480;

describe("deriveOutcome", () => {
  test("clean pass: rc 0 with cases and no failures", () => {
    expect(deriveOutcome({ rc: 0, durationSec: 30, realFailures: 0, totalCases: 2, timeoutThresholdSec: THRESHOLD })).toEqual({
      outcome: "clean_pass",
      timedOut: false,
    });
  });

  test("flaky: a real failure wins regardless of rc", () => {
    expect(deriveOutcome({ rc: 1, durationSec: 30, realFailures: 1, totalCases: 2, timeoutThresholdSec: THRESHOLD })).toEqual({
      outcome: "flaky_detected",
      timedOut: false,
    });
  });

  test("flaky AND timed out: a real failure keeps flaky but flags timedOut", () => {
    expect(deriveOutcome({ rc: 124, durationSec: 3400, realFailures: 2, totalCases: 50, timeoutThresholdSec: THRESHOLD })).toEqual({
      outcome: "flaky_detected",
      timedOut: true,
    });
  });

  test("rc 124 with no failures is a timeout", () => {
    expect(deriveOutcome({ rc: 124, durationSec: 3400, realFailures: 0, totalCases: 5, timeoutThresholdSec: THRESHOLD })).toEqual({
      outcome: "timeout",
      timedOut: true,
    });
  });

  test("rc 137 at/after the inner timeout is a timeout", () => {
    expect(deriveOutcome({ rc: 137, durationSec: 4000, realFailures: 0, totalCases: 5, timeoutThresholdSec: THRESHOLD })).toEqual({
      outcome: "timeout",
      timedOut: true,
    });
  });

  test("rc 137 with short duration is an OOM-kill", () => {
    expect(deriveOutcome({ rc: 137, durationSec: 20, realFailures: 0, totalCases: 5, timeoutThresholdSec: THRESHOLD })).toEqual({
      outcome: "infra_fail",
      timedOut: false,
      infraSubtype: "oom_killed",
    });
  });

  test("rc 137 boundary: exactly at the threshold is a timeout, one second before is an OOM-kill", () => {
    expect(deriveOutcome({ rc: 137, durationSec: THRESHOLD, realFailures: 0, totalCases: 5, timeoutThresholdSec: THRESHOLD })).toEqual({
      outcome: "timeout",
      timedOut: true,
    });
    expect(deriveOutcome({ rc: 137, durationSec: THRESHOLD - 1, realFailures: 0, totalCases: 5, timeoutThresholdSec: THRESHOLD })).toEqual({
      outcome: "infra_fail",
      timedOut: false,
      infraSubtype: "oom_killed",
    });
  });

  test("rc 137 honours the caller's timeoutThresholdSec", () => {
    // 1000s is above this caller's 900s threshold (a timeout) but below a 1200s
    // threshold (an OOM-kill).
    expect(deriveOutcome({ rc: 137, durationSec: 1000, realFailures: 0, totalCases: 5, timeoutThresholdSec: 900 })).toEqual({
      outcome: "timeout",
      timedOut: true,
    });
    expect(deriveOutcome({ rc: 137, durationSec: 1000, realFailures: 0, totalCases: 5, timeoutThresholdSec: 1200 })).toEqual({
      outcome: "infra_fail",
      timedOut: false,
      infraSubtype: "oom_killed",
    });
  });

  test("rc != 0 with no failures is a generic infra fail (no subtype)", () => {
    expect(deriveOutcome({ rc: 1, durationSec: 30, realFailures: 0, totalCases: 5, timeoutThresholdSec: THRESHOLD })).toEqual({
      outcome: "infra_fail",
      timedOut: false,
    });
  });

  test("rc 0 with no recorded cases is a hang", () => {
    expect(deriveOutcome({ rc: 0, durationSec: 30, realFailures: 0, totalCases: 0, timeoutThresholdSec: THRESHOLD })).toEqual({
      outcome: "hang",
      timedOut: false,
    });
  });
});
