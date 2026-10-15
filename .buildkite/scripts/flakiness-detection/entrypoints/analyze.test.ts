import { describe, expect, test } from "vitest";

import { BLOCKING_LABELS, type SkippedTest } from "../domain.ts";

import {
  allTargetTasksSkipped,
  buildFailedPayload,
  notApplicablePayload,
  parseReportInputs,
  provenFlakinessJobs,
  reportErrorPayload,
  shouldBlock,
} from "./analyze.ts";

describe("notApplicablePayload", () => {
  test("maps a skipped javaRestTest to a zeroed not_applicable record carrying the resolver's reason", () => {
    const t: SkippedTest = {
      gradleProject: ":x-pack:plugin:logsdb:qa:rolling-upgrade",
      kind: "javaRestTest",
      sourceSet: "javaRestTest",
      fqcn: "org.elasticsearch.xpack.logsdb.SomeIT",
      reason: "no-runnable-task",
    };

    expect(notApplicablePayload(t)).toEqual({
      jobId: "not-applicable:javaRestTest::x-pack:plugin:logsdb:qa:rolling-upgrade:org.elasticsearch.xpack.logsdb.SomeIT",
      stepKey: "flakiness-detection:java-rest",
      kind: "javaRestTest",
      rc: 0,
      durationSec: 0,
      realFailures: 0,
      suiteTimeouts: 0,
      totalCases: 0,
      outcome: "not_applicable",
      timedOut: false,
      failingClasses: [],
      reason: "no-runnable-task",
    });
  });

  test("falls back to a generic reason when the artifact carries none", () => {
    const t: SkippedTest = { gradleProject: ":qa:x", kind: "test", sourceSet: "test", fqcn: "org.FooTests" };
    expect(notApplicablePayload(t).reason).toBe("not-runnable");
  });

  test("uses the full yaml descriptor as the target for a yaml case", () => {
    const t: SkippedTest = {
      gradleProject: ":qa:mixed",
      kind: "yamlRestTestCase",
      sourceSet: "yamlRestTest",
      fqcn: "org.elasticsearch.SomeYamlIT",
      yamlTest: "test {yaml=10_basic/Foo}",
    };

    const payload = notApplicablePayload(t);
    expect(payload.outcome).toBe("not_applicable");
    expect(payload.jobId).toContain("org.elasticsearch.SomeYamlIT.test {yaml=10_basic/Foo}");
    expect(payload.stepKey).toBe("flakiness-detection:yaml-case");
  });
});

describe("buildFailedPayload", () => {
  test("is a single build_failed record keyed under flakiness-orchestration (not a test batch)", () => {
    const payload = buildFailedPayload();
    // Must NOT be under the `flakiness-detection:` prefix the external batch-job
    // metric predicate matches - otherwise this synthetic record would be counted
    // as a test batch.
    expect(payload.stepKey).toBe("flakiness-orchestration:compile");
    expect(payload.stepKey.startsWith("flakiness-detection:")).toBe(false);
    expect(payload).toEqual({
      jobId: "build-failed:precompile",
      stepKey: "flakiness-orchestration:compile",
      kind: "",
      rc: 1,
      durationSec: 0,
      realFailures: 0,
      suiteTimeouts: 0,
      totalCases: 0,
      outcome: "build_failed",
      timedOut: false,
      failingClasses: [],
      reason: "precompile",
    });
  });
});

describe("parseReportInputs", () => {
  const SKIP = '[{"gradleProject":":qa","kind":"javaRestTest","sourceSet":"javaRestTest","fqcn":"org.foo.SomeIT"}]';
  const env = (compileFailed: string | undefined, skippedCount: string | undefined) => ({
    FLAKINESS_COMPILE_FAILED: compileFailed,
    FLAKINESS_SKIPPED_COUNT: skippedCount,
  });

  /** The regression: the compile failure used to come from a marker file a failed download turned into "compiled". */
  test("a compile failure comes from the env alone, with no file involved", () => {
    expect(parseReportInputs(env("true", "0"), null)).toEqual({ compileFailed: true, skipped: [], problems: [] });
  });

  test("with nothing skipped the skip list is not consulted", () => {
    expect(parseReportInputs(env("false", "0"), null)).toEqual({ compileFailed: false, skipped: [], problems: [] });
  });

  test("a skip list holding the declared count is read", () => {
    const r = parseReportInputs(env("false", "1"), SKIP);
    expect(r.problems).toEqual([]);
    expect(r.skipped).toHaveLength(1);
  });

  test.each([
    ["absent", null, /did not reach this step, but the generate step declared 1/],
    ["not JSON", "[", /not valid JSON/],
    ["not an array", "{}", /not a JSON array/],
    ["shorter than declared", "[]", /has 0 entries, but the generate step declared 1/],
    ["longer than declared", `[${SKIP.slice(1, -1)},${SKIP.slice(1, -1)}]`, /has 2 entries, but the generate step declared 1/],
  ])("a skip list that is %s is a problem, not an empty list", (_, text, problem) => {
    const r = parseReportInputs(env("false", "1"), text);
    expect(r.problems).toEqual([expect.stringMatching(problem)]);
  });

  test("a missing or malformed declaration is a problem, never a default", () => {
    const r = parseReportInputs(env(undefined, "x"), null);
    expect(r.problems).toEqual([
      expect.stringContaining("FLAKINESS_COMPILE_FAILED is undefined"),
      expect.stringContaining('FLAKINESS_SKIPPED_COUNT is "x"'),
      "flakiness-skipped.json did not reach this step",
    ]);
  });
});

describe("reportErrorPayload", () => {
  test("is an infra_fail record keyed under flakiness-orchestration (not a test batch)", () => {
    const payload = reportErrorPayload();
    expect(payload.stepKey.startsWith("flakiness-detection:")).toBe(false);
    expect(payload).toEqual(
      expect.objectContaining({ jobId: "report-error:inputs", outcome: "infra_fail", infraSubtype: "report_error" })
    );
  });
});

describe("provenFlakinessJobs", () => {
  test("counts only the jobs where a test actually failed on a re-run", () => {
    const payloads = [
      { outcome: "clean_pass" },
      { outcome: "flaky_detected" },
      { outcome: "flaky_detected" },
    ];
    expect(provenFlakinessJobs(payloads)).toHaveLength(2);
  });

  test("no other outcome qualifies, including a timeout with no failing test", () => {
    const payloads = [
      { outcome: "timeout" },
      { outcome: "infra_fail" },
      { outcome: "hang" },
      { outcome: "not_applicable" },
      { outcome: "clean_pass" },
    ];
    expect(provenFlakinessJobs(payloads)).toEqual([]);
  });

  test("a build_failed PR is not blocked here - its own main build is already red", () => {
    expect(provenFlakinessJobs([buildFailedPayload()])).toEqual([]);
  });

  test("a proven failure alongside an unrelated timeout still counts", () => {
    expect(provenFlakinessJobs([{ outcome: "timeout" }, { outcome: "flaky_detected" }])).toHaveLength(1);
  });

  test("nothing to report when no job ran", () => {
    expect(provenFlakinessJobs([])).toEqual([]);
  });
});

describe("shouldBlock", () => {
  const OPTED_IN = BLOCKING_LABELS[0];
  const PROVEN = [{ outcome: "clean_pass" }, { outcome: "flaky_detected" }];
  const CLEAN = [{ outcome: "clean_pass" }, { outcome: "timeout" }];

  test("blocks only when flakiness was proven AND the PR opted in", () => {
    expect(shouldBlock(PROVEN, `>bug,${OPTED_IN}`)).toBe(true);
  });

  test("a proven failure on a PR that did not opt in is reported, not blocked", () => {
    // The default for the whole repo: the report and the outcomes artifact are identical either way, only
    // the exit code differs.
    expect(shouldBlock(PROVEN, ">bug,Team:NotIncludedTeam")).toBe(false);
  });

  test("an opted-in PR with nothing proven still passes", () => {
    expect(shouldBlock(CLEAN, OPTED_IN)).toBe(false);
    expect(shouldBlock([], OPTED_IN)).toBe(false);
  });

  test("a build that did not compile is never blocked here", () => {
    // Its own main build is already red for the same compile error.
    expect(shouldBlock([buildFailedPayload()], OPTED_IN)).toBe(false);
  });

  test("no labels at all never blocks, which is what keeps the manual pipeline green", () => {
    // GITHUB_PR_LABELS is unset outside a PR build, and the caller passes "" for that.
    expect(shouldBlock(PROVEN, "")).toBe(false);
  });
});

describe("allTargetTasksSkipped", () => {
  const skipped = (p: string) => ({ path: p, outcome: "SKIPPED" });
  const success = (p: string) => ({ path: p, outcome: "SUCCESS" });

  test("true only when every requested task was skipped", () => {
    expect(allTargetTasksSkipped([":a:test"], [skipped(":a:test")])).toBe(true);
    expect(allTargetTasksSkipped([":a:test", ":b:test"], [skipped(":a:test"), skipped(":b:test")])).toBe(true);
  });

  test("false when any requested task actually ran - a zero-test result is then not explained by onlyIf", () => {
    expect(allTargetTasksSkipped([":a:test", ":b:test"], [skipped(":a:test"), success(":b:test")])).toBe(false);
  });

  test("ignores unrelated SKIPPED tasks, so the muted-tests case is not mislabelled", () => {
    // A healthy build skips plenty of things (processResources with no resources). The target task ran, so
    // zero tests here means the filter matched nothing - a hang, not not_applicable.
    const entries = [success(":a:test"), skipped(":a:processResources"), skipped(":a:processTestResources")];
    expect(allTargetTasksSkipped([":a:test"], entries)).toBe(false);
  });

  test("no verdict when the report is missing or the plan carried no task paths", () => {
    expect(allTargetTasksSkipped([":a:test"], [])).toBe(false);
    expect(allTargetTasksSkipped([], [skipped(":a:test")])).toBe(false);
  });

  test("matches paths exactly, so a prefix cannot cross-match", () => {
    expect(allTargetTasksSkipped([":a:test"], [skipped(":a:testFixtures")])).toBe(false);
  });
});
