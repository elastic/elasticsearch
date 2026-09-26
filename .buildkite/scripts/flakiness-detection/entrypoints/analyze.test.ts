import { describe, expect, test } from "vitest";

import type { SkippedTest } from "../domain.ts";

import { allTargetTasksSkipped, buildFailedPayload, isPrecompileFailure, notApplicablePayload } from "./analyze.ts";

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

describe("isPrecompileFailure", () => {
  test("true for the marker the gate writes on failure", () => {
    expect(isPrecompileFailure('{"outcome":"build_failed","reason":"precompile"}')).toBe(true);
    // reason is not part of the decision - only the outcome is
    expect(isPrecompileFailure('{"outcome":"build_failed"}')).toBe(true);
  });

  test("false when the marker is absent (gate passed or never ran)", () => {
    expect(isPrecompileFailure(null)).toBe(false);
  });

  test("false for any other outcome", () => {
    expect(isPrecompileFailure('{"outcome":"clean_pass"}')).toBe(false);
    expect(isPrecompileFailure("{}")).toBe(false);
  });

  test("false for malformed or empty marker content", () => {
    expect(isPrecompileFailure("not json")).toBe(false);
    expect(isPrecompileFailure("")).toBe(false);
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
