import { describe, expect, test } from "vitest";

import type { ClassifiedTest } from "../domain.ts";

import { buildFailedPayload, isPrecompileFailure, notApplicablePayload } from "./analyze.ts";

describe("notApplicablePayload", () => {
  test("maps a skipped BWC javaRestTest to a zeroed not_applicable record", () => {
    const t: ClassifiedTest = {
      gradleProject: ":x-pack:plugin:logsdb:qa:rolling-upgrade",
      kind: "javaRestTest",
      sourceSet: "javaRestTest",
      fqcn: "org.elasticsearch.xpack.logsdb.SomeIT",
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
      reason: "bwc",
    });
  });

  test("uses the full yaml descriptor as the target for a yaml case", () => {
    const t: ClassifiedTest = {
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
  test("is a single build_failed record attributed to the precompile gate", () => {
    expect(buildFailedPayload()).toEqual({
      jobId: "build-failed:precompile",
      stepKey: "flakiness-detection:precompile",
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
