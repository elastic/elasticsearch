import { describe, expect, test } from "bun:test";

import { transformTaskStatus, runSmartRetry, resolveOriginJobId, resolveOriginJobName } from "./smart-retry";
import type {
  TaskStatusReport,
  SmartRetryEnv,
  SmartRetryDeps,
  SmartRetryResult,
  BuildkiteBuildJson,
} from "./types";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeReport(overrides: Partial<TaskStatusReport> = {}): TaskStatusReport {
  return { tasks: [], tests: [], cancelled: false, ...overrides };
}

function makeEnv(overrides: Partial<SmartRetryEnv> = {}): SmartRetryEnv {
  return {
    buildkiteApiToken: "test-token",
    buildkiteJobId: "job-2",
    buildkitePipelineSlug: "es-pipeline",
    buildkiteBuildNumber: "42",
    testsSeed: "SEED",
    ...overrides,
  };
}

function makeBuildJson(overrides: Partial<BuildkiteBuildJson> = {}): BuildkiteBuildJson {
  return {
    jobs: [
      { id: "job-1", name: "Test Step" },
      { id: "job-2", name: "Test Step (retry)", retry_source: { job_id: "job-1" } },
    ],
    meta_data: {},
    ...overrides,
  };
}

function makeDeps(overrides: Partial<SmartRetryDeps> = {}): SmartRetryDeps {
  return {
    fetchBuildJson: async () => makeBuildJson(),
    downloadArtifact: async () =>
      makeReport({
        tasks: [{ path: ":server:test", outcome: "FAILED" }],
        tests: [
          { taskPath: ":server:test", className: "org.es.FooTest", methodName: "testFoo", result: "FAILURE" },
        ],
      }),
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// transformTaskStatus
// ---------------------------------------------------------------------------

describe("transformTaskStatus", () => {
  test("empty report produces empty result", () => {
    const result = transformTaskStatus(makeReport(), "");

    expect(result).toEqual({
      workUnits: [],
      testseed: "",
      executedTestTasks: [],
      failedTestTasks: [],
    });
  });

  test("preserves testseed", () => {
    const result = transformTaskStatus(makeReport(), "DEADBEEF");

    expect(result.testseed).toBe("DEADBEEF");
  });

  test("all tests passed — no work units, tasks listed as executed", () => {
    const result = transformTaskStatus(
      makeReport({
        tasks: [
          { path: ":server:test", outcome: "SUCCESS" },
          { path: ":modules:test", outcome: "UP_TO_DATE" },
        ],
        tests: [
          { taskPath: ":server:test", className: "org.es.FooTest", methodName: "testFoo", result: "SUCCESS" },
          { taskPath: ":modules:test", className: "org.es.BarTest", methodName: "testBar", result: "SUCCESS" },
        ],
      }),
      "ABC123"
    );

    expect(result.workUnits).toEqual([]);
    expect(result.failedTestTasks).toEqual([]);
    expect(result.executedTestTasks).toEqual([":modules:test", ":server:test"]);
    expect(result.testseed).toBe("ABC123");
  });

  test("failed tests produce work units with correct structure", () => {
    const result = transformTaskStatus(
      makeReport({
        tasks: [{ path: ":server:test", outcome: "FAILED" }],
        tests: [
          { taskPath: ":server:test", className: "org.es.FooTest", methodName: "testFoo", result: "FAILURE" },
          { taskPath: ":server:test", className: "org.es.FooTest", methodName: "testBar", result: "SUCCESS" },
          { taskPath: ":server:test", className: "org.es.BarTest", methodName: "testBaz", result: "FAILURE" },
        ],
      }),
      ""
    );

    expect(result.workUnits).toHaveLength(1);
    expect(result.workUnits[0].name).toBe(":server:test");
    expect(result.workUnits[0].outcome).toBe("failed");
    expect(result.workUnits[0].tests).toHaveLength(2);

    const barTest = result.workUnits[0].tests.find((t) => t.name === "org.es.BarTest")!;
    expect(barTest.children).toHaveLength(1);
    expect(barTest.children[0].name).toBe("testBaz");
    expect(barTest.children[0].outcome).toEqual({ overall: "failed" });
    expect(barTest.children[0].children).toEqual([]);

    const fooTest = result.workUnits[0].tests.find((t) => t.name === "org.es.FooTest")!;
    expect(fooTest.children).toHaveLength(1);
    expect(fooTest.children[0].name).toBe("testFoo");
    expect(fooTest.children.map((c) => c.name)).not.toContain("testBar");
  });

  test("task failed at Gradle level without test failures goes into failedTestTasks", () => {
    const result = transformTaskStatus(
      makeReport({
        tasks: [
          { path: ":server:test", outcome: "FAILED" },
          { path: ":modules:test", outcome: "SUCCESS" },
        ],
        tests: [
          { taskPath: ":server:test", className: "org.es.FooTest", methodName: "testFoo", result: "SUCCESS" },
          { taskPath: ":modules:test", className: "org.es.BarTest", methodName: "testBar", result: "SUCCESS" },
        ],
      }),
      ""
    );

    expect(result.failedTestTasks).toEqual([":server:test"]);
    expect(result.workUnits).toEqual([]);
    expect(result.executedTestTasks).toEqual([":modules:test", ":server:test"]);
  });

  test("task with both test failures and Gradle failure goes into workUnits, not failedTestTasks", () => {
    const result = transformTaskStatus(
      makeReport({
        tasks: [{ path: ":server:test", outcome: "FAILED" }],
        tests: [
          { taskPath: ":server:test", className: "org.es.FooTest", methodName: "testFoo", result: "FAILURE" },
        ],
      }),
      ""
    );

    expect(result.workUnits).toHaveLength(1);
    expect(result.workUnits[0].name).toBe(":server:test");
    expect(result.failedTestTasks).toEqual([]);
  });

  test("non-test task failure is excluded from failedTestTasks", () => {
    const result = transformTaskStatus(
      makeReport({
        tasks: [
          { path: ":server:compileJava", outcome: "FAILED" },
          { path: ":server:test", outcome: "SUCCESS" },
        ],
        tests: [
          { taskPath: ":server:test", className: "org.es.FooTest", methodName: "testFoo", result: "SUCCESS" },
        ],
      }),
      ""
    );

    expect(result.failedTestTasks).toEqual([]);
    expect(result.executedTestTasks).toEqual([":server:test"]);
  });

  test("multiple task paths with mixed results", () => {
    const result = transformTaskStatus(
      makeReport({
        tasks: [
          { path: ":server:test", outcome: "FAILED" },
          { path: ":plugins:test", outcome: "FAILED" },
          { path: ":modules:test", outcome: "SUCCESS" },
          { path: ":libs:test", outcome: "SKIPPED" },
        ],
        tests: [
          { taskPath: ":server:test", className: "org.es.FooTest", methodName: "testFoo", result: "FAILURE" },
          { taskPath: ":server:test", className: "org.es.FooTest", methodName: "testBar", result: "SUCCESS" },
          { taskPath: ":plugins:test", className: "org.es.PluginTest", methodName: "testAll", result: "SUCCESS" },
          { taskPath: ":modules:test", className: "org.es.ModTest", methodName: "testMod", result: "SUCCESS" },
        ],
      }),
      "SEED42"
    );

    expect(result.workUnits).toHaveLength(1);
    expect(result.workUnits[0].name).toBe(":server:test");
    expect(result.failedTestTasks).toEqual([":plugins:test"]);
    expect(result.executedTestTasks).toEqual([":modules:test", ":plugins:test", ":server:test"]);
    expect(result.executedTestTasks).not.toContain(":libs:test");
    expect(result.testseed).toBe("SEED42");
  });

  test("interrupted and not-run tasks are not in executedTestTasks", () => {
    const result = transformTaskStatus(
      makeReport({
        tasks: [
          { path: ":server:test", outcome: "SUCCESS" },
          { path: ":plugins:test", outcome: "INTERRUPTED" },
          { path: ":modules:test", outcome: "NOT_RUN" },
        ],
        tests: [
          { taskPath: ":server:test", className: "org.es.FooTest", methodName: "testFoo", result: "SUCCESS" },
        ],
        cancelled: true,
      }),
      ""
    );

    expect(result.executedTestTasks).toEqual([":server:test"]);
  });

  test("skipped tests are not treated as failures", () => {
    const result = transformTaskStatus(
      makeReport({
        tasks: [{ path: ":server:test", outcome: "SUCCESS" }],
        tests: [
          { taskPath: ":server:test", className: "org.es.FooTest", methodName: "testFoo", result: "SKIPPED" },
          { taskPath: ":server:test", className: "org.es.FooTest", methodName: "testBar", result: "SUCCESS" },
        ],
      }),
      ""
    );

    expect(result.workUnits).toEqual([]);
    expect(result.failedTestTasks).toEqual([]);
    expect(result.executedTestTasks).toEqual([":server:test"]);
  });

  test("multiple failures across multiple classes in one task", () => {
    const result = transformTaskStatus(
      makeReport({
        tasks: [{ path: ":server:test", outcome: "FAILED" }],
        tests: [
          { taskPath: ":server:test", className: "org.es.AlphaTest", methodName: "testA1", result: "FAILURE" },
          { taskPath: ":server:test", className: "org.es.AlphaTest", methodName: "testA2", result: "FAILURE" },
          { taskPath: ":server:test", className: "org.es.BetaTest", methodName: "testB1", result: "FAILURE" },
          { taskPath: ":server:test", className: "org.es.BetaTest", methodName: "testB2", result: "SUCCESS" },
        ],
      }),
      ""
    );

    expect(result.workUnits).toHaveLength(1);
    const alpha = result.workUnits[0].tests.find((t) => t.name === "org.es.AlphaTest")!;
    expect(alpha.children).toHaveLength(2);
    expect(alpha.children.map((c) => c.name).sort()).toEqual(["testA1", "testA2"]);

    const beta = result.workUnits[0].tests.find((t) => t.name === "org.es.BetaTest")!;
    expect(beta.children).toHaveLength(1);
    expect(beta.children[0].name).toBe("testB1");
  });

  test("failures across multiple tasks produce separate work units", () => {
    const result = transformTaskStatus(
      makeReport({
        tasks: [
          { path: ":server:test", outcome: "FAILED" },
          { path: ":plugins:test", outcome: "FAILED" },
        ],
        tests: [
          { taskPath: ":server:test", className: "org.es.FooTest", methodName: "testFoo", result: "FAILURE" },
          { taskPath: ":plugins:test", className: "org.es.BarTest", methodName: "testBar", result: "FAILURE" },
        ],
      }),
      ""
    );

    expect(result.workUnits).toHaveLength(2);
    expect(result.workUnits.map((w) => w.name).sort()).toEqual([":plugins:test", ":server:test"]);
  });

  test("duplicate test task paths in tests array are deduplicated in executedTestTasks", () => {
    const result = transformTaskStatus(
      makeReport({
        tasks: [{ path: ":server:test", outcome: "SUCCESS" }],
        tests: [
          { taskPath: ":server:test", className: "org.es.FooTest", methodName: "testFoo", result: "SUCCESS" },
          { taskPath: ":server:test", className: "org.es.FooTest", methodName: "testBar", result: "SUCCESS" },
          { taskPath: ":server:test", className: "org.es.BarTest", methodName: "testBaz", result: "SUCCESS" },
        ],
      }),
      ""
    );

    expect(result.executedTestTasks).toEqual([":server:test"]);
  });

  test("cancelled build with no test entries", () => {
    const result = transformTaskStatus(
      makeReport({
        tasks: [
          { path: ":server:test", outcome: "INTERRUPTED" },
          { path: ":modules:test", outcome: "NOT_RUN" },
        ],
        tests: [],
        cancelled: true,
      }),
      ""
    );

    expect(result.workUnits).toEqual([]);
    expect(result.executedTestTasks).toEqual([]);
    expect(result.failedTestTasks).toEqual([]);
  });

  test("FROM_CACHE tasks with test entries are tracked as executed", () => {
    const result = transformTaskStatus(
      makeReport({
        tasks: [{ path: ":server:test", outcome: "FROM_CACHE" }],
        tests: [
          { taskPath: ":server:test", className: "org.es.FooTest", methodName: "testFoo", result: "SUCCESS" },
        ],
      }),
      ""
    );

    expect(result.executedTestTasks).toEqual([":server:test"]);
    expect(result.workUnits).toEqual([]);
  });

  test("test class outcome structure matches expected format", () => {
    const result = transformTaskStatus(
      makeReport({
        tasks: [{ path: ":server:test", outcome: "FAILED" }],
        tests: [
          { taskPath: ":server:test", className: "org.es.FooTest", methodName: "testFoo", result: "FAILURE" },
        ],
      }),
      ""
    );

    const testClass = result.workUnits[0].tests[0];
    expect(testClass.outcome).toEqual({ overall: "failed", own: "passed", children: "failed" });

    const testMethod = testClass.children[0];
    expect(testMethod.outcome).toEqual({ overall: "failed" });
    expect(testMethod.children).toEqual([]);
  });
});

// ---------------------------------------------------------------------------
// resolveOriginJobId
// ---------------------------------------------------------------------------

describe("resolveOriginJobId", () => {
  test("uses explicit origin job ID when provided", () => {
    const result = resolveOriginJobId(makeBuildJson(), "job-2", "explicit-id");

    expect(result).toBe("explicit-id");
  });

  test("ignores explicit origin if it is 'null' string", () => {
    const result = resolveOriginJobId(makeBuildJson(), "job-2", "null");

    expect(result).toBe("job-1");
  });

  test("looks up retry_source from build JSON when no explicit ID", () => {
    const result = resolveOriginJobId(makeBuildJson(), "job-2");

    expect(result).toBe("job-1");
  });

  test("returns null when current job has no retry_source", () => {
    const result = resolveOriginJobId(makeBuildJson(), "job-1");

    expect(result).toBeNull();
  });

  test("returns null when current job is not found in build JSON", () => {
    const result = resolveOriginJobId(makeBuildJson(), "nonexistent");

    expect(result).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// resolveOriginJobName
// ---------------------------------------------------------------------------

describe("resolveOriginJobName", () => {
  test("returns job name from build JSON", () => {
    expect(resolveOriginJobName(makeBuildJson(), "job-1")).toBe("Test Step");
  });

  test("falls back to 'previous attempt' when job not found", () => {
    expect(resolveOriginJobName(makeBuildJson(), "nonexistent")).toBe("previous attempt");
  });

  test("falls back to 'previous attempt' when job name is null", () => {
    const build = makeBuildJson({ jobs: [{ id: "job-1", name: undefined }] });
    expect(resolveOriginJobName(build, "job-1")).toBe("previous attempt");
  });
});

// ---------------------------------------------------------------------------
// runSmartRetry
// ---------------------------------------------------------------------------

describe("runSmartRetry", () => {
  test("fails when Buildkite API returns null", async () => {
    const result = await runSmartRetry(
      makeEnv(),
      makeDeps({ fetchBuildJson: async () => null })
    );

    expect(result.status).toBe("failed");
    expect(result.details).toBe("Buildkite API request failed");
    expect(result.failedTestHistory).toBeNull();
    expect(result.annotation).toBeNull();
    expect(result.metadata["smart-retry-status"]).toBe("failed");
  });

  test("fails when origin job ID cannot be resolved", async () => {
    const build = makeBuildJson({ jobs: [{ id: "orphan-job" }] });
    const result = await runSmartRetry(
      makeEnv({ buildkiteJobId: "orphan-job" }),
      makeDeps({ fetchBuildJson: async () => build })
    );

    expect(result.status).toBe("failed");
    expect(result.details).toBe("No origin job ID found");
  });

  test("fails when artifact download returns null", async () => {
    const result = await runSmartRetry(
      makeEnv(),
      makeDeps({ downloadArtifact: async () => null })
    );

    expect(result.status).toBe("failed");
    expect(result.details).toBe("Failed to download task-status artifact");
  });

  test("disabled when no test or task failures", async () => {
    const result = await runSmartRetry(
      makeEnv(),
      makeDeps({
        downloadArtifact: async () =>
          makeReport({
            tasks: [{ path: ":server:test", outcome: "SUCCESS" }],
            tests: [
              { taskPath: ":server:test", className: "org.es.FooTest", methodName: "testFoo", result: "SUCCESS" },
            ],
          }),
      })
    );

    expect(result.status).toBe("disabled");
    expect(result.details).toContain("not caused by test or task failures");
    expect(result.failedTestHistory).toBeNull();
    expect(result.annotation).toBeNull();
    expect(result.metadata["smart-retry-status"]).toBe("disabled");
    expect(result.metadata["smart-retry-disabled-reason"]).toBe("no-test-or-task-failures");
  });

  test("enabled with test failures", async () => {
    const result = await runSmartRetry(makeEnv(), makeDeps());

    expect(result.status).toBe("enabled");
    expect(result.failedTestHistory).not.toBeNull();
    expect(result.failedTestHistory!.workUnits).toHaveLength(1);
    expect(result.failedTestHistory!.workUnits[0].name).toBe(":server:test");
    expect(result.failedTestHistory!.testseed).toBe("SEED");
    expect(result.annotation).toContain("Rerunning failed build job [Test Step]");
    expect(result.annotation).toContain("**Gradle Tasks with Test Failures:** 1");
    expect(result.metadata["smart-retry-status"]).toBe("enabled");
    expect(result.metadata["smart-retry-work-units"]).toBe("1");
    expect(result.metadata["smart-retry-executed-tasks"]).toBe("1");
  });

  test("enabled with only Gradle-level task failures", async () => {
    const result = await runSmartRetry(
      makeEnv(),
      makeDeps({
        downloadArtifact: async () =>
          makeReport({
            tasks: [{ path: ":server:test", outcome: "FAILED" }],
            tests: [
              { taskPath: ":server:test", className: "org.es.FooTest", methodName: "testFoo", result: "SUCCESS" },
            ],
          }),
      })
    );

    expect(result.status).toBe("enabled");
    expect(result.failedTestHistory!.failedTestTasks).toEqual([":server:test"]);
    expect(result.failedTestHistory!.workUnits).toEqual([]);
    expect(result.annotation).toContain("**Gradle Tasks with Non-Test Failures:** 1");
    expect(result.metadata["smart-retry-failed-tasks"]).toBe("1");
  });

  test("does not include smart-retry-failed-tasks metadata when count is 0", async () => {
    const result = await runSmartRetry(makeEnv(), makeDeps());

    expect(result.metadata).not.toHaveProperty("smart-retry-failed-tasks");
  });

  test("uses explicit origin job ID from env", async () => {
    let downloadedFromJobId = "";
    const result = await runSmartRetry(
      makeEnv({ originJobId: "explicit-origin" }),
      makeDeps({
        downloadArtifact: async (jobId) => {
          downloadedFromJobId = jobId;
          return makeReport({
            tasks: [{ path: ":server:test", outcome: "FAILED" }],
            tests: [
              { taskPath: ":server:test", className: "org.es.FooTest", methodName: "testFoo", result: "FAILURE" },
            ],
          });
        },
      })
    );

    expect(downloadedFromJobId).toBe("explicit-origin");
    expect(result.status).toBe("enabled");
  });

  test("uses empty testseed when not provided", async () => {
    const result = await runSmartRetry(
      makeEnv({ testsSeed: undefined }),
      makeDeps()
    );

    expect(result.failedTestHistory!.testseed).toBe("");
  });

  test("annotation uses fallback job name when origin job not found", async () => {
    const build = makeBuildJson({
      jobs: [
        { id: "job-1" },
        { id: "job-2", retry_source: { job_id: "job-1" } },
      ],
    });

    const result = await runSmartRetry(
      makeEnv(),
      makeDeps({ fetchBuildJson: async () => build })
    );

    expect(result.annotation).toContain("Rerunning failed build job [previous attempt]");
  });

  test("annotation includes full detail block", async () => {
    const result = await runSmartRetry(
      makeEnv(),
      makeDeps({
        downloadArtifact: async () =>
          makeReport({
            tasks: [
              { path: ":server:test", outcome: "FAILED" },
              { path: ":plugins:test", outcome: "FAILED" },
              { path: ":modules:test", outcome: "SUCCESS" },
            ],
            tests: [
              { taskPath: ":server:test", className: "org.es.FooTest", methodName: "testFoo", result: "FAILURE" },
              { taskPath: ":plugins:test", className: "org.es.BarTest", methodName: "testBar", result: "SUCCESS" },
              { taskPath: ":modules:test", className: "org.es.ModTest", methodName: "testMod", result: "SUCCESS" },
            ],
          }),
      })
    );

    expect(result.annotation).toContain("**Gradle Tasks with Test Failures:** 1");
    expect(result.annotation).toContain("**Gradle Tasks with Non-Test Failures:** 1");
    expect(result.annotation).toContain("**Executed Test Tasks in Previous Run:** 3");
    expect(result.annotation).toContain("skip confirmed-passed tasks");
  });
});
