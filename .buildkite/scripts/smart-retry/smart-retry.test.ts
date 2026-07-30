import { describe, expect, test } from "vitest";

import {
  transformTaskStatus,
  runSmartRetry,
  resolveOriginJobId,
  resolveOriginJobName,
  mergeRuns,
  normalizeTaskStatus,
  wrapTaskStatus,
} from "./smart-retry.ts";
import type {
  TaskStatusReport,
  MultiRunTaskStatus,
  SmartRetryEnv,
  SmartRetryDeps,
  BuildkiteBuildJson,
} from "./types.ts";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeReport(overrides: Partial<TaskStatusReport> = {}): TaskStatusReport {
  return { tasks: [], tests: [], cancelled: false, ...overrides };
}

function makeMultiRun(...runs: TaskStatusReport[]): MultiRunTaskStatus {
  return { runs };
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
      makeMultiRun(
        makeReport({
          tasks: [
            { path: ":server:test", outcome: "FAILED" },
            { path: ":modules:test", outcome: "SUCCESS" },
          ],
          tests: [
            { taskPath: ":server:test", className: "org.es.FooTest", methodName: "testFoo", result: "FAILURE" },
            { taskPath: ":modules:test", className: "org.es.BarTest", methodName: "testBar", result: "SUCCESS" },
          ],
        }),
      ),
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// transformTaskStatus
// ---------------------------------------------------------------------------

describe("transformTaskStatus", () => {
  test("empty report produces empty result", () => {
    const result = transformTaskStatus(makeReport(), "");

    expect(result).toEqual({ successfulTasks: [], successfulSuites: {}, successfulTests: {}, testseed: "" });
  });

  test("preserves testseed", () => {
    const result = transformTaskStatus(makeReport(), "DEADBEEF");

    expect(result.testseed).toBe("DEADBEEF");
  });

  test("all tests passed — all tasks in successfulTasks, no successfulTests", () => {
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
      "ABC123",
    );

    expect(result.successfulTasks).toEqual([":modules:test", ":server:test"]);
    expect(result.successfulTests).toEqual({});
    expect(result.testseed).toBe("ABC123");
  });

  test("task with mixed test results — passing tests go into successfulTests", () => {
    const result = transformTaskStatus(
      makeReport({
        tasks: [{ path: ":server:test", outcome: "FAILED" }],
        tests: [
          { taskPath: ":server:test", className: "org.es.FooTest", methodName: "testFoo", result: "FAILURE" },
          { taskPath: ":server:test", className: "org.es.FooTest", methodName: "testBar", result: "SUCCESS" },
          { taskPath: ":server:test", className: "org.es.BarTest", methodName: "testBaz", result: "SUCCESS" },
        ],
      }),
      "",
    );

    expect(result.successfulTasks).toEqual([]);
    expect(result.successfulTests).toEqual({
      ":server:test": ["org.es.BarTest#testBaz", "org.es.FooTest#testBar"],
    });
  });

  test("task failed at Gradle level without test failures — passing tests go into successfulTests", () => {
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
      "",
    );

    expect(result.successfulTasks).toEqual([":modules:test"]);
    expect(result.successfulTests).toEqual({
      ":server:test": ["org.es.FooTest#testFoo"],
    });
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
      "SEED42",
    );

    expect(result.successfulTasks).toEqual([":modules:test"]);
    expect(result.successfulTests).toEqual({
      ":server:test": ["org.es.FooTest#testBar"],
      ":plugins:test": ["org.es.PluginTest#testAll"],
    });
  });

  test("skipped tests are not included in successfulTests", () => {
    const result = transformTaskStatus(
      makeReport({
        tasks: [{ path: ":server:test", outcome: "FAILED" }],
        tests: [
          { taskPath: ":server:test", className: "org.es.FooTest", methodName: "testFoo", result: "FAILURE" },
          { taskPath: ":server:test", className: "org.es.FooTest", methodName: "testBar", result: "SKIPPED" },
          { taskPath: ":server:test", className: "org.es.FooTest", methodName: "testBaz", result: "SUCCESS" },
        ],
      }),
      "",
    );

    expect(result.successfulTests).toEqual({
      ":server:test": ["org.es.FooTest#testBaz"],
    });
  });

  test("skipped tests do not prevent task from being successful", () => {
    const result = transformTaskStatus(
      makeReport({
        tasks: [{ path: ":server:test", outcome: "SUCCESS" }],
        tests: [
          { taskPath: ":server:test", className: "org.es.FooTest", methodName: "testFoo", result: "SKIPPED" },
          { taskPath: ":server:test", className: "org.es.FooTest", methodName: "testBar", result: "SUCCESS" },
        ],
      }),
      "",
    );

    expect(result.successfulTasks).toEqual([":server:test"]);
    expect(result.successfulTests).toEqual({});
  });

  test("UP_TO_DATE and FROM_CACHE count as successful outcomes", () => {
    const result = transformTaskStatus(
      makeReport({
        tasks: [
          { path: ":a:test", outcome: "UP_TO_DATE" },
          { path: ":b:test", outcome: "FROM_CACHE" },
        ],
        tests: [],
      }),
      "",
    );

    expect(result.successfulTasks).toEqual([":a:test", ":b:test"]);
    expect(result.successfulTests).toEqual({});
  });

  test("INTERRUPTED and NOT_RUN tasks are not successful", () => {
    const result = transformTaskStatus(
      makeReport({
        tasks: [
          { path: ":a:test", outcome: "INTERRUPTED" },
          { path: ":b:test", outcome: "NOT_RUN" },
          { path: ":c:test", outcome: "SUCCESS" },
        ],
        tests: [],
      }),
      "",
    );

    expect(result.successfulTasks).toEqual([":c:test"]);
    expect(result.successfulTests).toEqual({});
  });

  test("all tests in a failed task passed — no successfulTasks, all tests in successfulTests", () => {
    const result = transformTaskStatus(
      makeReport({
        tasks: [{ path: ":server:test", outcome: "FAILED" }],
        tests: [
          { taskPath: ":server:test", className: "org.es.FooTest", methodName: "testFoo", result: "SUCCESS" },
          { taskPath: ":server:test", className: "org.es.FooTest", methodName: "testBar", result: "SUCCESS" },
        ],
      }),
      "",
    );

    expect(result.successfulTasks).toEqual([]);
    expect(result.successfulTests).toEqual({
      ":server:test": ["org.es.FooTest#testBar", "org.es.FooTest#testFoo"],
    });
  });
});

// ---------------------------------------------------------------------------
// normalizeTaskStatus
// ---------------------------------------------------------------------------

describe("normalizeTaskStatus", () => {
  test("wraps a legacy single-run object in a runs array", () => {
    const legacy = makeReport({
      tasks: [{ path: ":server:test", outcome: "SUCCESS" }],
    });

    const result = normalizeTaskStatus(legacy);

    expect(result.runs).toHaveLength(1);
    expect(result.runs[0]).toEqual(legacy);
  });

  test("passes through an already multi-run object unchanged", () => {
    const multi = makeMultiRun(
      makeReport({ tasks: [{ path: ":a:test", outcome: "SUCCESS" }] }),
      makeReport({ tasks: [{ path: ":b:test", outcome: "FAILED" }] }),
    );

    const result = normalizeTaskStatus(multi);

    expect(result.runs).toHaveLength(2);
    expect(result).toEqual(multi);
  });
});

// ---------------------------------------------------------------------------
// wrapTaskStatus
// ---------------------------------------------------------------------------

describe("wrapTaskStatus", () => {
  test("wraps current run as sole entry when no previous", () => {
    const current = makeReport({ tasks: [{ path: ":a:test", outcome: "SUCCESS" }] });

    const result = wrapTaskStatus(current, null);

    expect(result.runs).toHaveLength(1);
    expect(result.runs[0]).toEqual(current);
  });

  test("appends current run after previous runs", () => {
    const prev = makeMultiRun(makeReport({ tasks: [{ path: ":a:test", outcome: "FAILED" }] }));
    const current = makeReport({ tasks: [{ path: ":a:test", outcome: "SUCCESS" }] });

    const result = wrapTaskStatus(current, prev);

    expect(result.runs).toHaveLength(2);
    expect(result.runs[0].tasks[0].outcome).toBe("FAILED");
    expect(result.runs[1].tasks[0].outcome).toBe("SUCCESS");
  });

  test("preserves multiple previous runs and appends current", () => {
    const prev = makeMultiRun(
      makeReport({ tasks: [{ path: ":a:test", outcome: "FAILED" }] }),
      makeReport({ tasks: [{ path: ":a:test", outcome: "FAILED" }] }),
    );
    const current = makeReport({ tasks: [{ path: ":a:test", outcome: "SUCCESS" }] });

    const result = wrapTaskStatus(current, prev);

    expect(result.runs).toHaveLength(3);
  });
});

// ---------------------------------------------------------------------------
// mergeRuns
// ---------------------------------------------------------------------------

describe("mergeRuns", () => {
  test("empty runs array produces empty merged report", () => {
    const result = mergeRuns({ runs: [] });

    expect(result.tasks).toEqual([]);
    expect(result.tests).toEqual([]);
    expect(result.cancelled).toBe(false);
  });

  test("single run is returned as-is", () => {
    const run = makeReport({
      tasks: [{ path: ":server:test", outcome: "FAILED" }],
      tests: [{ taskPath: ":server:test", className: "org.es.FooTest", methodName: "testFoo", result: "FAILURE" }],
    });

    const result = mergeRuns(makeMultiRun(run));

    expect(result).toEqual(run);
  });

  test("task SUCCESS in run 1 is preserved even if SKIPPED in run 2", () => {
    const result = mergeRuns(
      makeMultiRun(
        makeReport({ tasks: [{ path: ":server:test", outcome: "SUCCESS" }] }),
        makeReport({ tasks: [{ path: ":server:test", outcome: "SKIPPED" }] }),
      ),
    );

    expect(result.tasks).toHaveLength(1);
    expect(result.tasks[0].outcome).toBe("SUCCESS");
  });

  test("task SUCCESS in run 1 is preserved even if NOT_RUN in run 2", () => {
    const result = mergeRuns(
      makeMultiRun(
        makeReport({ tasks: [{ path: ":server:test", outcome: "SUCCESS" }] }),
        makeReport({ tasks: [{ path: ":server:test", outcome: "NOT_RUN" }] }),
      ),
    );

    expect(result.tasks[0].outcome).toBe("SUCCESS");
  });

  test("task FAILED in run 1 upgraded to SUCCESS in run 2", () => {
    const result = mergeRuns(
      makeMultiRun(
        makeReport({ tasks: [{ path: ":server:test", outcome: "FAILED" }] }),
        makeReport({ tasks: [{ path: ":server:test", outcome: "SUCCESS" }] }),
      ),
    );

    expect(result.tasks[0].outcome).toBe("SUCCESS");
  });

  test("UP_TO_DATE and FROM_CACHE treated same as SUCCESS", () => {
    const result1 = mergeRuns(
      makeMultiRun(
        makeReport({ tasks: [{ path: ":a:test", outcome: "UP_TO_DATE" }] }),
        makeReport({ tasks: [{ path: ":a:test", outcome: "SKIPPED" }] }),
      ),
    );
    expect(result1.tasks[0].outcome).toBe("UP_TO_DATE");

    const result2 = mergeRuns(
      makeMultiRun(
        makeReport({ tasks: [{ path: ":a:test", outcome: "FROM_CACHE" }] }),
        makeReport({ tasks: [{ path: ":a:test", outcome: "NOT_RUN" }] }),
      ),
    );
    expect(result2.tasks[0].outcome).toBe("FROM_CACHE");
  });

  test("FAILED overrides INTERRUPTED", () => {
    const result = mergeRuns(
      makeMultiRun(
        makeReport({ tasks: [{ path: ":a:test", outcome: "INTERRUPTED" }] }),
        makeReport({ tasks: [{ path: ":a:test", outcome: "FAILED" }] }),
      ),
    );
    expect(result.tasks[0].outcome).toBe("FAILED");
  });

  test("tasks from different runs are unioned", () => {
    const result = mergeRuns(
      makeMultiRun(
        makeReport({ tasks: [{ path: ":a:test", outcome: "SUCCESS" }] }),
        makeReport({ tasks: [{ path: ":b:test", outcome: "FAILED" }] }),
      ),
    );

    expect(result.tasks).toHaveLength(2);
    expect(result.tasks.map((t) => t.path).sort()).toEqual([":a:test", ":b:test"]);
  });

  test("test SUCCESS in any run makes merged result SUCCESS", () => {
    const result = mergeRuns(
      makeMultiRun(
        makeReport({
          tests: [{ taskPath: ":server:test", className: "org.es.FooTest", methodName: "testFoo", result: "FAILURE" }],
        }),
        makeReport({
          tests: [{ taskPath: ":server:test", className: "org.es.FooTest", methodName: "testFoo", result: "SUCCESS" }],
        }),
      ),
    );

    expect(result.tests).toHaveLength(1);
    expect(result.tests[0].result).toBe("SUCCESS");
  });

  test("test FAILURE overrides SKIPPED", () => {
    const result = mergeRuns(
      makeMultiRun(
        makeReport({
          tests: [{ taskPath: ":server:test", className: "org.es.FooTest", methodName: "testFoo", result: "SKIPPED" }],
        }),
        makeReport({
          tests: [{ taskPath: ":server:test", className: "org.es.FooTest", methodName: "testFoo", result: "FAILURE" }],
        }),
      ),
    );

    expect(result.tests[0].result).toBe("FAILURE");
  });

  test("test SUCCESS overrides prior FAILURE (test fixed on retry)", () => {
    const result = mergeRuns(
      makeMultiRun(
        makeReport({
          tests: [{ taskPath: ":server:test", className: "org.es.FooTest", methodName: "testFoo", result: "FAILURE" }],
        }),
        makeReport({
          tests: [{ taskPath: ":server:test", className: "org.es.FooTest", methodName: "testFoo", result: "SUCCESS" }],
        }),
      ),
    );

    expect(result.tests[0].result).toBe("SUCCESS");
  });

  test("tests from different runs are unioned", () => {
    const result = mergeRuns(
      makeMultiRun(
        makeReport({
          tests: [{ taskPath: ":server:test", className: "org.es.FooTest", methodName: "testFoo", result: "SUCCESS" }],
        }),
        makeReport({
          tests: [{ taskPath: ":server:test", className: "org.es.FooTest", methodName: "testBar", result: "FAILURE" }],
        }),
      ),
    );

    expect(result.tests).toHaveLength(2);
    const foo = result.tests.find((t) => t.methodName === "testFoo")!;
    const bar = result.tests.find((t) => t.methodName === "testBar")!;
    expect(foo.result).toBe("SUCCESS");
    expect(bar.result).toBe("FAILURE");
  });

  test("cancelled flag comes from the last run", () => {
    const result = mergeRuns(makeMultiRun(makeReport({ cancelled: true }), makeReport({ cancelled: false })));
    expect(result.cancelled).toBe(false);

    const result2 = mergeRuns(makeMultiRun(makeReport({ cancelled: false }), makeReport({ cancelled: true })));
    expect(result2.cancelled).toBe(true);
  });

  test("three-run scenario: progressive fix", () => {
    const result = mergeRuns(
      makeMultiRun(
        // Run 1: two tasks fail
        makeReport({
          tasks: [
            { path: ":server:test", outcome: "FAILED" },
            { path: ":plugins:test", outcome: "FAILED" },
            { path: ":modules:test", outcome: "SUCCESS" },
          ],
          tests: [
            { taskPath: ":server:test", className: "org.es.FooTest", methodName: "testA", result: "FAILURE" },
            { taskPath: ":server:test", className: "org.es.FooTest", methodName: "testB", result: "SUCCESS" },
            { taskPath: ":plugins:test", className: "org.es.BarTest", methodName: "testC", result: "FAILURE" },
            { taskPath: ":modules:test", className: "org.es.ModTest", methodName: "testD", result: "SUCCESS" },
          ],
        }),
        // Run 2: :server:test fixed, :plugins:test still fails
        makeReport({
          tasks: [
            { path: ":server:test", outcome: "SUCCESS" },
            { path: ":plugins:test", outcome: "FAILED" },
            { path: ":modules:test", outcome: "SKIPPED" },
          ],
          tests: [
            { taskPath: ":server:test", className: "org.es.FooTest", methodName: "testA", result: "SUCCESS" },
            { taskPath: ":plugins:test", className: "org.es.BarTest", methodName: "testC", result: "FAILURE" },
          ],
        }),
      ),
    );

    // :server:test upgraded to SUCCESS
    expect(result.tasks.find((t) => t.path === ":server:test")!.outcome).toBe("SUCCESS");
    // :plugins:test still FAILED
    expect(result.tasks.find((t) => t.path === ":plugins:test")!.outcome).toBe("FAILED");
    // :modules:test SUCCESS preserved despite SKIPPED in run 2
    expect(result.tasks.find((t) => t.path === ":modules:test")!.outcome).toBe("SUCCESS");

    // testA now SUCCESS across merged runs
    expect(result.tests.find((t) => t.methodName === "testA")!.result).toBe("SUCCESS");
    // testB only in run 1, still SUCCESS
    expect(result.tests.find((t) => t.methodName === "testB")!.result).toBe("SUCCESS");
    // testC still FAILURE (failed in both runs)
    expect(result.tests.find((t) => t.methodName === "testC")!.result).toBe("FAILURE");
    // testD only in run 1, still SUCCESS
    expect(result.tests.find((t) => t.methodName === "testD")!.result).toBe("SUCCESS");
  });
});

// ---------------------------------------------------------------------------
// resolveOriginJobId
// ---------------------------------------------------------------------------

describe("resolveOriginJobId", () => {
  test("uses explicit origin job ID when provided", () => {
    expect(resolveOriginJobId(makeBuildJson(), "job-2", "explicit-id")).toBe("explicit-id");
  });

  test("ignores explicit origin if it is 'null' string", () => {
    expect(resolveOriginJobId(makeBuildJson(), "job-2", "null")).toBe("job-1");
  });

  test("looks up retry_source from build JSON when no explicit ID", () => {
    expect(resolveOriginJobId(makeBuildJson(), "job-2")).toBe("job-1");
  });

  test("returns null when current job has no retry_source", () => {
    expect(resolveOriginJobId(makeBuildJson(), "job-1")).toBeNull();
  });

  test("returns null when current job is not found in build JSON", () => {
    expect(resolveOriginJobId(makeBuildJson(), "nonexistent")).toBeNull();
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

  test("falls back to 'previous attempt' when job name is undefined", () => {
    const build = makeBuildJson({ jobs: [{ id: "job-1", name: undefined }] });
    expect(resolveOriginJobName(build, "job-1")).toBe("previous attempt");
  });
});

// ---------------------------------------------------------------------------
// runSmartRetry
// ---------------------------------------------------------------------------

describe("runSmartRetry", () => {
  test("fails when Buildkite API returns null", async () => {
    const result = await runSmartRetry(makeEnv(), makeDeps({ fetchBuildJson: async () => null }));

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
      makeDeps({ fetchBuildJson: async () => build }),
    );

    expect(result.status).toBe("failed");
    expect(result.details).toBe("No origin job ID found");
  });

  test("fails when artifact download returns null", async () => {
    const result = await runSmartRetry(makeEnv(), makeDeps({ downloadArtifact: async () => null }));

    expect(result.status).toBe("failed");
    expect(result.details).toBe("Failed to download task-status artifact");
  });

  test("disabled when no successful tasks or tests found", async () => {
    const result = await runSmartRetry(
      makeEnv(),
      makeDeps({
        downloadArtifact: async () =>
          makeMultiRun(
            makeReport({
              tasks: [{ path: ":server:test", outcome: "FAILED" }],
              tests: [
                { taskPath: ":server:test", className: "org.es.FooTest", methodName: "testFoo", result: "FAILURE" },
              ],
            }),
          ),
      }),
    );

    expect(result.status).toBe("disabled");
    expect(result.details).toContain("No successful tasks or tests");
    expect(result.failedTestHistory).toBeNull();
    expect(result.metadata["smart-retry-disabled-reason"]).toBe("no-successful-tasks-or-tests");
  });

  test("enabled when only individual tests succeeded (no fully successful tasks)", async () => {
    const result = await runSmartRetry(
      makeEnv(),
      makeDeps({
        downloadArtifact: async () =>
          makeMultiRun(
            makeReport({
              tasks: [{ path: ":server:test", outcome: "FAILED" }],
              tests: [
                { taskPath: ":server:test", className: "org.es.FooTest", methodName: "testFoo", result: "FAILURE" },
                { taskPath: ":server:test", className: "org.es.FooTest", methodName: "testBar", result: "SUCCESS" },
              ],
            }),
          ),
      }),
    );

    expect(result.status).toBe("enabled");
    expect(result.failedTestHistory!.successfulTasks).toEqual([]);
    expect(result.failedTestHistory!.successfulTests).toEqual({
      ":server:test": ["org.es.FooTest#testBar"],
    });
    expect(result.metadata["smart-retry-successful-tests"]).toBe("1");
  });

  test("enabled with successful tasks to skip", async () => {
    const result = await runSmartRetry(
      makeEnv(),
      makeDeps({
        downloadArtifact: async () =>
          makeMultiRun(
            makeReport({
              tasks: [
                { path: ":server:test", outcome: "FAILED" },
                { path: ":modules:test", outcome: "SUCCESS" },
              ],
              tests: [
                { taskPath: ":server:test", className: "org.es.FooTest", methodName: "testFoo", result: "FAILURE" },
                { taskPath: ":modules:test", className: "org.es.BarTest", methodName: "testBar", result: "SUCCESS" },
              ],
            }),
          ),
      }),
    );

    expect(result.status).toBe("enabled");
    expect(result.failedTestHistory).not.toBeNull();
    expect(result.failedTestHistory!.successfulTasks).toEqual([":modules:test"]);
    expect(result.failedTestHistory!.successfulTests).toEqual({});
    expect(result.failedTestHistory!.testseed).toBe("SEED");
    expect(result.annotation).toContain("Rerunning failed build job [Test Step]");
    expect(result.annotation).toContain("**Successful Tasks to Skip:** 1");
    expect(result.annotation).toContain("**Successful Tests to Skip:** 0");
    expect(result.metadata["smart-retry-status"]).toBe("enabled");
    expect(result.metadata["smart-retry-successful-tasks"]).toBe("1");
    expect(result.metadata["smart-retry-successful-tests"]).toBe("0");
  });

  test("uses explicit origin job ID from env", async () => {
    let downloadedFromJobId = "";
    const result = await runSmartRetry(
      makeEnv({ originJobId: "explicit-origin" }),
      makeDeps({
        downloadArtifact: async (jobId) => {
          downloadedFromJobId = jobId;
          return makeMultiRun(
            makeReport({
              tasks: [
                { path: ":server:test", outcome: "FAILED" },
                { path: ":modules:test", outcome: "SUCCESS" },
              ],
              tests: [
                { taskPath: ":server:test", className: "org.es.FooTest", methodName: "testFoo", result: "FAILURE" },
                { taskPath: ":modules:test", className: "org.es.BarTest", methodName: "testBar", result: "SUCCESS" },
              ],
            }),
          );
        },
      }),
    );

    expect(downloadedFromJobId).toBe("explicit-origin");
    expect(result.status).toBe("enabled");
  });

  test("uses empty testseed when not provided", async () => {
    const result = await runSmartRetry(
      makeEnv({ testsSeed: undefined }),
      makeDeps({
        downloadArtifact: async () =>
          makeMultiRun(
            makeReport({
              tasks: [
                { path: ":server:test", outcome: "FAILED" },
                { path: ":modules:test", outcome: "SUCCESS" },
              ],
              tests: [
                { taskPath: ":server:test", className: "org.es.FooTest", methodName: "testFoo", result: "FAILURE" },
              ],
            }),
          ),
      }),
    );

    expect(result.failedTestHistory!.testseed).toBe("");
  });

  test("annotation uses fallback job name when origin job not found", async () => {
    const build = makeBuildJson({
      jobs: [{ id: "job-1" }, { id: "job-2", retry_source: { job_id: "job-1" } }],
    });

    const result = await runSmartRetry(
      makeEnv(),
      makeDeps({
        fetchBuildJson: async () => build,
        downloadArtifact: async () =>
          makeMultiRun(
            makeReport({
              tasks: [
                { path: ":server:test", outcome: "FAILED" },
                { path: ":modules:test", outcome: "SUCCESS" },
              ],
              tests: [
                { taskPath: ":server:test", className: "org.es.FooTest", methodName: "testFoo", result: "FAILURE" },
              ],
            }),
          ),
      }),
    );

    expect(result.annotation).toContain("[previous attempt]");
  });

  test("multi-run: task fixed in run 2 becomes successful", async () => {
    const result = await runSmartRetry(
      makeEnv(),
      makeDeps({
        downloadArtifact: async () =>
          makeMultiRun(
            makeReport({
              tasks: [
                { path: ":server:test", outcome: "FAILED" },
                { path: ":modules:test", outcome: "SUCCESS" },
              ],
              tests: [
                { taskPath: ":server:test", className: "org.es.FooTest", methodName: "testA", result: "FAILURE" },
                { taskPath: ":modules:test", className: "org.es.ModTest", methodName: "testC", result: "SUCCESS" },
              ],
            }),
            makeReport({
              tasks: [
                { path: ":server:test", outcome: "SUCCESS" },
                { path: ":modules:test", outcome: "SKIPPED" },
              ],
              tests: [
                { taskPath: ":server:test", className: "org.es.FooTest", methodName: "testA", result: "SUCCESS" },
              ],
            }),
          ),
      }),
    );

    expect(result.status).toBe("enabled");
    expect(result.failedTestHistory!.successfulTasks).toContain(":modules:test");
    expect(result.failedTestHistory!.successfulTasks).toContain(":server:test");
    expect(result.annotation).toContain("**Previous Runs Analyzed:** 2");
    expect(result.metadata["smart-retry-previous-runs"]).toBe("2");
  });

  test("multi-run: all failures fixed across runs → enabled with all tasks successful", async () => {
    const result = await runSmartRetry(
      makeEnv(),
      makeDeps({
        downloadArtifact: async () =>
          makeMultiRun(
            makeReport({
              tasks: [{ path: ":server:test", outcome: "FAILED" }],
              tests: [
                { taskPath: ":server:test", className: "org.es.FooTest", methodName: "testA", result: "FAILURE" },
              ],
            }),
            makeReport({
              tasks: [{ path: ":server:test", outcome: "SUCCESS" }],
              tests: [
                { taskPath: ":server:test", className: "org.es.FooTest", methodName: "testA", result: "SUCCESS" },
              ],
            }),
          ),
      }),
    );

    expect(result.status).toBe("enabled");
    expect(result.failedTestHistory!.successfulTasks).toEqual([":server:test"]);
  });

  test("no annotation when previous run was purely preempted (INTERRUPTED, no failures)", async () => {
    const result = await runSmartRetry(
      makeEnv(),
      makeDeps({
        downloadArtifact: async () =>
          makeMultiRun(
            makeReport({
              tasks: [
                { path: ":server:test", outcome: "SUCCESS" },
                { path: ":modules:test", outcome: "INTERRUPTED" },
              ],
              tests: [
                { taskPath: ":server:test", className: "org.es.FooTest", methodName: "testA", result: "SUCCESS" },
              ],
            }),
          ),
      }),
    );

    expect(result.status).toBe("enabled");
    expect(result.failedTestHistory!.successfulTasks).toEqual([":server:test"]);
    expect(result.annotation).toBeNull();
    expect(result.metadata["smart-retry-status"]).toBe("enabled");
  });

  test("annotation present when previous run had FAILED tasks", async () => {
    const result = await runSmartRetry(
      makeEnv(),
      makeDeps({
        downloadArtifact: async () =>
          makeMultiRun(
            makeReport({
              tasks: [
                { path: ":server:test", outcome: "FAILED" },
                { path: ":modules:test", outcome: "SUCCESS" },
              ],
              tests: [
                { taskPath: ":server:test", className: "org.es.FooTest", methodName: "testA", result: "SUCCESS" },
                { taskPath: ":modules:test", className: "org.es.BarTest", methodName: "testB", result: "SUCCESS" },
              ],
            }),
          ),
      }),
    );

    expect(result.status).toBe("enabled");
    expect(result.annotation).not.toBeNull();
    expect(result.annotation).toContain("Rerunning failed build job");
  });

  test("multi-run: task SUCCESS in early run preserved despite SKIPPED later", async () => {
    const result = await runSmartRetry(
      makeEnv(),
      makeDeps({
        downloadArtifact: async () =>
          makeMultiRun(
            makeReport({
              tasks: [
                { path: ":server:test", outcome: "SUCCESS" },
                { path: ":plugins:test", outcome: "FAILED" },
              ],
              tests: [
                { taskPath: ":server:test", className: "org.es.FooTest", methodName: "testA", result: "SUCCESS" },
                { taskPath: ":plugins:test", className: "org.es.BarTest", methodName: "testB", result: "FAILURE" },
              ],
            }),
            makeReport({
              tasks: [
                { path: ":server:test", outcome: "SKIPPED" },
                { path: ":plugins:test", outcome: "FAILED" },
              ],
              tests: [
                { taskPath: ":plugins:test", className: "org.es.BarTest", methodName: "testB", result: "FAILURE" },
              ],
            }),
          ),
      }),
    );

    expect(result.status).toBe("enabled");
    expect(result.failedTestHistory!.successfulTasks).toEqual([":server:test"]);
  });
});
