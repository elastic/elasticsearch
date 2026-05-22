import { describe, expect, test } from "vitest";

import { classifyRuns, buildkiteInlineLink } from "./summarize-task-status.ts";
import type { TaskStatusReport, MultiRunTaskStatus } from "./types.ts";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeReport(overrides: Partial<TaskStatusReport> = {}): TaskStatusReport {
  return { tasks: [], tests: [], cancelled: false, ...overrides };
}

function makeMultiRun(...runs: TaskStatusReport[]): MultiRunTaskStatus {
  return { runs };
}

/** Extract just the paths from TaskRef[] for concise assertions. */
function paths(refs: { path: string }[]): string[] {
  return refs.map((r) => r.path);
}

/** Extract just task+test from TestRef[] for concise assertions. */
function testKeys(refs: { task: string; test: string }[]): string[] {
  return refs.map((r) => `${r.task} ${r.test}`);
}

// ---------------------------------------------------------------------------
// classifyRuns
// ---------------------------------------------------------------------------

describe("classifyRuns", () => {
  test("empty runs returns all-empty summary", () => {
    const result = classifyRuns({ runs: [] });
    expect(result.totalRuns).toBe(0);
    expect(result.failedTasks).toEqual([]);
    expect(result.failedTests).toEqual([]);
    expect(result.flakyTasks).toEqual([]);
    expect(result.flakyTests).toEqual([]);
    expect(result.interruptedTasks).toEqual([]);
  });

  test("single run — all passing", () => {
    const result = classifyRuns(
      makeMultiRun(
        makeReport({
          tasks: [
            { path: ":server:test", outcome: "SUCCESS" },
            { path: ":server:compileJava", outcome: "UP_TO_DATE" },
          ],
          tests: [{ taskPath: ":server:test", className: "FooTests", methodName: "testBar", result: "SUCCESS" }],
        }),
      ),
    );
    expect(result.totalRuns).toBe(1);
    expect(result.failedTasks).toEqual([]);
    expect(result.failedTests).toEqual([]);
    expect(result.flakyTasks).toEqual([]);
    expect(result.flakyTests).toEqual([]);
    expect(result.interruptedTasks).toEqual([]);
  });

  test("single run — failed task", () => {
    const result = classifyRuns(
      makeMultiRun(
        makeReport({
          tasks: [
            { path: ":server:test", outcome: "FAILED" },
            { path: ":server:compileJava", outcome: "SUCCESS" },
          ],
        }),
      ),
    );
    expect(paths(result.failedTasks)).toEqual([":server:test"]);
    expect(result.flakyTasks).toEqual([]);
  });

  test("single run — failed tests", () => {
    const result = classifyRuns(
      makeMultiRun(
        makeReport({
          tasks: [{ path: ":server:test", outcome: "FAILED" }],
          tests: [
            { taskPath: ":server:test", className: "FooTests", methodName: "testA", result: "FAILURE" },
            { taskPath: ":server:test", className: "FooTests", methodName: "testB", result: "SUCCESS" },
          ],
        }),
      ),
    );
    expect(testKeys(result.failedTests)).toEqual([":server:test FooTests#testA"]);
  });

  test("single run — interrupted tasks", () => {
    const result = classifyRuns(
      makeMultiRun(
        makeReport({
          tasks: [
            { path: ":server:test", outcome: "INTERRUPTED" },
            { path: ":x-pack:test", outcome: "INTERRUPTED" },
            { path: ":server:compileJava", outcome: "SUCCESS" },
          ],
        }),
      ),
    );
    expect(paths(result.interruptedTasks)).toEqual([":server:test", ":x-pack:test"]);
    expect(result.failedTasks).toEqual([]);
  });

  test("single run — no flaky without previous runs", () => {
    const result = classifyRuns(
      makeMultiRun(
        makeReport({
          tasks: [{ path: ":server:test", outcome: "FAILED" }],
          tests: [{ taskPath: ":server:test", className: "FooTests", methodName: "testA", result: "FAILURE" }],
        }),
      ),
    );
    expect(result.flakyTasks).toEqual([]);
    expect(result.flakyTests).toEqual([]);
  });

  test("two runs — task failed then passed is flaky", () => {
    const result = classifyRuns(
      makeMultiRun(
        makeReport({ tasks: [{ path: ":server:test", outcome: "FAILED" }] }),
        makeReport({ tasks: [{ path: ":server:test", outcome: "SUCCESS" }] }),
      ),
    );
    expect(result.failedTasks).toEqual([]);
    expect(paths(result.flakyTasks)).toEqual([":server:test"]);
  });

  test("two runs — task failed both times is failed, not flaky", () => {
    const result = classifyRuns(
      makeMultiRun(
        makeReport({ tasks: [{ path: ":server:test", outcome: "FAILED" }] }),
        makeReport({ tasks: [{ path: ":server:test", outcome: "FAILED" }] }),
      ),
    );
    expect(paths(result.failedTasks)).toEqual([":server:test"]);
    expect(result.flakyTasks).toEqual([]);
  });

  test("two runs — test failed then passed is flaky", () => {
    const result = classifyRuns(
      makeMultiRun(
        makeReport({
          tasks: [{ path: ":server:test", outcome: "FAILED" }],
          tests: [{ taskPath: ":server:test", className: "FooTests", methodName: "testA", result: "FAILURE" }],
        }),
        makeReport({
          tasks: [{ path: ":server:test", outcome: "SUCCESS" }],
          tests: [{ taskPath: ":server:test", className: "FooTests", methodName: "testA", result: "SUCCESS" }],
        }),
      ),
    );
    expect(result.failedTests).toEqual([]);
    expect(testKeys(result.flakyTests)).toEqual([":server:test FooTests#testA"]);
  });

  test("two runs — test failed both times is failed, not flaky", () => {
    const result = classifyRuns(
      makeMultiRun(
        makeReport({
          tests: [{ taskPath: ":server:test", className: "FooTests", methodName: "testA", result: "FAILURE" }],
        }),
        makeReport({
          tests: [{ taskPath: ":server:test", className: "FooTests", methodName: "testA", result: "FAILURE" }],
        }),
      ),
    );
    expect(testKeys(result.failedTests)).toEqual([":server:test FooTests#testA"]);
    expect(result.flakyTests).toEqual([]);
  });

  test("three runs — failure only in middle run is flaky", () => {
    const result = classifyRuns(
      makeMultiRun(
        makeReport({ tasks: [{ path: ":server:test", outcome: "SUCCESS" }] }),
        makeReport({ tasks: [{ path: ":server:test", outcome: "FAILED" }] }),
        makeReport({ tasks: [{ path: ":server:test", outcome: "SUCCESS" }] }),
      ),
    );
    expect(result.failedTasks).toEqual([]);
    expect(paths(result.flakyTasks)).toEqual([":server:test"]);
  });

  test("mixed — some tasks failed, some flaky, some interrupted", () => {
    const result = classifyRuns(
      makeMultiRun(
        makeReport({
          tasks: [
            { path: ":server:test", outcome: "FAILED" },
            { path: ":modules:painless:test", outcome: "FAILED" },
            { path: ":x-pack:esql:test", outcome: "SUCCESS" },
          ],
          tests: [
            { taskPath: ":server:test", className: "FooTests", methodName: "testA", result: "FAILURE" },
            { taskPath: ":modules:painless:test", className: "DefTests", methodName: "testCast", result: "FAILURE" },
          ],
        }),
        makeReport({
          tasks: [
            { path: ":server:test", outcome: "SUCCESS" },
            { path: ":modules:painless:test", outcome: "FAILED" },
            { path: ":x-pack:esql:test", outcome: "INTERRUPTED" },
          ],
          tests: [
            { taskPath: ":server:test", className: "FooTests", methodName: "testA", result: "SUCCESS" },
            { taskPath: ":modules:painless:test", className: "DefTests", methodName: "testCast", result: "FAILURE" },
          ],
          cancelled: true,
          preemptedAt: "2026-05-21T15:00:00Z",
        }),
      ),
    );
    expect(paths(result.failedTasks)).toEqual([":modules:painless:test"]);
    expect(testKeys(result.failedTests)).toEqual([":modules:painless:test DefTests#testCast"]);
    expect(paths(result.flakyTasks)).toEqual([":server:test"]);
    expect(testKeys(result.flakyTests)).toEqual([":server:test FooTests#testA"]);
    expect(paths(result.interruptedTasks)).toEqual([":x-pack:esql:test"]);
    expect(result.preemptedAt).toBe("2026-05-21T15:00:00Z");
  });

  test("preemption-only — interrupted but no failures", () => {
    const result = classifyRuns(
      makeMultiRun(
        makeReport({
          tasks: [
            { path: ":server:test", outcome: "INTERRUPTED" },
            { path: ":server:compileJava", outcome: "SUCCESS" },
            { path: ":x-pack:test", outcome: "NOT_RUN" },
          ],
          cancelled: true,
          preemptedAt: "2026-05-21T14:00:00Z",
        }),
      ),
    );
    expect(result.failedTasks).toEqual([]);
    expect(result.failedTests).toEqual([]);
    expect(result.flakyTasks).toEqual([]);
    expect(result.flakyTests).toEqual([]);
    expect(paths(result.interruptedTasks)).toEqual([":server:test"]);
    expect(result.preemptedAt).toBe("2026-05-21T14:00:00Z");
  });

  test("SKIPPED and NOT_RUN tasks are not classified as anything", () => {
    const result = classifyRuns(
      makeMultiRun(
        makeReport({
          tasks: [
            { path: ":a", outcome: "SKIPPED" },
            { path: ":b", outcome: "NOT_RUN" },
            { path: ":c", outcome: "UP_TO_DATE" },
            { path: ":d", outcome: "FROM_CACHE" },
          ],
        }),
      ),
    );
    expect(result.failedTasks).toEqual([]);
    expect(result.flakyTasks).toEqual([]);
    expect(result.interruptedTasks).toEqual([]);
  });

  test("SKIPPED tests are not classified as failed or flaky", () => {
    const result = classifyRuns(
      makeMultiRun(
        makeReport({
          tests: [{ taskPath: ":server:test", className: "FooTests", methodName: "testA", result: "SKIPPED" }],
        }),
        makeReport({
          tests: [{ taskPath: ":server:test", className: "FooTests", methodName: "testA", result: "SKIPPED" }],
        }),
      ),
    );
    expect(result.failedTests).toEqual([]);
    expect(result.flakyTests).toEqual([]);
  });

  test("flaky task deduplicated across multiple previous runs", () => {
    const result = classifyRuns(
      makeMultiRun(
        makeReport({ tasks: [{ path: ":server:test", outcome: "FAILED" }] }),
        makeReport({ tasks: [{ path: ":server:test", outcome: "FAILED" }] }),
        makeReport({ tasks: [{ path: ":server:test", outcome: "SUCCESS" }] }),
      ),
    );
    expect(paths(result.flakyTasks)).toEqual([":server:test"]);
  });

  test("flaky test deduplicated across multiple previous runs", () => {
    const result = classifyRuns(
      makeMultiRun(
        makeReport({
          tests: [{ taskPath: ":server:test", className: "FooTests", methodName: "testA", result: "FAILURE" }],
        }),
        makeReport({
          tests: [{ taskPath: ":server:test", className: "FooTests", methodName: "testA", result: "FAILURE" }],
        }),
        makeReport({
          tests: [{ taskPath: ":server:test", className: "FooTests", methodName: "testA", result: "SUCCESS" }],
        }),
      ),
    );
    expect(testKeys(result.flakyTests)).toEqual([":server:test FooTests#testA"]);
  });

  test("interrupted task in previous run is not flaky", () => {
    const result = classifyRuns(
      makeMultiRun(
        makeReport({ tasks: [{ path: ":server:test", outcome: "INTERRUPTED" }] }),
        makeReport({ tasks: [{ path: ":server:test", outcome: "SUCCESS" }] }),
      ),
    );
    expect(result.flakyTasks).toEqual([]);
    expect(result.interruptedTasks).toEqual([]);
  });

  test("totalRuns reflects number of runs", () => {
    expect(classifyRuns(makeMultiRun(makeReport())).totalRuns).toBe(1);
    expect(classifyRuns(makeMultiRun(makeReport(), makeReport())).totalRuns).toBe(2);
    expect(classifyRuns(makeMultiRun(makeReport(), makeReport(), makeReport())).totalRuns).toBe(3);
  });

  test("preemptedAt comes from the current (final) run", () => {
    const result = classifyRuns(
      makeMultiRun(makeReport({ preemptedAt: "2026-05-21T14:00:00Z" }), makeReport({ preemptedAt: null })),
    );
    expect(result.preemptedAt).toBeNull();

    const result2 = classifyRuns(
      makeMultiRun(makeReport({ preemptedAt: null }), makeReport({ preemptedAt: "2026-05-21T15:00:00Z" })),
    );
    expect(result2.preemptedAt).toBe("2026-05-21T15:00:00Z");
  });

  test("multiple different flaky tasks and tests across runs", () => {
    const result = classifyRuns(
      makeMultiRun(
        makeReport({
          tasks: [
            { path: ":a:test", outcome: "FAILED" },
            { path: ":b:test", outcome: "SUCCESS" },
          ],
          tests: [{ taskPath: ":a:test", className: "ATests", methodName: "testX", result: "FAILURE" }],
        }),
        makeReport({
          tasks: [
            { path: ":a:test", outcome: "SUCCESS" },
            { path: ":b:test", outcome: "FAILED" },
          ],
          tests: [{ taskPath: ":b:test", className: "BTests", methodName: "testY", result: "FAILURE" }],
        }),
        makeReport({
          tasks: [
            { path: ":a:test", outcome: "SUCCESS" },
            { path: ":b:test", outcome: "SUCCESS" },
          ],
        }),
      ),
    );
    expect(result.failedTasks).toEqual([]);
    expect(paths(result.flakyTasks)).toEqual([":a:test", ":b:test"]);
    expect(testKeys(result.flakyTests)).toEqual([":a:test ATests#testX", ":b:test BTests#testY"]);
  });

  // --- Build scan association ---

  test("build scans collected from all runs that have them", () => {
    const result = classifyRuns(
      makeMultiRun(
        makeReport({ buildScanId: "scan-1", buildScanUrl: "https://ge.elastic.co/s/scan-1" }),
        makeReport({ buildScanId: "scan-2", buildScanUrl: "https://ge.elastic.co/s/scan-2" }),
      ),
    );
    expect(result.buildScans).toEqual([
      { id: "scan-1", url: "https://ge.elastic.co/s/scan-1" },
      { id: "scan-2", url: "https://ge.elastic.co/s/scan-2" },
    ]);
  });

  test("runs without build scan are excluded from buildScans", () => {
    const result = classifyRuns(
      makeMultiRun(makeReport({}), makeReport({ buildScanId: "scan-2", buildScanUrl: "https://ge.elastic.co/s/scan-2" })),
    );
    expect(result.buildScans).toEqual([{ id: "scan-2", url: "https://ge.elastic.co/s/scan-2" }]);
  });

  test("runs with null build scan fields are excluded", () => {
    const result = classifyRuns(makeMultiRun(makeReport({ buildScanId: null, buildScanUrl: null })));
    expect(result.buildScans).toEqual([]);
  });

  test("no build scans when none present", () => {
    const result = classifyRuns(makeMultiRun(makeReport()));
    expect(result.buildScans).toEqual([]);
  });

  test("failed tasks carry build scan URL from the current run", () => {
    const result = classifyRuns(
      makeMultiRun(
        makeReport({
          tasks: [{ path: ":server:test", outcome: "FAILED" }],
          buildScanId: "scan-1",
          buildScanUrl: "https://ge.elastic.co/s/scan-1",
        }),
      ),
    );
    expect(result.failedTasks).toEqual([{ path: ":server:test", buildScanUrl: "https://ge.elastic.co/s/scan-1" }]);
  });

  test("failed tests carry build scan URL from the current run", () => {
    const result = classifyRuns(
      makeMultiRun(
        makeReport({
          tests: [{ taskPath: ":server:test", className: "FooTests", methodName: "testA", result: "FAILURE" }],
          buildScanId: "scan-1",
          buildScanUrl: "https://ge.elastic.co/s/scan-1",
        }),
      ),
    );
    expect(result.failedTests).toEqual([
      { task: ":server:test", test: "FooTests#testA", buildScanUrl: "https://ge.elastic.co/s/scan-1" },
    ]);
  });

  test("interrupted tasks carry build scan URL from the current run", () => {
    const result = classifyRuns(
      makeMultiRun(
        makeReport({
          tasks: [{ path: ":server:test", outcome: "INTERRUPTED" }],
          buildScanId: "scan-1",
          buildScanUrl: "https://ge.elastic.co/s/scan-1",
        }),
      ),
    );
    expect(result.interruptedTasks).toEqual([{ path: ":server:test", buildScanUrl: "https://ge.elastic.co/s/scan-1" }]);
  });

  test("flaky tasks carry build scan URL from the previous run where they failed", () => {
    const result = classifyRuns(
      makeMultiRun(
        makeReport({
          tasks: [{ path: ":server:test", outcome: "FAILED" }],
          buildScanId: "scan-1",
          buildScanUrl: "https://ge.elastic.co/s/scan-1",
        }),
        makeReport({
          tasks: [{ path: ":server:test", outcome: "SUCCESS" }],
          buildScanId: "scan-2",
          buildScanUrl: "https://ge.elastic.co/s/scan-2",
        }),
      ),
    );
    expect(result.flakyTasks).toEqual([{ path: ":server:test", buildScanUrl: "https://ge.elastic.co/s/scan-1" }]);
  });

  test("flaky tests carry build scan URL from the previous run where they failed", () => {
    const result = classifyRuns(
      makeMultiRun(
        makeReport({
          tests: [{ taskPath: ":server:test", className: "FooTests", methodName: "testA", result: "FAILURE" }],
          buildScanId: "scan-1",
          buildScanUrl: "https://ge.elastic.co/s/scan-1",
        }),
        makeReport({
          tests: [{ taskPath: ":server:test", className: "FooTests", methodName: "testA", result: "SUCCESS" }],
          buildScanId: "scan-2",
          buildScanUrl: "https://ge.elastic.co/s/scan-2",
        }),
      ),
    );
    expect(result.flakyTests).toEqual([
      { task: ":server:test", test: "FooTests#testA", buildScanUrl: "https://ge.elastic.co/s/scan-1" },
    ]);
  });

  test("entries without build scan URL have undefined buildScanUrl", () => {
    const result = classifyRuns(
      makeMultiRun(
        makeReport({
          tasks: [
            { path: ":server:test", outcome: "FAILED" },
            { path: ":other:test", outcome: "INTERRUPTED" },
          ],
          tests: [{ taskPath: ":server:test", className: "FooTests", methodName: "testA", result: "FAILURE" }],
        }),
      ),
    );
    expect(result.failedTasks[0].buildScanUrl).toBeUndefined();
    expect(result.failedTests[0].buildScanUrl).toBeUndefined();
    expect(result.interruptedTasks[0].buildScanUrl).toBeUndefined();
  });

  test("flaky task uses first previous run's scan URL when failed in multiple previous runs", () => {
    const result = classifyRuns(
      makeMultiRun(
        makeReport({
          tasks: [{ path: ":server:test", outcome: "FAILED" }],
          buildScanId: "scan-1",
          buildScanUrl: "https://ge.elastic.co/s/scan-1",
        }),
        makeReport({
          tasks: [{ path: ":server:test", outcome: "FAILED" }],
          buildScanId: "scan-2",
          buildScanUrl: "https://ge.elastic.co/s/scan-2",
        }),
        makeReport({
          tasks: [{ path: ":server:test", outcome: "SUCCESS" }],
          buildScanId: "scan-3",
          buildScanUrl: "https://ge.elastic.co/s/scan-3",
        }),
      ),
    );
    expect(result.flakyTasks).toEqual([{ path: ":server:test", buildScanUrl: "https://ge.elastic.co/s/scan-1" }]);
  });
});

// ---------------------------------------------------------------------------
// buildkiteInlineLink
// ---------------------------------------------------------------------------

describe("buildkiteInlineLink", () => {
  test("url only", () => {
    const link = buildkiteInlineLink("https://example.com");
    expect(link).toBe("\x1b]1339;url='https://example.com'\x07");
  });

  test("url with content", () => {
    const link = buildkiteInlineLink("https://example.com/scan", "Build Scan");
    expect(link).toBe("\x1b]1339;url='https://example.com/scan';content='Build Scan'\x07");
  });
});
