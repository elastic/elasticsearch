import { describe, expect, test } from "vitest";

import { classifyRuns } from "./summarize-task-status.ts";
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
    expect(result.failedTasks).toEqual([":server:test"]);
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
    expect(result.failedTests).toEqual([{ task: ":server:test", test: "FooTests#testA" }]);
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
    expect(result.interruptedTasks).toEqual([":server:test", ":x-pack:test"]);
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
    expect(result.flakyTasks).toEqual([":server:test"]);
  });

  test("two runs — task failed both times is failed, not flaky", () => {
    const result = classifyRuns(
      makeMultiRun(
        makeReport({ tasks: [{ path: ":server:test", outcome: "FAILED" }] }),
        makeReport({ tasks: [{ path: ":server:test", outcome: "FAILED" }] }),
      ),
    );
    expect(result.failedTasks).toEqual([":server:test"]);
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
    expect(result.flakyTests).toEqual([{ task: ":server:test", test: "FooTests#testA" }]);
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
    expect(result.failedTests).toEqual([{ task: ":server:test", test: "FooTests#testA" }]);
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
    expect(result.flakyTasks).toEqual([":server:test"]);
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
    expect(result.failedTasks).toEqual([":modules:painless:test"]);
    expect(result.failedTests).toEqual([{ task: ":modules:painless:test", test: "DefTests#testCast" }]);
    expect(result.flakyTasks).toEqual([":server:test"]);
    expect(result.flakyTests).toEqual([{ task: ":server:test", test: "FooTests#testA" }]);
    expect(result.interruptedTasks).toEqual([":x-pack:esql:test"]);
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
    expect(result.interruptedTasks).toEqual([":server:test"]);
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
    expect(result.flakyTasks).toEqual([":server:test"]);
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
    expect(result.flakyTests).toEqual([{ task: ":server:test", test: "FooTests#testA" }]);
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
      makeMultiRun(
        makeReport({ preemptedAt: "2026-05-21T14:00:00Z" }),
        makeReport({ preemptedAt: null }),
      ),
    );
    expect(result.preemptedAt).toBeNull();

    const result2 = classifyRuns(
      makeMultiRun(
        makeReport({ preemptedAt: null }),
        makeReport({ preemptedAt: "2026-05-21T15:00:00Z" }),
      ),
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
    expect(result.flakyTasks).toEqual([":a:test", ":b:test"]);
    expect(result.flakyTests).toEqual([
      { task: ":a:test", test: "ATests#testX" },
      { task: ":b:test", test: "BTests#testY" },
    ]);
  });
});
