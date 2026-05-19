import { describe, expect, test } from "bun:test";

import { transformTaskStatus } from "./smart-retry";
import type { TaskStatusReport, FailedTestsReport } from "./types";

function makeReport(overrides: Partial<TaskStatusReport> = {}): TaskStatusReport {
  return {
    tasks: [],
    tests: [],
    cancelled: false,
    ...overrides,
  };
}

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

    // Two test classes with failures
    expect(result.workUnits[0].tests).toHaveLength(2);

    const barTest = result.workUnits[0].tests.find((t) => t.name === "org.es.BarTest")!;
    expect(barTest.children).toHaveLength(1);
    expect(barTest.children[0].name).toBe("testBaz");
    expect(barTest.children[0].outcome).toEqual({ overall: "failed" });
    expect(barTest.children[0].children).toEqual([]);

    const fooTest = result.workUnits[0].tests.find((t) => t.name === "org.es.FooTest")!;
    expect(fooTest.children).toHaveLength(1);
    expect(fooTest.children[0].name).toBe("testFoo");

    // Only the failed method should be in work units, not testBar which passed
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

    // :server:test FAILED but all individual tests passed → resource leak / Gradle-level failure
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

    // compileJava failed but is not a test task (not in tests array), so excluded
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

    // :server:test → workUnits (has test failure)
    expect(result.workUnits).toHaveLength(1);
    expect(result.workUnits[0].name).toBe(":server:test");

    // :plugins:test → failedTestTasks (FAILED at Gradle, no test failures)
    expect(result.failedTestTasks).toEqual([":plugins:test"]);

    // :modules:test, :plugins:test, :server:test all executed
    expect(result.executedTestTasks).toEqual([":modules:test", ":plugins:test", ":server:test"]);

    // :libs:test has no test entries (SKIPPED), so not in executedTestTasks
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

    // Only :server:test had test entries
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
    const workUnit = result.workUnits[0];

    // AlphaTest: two failed methods
    const alpha = workUnit.tests.find((t) => t.name === "org.es.AlphaTest")!;
    expect(alpha.children).toHaveLength(2);
    expect(alpha.children.map((c) => c.name).sort()).toEqual(["testA1", "testA2"]);

    // BetaTest: one failed method (testB2 passed, excluded)
    const beta = workUnit.tests.find((t) => t.name === "org.es.BetaTest")!;
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
    expect(testClass.outcome).toEqual({
      overall: "failed",
      own: "passed",
      children: "failed",
    });

    const testMethod = testClass.children[0];
    expect(testMethod.outcome).toEqual({ overall: "failed" });
    expect(testMethod.children).toEqual([]);
  });
});
