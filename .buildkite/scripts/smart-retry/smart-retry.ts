import type {
  TaskStatusReport,
  FailedTestsReport,
  WorkUnit,
  TestClassEntry,
  TestMethodEntry,
} from "./types";

/**
 * Transforms a TaskStatusReport (from task-status.json) into a FailedTestsReport
 * (.failed-test-history.json) for consumption by InternalTestRerunPlugin.
 */
export function transformTaskStatus(report: TaskStatusReport, testseed: string): FailedTestsReport {
  const executedTestTasks = uniqueSorted(report.tests.map((t) => t.taskPath));

  const failedTaskPaths = new Set(report.tasks.filter((t) => t.outcome === "FAILED").map((t) => t.path));

  const failedTests = report.tests.filter((t) => t.result === "FAILURE");
  const tasksWithTestFailures = new Set(failedTests.map((t) => t.taskPath));

  const workUnits = buildWorkUnits(failedTests);

  const failedTestTasks = [...failedTaskPaths]
    .filter((p) => !tasksWithTestFailures.has(p) && executedTestTasks.includes(p))
    .sort();

  return {
    workUnits,
    testseed,
    executedTestTasks,
    failedTestTasks,
  };
}

function buildWorkUnits(failedTests: TaskStatusReport["tests"]): WorkUnit[] {
  const byTask = groupBy(failedTests, (t) => t.taskPath);

  return Object.entries(byTask)
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([taskPath, tests]) => ({
      name: taskPath,
      outcome: "failed",
      tests: buildTestClasses(tests),
    }));
}

function buildTestClasses(tests: TaskStatusReport["tests"]): TestClassEntry[] {
  const byClass = groupBy(tests, (t) => t.className);

  return Object.entries(byClass)
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([className, methods]) => ({
      name: className,
      outcome: { overall: "failed", own: "passed", children: "failed" },
      children: methods.map(
        (m): TestMethodEntry => ({
          name: m.methodName,
          outcome: { overall: "failed" },
          children: [],
        })
      ),
    }));
}

function groupBy<T>(items: T[], keyFn: (item: T) => string): Record<string, T[]> {
  const result: Record<string, T[]> = {};
  for (const item of items) {
    const key = keyFn(item);
    (result[key] ??= []).push(item);
  }
  return result;
}

function uniqueSorted(arr: string[]): string[] {
  return [...new Set(arr)].sort();
}
