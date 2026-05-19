/** Shape of task-status.json produced by TaskStatusTrackerPlugin. */
export interface TaskStatusReport {
  tasks: TaskEntry[];
  tests: TestEntry[];
  cancelled: boolean;
}

export interface TaskEntry {
  path: string;
  outcome: "SUCCESS" | "UP_TO_DATE" | "FROM_CACHE" | "FAILED" | "SKIPPED" | "INTERRUPTED" | "NOT_RUN";
}

export interface TestEntry {
  taskPath: string;
  className: string;
  methodName: string;
  result: "SUCCESS" | "FAILURE" | "SKIPPED";
}

/**
 * Shape of .failed-test-history.json consumed by InternalTestRerunPlugin.
 *
 * The plugin uses four-state logic per test task:
 *  1. Task in workUnits → rerun only failed tests
 *  2. Task in failedTestTasks (but not workUnits) → rerun all tests
 *  3. Task in executedTestTasks (but not 1 or 2) → skip (confirmed passed)
 *  4. Task not in executedTestTasks → run all tests (never executed)
 */
export interface FailedTestsReport {
  workUnits: WorkUnit[];
  testseed: string;
  executedTestTasks: string[];
  failedTestTasks: string[];
}

export interface WorkUnit {
  name: string;
  outcome: string;
  tests: TestClassEntry[];
}

export interface TestClassEntry {
  name: string;
  outcome: { overall: string; own: string; children: string };
  children: TestMethodEntry[];
}

export interface TestMethodEntry {
  name: string;
  outcome: { overall: string };
  children: never[];
}
