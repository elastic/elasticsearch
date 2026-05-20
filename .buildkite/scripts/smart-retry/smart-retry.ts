import type {
  TaskStatusReport,
  MultiRunTaskStatus,
  FailedTestsReport,
  WorkUnit,
  TestClassEntry,
  TestMethodEntry,
  SmartRetryEnv,
  SmartRetryResult,
  SmartRetryDeps,
  BuildkiteBuildJson,
  TaskEntry,
  TestEntry,
} from "./types";

// ---------------------------------------------------------------------------
// Multi-run helpers
// ---------------------------------------------------------------------------

/**
 * Normalizes a raw artifact into the multi-run format. Handles both the legacy
 * single-run shape ({tasks, tests, cancelled}) and the new multi-run shape ({runs}).
 */
export function normalizeTaskStatus(raw: unknown): MultiRunTaskStatus {
  if (raw != null && typeof raw === "object" && "runs" in raw && Array.isArray((raw as MultiRunTaskStatus).runs)) {
    return raw as MultiRunTaskStatus;
  }
  return { runs: [raw as TaskStatusReport] };
}

/**
 * Wraps a single TaskStatusReport (from the current Gradle build) into the
 * multi-run format, prepending any previous runs from an earlier artifact.
 */
export function wrapTaskStatus(current: TaskStatusReport, previous: MultiRunTaskStatus | null): MultiRunTaskStatus {
  const previousRuns = previous?.runs ?? [];
  return { runs: [...previousRuns, current] };
}

/**
 * Merges all runs into a single TaskStatusReport for smart-retry processing.
 *
 * The merge is "last non-skippable wins" — a task/test that completed
 * successfully in any run is treated as successful. Specifically:
 *
 * Tasks: a task that was SUCCESS/UP_TO_DATE/FROM_CACHE in any run keeps
 * that outcome. FAILED/INTERRUPTED outcomes from later runs override earlier
 * successes only if the task actually re-ran (was not skipped by smart-retry).
 * SKIPPED/NOT_RUN outcomes never override a prior definitive result.
 *
 * Tests: all test entries across runs are collected. If the same
 * (taskPath, className, methodName) appears in multiple runs, SUCCESS in
 * any run makes it SUCCESS (the test passed at least once).
 */
export function mergeRuns(multi: MultiRunTaskStatus): TaskStatusReport {
  if (multi.runs.length === 0) {
    return { tasks: [], tests: [], cancelled: false };
  }
  if (multi.runs.length === 1) {
    return multi.runs[0];
  }

  const taskMap = new Map<string, TaskEntry>();
  for (const run of multi.runs) {
    for (const task of run.tasks) {
      const existing = taskMap.get(task.path);
      if (!existing) {
        taskMap.set(task.path, task);
      } else {
        taskMap.set(task.path, { path: task.path, outcome: pickTaskOutcome(existing.outcome, task.outcome) });
      }
    }
  }

  const testMap = new Map<string, TestEntry>();
  for (const run of multi.runs) {
    for (const test of run.tests) {
      const key = `${test.taskPath}\0${test.className}\0${test.methodName}`;
      const existing = testMap.get(key);
      if (!existing) {
        testMap.set(key, test);
      } else {
        testMap.set(key, {
          ...test,
          result: pickTestResult(existing.result, test.result),
        });
      }
    }
  }

  const cancelled = multi.runs[multi.runs.length - 1].cancelled;

  return {
    tasks: [...taskMap.values()].sort((a, b) => a.path.localeCompare(b.path)),
    tests: [...testMap.values()].sort(
      (a, b) => a.taskPath.localeCompare(b.taskPath) || a.className.localeCompare(b.className) || a.methodName.localeCompare(b.methodName)
    ),
    cancelled,
  };
}

const TASK_OUTCOME_RANK: Record<string, number> = {
  SUCCESS: 3,
  UP_TO_DATE: 3,
  FROM_CACHE: 3,
  FAILED: 2,
  INTERRUPTED: 1,
  NOT_RUN: 0,
  SKIPPED: 0,
};

function pickTaskOutcome(a: TaskEntry["outcome"], b: TaskEntry["outcome"]): TaskEntry["outcome"] {
  const rankA = TASK_OUTCOME_RANK[a] ?? 0;
  const rankB = TASK_OUTCOME_RANK[b] ?? 0;
  return rankA >= rankB ? a : b;
}

function pickTestResult(a: TestEntry["result"], b: TestEntry["result"]): TestEntry["result"] {
  if (a === "SUCCESS" || b === "SUCCESS") return "SUCCESS";
  if (a === "FAILURE" || b === "FAILURE") return "FAILURE";
  return "SKIPPED";
}

// ---------------------------------------------------------------------------
// Transform
// ---------------------------------------------------------------------------

/**
 * Transforms a TaskStatusReport (from task-status.json, possibly merged from
 * multiple runs) into a FailedTestsReport (.failed-test-history.json) for
 * consumption by InternalTestRerunPlugin.
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

// ---------------------------------------------------------------------------
// Orchestrator
// ---------------------------------------------------------------------------

/**
 * Orchestrates the full smart-retry flow: resolve origin job, download the
 * task-status artifact, merge all runs, transform, and decide what to report.
 *
 * Pure decision logic — all I/O is injected via `deps` so tests can stub it.
 */
export async function runSmartRetry(env: SmartRetryEnv, deps: SmartRetryDeps): Promise<SmartRetryResult> {
  const fail = (details: string): SmartRetryResult => ({
    status: "failed",
    details,
    failedTestHistory: null,
    annotation: null,
    metadata: { "smart-retry-status": "failed", "smart-retry-details": details },
  });

  const buildJson = await deps.fetchBuildJson(env.buildkiteApiToken, env.buildkitePipelineSlug, env.buildkiteBuildNumber);
  if (!buildJson) {
    return fail("Buildkite API request failed");
  }

  const originJobId = resolveOriginJobId(buildJson, env.buildkiteJobId, env.originJobId);
  if (!originJobId) {
    return fail("No origin job ID found");
  }

  const multiRun = await deps.downloadArtifact(originJobId);
  if (!multiRun) {
    return fail("Failed to download task-status artifact");
  }

  const merged = mergeRuns(multiRun);
  const testseed = env.testsSeed ?? "";
  const report = transformTaskStatus(merged, testseed);
  const workUnitCount = report.workUnits.length;
  const failedTaskCount = report.failedTestTasks.length;
  const executedCount = report.executedTestTasks.length;

  if (workUnitCount === 0 && failedTaskCount === 0) {
    return {
      status: "disabled",
      details: "Previous failure was not caused by test or task failures — rerunning all tests",
      failedTestHistory: null,
      annotation: null,
      metadata: {
        "smart-retry-status": "disabled",
        "smart-retry-details": "Previous failure was not caused by test or task failures — rerunning all tests",
        "smart-retry-disabled-reason": "no-test-or-task-failures",
      },
    };
  }

  const originJobName = resolveOriginJobName(buildJson, originJobId);
  const runCount = multiRun.runs.length;
  const details = `Filtering to ${workUnitCount} work units with test failures, ${failedTaskCount} tasks with non-test failures (across ${runCount} previous run${runCount !== 1 ? "s" : ""})`;

  const annotation = [
    `Rerunning failed build job [${originJobName}]`,
    "",
    `**Gradle Tasks with Test Failures:** ${workUnitCount}`,
    `**Gradle Tasks with Non-Test Failures:** ${failedTaskCount}`,
    `**Executed Test Tasks in Previous Runs:** ${executedCount}`,
    `**Previous Runs Analyzed:** ${runCount}`,
    "",
    "This retry will rerun failed tests, rerun all tests for tasks that failed at the Gradle level (e.g. resource leaks), skip confirmed-passed tasks, and run all tests for tasks not executed in previous runs.",
  ].join("\n");

  const metadata: Record<string, string> = {
    "smart-retry-status": "enabled",
    "smart-retry-details": details,
    "smart-retry-work-units": String(workUnitCount),
    "smart-retry-executed-tasks": String(executedCount),
    "smart-retry-previous-runs": String(runCount),
  };
  if (failedTaskCount > 0) {
    metadata["smart-retry-failed-tasks"] = String(failedTaskCount);
  }

  return {
    status: "enabled",
    details,
    failedTestHistory: report,
    annotation,
    metadata,
  };
}

export function resolveOriginJobId(
  buildJson: BuildkiteBuildJson,
  currentJobId: string,
  explicitOriginJobId?: string
): string | null {
  if (explicitOriginJobId && explicitOriginJobId !== "null") {
    return explicitOriginJobId;
  }
  const currentJob = buildJson.jobs.find((j) => j.id === currentJobId);
  const resolved = currentJob?.retry_source?.job_id ?? null;
  return resolved && resolved !== "null" ? resolved : null;
}

export function resolveOriginJobName(buildJson: BuildkiteBuildJson, originJobId: string): string {
  const job = buildJson.jobs.find((j) => j.id === originJobId);
  return job?.name && job.name !== "null" ? job.name : "previous attempt";
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

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
