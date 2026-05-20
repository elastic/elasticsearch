import type {
  TaskStatusReport,
  FailedTestsReport,
  WorkUnit,
  TestClassEntry,
  TestMethodEntry,
  SmartRetryEnv,
  SmartRetryResult,
  SmartRetryDeps,
  BuildkiteBuildJson,
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

/**
 * Orchestrates the full smart-retry flow: resolve origin job, download the
 * task-status artifact, transform it, and decide what to report.
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

  const taskStatus = await deps.downloadArtifact(originJobId);
  if (!taskStatus) {
    return fail("Failed to download task-status artifact");
  }

  const testseed = env.testsSeed ?? "";
  const report = transformTaskStatus(taskStatus, testseed);
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
  const details = `Filtering to ${workUnitCount} work units with test failures, ${failedTaskCount} tasks with non-test failures`;

  const annotation = [
    `Rerunning failed build job [${originJobName}]`,
    "",
    `**Gradle Tasks with Test Failures:** ${workUnitCount}`,
    `**Gradle Tasks with Non-Test Failures:** ${failedTaskCount}`,
    `**Executed Test Tasks in Previous Run:** ${executedCount}`,
    "",
    "This retry will rerun failed tests, rerun all tests for tasks that failed at the Gradle level (e.g. resource leaks), skip confirmed-passed tasks, and run all tests for tasks not executed in the previous run.",
  ].join("\n");

  const metadata: Record<string, string> = {
    "smart-retry-status": "enabled",
    "smart-retry-details": details,
    "smart-retry-work-units": String(workUnitCount),
    "smart-retry-executed-tasks": String(executedCount),
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
