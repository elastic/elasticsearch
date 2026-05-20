import type {
  TaskStatusReport,
  MultiRunTaskStatus,
  FailedTestsReport,
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
 *
 * The plugin skips tasks in successfulTasks and runs everything else.
 * A task is "successful" if it ran with outcome SUCCESS/UP_TO_DATE/FROM_CACHE,
 * all its tests passed (no FAILURE results), and the task itself didn't FAIL.
 */
export function transformTaskStatus(report: TaskStatusReport, testseed: string): FailedTestsReport {
  const SUCCESSFUL_OUTCOMES = new Set(["SUCCESS", "UP_TO_DATE", "FROM_CACHE"]);

  const failedTaskPaths = new Set(report.tasks.filter((t) => t.outcome === "FAILED").map((t) => t.path));

  const tasksWithTestFailures = new Set(report.tests.filter((t) => t.result === "FAILURE").map((t) => t.taskPath));

  const successfulTasks = report.tasks
    .filter((t) => SUCCESSFUL_OUTCOMES.has(t.outcome) && !failedTaskPaths.has(t.path) && !tasksWithTestFailures.has(t.path))
    .map((t) => t.path)
    .sort();

  return { successfulTasks, testseed };
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
  const successfulCount = report.successfulTasks.length;
  const totalTaskCount = merged.tasks.length;

  if (successfulCount === 0) {
    return {
      status: "disabled",
      details: "No successful tasks found in previous runs — rerunning all tests",
      failedTestHistory: null,
      annotation: null,
      metadata: {
        "smart-retry-status": "disabled",
        "smart-retry-details": "No successful tasks found in previous runs — rerunning all tests",
        "smart-retry-disabled-reason": "no-successful-tasks",
      },
    };
  }

  const originJobName = resolveOriginJobName(buildJson, originJobId);
  const runCount = multiRun.runs.length;
  const details = `Skipping ${successfulCount} successful tasks out of ${totalTaskCount} total (across ${runCount} previous run${runCount !== 1 ? "s" : ""})`;

  const annotation = [
    `Rerunning failed build job [${originJobName}]`,
    "",
    `**Successful Tasks to Skip:** ${successfulCount}`,
    `**Total Tasks in Previous Runs:** ${totalTaskCount}`,
    `**Previous Runs Analyzed:** ${runCount}`,
    "",
    "This retry will skip tasks that succeeded in previous runs and run everything else.",
  ].join("\n");

  const metadata: Record<string, string> = {
    "smart-retry-status": "enabled",
    "smart-retry-details": details,
    "smart-retry-successful-tasks": String(successfulCount),
    "smart-retry-total-tasks": String(totalTaskCount),
    "smart-retry-previous-runs": String(runCount),
  };

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

