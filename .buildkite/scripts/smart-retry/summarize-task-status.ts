/**
 * Reads a multi-run task-status.json and prints a failure summary to the console.
 *
 * Usage: node summarize-task-status.ts <task-status.json>
 *
 * Categories:
 *   Failed Tasks      — FAILED in the current (final) run
 *   Failed Tests      — FAILURE in the current (final) run
 *   Flaky Tasks       — FAILED in a previous run but not in the current run
 *   Flaky Tests       — FAILURE in a previous run but not in the current run
 *   Interrupted Tasks — INTERRUPTED in the current run
 */
import { readFileSync } from "node:fs";

import { normalizeTaskStatus } from "./smart-retry.ts";
import type { MultiRunTaskStatus, TaskStatusReport } from "./types.ts";

// ---------------------------------------------------------------------------
// Classification
// ---------------------------------------------------------------------------

export interface TestRef {
  task: string;
  test: string;
}

export interface TaskStatusSummary {
  totalRuns: number;
  preemptedAt: string | null | undefined;
  failedTasks: string[];
  failedTests: TestRef[];
  flakyTasks: string[];
  flakyTests: TestRef[];
  interruptedTasks: string[];
}

export function classifyRuns(multi: MultiRunTaskStatus): TaskStatusSummary {
  const runs = multi.runs;

  if (runs.length === 0) {
    return {
      totalRuns: 0,
      preemptedAt: null,
      failedTasks: [],
      failedTests: [],
      flakyTasks: [],
      flakyTests: [],
      interruptedTasks: [],
    };
  }

  const current = runs[runs.length - 1];
  const previous = runs.slice(0, -1);

  const failedTasks = current.tasks.filter((t) => t.outcome === "FAILED").map((t) => t.path);
  const interruptedTasks = current.tasks.filter((t) => t.outcome === "INTERRUPTED").map((t) => t.path);

  const failedTests: TestRef[] = current.tests
    .filter((t) => t.result === "FAILURE")
    .map((t) => ({ task: t.taskPath, test: `${t.className}#${t.methodName}` }));

  const currentFailedTaskSet = new Set(failedTasks);
  const flakyTasks: string[] = [];
  const seenFlakyTasks = new Set<string>();
  for (const run of previous) {
    for (const task of run.tasks) {
      if (task.outcome === "FAILED" && !currentFailedTaskSet.has(task.path) && !seenFlakyTasks.has(task.path)) {
        flakyTasks.push(task.path);
        seenFlakyTasks.add(task.path);
      }
    }
  }

  const currentFailedTestKeys = new Set(
    current.tests.filter((t) => t.result === "FAILURE").map((t) => `${t.taskPath}\0${t.className}\0${t.methodName}`),
  );
  const flakyTests: TestRef[] = [];
  const seenFlakyTests = new Set<string>();
  for (const run of previous) {
    for (const test of run.tests) {
      const key = `${test.taskPath}\0${test.className}\0${test.methodName}`;
      if (test.result === "FAILURE" && !currentFailedTestKeys.has(key) && !seenFlakyTests.has(key)) {
        flakyTests.push({ task: test.taskPath, test: `${test.className}#${test.methodName}` });
        seenFlakyTests.add(key);
      }
    }
  }

  return {
    totalRuns: runs.length,
    preemptedAt: current.preemptedAt,
    failedTasks,
    failedTests,
    flakyTasks,
    flakyTests,
    interruptedTasks,
  };
}

// ---------------------------------------------------------------------------
// Console output
// ---------------------------------------------------------------------------

function printHeaderFooter(summary: TaskStatusSummary) {
  console.log(`${"=".repeat(80)}`);
  console.log(`  Task Status Summary — ${summary.totalRuns} run${summary.totalRuns !== 1 ? "s" : ""}`);
  if (summary.preemptedAt) {
    console.log(`  Preempted at: ${summary.preemptedAt}`);
    console.log(`  This means that GCP terminated the spot instance before its work was complete.`);
    console.log(`  The job should be auto-retrying. If not, likely because it reached the maximum number of automatic retries, you can manually retry it.`);
  }
  console.log(`${"=".repeat(80)}\n`);
}

export function printSummary(summary: TaskStatusSummary): void {
  printHeaderFooter(summary);

  function printSection(title: string, items: TestRef[] | string[], emoji: string) {
    if (items.length === 0) return;

    console.log(`${emoji} ${title} (${items.length}):`);
    console.log(`${"-".repeat(60)}`);

    if (typeof items[0] === "string") {
      console.table((items as string[]).map((path) => ({ "Task Path": path })));
    } else {
      console.table((items as TestRef[]).map((i) => ({ "Task Path": i.task, Test: i.test })));
    }
    console.log();
  }

  printSection("Failed Tasks", summary.failedTasks, "❌");
  printSection("Failed Tests", summary.failedTests, "❌");
  printSection("Flaky Tasks", summary.flakyTasks, "⚠️");
  printSection("Flaky Tests", summary.flakyTests, "⚠️");
  printSection("Interrupted Tasks", summary.interruptedTasks, "⏸️");

  printHeaderFooter(summary);

  const hasIssues =
    summary.failedTasks.length +
      summary.failedTests.length +
      summary.flakyTasks.length +
      summary.flakyTests.length +
      summary.interruptedTasks.length >
    0;
  if (!hasIssues) {
    console.log("✅ All tasks and tests passed across all runs.\n");
  }
}

// ---------------------------------------------------------------------------
// CLI entry point
// ---------------------------------------------------------------------------

const filePath = process.argv[2];
if (filePath) {
  const raw = JSON.parse(readFileSync(filePath, "utf-8"));
  const multi = normalizeTaskStatus(raw);
  const summary = classifyRuns(multi);
  printSummary(summary);
}
