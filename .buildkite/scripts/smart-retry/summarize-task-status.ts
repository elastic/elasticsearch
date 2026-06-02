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

export interface TaskRef {
  path: string;
  buildScanUrl?: string;
}

export interface TestRef {
  task: string;
  test: string;
  buildScanUrl?: string;
}

export interface BuildScanRef {
  id: string;
  url: string;
}

export interface TaskStatusSummary {
  totalRuns: number;
  preemptedAt: string | null | undefined;
  buildScans: BuildScanRef[];
  failedTasks: TaskRef[];
  failedTests: TestRef[];
  flakyTasks: TaskRef[];
  flakyTests: TestRef[];
  interruptedTasks: TaskRef[];
}

export function classifyRuns(multi: MultiRunTaskStatus): TaskStatusSummary {
  const runs = multi.runs;

  if (runs.length === 0) {
    return {
      totalRuns: 0,
      preemptedAt: null,
      buildScans: [],
      failedTasks: [],
      failedTests: [],
      flakyTasks: [],
      flakyTests: [],
      interruptedTasks: [],
    };
  }

  const current = runs[runs.length - 1];
  const previous = runs.slice(0, -1);
  const currentScanUrl = current.buildScanUrl ?? undefined;

  const failedTasks: TaskRef[] = current.tasks
    .filter((t) => t.outcome === "FAILED")
    .map((t) => ({ path: t.path, buildScanUrl: currentScanUrl }));

  const interruptedTasks: TaskRef[] = current.tasks
    .filter((t) => t.outcome === "INTERRUPTED")
    .map((t) => ({ path: t.path, buildScanUrl: currentScanUrl }));

  const failedTests: TestRef[] = current.tests
    .filter((t) => t.result === "FAILURE")
    .map((t) => ({ task: t.taskPath, test: `${t.className}#${t.methodName}`, buildScanUrl: currentScanUrl }));

  const currentFailedTaskPaths = new Set(failedTasks.map((t) => t.path));
  const flakyTasks: TaskRef[] = [];
  const seenFlakyTasks = new Set<string>();
  for (const run of previous) {
    const scanUrl = run.buildScanUrl ?? undefined;
    for (const task of run.tasks) {
      if (task.outcome === "FAILED" && !currentFailedTaskPaths.has(task.path) && !seenFlakyTasks.has(task.path)) {
        flakyTasks.push({ path: task.path, buildScanUrl: scanUrl });
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
    const scanUrl = run.buildScanUrl ?? undefined;
    for (const test of run.tests) {
      const key = `${test.taskPath}\0${test.className}\0${test.methodName}`;
      if (test.result === "FAILURE" && !currentFailedTestKeys.has(key) && !seenFlakyTests.has(key)) {
        flakyTests.push({ task: test.taskPath, test: `${test.className}#${test.methodName}`, buildScanUrl: scanUrl });
        seenFlakyTests.add(key);
      }
    }
  }

  const buildScans: BuildScanRef[] = runs
    .filter((r) => r.buildScanId && r.buildScanUrl)
    .map((r) => ({ id: r.buildScanId!, url: r.buildScanUrl! }));

  return {
    totalRuns: runs.length,
    preemptedAt: current.preemptedAt,
    buildScans,
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

export function buildkiteInlineLink(url: string, content?: string): string {
  let link = `url='${url}'`;
  if (content) {
    link += `;content='${content}'`;
  }
  return `\x1b]1339;${link}\x07`;
}

function printHeaderFooter(summary: TaskStatusSummary) {
  console.log(`${"=".repeat(80)}`);
  console.log(`  Task Status Summary — ${summary.totalRuns} run${summary.totalRuns !== 1 ? "s" : ""}`);
  if (summary.preemptedAt) {
    console.log(`  Preempted at: ${summary.preemptedAt}`);
    console.log(`  This means that GCP terminated the spot instance before its work was complete.`);
    console.log(
      `  The job should be auto-retrying. If not, likely because it reached the maximum number of automatic retries, you can manually retry it.`,
    );
  }
  if (summary.buildScans.length > 0) {
    const links = summary.buildScans
      .map((scan, i) =>
        buildkiteInlineLink(scan.url, summary.buildScans.length > 1 ? `Build Scan (run ${i + 1})` : "Build Scan"),
      )
      .join("  ");
    console.log(`  ${links}`);
  }
  console.log(`${"=".repeat(80)}\n`);
}

function scanLink(url?: string): string {
  return url ? buildkiteInlineLink(url, "Build Scan") : "";
}

export function printSummary(summary: TaskStatusSummary): void {
  printHeaderFooter(summary);

  function printTaskSection(title: string, items: TaskRef[], emoji: string) {
    if (items.length === 0) return;
    console.log(`${emoji} ${title} (${items.length}):`);
    console.log(`${"-".repeat(60)}`);

    for (const item of items) {
      console.log(` - ${process.env.CI ? `[${scanLink(item.buildScanUrl)}]` : `${item.buildScanUrl} -`} ${item.path}`);
    }
    console.log("\n");
  }

  function printTestSection(title: string, items: TestRef[], emoji: string) {
    if (items.length === 0) return;
    console.log(`${emoji} ${title} (${items.length}):`);
    console.log(`${"-".repeat(60)}`);

    for (const item of items) {
      console.log(` - ${process.env.CI ? `[${scanLink(item.buildScanUrl)}]` : `${item.buildScanUrl} -`} ${item.test}`);
    }
    console.log("\n");
  }

  printTaskSection("Failed Tasks", summary.failedTasks, "❌");
  printTestSection("Failed Tests", summary.failedTests, "❌");
  printTaskSection("Flaky Tasks", summary.flakyTasks, "⚠️");
  printTestSection("Flaky Tests", summary.flakyTests, "⚠️");
  printTaskSection("Interrupted Tasks", summary.interruptedTasks, "⏸️");

  const hasIssues =
    summary.preemptedAt ||
    summary.failedTasks.length +
      summary.failedTests.length +
      summary.flakyTasks.length +
      summary.flakyTests.length +
      summary.interruptedTasks.length >
      0;
  if (!hasIssues) {
    console.log("✅ All tasks and tests passed across all runs.\n");
  } else if (!summary.preemptedAt) {
    printHeaderFooter(summary);
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
