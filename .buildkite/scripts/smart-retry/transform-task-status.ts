/**
 * Reads a (possibly multi-run) task-status.json, merges all runs, and produces
 * .failed-test-history.json for consumption by InternalTestRerunPlugin.
 *
 * Usage: node transform-task-status.ts <task-status.json> <output.json> [testseed]
 */
import { readFileSync, writeFileSync } from "node:fs";

import { normalizeTaskStatus, mergeRuns, transformTaskStatus } from "./smart-retry.ts";

const inputPath = process.argv[2];
const outputPath = process.argv[3];
const testseed = process.argv[4] ?? "";

if (!inputPath || !outputPath) {
  console.error("Usage: node transform-task-status.ts <task-status.json> <output.json> [testseed]");
  process.exit(1);
}

const raw = JSON.parse(readFileSync(inputPath, "utf-8"));
const multi = normalizeTaskStatus(raw);
const merged = mergeRuns(multi);
const report = transformTaskStatus(merged, testseed);

writeFileSync(outputPath, JSON.stringify(report, null, 2));

console.log(`Wrote ${outputPath}`);
console.log(`  Successful tasks to skip: ${report.successfulTasks.length}`);
const testCount = Object.values(report.successfulTests).reduce((sum, tests) => sum + tests.length, 0);
console.log(`  Successful tests to skip: ${testCount}`);
