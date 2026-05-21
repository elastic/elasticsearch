/**
 * Merges the current build's task-status.json with any previous runs from the
 * downloaded artifact, producing a multi-run task-status.json for upload.
 *
 * Usage: node merge-task-status.ts <current> <previous-or-empty> <output>
 *
 * If <previous-or-empty> does not exist or is empty, the current run is wrapped
 * in the multi-run format as the sole entry.
 */
import { existsSync, readFileSync, writeFileSync } from "node:fs";

import { normalizeTaskStatus, wrapTaskStatus } from "./smart-retry.ts";
import type { TaskStatusReport, MultiRunTaskStatus } from "./types.ts";

const currentPath = process.argv[2];
const previousPath = process.argv[3];
const outputPath = process.argv[4];

if (!currentPath || !previousPath || !outputPath) {
  console.error("Usage: node merge-task-status.ts <current.json> <previous.json> <output.json>");
  process.exit(1);
}

const current: TaskStatusReport = JSON.parse(readFileSync(currentPath, "utf-8"));

let previous: MultiRunTaskStatus | null = null;
if (existsSync(previousPath)) {
  const raw = JSON.parse(readFileSync(previousPath, "utf-8"));
  previous = normalizeTaskStatus(raw);
}

const merged = wrapTaskStatus(current, previous);
writeFileSync(outputPath, JSON.stringify(merged, null, 2));
