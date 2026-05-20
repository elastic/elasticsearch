/**
 * Merges the current build's task-status.json with any previous runs from the
 * downloaded artifact, producing a multi-run task-status.json for upload.
 *
 * Usage: bun merge-task-status.ts <current> <previous-or-empty> <output>
 *
 * If <previous-or-empty> does not exist or is empty, the current run is wrapped
 * in the multi-run format as the sole entry.
 */
import { existsSync } from "node:fs";

import { normalizeTaskStatus, wrapTaskStatus } from "./smart-retry";
import type { TaskStatusReport, MultiRunTaskStatus } from "./types";

const currentPath = process.argv[2];
const previousPath = process.argv[3];
const outputPath = process.argv[4];

if (!currentPath || !previousPath || !outputPath) {
  console.error("Usage: bun merge-task-status.ts <current.json> <previous.json> <output.json>");
  process.exit(1);
}

const current: TaskStatusReport = JSON.parse(await Bun.file(currentPath).text());

let previous: MultiRunTaskStatus | null = null;
if (existsSync(previousPath)) {
  const raw = JSON.parse(await Bun.file(previousPath).text());
  previous = normalizeTaskStatus(raw);
}

const merged = wrapTaskStatus(current, previous);
await Bun.write(outputPath, JSON.stringify(merged, null, 2));
