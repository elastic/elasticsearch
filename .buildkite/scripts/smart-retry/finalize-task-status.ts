/**
 * Finalizes the current build's task-status.json: stamps it with the Gradle
 * build scan URL (read from build/.buildscan-id written by the build scan
 * plugin), then merges it with any previous runs from an earlier artifact to
 * produce a multi-run task-status.json.
 *
 * Usage: node finalize-task-status.ts <current> <previous-or-empty> <output>
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
const buildscanIdPath = process.argv[5];

if (!currentPath || !previousPath || !outputPath || !buildscanIdPath) {
  console.error("Usage: node finalize-task-status.ts <current.json> <previous.json> <output.json> <.buildscan-id>");
  process.exit(1);
}

const current: TaskStatusReport = JSON.parse(readFileSync(currentPath, "utf-8"));

// ---------------------------------------------------------------------------
// Stamp build scan URL from build/.buildscan-id file
// ---------------------------------------------------------------------------

if (existsSync(buildscanIdPath)) {
  const buildScanId = readFileSync(buildscanIdPath, "utf-8").trim();
  if (buildScanId) {
    current.buildScanId = buildScanId;
    current.buildScanUrl = `https://gradle-enterprise.elastic.co/s/${buildScanId}`;
  }
} else {
  console.error("Warning: build/.buildscan-id not found, skipping build scan URL");
}

// ---------------------------------------------------------------------------
// Merge with previous runs
// ---------------------------------------------------------------------------

let previous: MultiRunTaskStatus | null = null;
if (existsSync(previousPath)) {
  const raw = JSON.parse(readFileSync(previousPath, "utf-8"));
  previous = normalizeTaskStatus(raw);
}

const merged = wrapTaskStatus(current, previous);
writeFileSync(outputPath, JSON.stringify(merged, null, 2));
