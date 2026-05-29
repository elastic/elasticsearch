/**
 * Finalizes the current build's task-status.json: stamps it with the Gradle
 * build scan URL (looked up from Buildkite metadata), then merges it with any
 * previous runs from an earlier artifact to produce a multi-run task-status.json.
 *
 * Usage: node finalize-task-status.ts <current> <previous-or-empty> <output>
 *
 * If <previous-or-empty> does not exist or is empty, the current run is wrapped
 * in the multi-run format as the sole entry.
 *
 * Environment:
 *   BUILDKITE_JOB_ID — used to look up build scan metadata via buildkite-agent.
 *                       If unset or the lookup fails, the build scan fields are
 *                       left as-is (null) and processing continues.
 */
import { execSync } from "node:child_process";
import { existsSync, readFileSync, writeFileSync } from "node:fs";

import { normalizeTaskStatus, wrapTaskStatus } from "./smart-retry.ts";
import type { TaskStatusReport, MultiRunTaskStatus } from "./types.ts";

const currentPath = process.argv[2];
const previousPath = process.argv[3];
const outputPath = process.argv[4];

if (!currentPath || !previousPath || !outputPath) {
  console.error("Usage: node finalize-task-status.ts <current.json> <previous.json> <output.json>");
  process.exit(1);
}

const current: TaskStatusReport = JSON.parse(readFileSync(currentPath, "utf-8"));

// ---------------------------------------------------------------------------
// Stamp build scan URL from Buildkite metadata
// ---------------------------------------------------------------------------

const jobId = process.env.BUILDKITE_JOB_ID;
if (jobId) {
  try {
    const buildScanUrl = execSync(`buildkite-agent meta-data get 'build-scan-${jobId}'`, {
      encoding: "utf-8",
      timeout: 10_000,
    }).trim();

    if (buildScanUrl) {
      current.buildScanUrl = buildScanUrl;
      current.buildScanId = buildScanUrl.split("/").pop();
    }
  } catch (e) {
    console.error(`Warning: could not look up build scan URL for job ${jobId}: ${e instanceof Error ? e.message : e}`);
  }
} else {
  console.error("Warning: BUILDKITE_JOB_ID not set, skipping build scan URL lookup");
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
