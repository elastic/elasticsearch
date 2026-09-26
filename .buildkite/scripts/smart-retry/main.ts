import { execFileSync } from "node:child_process";
import { chmodSync, mkdirSync, readFileSync, unlinkSync, writeFileSync } from "node:fs";

import { runSmartRetry, normalizeTaskStatus } from "./smart-retry.ts";
import type { SmartRetryEnv, SmartRetryDeps, BuildkiteBuildJson } from "./types.ts";

const env: SmartRetryEnv = {
  buildkiteApiToken: process.env.BUILDKITE_API_TOKEN ?? "",
  buildkiteJobId: process.env.BUILDKITE_JOB_ID ?? "",
  buildkitePipelineSlug: process.env.BUILDKITE_PIPELINE_SLUG ?? "",
  buildkiteBuildNumber: process.env.BUILDKITE_BUILD_NUMBER ?? "",
  originJobId: process.env.ORIGIN_JOB_ID || undefined,
  testsSeed: process.env.TESTS_SEED || undefined,
};

const PREVIOUS_TASK_STATUS_PATH = "build/previous-task-status.json";

const deps: SmartRetryDeps = {
  fetchBuildJson: async (apiToken, pipelineSlug, buildNumber) => {
    try {
      const url = `https://api.buildkite.com/v2/organizations/elastic/pipelines/${pipelineSlug}/builds/${buildNumber}?include_retried_jobs=true`;
      const resp = await fetch(url, {
        headers: { Authorization: `Bearer ${apiToken}` },
        signal: AbortSignal.timeout(30_000),
      });
      if (!resp.ok) return null;
      return (await resp.json()) as BuildkiteBuildJson;
    } catch {
      return null;
    }
  },

  downloadArtifact: async (originJobId) => {
    try {
      execFileSync("buildkite-agent", ["artifact", "download", "task-status.json.gz", ".", "--step", originJobId]);

      execFileSync("gunzip", ["-f", "task-status.json.gz"]);

      const text = readFileSync("task-status.json", "utf-8");
      const raw = JSON.parse(text);
      const multi = normalizeTaskStatus(raw);

      mkdirSync("build", { recursive: true });
      writeFileSync(PREVIOUS_TASK_STATUS_PATH, JSON.stringify(multi, null, 2));

      return multi;
    } catch {
      return null;
    } finally {
      try {
        unlinkSync("task-status.json.gz");
        unlinkSync("task-status.json");
      } catch {
        // best-effort cleanup
      }
    }
  },
};

console.log("--- [Smart Retry] Resolving previously failed tests");

const result = await runSmartRetry(env, deps);

if (result.failedTestHistory) {
  const json = JSON.stringify(result.failedTestHistory, null, 2);
  writeFileSync(".failed-test-history.json", json);
  chmodSync(".failed-test-history.json", 0o600);
}

if (result.annotation) {
  const jobId = env.buildkiteJobId;
  execFileSync("buildkite-agent", ["annotate", "--style", "info", "--context", `smart-retry-${jobId}`], {
    input: result.annotation,
  });
}

for (const [key, value] of Object.entries(result.metadata)) {
  execFileSync("buildkite-agent", ["meta-data", "set", key, value]);
}

if (result.status === "enabled") {
  console.log(`[Smart Retry] Enabled: ${result.details}`);
} else if (result.status === "disabled") {
  console.log(`[Smart Retry] Disabled: ${result.details}`);
  console.log("[Smart Retry] All tests will run.");
} else {
  console.log(`[Smart Retry] ${result.details}`);
  console.log("[Smart Retry] will be disabled - all tests will run.");
}
