import { runSmartRetry, normalizeTaskStatus } from "./smart-retry";
import type { SmartRetryEnv, SmartRetryDeps, BuildkiteBuildJson, MultiRunTaskStatus } from "./types";

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
      const dl = Bun.spawnSync(["buildkite-agent", "artifact", "download", "task-status.json.gz", ".", "--step", originJobId]);
      if (dl.exitCode !== 0) return null;

      const gz = Bun.spawnSync(["gunzip", "-f", "task-status.json.gz"]);
      if (gz.exitCode !== 0) return null;

      const text = await Bun.file("task-status.json").text();
      const raw = JSON.parse(text);
      const multi = normalizeTaskStatus(raw);

      // Persist the raw artifact so post-command can merge the current run into it
      const { mkdirSync } = await import("node:fs");
      mkdirSync("build", { recursive: true });
      await Bun.write(PREVIOUS_TASK_STATUS_PATH, JSON.stringify(multi, null, 2));

      return multi;
    } catch {
      return null;
    } finally {
      try {
        const { unlinkSync } = await import("node:fs");
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
  await Bun.write(".failed-test-history.json", json);
  const { chmodSync } = await import("node:fs");
  chmodSync(".failed-test-history.json", 0o600);
}

if (result.annotation) {
  const jobId = env.buildkiteJobId;
  Bun.spawnSync(["buildkite-agent", "annotate", "--style", "info", "--context", `smart-retry-${jobId}`], {
    stdin: new TextEncoder().encode(result.annotation),
  });
}

for (const [key, value] of Object.entries(result.metadata)) {
  Bun.spawnSync(["buildkite-agent", "meta-data", "set", key, value]);
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
