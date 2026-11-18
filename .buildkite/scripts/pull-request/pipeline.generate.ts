import { stringify } from "yaml";
import { execSync } from "child_process";

import { generatePipelines } from "./pipeline.ts";
import { getSnapshotBwcVersions, setIncludedSnapshotBwcVersions } from "./bwc-versions.ts";
import { DraClient } from "../bwc-snapshots/dra.ts";
import { loadIncludedSnapshotBwcVersions, storeFromEnv } from "../bwc-snapshots/known-good.ts";

// Leave bwc snapshot versions without a known-good DRA build out of the bwc jobs. The jobs run in dra mode, where
// Gradle reads the known-good builds itself.
setIncludedSnapshotBwcVersions(
  await loadIncludedSnapshotBwcVersions(
    getSnapshotBwcVersions(),
    storeFromEnv(),
    new DraClient(process.env["BWC_DRA_BASE_URL"]),
  ),
);

const pipelines = generatePipelines();

// Log each included pipeline for debugging
for (const pipeline of pipelines) {
  console.log(`--- Generated pipeline: ${pipeline.name}`);
  console.log(stringify(pipeline.pipeline));
}

// Combine all pipelines into a single upload so that Buildkite preserves step
// ordering.  When pipeline upload is called multiple times, each call inserts
// its steps immediately after the currently-running step, which reverses the
// intended order.  A single upload avoids that problem entirely.
const combinedSteps = pipelines.flatMap((p) => p.pipeline.steps ?? []);

// Merge any top-level env blocks (individual pipelines may define pipeline-wide
// env vars; merging them keeps those variables in scope for their steps).
const combinedEnv = pipelines.reduce(
  (acc, p) => ({ ...acc, ...(p.pipeline.env ?? {}) }),
  {} as Record<string, string>,
);

const combined =
  Object.keys(combinedEnv).length > 0 ? { env: combinedEnv, steps: combinedSteps } : { steps: combinedSteps };

// Only do the pipeline upload if we're actually in CI
// This lets us run the tool locally and see the output
if (process.env.CI) {
  console.log("--- Uploading combined pipeline");
  execSync(`buildkite-agent pipeline upload`, {
    input: stringify(combined),
    stdio: ["pipe", "inherit", "inherit"],
  });
}
