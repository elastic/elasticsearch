import { stringify } from "yaml";
import { execSync } from "child_process";

import { generateMergeQueuePipeline, getBaseBranch, getPullRequestNumber } from "./pipeline.ts";
import { getSnapshotBwcVersions, setIncludedSnapshotBwcVersions } from "../pull-request/bwc-versions.ts";
import { DraClient } from "../bwc-snapshots/dra.ts";
import { loadIncludedSnapshotBwcVersions, storeFromEnv } from "../bwc-snapshots/known-good.ts";

const branch = process.env["BUILDKITE_BRANCH"];
if (!branch) {
  throw new Error("BUILDKITE_BRANCH is not set");
}

const baseBranch = getBaseBranch(branch);
console.log(`Generating the merge queue pipeline for ${branch} (base branch ${baseBranch})`);

// Same as the pull request pipeline: leave bwc snapshot versions without a known-good DRA build out of the bwc jobs.
// The jobs run in dra mode, where Gradle reads the known-good builds itself.
setIncludedSnapshotBwcVersions(
  await loadIncludedSnapshotBwcVersions(
    getSnapshotBwcVersions(),
    storeFromEnv(),
    new DraClient(process.env["BWC_DRA_BASE_URL"]),
  ),
);

const pipeline = generateMergeQueuePipeline(baseBranch, getPullRequestNumber(branch));
console.log(stringify(pipeline));

// Only do the pipeline upload if we're actually in CI
// This lets us run the tool locally and see the output
if (process.env.CI) {
  console.log("--- Uploading pipeline");
  execSync(`buildkite-agent pipeline upload`, {
    input: stringify(pipeline),
    stdio: ["pipe", "inherit", "inherit"],
  });
}
