import { stringify } from "yaml";
import { execSync } from "child_process";

import { generateMergeQueuePipeline, getBaseBranch } from "./pipeline.ts";
import {
  getSnapshotBwcVersions,
  setIncludedSnapshotBwcVersions,
  setSnapshotBwcPinArgs,
} from "../pull-request/bwc-versions.ts";
import { DraClient } from "../bwc-snapshots/dra.ts";
import { loadSnapshotBwcPins, storeFromEnv } from "../bwc-snapshots/known-good.ts";

const branch = process.env["BUILDKITE_BRANCH"];
if (!branch) {
  throw new Error("BUILDKITE_BRANCH is not set");
}

const baseBranch = getBaseBranch(branch);
console.log(`Generating the merge queue pipeline for ${branch} (base branch ${baseBranch})`);

// Same as the pull request pipeline: pin bwc snapshot jobs to the last DRA builds that passed bwc tests, and leave out
// versions that have none yet. Read once so every job of this build agrees.
const bwcPins = await loadSnapshotBwcPins(
  getSnapshotBwcVersions(),
  storeFromEnv(),
  new DraClient(process.env["BWC_DRA_BASE_URL"]),
);
setSnapshotBwcPinArgs(bwcPins.pinArgs);
setIncludedSnapshotBwcVersions(bwcPins.includedVersions);

const pipeline = generateMergeQueuePipeline(baseBranch);
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
