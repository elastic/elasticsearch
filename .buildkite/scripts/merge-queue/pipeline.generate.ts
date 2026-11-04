import { stringify } from "yaml";
import { execSync } from "child_process";

import { generateMergeQueuePipeline, getBaseBranch } from "./pipeline.ts";

const branch = process.env["BUILDKITE_BRANCH"];
if (!branch) {
  throw new Error("BUILDKITE_BRANCH is not set");
}

const baseBranch = getBaseBranch(branch);
console.log(`Generating the merge queue pipeline for ${branch} (base branch ${baseBranch})`);

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
