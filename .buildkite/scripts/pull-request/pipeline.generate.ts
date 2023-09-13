import { stringify } from "yaml";
import { execSync } from "child_process";

// TODO remove these before PR
process.env["GITHUB_PR_LABELS"] = process.env["GITHUB_PR_LABELS"] || "release_note:skip,:Delivery/Packaging,v8.11.0";
process.env["GITHUB_PR_TARGET_BRANCH"] = process.env["GITHUB_PR_TARGET_BRANCH"] || "main";
// process.env["GITHUB_PR_TRIGGER_COMMENT"] =
//   "hey run elasticsearch-ci/build-benchmarks please and run elasticsearch-ci/part-2";

import { generatePipelines } from "./pipeline";

const pipelines = generatePipelines();

for (const pipeline of pipelines) {
  if (!process.env.CI) {
    // Just for local debugging purposes
    console.log("");
    console.log(stringify(pipeline));
  } else {
    execSync(`buildkite-agent pipeline upload`, {
      input: stringify(pipeline),
      stdio: ["pipe", "inherit", "inherit"],
    });
  }
}
