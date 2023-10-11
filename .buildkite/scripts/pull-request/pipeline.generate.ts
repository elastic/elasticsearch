import { stringify } from "yaml";
import { execSync } from "child_process";

import { generatePipelines } from "./pipeline";

const pipelines = generatePipelines();

for (const pipeline of pipelines) {
  const yaml = stringify(pipeline);

  console.log("--- Generated pipeline");
  console.log(yaml);

  // Only do the pipeline upload if we're actually in CI
  // This lets us run the tool locally and see the output
  if (process.env.CI) {
    console.log("--- Uploading pipeline");

    execSync(`buildkite-agent pipeline upload`, {
      input: stringify(pipeline),
      stdio: ["pipe", "inherit", "inherit"],
    });
  }
}
