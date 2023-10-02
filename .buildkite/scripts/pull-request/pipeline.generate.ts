import { stringify } from "yaml";
import { execSync } from "child_process";

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
