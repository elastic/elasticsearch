import { execSync } from "child_process";
import { resolve } from "path";

import { analyzeReports } from "../analyzer/analyze.ts";
import { renderMarkdown, severity } from "../analyzer/render.ts";

const PROJECT_ROOT = resolve(`${import.meta.dirname}/../../../..`);

async function run(): Promise<void> {
  const report = await analyzeReports([PROJECT_ROOT]);
  const md = renderMarkdown(report);
  console.log(md);
  if (process.env.CI) {
    try {
      execSync(
        `buildkite-agent annotate --style "${severity(report)}" --context "flakiness-detection-report"`,
        { input: md, cwd: PROJECT_ROOT, stdio: ["pipe", "inherit", "inherit"] }
      );
    } catch (err) {
      console.error("Failed to post annotation:", err);
    }
  }
}

if (import.meta.main) run();
