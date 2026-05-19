import { execSync } from "child_process";
import { resolve } from "path";

import { classifyExplicitList } from "../detectors/explicit-list";
import { buildCommands } from "../commands";
import { uploadBuildkitePipeline } from "../runners/buildkite";
import { DEFAULT_AGENT_CONFIG, DEFAULT_BATCHING_CONFIG } from "../domain";

const PROJECT_ROOT = resolve(`${import.meta.dir}/../../../..`);

export function run(): void {
  const rawClasses = process.env.FLAKINESS_CLASSES;
  if (!rawClasses || rawClasses.trim() === "") {
    console.error("FLAKINESS_CLASSES environment variable is required (newline- or comma-separated)");
    process.exit(2);
  }
  const specs = rawClasses
    .split(/[\n,]/)
    .map((s) => s.trim())
    .filter((s) => s !== "");

  const repoFilesOutput = execSync("git ls-files", {
    cwd: PROJECT_ROOT,
    maxBuffer: 256 * 1024 * 1024,
  }).toString();
  const repoFiles = repoFilesOutput.split("\n").map((f) => f.trim()).filter((f) => f !== "");

  const { located, unlocated } = classifyExplicitList(specs, repoFiles);
  if (unlocated.length > 0) {
    console.error(`Could not resolve ${unlocated.length} spec(s) to a source file:`);
    for (const u of unlocated) console.error(`  - ${u.spec}`);
    process.exit(1); // fail-fast on typos (per design)
  }

  const itersRaw = process.env.FLAKINESS_ITERS;
  const itersOverride = itersRaw ? parseInt(itersRaw, 10) : NaN;
  const cfg = Number.isFinite(itersOverride) && itersOverride > 0
    ? {
        ...DEFAULT_BATCHING_CONFIG,
        itersByKind: {
          test: itersOverride,
          internalClusterTest: itersOverride,
        },
        restIters: itersOverride,
      }
    : DEFAULT_BATCHING_CONFIG;

  uploadBuildkitePipeline(buildCommands(located, cfg), DEFAULT_AGENT_CONFIG);
}

if (import.meta.main) run();
