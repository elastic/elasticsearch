import { execSync } from "child_process";
import { resolve } from "path";

import { classifyExplicitList } from "../detectors/explicit-list";
import { buildCommands } from "../commands";
import { runLocally } from "../runners/local";
import { DEFAULT_BATCHING_CONFIG } from "../domain";
import { analyzeReports } from "../analyzer/analyze";
import { renderMarkdown } from "../analyzer/render";

const PROJECT_ROOT = resolve(`${import.meta.dir}/../../../..`);

export async function run(): Promise<void> {
  const args = process.argv.slice(2);
  const itersIdx = args.findIndex((a) => a === "--iters");
  let itersOverride: number | undefined;
  if (itersIdx !== -1) {
    const parsed = parseInt(args[itersIdx + 1] ?? "", 10);
    if (!Number.isFinite(parsed) || parsed <= 0) {
      console.error("--iters requires a positive integer");
      process.exit(2);
    }
    itersOverride = parsed;
    args.splice(itersIdx, 2);
  }
  const specs = args.filter((a) => a.trim() !== "");
  if (specs.length === 0) {
    console.error("Usage: bun .buildkite/scripts/flakiness-detection/entrypoints/local.ts [--iters N] <Class>[ <Class>...]");
    process.exit(2);
  }

  const repoFilesOutput = execSync("git ls-files", {
    cwd: PROJECT_ROOT,
    maxBuffer: 256 * 1024 * 1024,
  }).toString();
  const repoFiles = repoFilesOutput.split("\n").map((f) => f.trim()).filter((f) => f !== "");

  const { located, unlocated } = classifyExplicitList(specs, repoFiles);
  if (unlocated.length > 0) {
    console.error(`Could not resolve ${unlocated.length} spec(s):`);
    for (const u of unlocated) console.error(`  - ${u.spec}`);
    process.exit(1); // fail-fast (per design)
  }

  const cfg = itersOverride !== undefined
    ? {
        ...DEFAULT_BATCHING_CONFIG,
        itersByKind: {
          test: itersOverride,
          internalClusterTest: itersOverride,
        },
      }
    : DEFAULT_BATCHING_CONFIG;

  const startMs = Date.now();
  const exitCode = runLocally(buildCommands(located, cfg), PROJECT_ROOT);
  const report = await analyzeReports([PROJECT_ROOT], startMs);
  console.log("\n" + renderMarkdown(report));
  process.exit(exitCode);
}

if (import.meta.main) run();
