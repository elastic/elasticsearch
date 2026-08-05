import { execSync } from "child_process";
import { resolve } from "path";

import { classifyExplicitList } from "../detectors/explicit-list.ts";
import { buildCommands } from "../commands.ts";
import { runLocally } from "../runners/local.ts";
import { DEFAULT_BATCHING_CONFIG } from "../domain.ts";
import { analyzeReports } from "../analyzer/analyze.ts";
import { renderMarkdown } from "../analyzer/render.ts";

const PROJECT_ROOT = resolve(`${import.meta.dirname}/../../../..`);

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
    console.error("Usage: node .buildkite/scripts/flakiness-detection/entrypoints/local.ts [--iters N] <Class>[ <Class>...]");
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

  const baseCfg: typeof DEFAULT_BATCHING_CONFIG = {
    ...DEFAULT_BATCHING_CONFIG,
    target: "local",
  };
  const cfg = itersOverride !== undefined
    ? {
        ...baseCfg,
        itersByKind: {
          test: itersOverride,
          internalClusterTest: itersOverride,
        },
        restIters: itersOverride,
      }
    : baseCfg;

  const startMs = Date.now();
  const exitCode = runLocally(buildCommands(located, cfg), PROJECT_ROOT);
  const report = await analyzeReports([PROJECT_ROOT], startMs);
  console.log("\n" + renderMarkdown(report));
  process.exit(exitCode);
}

if (import.meta.main) run();
