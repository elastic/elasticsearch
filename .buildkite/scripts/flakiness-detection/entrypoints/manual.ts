import { execSync } from "child_process";
import { resolve } from "path";
import { writeFileSync } from "fs";

import { classifyExplicitList } from "../detectors/explicit-list.ts";
import { defaultJavaSourceReader } from "../detectors/abstract.ts";
import { partitionByBwc, defaultBuildScriptReader } from "../detectors/bwc.ts";
import { buildCommands, compileTasksFor } from "../commands.ts";
import { uploadBuildkitePipeline } from "../runners/buildkite.ts";
import { DEFAULT_AGENT_CONFIG, DEFAULT_BATCHING_CONFIG } from "../domain.ts";

const PROJECT_ROOT = resolve(`${import.meta.dirname}/../../../..`);

// Keep in sync with FLAKINESS_SKIPPED_ARTIFACT (runners/buildkite.ts) and the
// manual pipeline's `artifact_paths`.
const SKIPPED_FILE = "flakiness-skipped.json";

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

  // Skip abstract base classes (they match the *Tests/*IT convention but run no
  // tests) via the source reader, same as the PR flow.
  const { located, unlocated } = classifyExplicitList(specs, repoFiles, defaultJavaSourceReader(PROJECT_ROOT));
  if (unlocated.length > 0) {
    console.error(`Could not resolve ${unlocated.length} spec(s) to a source file (or they are abstract base classes):`);
    for (const u of unlocated) console.error(`  - ${u.spec}`);
    process.exit(1); // fail-fast on typos
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

  // BWC qa projects disable the bare task, so partition them out (recorded as
  // not_applicable) rather than fanning out doomed no-op batches.
  const { runnable, notApplicable } = partitionByBwc(located, defaultBuildScriptReader(PROJECT_ROOT));
  if (notApplicable.length > 0) {
    console.log(`Skipping ${notApplicable.length} BWC test(s) - not re-runnable via the bare task (recorded as not_applicable).`);
    if (process.env.CI) {
      try {
        writeFileSync(resolve(PROJECT_ROOT, SKIPPED_FILE), JSON.stringify(notApplicable));
      } catch (err) {
        console.error(`Failed to write ${SKIPPED_FILE}:`, err);
      }
    }
  }

  uploadBuildkitePipeline(buildCommands(runnable, cfg), DEFAULT_AGENT_CONFIG, {
    hasNotApplicable: notApplicable.length > 0,
    compileTasks: compileTasksFor(runnable),
  });
}

if (import.meta.main) run();
