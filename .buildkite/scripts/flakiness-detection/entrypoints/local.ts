import { execSync } from "child_process";
import { readFileSync, writeFileSync } from "fs";
import { resolve } from "path";

import { explicitRefs } from "../detectors/explicit-list.ts";
import { planCommandsToRunnable } from "../commands.ts";
import { runLocally } from "../runners/local.ts";
import { type FlakinessPlan, type FlakinessRefsFile } from "../domain.ts";
import { analyzeReports } from "../analyzer/analyze.ts";
import { renderMarkdown } from "../analyzer/render.ts";

const REFS_FILE = "flakiness-refs.json";
const PLAN_FILE = "flakiness-plan.json";
// Newline-separated Gradle compile task paths the resolve step wrote (possibly empty).
const COMPILE_TASKS_FILE = "flakiness-compile-tasks.txt";
const PROJECT_ROOT = resolve(`${import.meta.dirname}/../../../..`);

// Read the resolver's compile task list. Absent/empty = nothing to compile.
function readCompileTasks(root: string): string[] {
  try {
    return readFileSync(resolve(root, COMPILE_TASKS_FILE), "utf8")
      .split("\n")
      .map((t) => t.trim())
      .filter((t) => t !== "");
  } catch {
    return [];
  }
}

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

  // Phase 1 (bootstrap): write refs.
  const refsFile: FlakinessRefsFile = { mergeBase: "", refs: explicitRefs(specs) };
  writeFileSync(resolve(PROJECT_ROOT, REFS_FILE), JSON.stringify(refsFile, null, 2));

  // Phase 2 (resolve): run the Java resolver locally to produce the base targets + compile task list.
  // Requires the root build to apply `elasticsearch.internal-flakiness-resolve` (see JAVA_RESOLVER_NOTES.md).
  // --no-configuration-cache is required so every project configures and contributes its model to the
  // shared build service (see runners/buildkite.ts + JAVA_RESOLVER_NOTES.md).
  console.log(">>> ./gradlew -Pflakiness.resolve --no-configuration-cache flakinessResolve");
  execSync("./gradlew -Pflakiness.resolve --no-configuration-cache flakinessResolve", { cwd: PROJECT_ROOT, stdio: "inherit" });

  // Phase 3 (compile): plainly compile the tasks the resolver listed. A compile failure is the only
  // build_failed signal (mirroring the CI compile step); bail early so we do not scan doomed output.
  const compileTasks = readCompileTasks(PROJECT_ROOT);
  if (compileTasks.length > 0) {
    console.log(`>>> ./gradlew ${compileTasks.join(" ")}`);
    try {
      execSync(`./gradlew ${compileTasks.join(" ")}`, { cwd: PROJECT_ROOT, stdio: "inherit" });
    } catch {
      console.error("buildFailed: the affected source sets did not compile");
      process.exit(1);
    }
  } else {
    console.log("No compile tasks listed by the resolver; nothing to compile.");
  }

  // Phase 4 (scan): ASM-scan the compiled output into the final plan. Java now owns iteration counts and
  // bakes them into the plan's batch commands, so an `--iters N` override is passed through to the scan
  // task via `-Pflakiness.iters=N` (rather than being applied by TS after the fact).
  const scanCmd =
    itersOverride !== undefined
      ? `./gradlew -Pflakiness.resolve -Pflakiness.iters=${itersOverride} flakinessScan`
      : "./gradlew -Pflakiness.resolve flakinessScan";
  console.log(`>>> ${scanCmd}`);
  execSync(scanCmd, { cwd: PROJECT_ROOT, stdio: "inherit" });

  // Phase 5 (generate/run): read the plan and run the ready batch commands locally.
  const plan = JSON.parse(readFileSync(resolve(PROJECT_ROOT, PLAN_FILE), "utf8")) as FlakinessPlan;
  if (plan.buildFailed) {
    console.error(`Plan reported buildFailed (${plan.reason ?? "precompile"})`);
    process.exit(1);
  }

  for (const u of plan.unresolved ?? []) {
    console.error(`Unresolved (${u.reason}): ${u.ref.spec ?? u.ref.className ?? u.ref.path}`);
  }

  const runnable = planCommandsToRunnable(plan.commands ?? [], "local");
  if (runnable.length === 0) {
    console.error("Nothing runnable in plan");
    process.exit(1);
  }

  const startMs = Date.now();
  const exitCode = runLocally(runnable, PROJECT_ROOT);
  const report = await analyzeReports([PROJECT_ROOT], startMs);
  console.log("\n" + renderMarkdown(report));
  process.exit(exitCode);
}

if (import.meta.main) run();
