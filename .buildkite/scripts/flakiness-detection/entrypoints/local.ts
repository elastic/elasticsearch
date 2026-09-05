import { execSync } from "child_process";
import { readFileSync, rmSync, writeFileSync } from "fs";
import { resolve } from "path";

import { explicitRefs } from "../detectors/explicit-list.ts";
import { planCommandsToRunnable } from "../commands.ts";
import { runLocally } from "../runners/local.ts";
import { COMPILE_TASKS, type FlakinessPlan, type FlakinessRefsFile } from "../domain.ts";
import { analyzeReports } from "../analyzer/analyze.ts";
import { renderMarkdown } from "../analyzer/render.ts";

const REFS_FILE = "flakiness-refs.json";
const PLAN_FILE = "flakiness-plan.json";
// Where each project drops its share of the resolve answer. Keep in sync with
// FlakinessProjectResolvePlugin.TARGETS_DIR on the Java side.
const TARGETS_DIR = "build/flakiness/project-targets";
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

  // Phase 1 (bootstrap): write refs.
  const refsFile: FlakinessRefsFile = { mergeBase: "", refs: explicitRefs(specs) };
  writeFileSync(resolve(PROJECT_ROOT, REFS_FILE), JSON.stringify(refsFile, null, 2));

  // Phase 2 (resolve): run the Java resolver locally. The task name is deliberately UNQUALIFIED - Gradle
  // runs it in every project that registered it and each project self-selects on whether it owns a ref
  // (see FlakinessProjectResolve). Requires the root build to apply
  // `elasticsearch.internal-flakiness-resolve` (see JAVA_RESOLVER_NOTES.md). The configuration cache is left
  // ON: the model travels through task inputs, so it survives the configuration/execution boundary.
  rmSync(resolve(PROJECT_ROOT, TARGETS_DIR), { recursive: true, force: true });
  console.log(">>> ./gradlew -Pflakiness.resolve flakinessResolveProject");
  execSync("./gradlew -Pflakiness.resolve flakinessResolveProject", { cwd: PROJECT_ROOT, stdio: "inherit" });

  // Phase 3 (compile): compile every test source set in the repo, UNQUALIFIED, reading nothing back from
  // resolve - the scan needs the whole repo's bytecode to resolve cross-project class hierarchies. A compile
  // failure is the only build_failed signal (mirroring the CI compile step); bail early so we do not scan
  // doomed output. Locally this is the slow phase on a cold build directory; there is no remote cache here.
  const compileCmd = `./gradlew ${COMPILE_TASKS.join(" ")}`;
  console.log(`>>> ${compileCmd}`);
  try {
    execSync(compileCmd, { cwd: PROJECT_ROOT, stdio: "inherit" });
  } catch {
    console.error("buildFailed: the test source sets did not compile");
    process.exit(1);
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

  // Surface the resolver's dispositions: a skipped target and a capped task fan-out are both things a
  // developer running this locally needs to see, not silently dropped work.
  for (const e of plan.entries) {
    if (e.disposition === "skip") {
      console.error(`Skipped (${e.reason}): ${e.fqcn ?? e.suitePath ?? e.gradleProject}`);
    }
  }
  for (const s of plan.taskSelections ?? []) {
    console.log(
      `${s.gradleProject} (${s.sourceSet}): selected ${s.selected.length} of ${s.total} candidate tasks ` +
        `(cap ${s.cap}): ${s.selected.join(", ")}`
    );
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
