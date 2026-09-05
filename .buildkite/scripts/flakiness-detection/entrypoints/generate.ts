import { execSync } from "child_process";
import { existsSync, readFileSync, rmSync, writeFileSync } from "fs";
import { resolve } from "path";

import { planCommandsToRunnable, planEntryToSkippedTest } from "../commands.ts";
import { uploadBuildkitePipeline } from "../runners/buildkite.ts";
import { DEFAULT_AGENT_CONFIG, type FlakinessPlan, type RunnableCommand } from "../domain.ts";

const PROJECT_ROOT = resolve(`${import.meta.dirname}/../../../..`);

// The plan the Java scan task wrote. generate now runs on its OWN Buildkite step/agent (separate from the
// orchestration step that produced the plan), so in CI it downloads the plan from the orchestration step's
// artifacts. The LOCAL path is still live for the `local.ts` flow, which reads the file straight off disk.
const PLAN_FILE = "flakiness-plan.json";
// Written here for the analyze step to fold in as `not_applicable`. Keep in sync with entrypoints/analyze.ts.
const SKIPPED_FILE = "flakiness-skipped.json";
// Written here on buildFailed so the analyze step records a single `build_failed`. Keep in sync with
// entrypoints/analyze.ts and runners/buildkite.ts.
const PRECOMPILE_FILE = "flakiness-precompile.json";

/**
 * I/O boundary for {@link run}. Injecting it keeps run's decision logic (which plan branch, whether to
 * upload, what to annotate) testable without touching disk or shelling out to buildkite-agent.
 */
export interface GenerateIO {
  isCI: boolean;
  // The plan, or undefined when none exists even after a CI download attempt (upstream failed pre-plan).
  readPlan(): FlakinessPlan | undefined;
  writeFile(name: string, body: string): void;
  annotate(style: string, body: string): void;
  upload(commands: RunnableCommand[], opts: { hasNotApplicable: boolean }): void;
  log(msg: string): void;
}

/**
 * Read the plan.
 *
 * In CI the authoritative copy is always the orchestration step's artifact: generate runs on a DIFFERENT
 * agent, so any local `flakiness-plan.json` in its workspace can only be left over from a previous build on
 * a reused agent. Preferring the local file there would silently act on a stale plan, so the local file is
 * removed first and the artifact is downloaded unconditionally.
 *
 * Locally (no CI) there is no artifact to fetch and the local file written by the scan step IS the plan.
 *
 * Returns `undefined` when no plan exists even after the download attempt - which is the legitimate
 * "upstream produced nothing" case, not an error.
 */
function readPlanFromDisk(root: string): FlakinessPlan | undefined {
  const planPath = resolve(root, PLAN_FILE);
  if (process.env.CI) {
    rmSync(planPath, { force: true });
    try {
      execSync(`buildkite-agent artifact download "${PLAN_FILE}" .`, {
        cwd: root,
        stdio: ["pipe", "inherit", "inherit"],
      });
    } catch (err) {
      console.error(`Could not download ${PLAN_FILE}:`, err);
    }
  }
  if (existsSync(planPath) === false) return undefined;
  return JSON.parse(readFileSync(planPath, "utf8")) as FlakinessPlan;
}

function defaultIO(): GenerateIO {
  return {
    isCI: Boolean(process.env.CI),
    readPlan: () => readPlanFromDisk(PROJECT_ROOT),
    writeFile: (name, body) => writeFileSync(resolve(PROJECT_ROOT, name), body),
    annotate: (style, body) => {
      try {
        execSync(`buildkite-agent annotate --style "${style}" --context "flakiness-detection"`, {
          input: body,
          cwd: PROJECT_ROOT,
          stdio: ["pipe", "inherit", "inherit"],
        });
      } catch {
        // Annotation failures are non-fatal.
      }
    },
    upload: (commands, opts) => uploadBuildkitePipeline(commands, DEFAULT_AGENT_CONFIG, opts),
    log: (msg) => console.log(msg),
  };
}

/**
 * Surface the resolver's enrichment so abstract expansions, capped task fan-outs and unresolved refs are
 * never silently dropped.
 *
 * Expansions and task selections are logged to the console only - they are already recorded in
 * flakiness-plan.json, so an annotation would just be noise. Unresolved refs, by contrast, get a `warning`
 * annotation WHEN non-empty: a silently-unresolved unmute is a real false-negative (a test we meant to
 * re-check but never did). When there are no unresolved refs, no annotation is emitted.
 */
function reportEnrichment(plan: FlakinessPlan, io: GenerateIO): void {
  for (const e of plan.expansions ?? []) {
    io.log(`expanded abstract ${e.abstractFqcn} -> ran ${e.ran} of ${e.total} concrete subclasses (cap ${e.cap})`);
  }

  for (const s of plan.taskSelections ?? []) {
    io.log(
      `${s.gradleProject} (${s.sourceSet}): selected ${s.selected.length} of ${s.total} candidate tasks ` +
        `(cap ${s.cap}): ${s.selected.join(", ")}`
    );
  }

  const unresolved = plan.unresolved ?? [];
  if (unresolved.length > 0) {
    const lines = unresolved.map((u) => {
      const ref = u.ref.spec ?? u.ref.className ?? u.ref.path ?? JSON.stringify(u.ref);
      return `- unresolved (${u.reason}): \`${ref}\``;
    });
    io.annotate("warning", ["**Flakiness: unresolved references**", ...lines].join("\n"));
  }
}

export function run(io: GenerateIO = defaultIO()): void {
  const plan = io.readPlan();

  if (plan === undefined) {
    io.log(`No ${PLAN_FILE}; upstream orchestration failed - nothing to upload.`);
    return;
  }

  if (plan.buildFailed) {
    io.log(`Resolver reported buildFailed (${plan.reason ?? "precompile"}); uploading analyze-only pipeline`);
    if (io.isCI) {
      io.writeFile(PRECOMPILE_FILE, JSON.stringify({ outcome: "build_failed", reason: plan.reason ?? "precompile" }));
    }
    // hasNotApplicable forces the analyze step to be emitted with zero batches, so it can record the
    // build_failed outcome from the marker above.
    io.upload([], { hasNotApplicable: true });
    return;
  }

  const runEntries = plan.entries.filter((e) => e.disposition === "run");
  const skipEntries = plan.entries.filter((e) => e.disposition === "skip");
  const runnable = planCommandsToRunnable(plan.commands ?? [], "buildkite");

  io.log(
    `Plan: ${runEntries.length} run (${runnable.length} batch commands), ${skipEntries.length} skip, ` +
      `${(plan.unresolved ?? []).length} unresolved`
  );

  reportEnrichment(plan, io);

  if (skipEntries.length > 0 && io.isCI) {
    io.writeFile(SKIPPED_FILE, JSON.stringify(skipEntries.map(planEntryToSkippedTest)));
  }

  if (runnable.length === 0 && skipEntries.length === 0) {
    io.log("No runnable or skipped tests in plan");
    return;
  }

  // hasNotApplicable emits the analyze step even when every entry was skipped (zero batches).
  io.upload(runnable, { hasNotApplicable: skipEntries.length > 0 });
}

if (import.meta.main) run();
