import { execSync } from "child_process";
import { resolve } from "path";
import { stringify } from "yaml";

import {
  COMPILE_TASKS,
  FLAKINESS_PLAN_ARTIFACT,
  FLAKINESS_PRECOMPILE_ARTIFACT,
  FLAKINESS_REFS_ARTIFACT,
  FLAKINESS_TARGETS_ARCHIVE,
  FLAKINESS_TARGETS_DIR,
  JOB_STATUS_FILE_PREFIX,
  STATUS_DIR_NAME,
  TASK_STATUS_FILE_PREFIX,
} from "../domain.ts";
import type { AgentConfig, RunnableCommand, TestKind } from "../domain.ts";

const PROJECT_ROOT = resolve(`${import.meta.dirname}/../../../..`);

interface PipelineStep {
  label: string;
  key: string;
  command: string;
  timeout_in_minutes: number;
  // Optional so the analyze step can inherit the parent pipeline's default agent, which has npm.
  agents?: AgentConfig["agents"];
  parallelism?: number;
  env?: Record<string, string>;
  depends_on?: { step: string; allow_failure: boolean }[];
  artifact_paths?: string | string[];
  retry?: { automatic: boolean };
}

// Smart retry skips tests that already passed on a prior attempt - the opposite of what these steps need,
// since the batch steps deliberately re-run tests to surface flakiness. These steps are uploaded straight
// via `buildkite-agent pipeline upload`, so they never pass through injectAutoRetry
// (.buildkite/scripts/pull-request/pipeline.ts) and have to opt out here.
const NO_AUTO_RETRY: PipelineStep["retry"] = { automatic: false };

// Headroom between our inner `timeout` and Buildkite's outer `timeout_in_minutes`, which the agent
// enforces by SIGKILLing the step. Ours must win so the wrapper can still annotate and exit 0; if the
// agent fires first the step ends up in state "timed_out".
export const NEVER_FAIL_GRACE_MINUTES = 2;

// Where gradle-runner (the Tooling API client `.ci/scripts/run-gradle.sh` invokes) writes each task's
// outcome. Keep in sync with GradleRunner.writeStatusReport.
const TASK_STATUS_FILE = "build/task-status.json";

// The wrapped command and its task paths travel in env vars rather than being baked into the wrapper, so
// one copy of the wrapper serves every batch of a step. Indexed by BUILDKITE_PARALLEL_JOB at run time.
const CMD_VAR_PREFIX = "FLAKINESS_CMD_";
const TASK_PATHS_VAR_PREFIX = "FLAKINESS_TASK_PATHS_";

const NEVER_FAIL_SCRIPT = ".buildkite/scripts/flakiness-detection/runners/never-fail.sh";

/**
 * Runs command so that the Buildkite step ALWAYS exits 0. Why the step must exit 0, why soft_fail
 * is not an option, and what the per-job status file carries are all documented in the script itself.
 *
 * The inner timeout has to beat Buildkite's outer `timeout_in_minutes`, which the agent enforces by
 * SIGKILLing the step: ours must win so the runner can still annotate and exit 0, otherwise the step ends
 * up in state "timed_out".
 *
 * Omitting `kind` gives never-fail behaviour with no batch outcome, which is what the analyze step wants.
 */
function wrapNeverFail(contextKey: string, outerTimeoutMin: number, emitOutcome?: { kind: TestKind }): string {
  const args = [`--context ${contextKey}`, `--inner-timeout-minutes ${innerTimeout(outerTimeoutMin)}`];
  if (emitOutcome) {
    args.push(`--kind ${emitOutcome.kind}`);
  }
  return `${NEVER_FAIL_SCRIPT} ${args.join(" ")}`;
}

// Steps get fresh agents with no shared workspace, so batch steps upload their JUnit XML and analyze
// downloads it per job (`--step <jobId>`) to keep results attributed to a job.
const TEST_RESULTS_ARTIFACTS = "**/build/test-results/**/TEST-*.xml";

// Per-job status files (rc + duration) written by the never-fail wrapper and
// consumed by the analyze step. Glob is shallow so the upload is cheap.
const FLAKINESS_STATUS_ARTIFACTS = `${STATUS_DIR_NAME}/*.json`;

// Single structured artifact the analyze step uploads: a JSON array of per-job
// outcomes consumed by the external observability pipeline. Uploaded as an
// artifact (not an annotation) to keep the developer-facing build view clean.
// Keep this filename in sync with entrypoints/analyze.ts.
const FLAKINESS_OUTCOMES_ARTIFACT = "flakiness-outcomes.json";

// Written by the generate step from the plan's `skip` entries, whatever their reason: a source set with
// no enabled Test task, a packaging-host-only target, a class no Test task can address. Uploaded by that
// same step's `artifact_paths` and downloaded by analyze, which folds each one in as `not_applicable`.
// Keep in sync with SKIPPED_FILE in entrypoints/generate.ts and entrypoints/analyze.ts.
const FLAKINESS_SKIPPED_ARTIFACT = "flakiness-skipped.json";

// The pipeline topology and the reasoning behind its step split are described in README.md
// ("Pipeline topology"). Kept here are only the facts that would be silently re-broken if forgotten,
// each next to the code that depends on it.

const ORCHESTRATION_KEY = "flakiness-orchestration:run";
const GENERATE_KEY = "flakiness-orchestration:generate";

// Per-phase gradle budgets (each phase's own `timeout --foreground`). The resolve/compile/scan phases run
// back-to-back on one agent, so the orchestration step's outer timeout is their sum. generate runs on a
// separate step/agent with its own budget.
const RESOLVE_TIMEOUT_MINUTES = 30;
const COMPILE_TIMEOUT_MINUTES = 30;
const SCAN_TIMEOUT_MINUTES = 30;
const GENERATE_TIMEOUT_MINUTES = 15;
const ORCHESTRATION_TIMEOUT_MINUTES =
  RESOLVE_TIMEOUT_MINUTES + COMPILE_TIMEOUT_MINUTES + SCAN_TIMEOUT_MINUTES;

const GENERATE_ENTRYPOINT = "node .buildkite/scripts/flakiness-detection/entrypoints/generate.ts";

/**
 * The timeout a runner script should apply, in minutes: far enough inside Buildkite's outer
 * `timeout_in_minutes` that the script wins the race and can still classify the failure. If the agent
 * fires first it SIGKILLs the step and the run is reported as "timed_out" with nothing attributed.
 */
function innerTimeout(outerTimeoutMin: number): number {
  return Math.max(1, outerTimeoutMin - NEVER_FAIL_GRACE_MINUTES);
}

const ORCHESTRATE_SCRIPT = ".buildkite/scripts/flakiness-detection/runners/orchestrate.sh";

/**
 * Everything the orchestration step needs.
 */
function orchestrationEnv(): Record<string, string> {
  return {
    FLAKINESS_REFS_ARTIFACT,
    FLAKINESS_PLAN_ARTIFACT,
    FLAKINESS_PRECOMPILE_ARTIFACT,
    FLAKINESS_TARGETS_DIR,
    FLAKINESS_TARGETS_ARCHIVE,
    FLAKINESS_COMPILE_TASKS: COMPILE_TASKS.join(" "),
    FLAKINESS_RESOLVE_INNER_TIMEOUT: String(innerTimeout(RESOLVE_TIMEOUT_MINUTES)),
    FLAKINESS_COMPILE_INNER_TIMEOUT: String(innerTimeout(COMPILE_TIMEOUT_MINUTES)),
    FLAKINESS_SCAN_INNER_TIMEOUT: String(innerTimeout(SCAN_TIMEOUT_MINUTES)),
  };
}

/**
 * The generate shell: download the plan the orchestration step produced, then run the node entrypoint,
 * which uploads the batch + analyze steps. `|| true` tolerates orchestration having failed before writing
 * them; generate then logs and exits 0 without uploading.
 */
function generateCommand(): string {
  return [
    // The plan is NOT downloaded here: generate.ts removes any local copy and downloads it itself, so that
    // a stale file on a reused workspace can never win. Doing it twice would only mask that.
    `buildkite-agent artifact download "${FLAKINESS_PRECOMPILE_ARTIFACT}" . || true`,
    GENERATE_ENTRYPOINT,
  ].join("\n");
}

/**
 * Pure: the orchestration sub-pipeline the bootstrap step uploads - the orchestration step plus the
 * separate generate step.
 *
 * Both keys MUST stay under `flakiness-orchestration:`, never `flakiness-detection:`. An external metric
 * predicate treats a job as a test batch iff `step_key.startsWith("flakiness-detection:")` and it is not
 * `flakiness-detection:analyze`, so the wrong prefix here makes a failed orchestration run get recorded as
 * a test batch.
 */
export function toResolvePipeline(cfg: AgentConfig): Pipeline {
  const orchestration: PipelineStep = {
    label: "Flakiness / resolve · compile · scan",
    key: ORCHESTRATION_KEY,
    command: ORCHESTRATE_SCRIPT,
    timeout_in_minutes: ORCHESTRATION_TIMEOUT_MINUTES,
    env: orchestrationEnv(),
    // gradle-tuned image; it has no node (that is the generate step below).
    agents: { ...cfg.agents },
    // What a later, separate agent needs: the plan, the compile-failure marker, and the debug tarball.
    artifact_paths: [FLAKINESS_TARGETS_ARCHIVE, FLAKINESS_PLAN_ARTIFACT, FLAKINESS_PRECOMPILE_ARTIFACT],
    retry: NO_AUTO_RETRY,
  };
  const generate: PipelineStep = {
    label: "Flakiness / generate",
    key: GENERATE_KEY,
    command: generateCommand(),
    timeout_in_minutes: GENERATE_TIMEOUT_MINUTES,
    // No `agents:` pin, so this gets the default node-capable image - the reason generate is not inline in
    // the orchestration step, whose gradle-tuned image has no node.
    // allow_failure so a compile-failed (red) orchestration run still triggers generate, which then uploads
    // the analyze-only pipeline that records the single build_failed.
    depends_on: [{ step: ORCHESTRATION_KEY, allow_failure: true }],
    // All consumed by the later analyze step.
    artifact_paths: [FLAKINESS_SKIPPED_ARTIFACT, FLAKINESS_PRECOMPILE_ARTIFACT, FLAKINESS_PLAN_ARTIFACT],
    retry: NO_AUTO_RETRY,
  };
  return { steps: [{ group: cfg.groupName, steps: [orchestration, generate] }] };
}

/** Impure: serialize and upload the orchestration sub-pipeline. Called by the bootstrap entrypoints. */
export function uploadResolvePipeline(cfg: AgentConfig, opts: { cwd?: string } = {}): void {
  const cwd = opts.cwd ?? PROJECT_ROOT;
  const yaml = stringify(toResolvePipeline(cfg));
  console.log("--- Generated resolve pipeline");
  console.log(yaml);
  if (process.env.CI) {
    console.log("Uploading resolve pipeline...");
    execSync(`buildkite-agent pipeline upload`, { input: yaml, stdio: ["pipe", "inherit", "inherit"], cwd });
  }
}

interface PipelineGroup {
  group: string;
  steps: PipelineStep[];
}

interface Pipeline {
  // Pipeline-level env, so the layout constants appear once rather than on every step. The runner scripts
  // read them from here, which keeps domain.ts the single source for the names.
  env?: Record<string, string>;
  steps: [PipelineGroup];
}

/** The artifact layout both runner scripts need, named once for the whole pipeline. */
const RUNNER_LAYOUT_ENV: Record<string, string> = {
  FLAKINESS_STATUS_DIR: STATUS_DIR_NAME,
  FLAKINESS_JOB_STATUS_PREFIX: JOB_STATUS_FILE_PREFIX,
  FLAKINESS_TASK_STATUS_PREFIX: TASK_STATUS_FILE_PREFIX,
  FLAKINESS_TASK_STATUS_FILE: TASK_STATUS_FILE,
};

/**
 * Pure: build the BK pipeline structure. Groups commands by step `key`; a key with more than one batch
 * fans out via `parallelism` + BUILDKITE_PARALLEL_JOB.
 */
export function toBuildkitePipeline(
  commands: RunnableCommand[],
  cfg: AgentConfig,
  // `hasNotApplicable`: emit the analyze step even with zero batch steps, so BWC `not_applicable` records
  // still reach the outcomes artifact.
  opts: { hasNotApplicable?: boolean } = {}
): Pipeline {
  const byKey = new Map<string, RunnableCommand[]>();
  for (const c of commands) {
    const list = byKey.get(c.key);
    if (list) list.push(c);
    else byKey.set(c.key, [c]);
  }

  const steps: PipelineStep[] = [];
  for (const [key, batches] of byKey) {
    const head = batches[0];
    // Every batch contributes its raw command and its own task paths; the wrapper is emitted once and
    // indexes into these at run time. All batches of a step share a kind (they are grouped by key), so
    // kind stays a build-time literal.
    const env: Record<string, string> = {};
    batches.forEach((b, i) => {
      env[`${CMD_VAR_PREFIX}${i}`] = b.command;
      env[`${TASK_PATHS_VAR_PREFIX}${i}`] = JSON.stringify(b.taskPaths ?? []);
    });

    const step: PipelineStep = {
      label: head.label,
      key,
      command: wrapNeverFail(key, cfg.timeoutInMinutes, { kind: head.kind }),
      timeout_in_minutes: cfg.timeoutInMinutes,
      agents: { ...cfg.agents },
      artifact_paths: [TEST_RESULTS_ARTIFACTS, FLAKINESS_STATUS_ARTIFACTS],
      env,
      retry: NO_AUTO_RETRY,
    };
    if (batches.length > 1) {
      step.parallelism = batches.length;
    }

    steps.push(step);
  }

  if (steps.length > 0 || opts.hasNotApplicable) {
    // allow_failure so the report still runs when a batch fails - it has to record those outcomes too.
    const deps = steps.map((s) => ({ step: s.key, allow_failure: true }));
    steps.push({
      label: "Flakiness / report",
      key: "flakiness-detection:analyze",
      // The analyzer downloads each job's JUnit XML itself (`--step <jobId>`) so results stay attributed
      // to a job before classification. `|| true` tolerates a build with no status/skipped artifacts.
      command: wrapNeverFail("flakiness-detection:analyze", 10),
      // Never-fail like a batch step, but with no `kind`, so it writes no batch outcome of its own.
      env: {
        [`${CMD_VAR_PREFIX}0`]: [
          `buildkite-agent artifact download "${FLAKINESS_STATUS_ARTIFACTS}" . || true`,
          `buildkite-agent artifact download "${FLAKINESS_SKIPPED_ARTIFACT}" . || true`,
          `buildkite-agent artifact download "${FLAKINESS_PRECOMPILE_ARTIFACT}" . || true`,
          "node .buildkite/scripts/flakiness-detection/entrypoints/analyze.ts",
        ].join("\n"),
      },
      timeout_in_minutes: 10,
      // No `agents:` on purpose: this is lightweight markdown rendering, and the gradle-tuned image has no
      // npm. The parent pipeline's default agent has the standard Node toolchain.
      artifact_paths: FLAKINESS_OUTCOMES_ARTIFACT,
      depends_on: deps,
      retry: NO_AUTO_RETRY,
    });
  }

  return {
    env: { ...RUNNER_LAYOUT_ENV },
    steps: [{ group: cfg.groupName, steps }],
  };
}

/**
 * Impure: serialize and upload the pipeline via buildkite-agent.
 */
export function uploadBuildkitePipeline(
  commands: RunnableCommand[],
  cfg: AgentConfig,
  opts: { hasNotApplicable?: boolean; cwd?: string } = {}
): void {
  const cwd = opts.cwd ?? PROJECT_ROOT;
  const yaml = stringify(toBuildkitePipeline(commands, cfg, { hasNotApplicable: opts.hasNotApplicable }));
  console.log("--- Generated pipeline");
  console.log(yaml);

  if (process.env.CI) {
    console.log("Uploading pipeline...");
    execSync(`buildkite-agent pipeline upload`, {
      input: yaml,
      stdio: ["pipe", "inherit", "inherit"],
      cwd,
    });
  }
}
