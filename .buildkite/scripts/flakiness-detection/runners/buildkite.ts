import { execSync } from "child_process";
import { resolve } from "path";
import { stringify } from "yaml";

import { COMPILE_TASKS, JOB_STATUS_FILE_PREFIX, STATUS_DIR_NAME, TASK_STATUS_FILE_PREFIX } from "../domain.ts";
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

// Per-job copy of the above, uploaded by the status-dir glob and read back by the analyze step, which
// rebuilds this path from the jobId.
const TASK_STATUS_COPY_PREFIX = `${STATUS_DIR_NAME}/${TASK_STATUS_FILE_PREFIX}`;

// The wrapped command and its task paths travel in env vars rather than being baked into the wrapper, so
// one copy of the wrapper serves every batch of a step. Indexed by BUILDKITE_PARALLEL_JOB at run time.
const CMD_VAR_PREFIX = "FLAKINESS_CMD_";
const TASK_PATHS_VAR_PREFIX = "FLAKINESS_TASK_PATHS_";

// Wraps a shell command so it always exits 0, appending a Buildkite warning annotation when the wrapped
// command fails. The step state stays "passed", so Buildkite's per-step and group-aggregate GitHub commit
// statuses report success.
//
// soft_fail is not used: Buildkite's GitHub commit-status integration mirrors step.state ("failed" /
// "timed_out") and ignores the soft_failed flag, so a soft_fail step that fails still shows a red check.
//
// The command runs under GNU `timeout` (see NEVER_FAIL_GRACE_MINUTES) so an overrun is killed by us rather
// than by the agent, and the wrapper still reaches `exit 0`.
//
// With `emitOutcome` (batch steps; not analyze) the wrapper also records rc + wall-clock duration to
// `<status dir>/status-<jobId>.json` - the two facts JUnit XML cannot supply. It classifies nothing;
// see entrypoints/analyze.ts and README "Observability".
function wrapNeverFail(contextKey: string, outerTimeoutMin: number, emitOutcome?: { kind: TestKind }): string {
  const innerTimeoutMin = Math.max(1, outerTimeoutMin - NEVER_FAIL_GRACE_MINUTES);
  return [
    "set +e",
    // The command is not baked in: it comes from an env var picked at run time, so this wrapper appears
    // once per step instead of once per batch. BUILDKITE_PARALLEL_JOB is unset on a non-parallel step, so
    // `:-0` selects the single batch and both step shapes share one code path.
    '_fd_i="$${BUILDKITE_PARALLEL_JOB:-0}"',
    `_fd_cmd_var="${CMD_VAR_PREFIX}$$_fd_i"`,
    "WRAPPED_CMD_FILE=$(mktemp)",
    // `$${!var}` is indirect expansion deferred past Buildkite's upload pass, which cannot parse `!` as the
    // start of an identifier. printf keeps the command unexpanded until bash runs the file, which is what
    // the quoted heredoc used to do.
    'printf \'%s\\n\' "$${!_fd_cmd_var}" > "$$WRAPPED_CMD_FILE"',
    // Lets the self-report tell a timeout SIGKILL from a kernel OOM-kill by duration. `$(...)` survives
    // Buildkite's upload-time interpolation, which only substitutes `$VAR`/`${VAR}`.
    ...(emitOutcome ? ["_fd_start=$(date +%s)"] : []),
    // --foreground keeps the command in the parent's process group. Without it `timeout` setpgid()s its
    // child, the gradle CLI loses the controlling-TTY plumbing the develocity scan plugin relies on, and
    // the CLI JVM hangs ~36min after BUILD SUCCESSFUL. Diagnosed on build #2 of
    // elasticsearch-flakiness-detection-manual.
    `timeout --foreground --signal=TERM --kill-after=30s ${innerTimeoutMin}m bash "$$WRAPPED_CMD_FILE"`,
    "rc=$?",
    "rm -f \"$$WRAPPED_CMD_FILE\"",
    `if [ "$$rc" -eq 124 ] || [ "$$rc" -eq 137 ]; then`,
    `  buildkite-agent annotate --style warning --context "${contextKey}-failures" --append "[$$BUILDKITE_LABEL] (job $$BUILDKITE_JOB_ID) timed out after ${innerTimeoutMin}m (rc=$$rc) - see job log"`,
    `elif [ "$$rc" -ne 0 ]; then`,
    `  buildkite-agent annotate --style warning --context "${contextKey}-failures" --append "[$$BUILDKITE_LABEL] (job $$BUILDKITE_JOB_ID) exited with $$rc - see job log"`,
    "fi",
    // Best-effort per-job status file for analyze. `|| true` plus the trailing `exit 0` mean observability
    // can never fail a batch. Runtime values (`rc`, duration, oom) are `$$`-escaped to defer past
    // Buildkite's upload pass; `stepKey`/`kind` are build-time constants.
    //
    // OOM: every ES test JVM runs with `-XX:+HeapDumpOnOutOfMemoryError` and a heapdump path under
    // buildDir (ElasticsearchTestBasePlugin), so a leftover `.hprof` means a JVM-heap OutOfMemoryError -
    // which exits rc=1 via Gradle, unlike the rc=137 SIGKILL a kernel OOM-kill gives. Detected from the
    // file rather than the log so we never touch the wrapped command's stdout/`--foreground` plumbing.
    // analyze.ts turns it into the `oom` infraSubtype.
    ...(emitOutcome
      ? [
          "_fd_end=$(date +%s)",
          `mkdir -p ${STATUS_DIR_NAME}`,
          "_fd_oom=\"\"",
          "if [ -n \"$(find . -type f -path '*/build/heapdump/*.hprof' -print -quit 2>/dev/null)\" ]; then _fd_oom=\"oom\"; fi",
          // A task rejected by `onlyIf` (bwc's `bwc_tests_enabled`, the distro arch check) or with no
          // source is reported SKIPPED: zero tests, exit 0 - from rc alone, indistinguishable from a hang.
          // gradle-runner records every task's outcome in build/task-status.json, so analyze reads the
          // verdict for THIS batch's task paths. The scoping is the point: a healthy build has unrelated
          // SKIPPED entries, and an unscoped check would mislabel the muted-tests case (task ran, filter
          // matched nothing) as not_applicable.
          //
          // For REST kinds each repeat-rest-test.sh iteration overwrites the file, so this is the last
          // iteration's verdict - sound, because `onlyIf` predicates do not flip mid-job.
          //
          // Only COPIED here; analyze.ts parses the JSON, so this shell stays uncoupled from its spacing.
          `cp ${TASK_STATUS_FILE} "${TASK_STATUS_COPY_PREFIX}$$BUILDKITE_JOB_ID.json" 2>/dev/null || true`,
          `_fd_tp_var="${TASK_PATHS_VAR_PREFIX}$$_fd_i"`,
          // Resolved into a plain variable here so the printf below needs no braces: `$${...}` inside a TS
          // template literal would be read as an interpolation. `:-[]` keeps the JSON well formed if the
          // var is somehow unset.
          '_fd_tp="$${!_fd_tp_var:-[]}"',
          `printf '{"jobId":"%s","stepKey":"%s","kind":"%s","rc":%s,"durationSec":%s,"infraSubtype":"%s","taskPaths":%s}' "$$BUILDKITE_JOB_ID" "${contextKey}" "${emitOutcome.kind}" "$$rc" "$(( _fd_end - _fd_start ))" "$$_fd_oom" "$$_fd_tp" > "${STATUS_DIR_NAME}/${JOB_STATUS_FILE_PREFIX}$$BUILDKITE_JOB_ID.json" || true`,
        ]
      : []),
    "exit 0",
  ].join("\n");
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

// Written by the bootstrap step (entrypoints/pr.ts) listing tests that could not
// be re-run (BWC projects). Downloaded by the analyze step, which folds them into
// the outcomes artifact as `not_applicable`. Keep in sync with entrypoints/pr.ts
// and the bootstrap step's `artifact_paths` in pipelines/pull-request/flakiness-detection.yml.
const FLAKINESS_SKIPPED_ARTIFACT = "flakiness-skipped.json";

// Written by the compile orchestration step only when compilation fails, so the
// analyze step (run from generate) can record a single `build_failed` outcome
// instead of the batches (which are skipped) producing none. Keep in sync with
// entrypoints/analyze.ts.
const FLAKINESS_PRECOMPILE_ARTIFACT = "flakiness-precompile.json";

// The pipeline topology and the reasoning behind its step split are described in README.md
// ("Pipeline topology"). Kept here are only the facts that would be silently re-broken if forgotten,
// each next to the code that depends on it.

// Written by the bootstrap step, consumed by the resolve step (downloaded onto its fresh agent).
const FLAKINESS_REFS_ARTIFACT = "flakiness-refs.json";
// Written by the scan step (or, on failure, by the compile step), consumed by the generate step.
const FLAKINESS_PLAN_ARTIFACT = "flakiness-plan.json";
// Where each project drops its share of the resolve answer: `<project>.json`, carrying its resolved targets
// and its class directories, both consumed by the scan step. Shell/Java contract only (no TS type). Keep in
// sync with FlakinessProjectResolvePlugin.TARGETS_DIR on the Java side.
const FLAKINESS_TARGETS_DIR = "build/flakiness/project-targets";
// One tarball, not a `*.json` glob: every project writes a file whether or not it owns a ref, so a glob
// would mean ~450 uploads per build of pure debugging detail. Nothing downstream reads them - resolve,
// compile and scan share an agent, so scan reads them off local disk.
const FLAKINESS_TARGETS_ARCHIVE = "flakiness-project-targets.tgz";

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

// Fire each phase's inner gradle timeout a grace period before its budget so we can capture the exit code
// (and write the buildFailed markers, for compile) before it runs long. `--foreground` keeps the gradle CLI
// in the parent process group so its develocity scan plugin does not hang (see wrapNeverFail).
function innerGradleTimeout(outerTimeoutMin: number): string {
  const inner = Math.max(1, outerTimeoutMin - NEVER_FAIL_GRACE_MINUTES);
  return `timeout --foreground --signal=TERM --kill-after=30s ${inner}m`;
}

/**
 * The orchestration shell: resolve -> compile -> scan, sequentially on ONE gradle agent (scan reads the
 * `build/classes` output compile produced, and separate agents share no workspace).
 *
 * Which phase failed decides how the run is reported, so the exit codes are not interchangeable:
 *  - compile non-zero -> the SOLE build_failed signal: write the buildFailed plan + precompile marker,
 *                        then exit rc. generate depends on this step with allow_failure and turns those
 *                        markers into the single build_failed record.
 *  - resolve or scan non-zero -> a resolver/infra defect, NOT build_failed: write no marker, exit rc.
 *
 * `$$rc` defers past Buildkite's pipeline-upload interpolation pass.
 */
function orchestrationCommand(): string {
  return [
    // refs are produced by the bootstrap step on a different agent, so fetch them onto this one.
    `buildkite-agent artifact download "${FLAKINESS_REFS_ARTIFACT}" . || true`,
    "set +e",
    "",
    "# --- resolve ---",
    // UNQUALIFIED on purpose: every project that registered the task runs it and self-selects on whether a
    // ref lands in its own source sets. The configuration cache stays ON - each project's model reaches the
    // task as an @Input, which survives the configuration/execution boundary.
    `${innerGradleTimeout(RESOLVE_TIMEOUT_MINUTES)} .ci/scripts/run-gradle.sh -Pflakiness.resolve flakinessResolveProject`,
    "rc=$?",
    `if [ "$$rc" -ne 0 ]; then`,
    `  echo "flakiness resolve failed (rc=$$rc): resolver/infra defect, not a PR build failure."`,
    "  exit $$rc",
    "fi",
    "",
    `tar -czf ${FLAKINESS_TARGETS_ARCHIVE} -C ${FLAKINESS_TARGETS_DIR} . 2>/dev/null || true`,
    "",

    "# --- compile (every test source set in the repo; see COMPILE_TASKS) ---",
    // UNQUALIFIED, so the whole repo compiles rather than only the projects that owned a ref - that is what
    // lets scan resolve an abstract base against subclasses in other projects.
    //
    // The guard is the second of two gates (`pr.ts` is the first, and coarser), and it exists because a PR
    // touching only `src/main/java` still produces refs that resolve to nothing runnable. `"refIndex"` is
    // the marker: it appears in a per-project file exactly when that project resolved a target, which keeps
    // this a single-token grep instead of parsing JSON in shell. Keep in sync with
    // FlakinessJson.RefTarget#refIndex.
    `if grep -qs '"refIndex"' ${FLAKINESS_TARGETS_DIR}/*.json; then`,
    `  ${innerGradleTimeout(COMPILE_TIMEOUT_MINUTES)} .ci/scripts/run-gradle.sh ${COMPILE_TASKS.join(" ")}`,
    "  rc=$?",
    `  if [ "$$rc" -ne 0 ]; then`,
    // compile is the ONLY build_failed signal. Leave the markers, then propagate the red exit. The separate
    // generate step (depends_on allow_failure) picks up the markers and records the single build_failed.
    `    printf '{"buildFailed":true,"reason":"precompile","entries":[]}' > ${FLAKINESS_PLAN_ARTIFACT}`,
    `    printf '{"outcome":"build_failed","reason":"precompile"}' > ${FLAKINESS_PRECOMPILE_ARTIFACT}`,
    "    exit $$rc",
    "  fi",
    "else",
    `  echo "resolve produced no runnable targets; skipping the repo-wide test compile."`,
    "fi",
    "",

    // Runs even when compile was skipped: scan is what reports refs no project could claim (a muted-tests
    // entry naming a deleted class). Worth its ~9s - it is the only signal for those refs.
    "# --- scan (reads the now-local compiled output; no cross-agent shipping needed) ---",
    `${innerGradleTimeout(SCAN_TIMEOUT_MINUTES)} .ci/scripts/run-gradle.sh -Pflakiness.resolve flakinessScan`,
    "rc=$?",
    `if [ "$$rc" -ne 0 ]; then`,
    `  echo "flakiness scan failed (rc=$$rc): resolver/infra defect, not a PR build failure."`,
    "  exit $$rc",
    "fi",
    "",
    "# --- happy path: scan wrote the plan; the separate generate step uploads batch + analyze ---",
    "exit 0",
  ].join("\n");
}

/**
 * The generate shell: download the plan the orchestration step produced, then run the node entrypoint,
 * which uploads the batch + analyze steps. `|| true` tolerates orchestration having failed before writing
 * them; generate then logs and exits 0 without uploading.
 */
function generateCommand(): string {
  return [
    `buildkite-agent artifact download "${FLAKINESS_PLAN_ARTIFACT}" . || true`,
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
    command: orchestrationCommand(),
    timeout_in_minutes: ORCHESTRATION_TIMEOUT_MINUTES,
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
  steps: [PipelineGroup];
}

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
      label: "flakiness report",
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
