import { execSync } from "child_process";
import { resolve } from "path";
import { stringify } from "yaml";

import type { AgentConfig, RunnableCommand, TestKind } from "../domain.ts";

const PROJECT_ROOT = resolve(`${import.meta.dirname}/../../../..`);

interface PipelineStep {
  label: string;
  key: string;
  command: string;
  timeout_in_minutes: number;
  // Optional so the analyze step can inherit the parent PR pipeline's default
  // agent (which has npm). Batch steps still set this to the gradle-tuned image.
  agents?: AgentConfig["agents"];
  parallelism?: number;
  env?: Record<string, string>;
  depends_on?: { step: string; allow_failure: boolean }[];
  artifact_paths?: string | string[];
  retry?: { automatic: boolean };
}

// Flakiness-detection steps must never be smart-retried. Buildkite's smart retry
// skips tests that already passed on a prior attempt, which is the opposite of
// what these steps need: the batch steps deliberately re-run tests many times to
// surface flakiness, and the analyze step just aggregates their results.
//
// The static bootstrap step in flakiness-detection.yml opts out via
// `config.auto-retry: false`, but these steps are uploaded directly via
// `buildkite-agent pipeline upload` and never pass through injectAutoRetry
// (.buildkite/scripts/pull-request/pipeline.ts), so we disable automatic retries
// explicitly here. Today they also can't fail because wrapNeverFail forces
// `exit 0`; setting this keeps them excluded from smart retry even if that
// always-pass wrapper is ever removed.
const NO_AUTO_RETRY: PipelineStep["retry"] = { automatic: false };

// Minutes of headroom kept between the inner `timeout` (which we own) and the
// outer Buildkite `timeout_in_minutes` (which the agent enforces by SIGKILLing
// the whole step). The wrapper needs to win the race so it can annotate and
// exit 0; if the BK agent fires first the step ends up in state "timed_out".
export const NEVER_FAIL_GRACE_MINUTES = 2;

// Wraps a shell command so it always exits 0. If the wrapped command exits
// non-zero, a Buildkite warning annotation is appended so the failure is still
// visible on the build, but the step's state stays "passed" so that Buildkite's
// per-step and group-aggregate GitHub commit statuses report success.
//
// To also handle the case where the wrapped command runs past the step's
// timeout_in_minutes, the command is run under GNU `timeout` set to fire a few
// minutes before the BK outer timeout. When the inner timeout fires, `timeout`
// exits 124 (SIGTERM cleanup) or 137 (SIGKILL after the grace period) and the
// wrapper still reaches `exit 0`. Without this, the BK agent would SIGKILL the
// whole bash process tree externally and the wrapper would never get to run.
//
// soft_fail is not used because Buildkite's GitHub commit-status integration
// mirrors step.state ("failed" / "timed_out") and ignores the soft_failed
// flag, so a soft_fail step that exits non-zero or times out still surfaces
// as a red check on the PR.
//
// When `emitOutcome` is set (true for test-batch steps, omitted for the
// lightweight analyze step) the wrapper additionally records a tiny per-job
// status file (`flakiness-status/status-<jobId>.json`) carrying the wrapped
// command's return code and wall-clock duration. The status files are uploaded
// as artifacts; the analyze step downloads them, classifies each job from its
// rc + JUnit XML, and uploads a single structured artifact. The wrapper
// itself does no classification - it only captures rc + duration, which the
// JUnit XML cannot provide. See entrypoints/analyze.ts and README "Observability".
// Where gradle-runner (the Tooling API client `.ci/scripts/run-gradle.sh` invokes)
// writes each task's outcome. Keep in sync with GradleRunner.writeStatusReport.
const TASK_STATUS_FILE = "build/task-status.json";

// Per-job copy of the above, uploaded by the existing `flakiness-status/*.json` glob and read back by the
// analyze step. Keep the prefix in sync with taskStatusFileFor() in entrypoints/analyze.ts.
export const TASK_STATUS_COPY_PREFIX = "flakiness-status/tasks-";

function wrapNeverFail(
  command: string,
  contextKey: string,
  outerTimeoutMin: number,
  emitOutcome?: { kind: TestKind; taskPaths?: string[] }
): string {
  const innerTimeoutMin = Math.max(1, outerTimeoutMin - NEVER_FAIL_GRACE_MINUTES);
  return [
    "set +e",
    "WRAPPED_CMD_FILE=$(mktemp)",
    // Quoted heredoc avoids any shell-expansion of the inner command at
    // write time; variables in it are evaluated when bash runs the file.
    "cat > \"$$WRAPPED_CMD_FILE\" <<'__NEVER_FAIL_EOF__'",
    command,
    "__NEVER_FAIL_EOF__",
    // Wall-clock start, captured just before the run so the self-report below
    // can disambiguate a real timeout SIGKILL from a kernel OOM-kill by
    // duration. `$(...)` survives Buildkite's upload-time interpolation
    // (it only substitutes `$VAR`/`${VAR}`), same as `$(mktemp)` above. Only
    // emitted when this step self-reports (the analyze step does not).
    ...(emitOutcome ? ["_fd_start=$(date +%s)"] : []),
    // --foreground keeps the wrapped command in the parent's process group;
    // without it `timeout` setpgid()s its child, the gradle CLI loses the
    // controlling-TTY plumbing the develocity scan plugin relies on, and the
    // CLI JVM hangs ~36 minutes after BUILD SUCCESSFUL until the inner
    // timeout fires. Diagnosed on build #2 of elasticsearch-flakiness-detection-manual.
    `timeout --foreground --signal=TERM --kill-after=30s ${innerTimeoutMin}m bash "$$WRAPPED_CMD_FILE"`,
    "rc=$?",
    "rm -f \"$$WRAPPED_CMD_FILE\"",
    `if [ "$$rc" -eq 124 ] || [ "$$rc" -eq 137 ]; then`,
    `  buildkite-agent annotate --style warning --context "${contextKey}-failures" --append "[$$BUILDKITE_LABEL] (job $$BUILDKITE_JOB_ID) timed out after ${innerTimeoutMin}m (rc=$$rc) - see job log"`,
    `elif [ "$$rc" -ne 0 ]; then`,
    `  buildkite-agent annotate --style warning --context "${contextKey}-failures" --append "[$$BUILDKITE_LABEL] (job $$BUILDKITE_JOB_ID) exited with $$rc - see job log"`,
    "fi",
    // Best-effort per-job status file for the analyze step to pick up. `|| true`
    // and the trailing `exit 0` ensure observability can never fail a batch.
    // `stepKey`/`kind` are build-time constants; `rc`/duration/oom are runtime, so
    // they are `$$`-escaped to defer past Buildkite's pipeline-upload pass.
    //
    // OOM detection: every ES test JVM runs with `-XX:+HeapDumpOnOutOfMemoryError`
    // and `-XX:HeapDumpPath=<buildDir>/heapdump` (ElasticsearchTestBasePlugin), so
    // a `*/build/heapdump/*.hprof` file after the run means a JVM-heap
    // OutOfMemoryError occurred - which exits via Gradle with rc=1, not the
    // SIGKILL rc=137 the kernel OOM-killer produces. We detect it from the file
    // (not the log) to avoid touching the wrapped command's stdout/`--foreground`
    // plumbing; `-quit` stops at the first match. analyze.ts turns this into the
    // `oom` infraSubtype.
    ...(emitOutcome
      ? [
          "_fd_end=$(date +%s)",
          "mkdir -p flakiness-status",
          "_fd_oom=\"\"",
          "if [ -n \"$(find . -type f -path '*/build/heapdump/*.hprof' -print -quit 2>/dev/null)\" ]; then _fd_oom=\"oom\"; fi",
          // Skipped-task detection: Gradle reports a task rejected by `onlyIf` (bwc's `bwc_tests_enabled`,
          // the distro architecture check) - and one with no source - as SKIPPED, running zero tests and
          // exiting 0. From rc alone that is indistinguishable from a hang. gradle-runner (the Tooling API
          // client every CI invocation goes through) records each task's outcome in build/task-status.json,
          // so we read the verdict for THIS batch's own task paths. Scoping to those paths matters: a
          // healthy build contains unrelated SKIPPED entries, so an unscoped check would mislabel the
          // muted-tests case (task ran, filter matched nothing) as not_applicable. analyze.ts turns this
          // into the `task-skipped` not_applicable reason.
          //
          // For REST kinds repeat-rest-test.sh loops the gradle invocation and each iteration overwrites
          // task-status.json, so this is the LAST iteration's verdict. That is sound here because `onlyIf`
          // predicates are static for the life of a job - they do not flip between iterations seconds apart.
          //
          // The wrapper only COPIES the report; analyze.ts parses it as JSON and does the matching. Grepping
          // it here would couple this shell to gradle-runner's exact spacing (`{ "path" : ... }`).
          `cp ${TASK_STATUS_FILE} "${TASK_STATUS_COPY_PREFIX}$$BUILDKITE_JOB_ID.json" 2>/dev/null || true`,
          `printf '{"jobId":"%s","stepKey":"%s","kind":"%s","rc":%s,"durationSec":%s,"infraSubtype":"%s","taskPaths":%s}' "$$BUILDKITE_JOB_ID" "${contextKey}" "${emitOutcome.kind}" "$$rc" "$(( _fd_end - _fd_start ))" "$$_fd_oom" '${JSON.stringify(emitOutcome.taskPaths ?? [])}' > "flakiness-status/status-$$BUILDKITE_JOB_ID.json" || true`,
        ]
      : []),
    "exit 0",
  ].join("\n");
}

// Each BK step runs on its own fresh agent — workspaces are not shared. To get
// the JUnit XML written by the batch steps to the analyze step's agent, the
// batch steps upload them as build artifacts and the analyze step downloads
// them per job (via `--step <jobId>`) so it can attribute results to a job.
// The walker in `analyzer/analyze.ts` picks the files up at `*/build/test-results/...`.
const TEST_RESULTS_ARTIFACTS = "**/build/test-results/**/TEST-*.xml";

// Per-job status files (rc + duration) written by the never-fail wrapper and
// consumed by the analyze step. Glob is shallow so the upload is cheap.
const FLAKINESS_STATUS_ARTIFACTS = "flakiness-status/*.json";

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

// ---------------------------------------------------------------------------
// Topology: the bootstrap step (pr.ts / manual.ts) uploads TWO steps into the group:
// an orchestration step (resolve -> compile -> scan on a SINGLE gradle agent) and a
// separate generate step (TS, on the default node-capable agent). The batch + analyze
// steps are uploaded later, dynamically, by the generate step (toBuildkitePipeline).
//
//   orchestration (Gradle agent):
//     resolve  runs `flakinessResolveProject` UNQUALIFIED, so every project runs it and each one
//              decides for itself whether it owns a ref; the owners write their share into
//              build/flakiness/project-targets/ (<project>.json + <project>.compile-tasks.txt).
//     compile  PLAIN invocation of the concatenated *.compile-tasks.txt. Its non-zero exit is the
//              ONLY build_failed signal; on failure it writes flakiness-plan.json (buildFailed) +
//              flakiness-precompile.json, then exits non-zero. It does NOT run generate - the
//              separate generate step handles it.
//     scan     reads the per-project targets directly (no merge task), ASM-scans the LOCAL compiled
//              output (produced by the compile phase on the same agent), writes flakiness-plan.json.
//
//   generate (TS, default agent): downloads flakiness-plan.json (+ precompile marker) from the
//     orchestration step's artifacts and uploads the batch + analyze steps.
//
// Why resolve/compile/scan share one step: BK steps run on fresh agents with no shared workspace, and
// nothing ships compile's build/classes to a separate scan step - so on real agents scan would find zero
// compiled classes. One agent keeps that output on local disk for scan and warms the gradle daemon. See
// orchestrationCommand + JAVA_RESOLVER_NOTES.md.
//
// Why generate is its OWN step (not inline in orchestration): generate is node, and the gradle-tuned image
// the orchestration step pins lacks node (the prior residual risk). A separate step with NO `agents:` pin
// uses the default node-capable image; it downloads the plan the orchestration step produced.
//
// CRITICAL: BOTH orchestration steps are keyed under `flakiness-orchestration:` - NOT
// `flakiness-detection:`. An external metric predicate treats any job whose step_key
// starts with `flakiness-detection:` (except `:analyze`) as a test batch; keying an
// orchestration step under that prefix would make a red/failed orchestration run get
// fallback-recorded as a test batch. Only the actual test batch steps (KIND_KEYS) and
// the analyze step keep the `flakiness-detection:` prefix.
// ---------------------------------------------------------------------------

// Written by the bootstrap step, consumed by the resolve step (downloaded onto its fresh agent).
const FLAKINESS_REFS_ARTIFACT = "flakiness-refs.json";
// Written by the scan step (or, on failure, by the compile step), consumed by the generate step.
const FLAKINESS_PLAN_ARTIFACT = "flakiness-plan.json";
// Where each project drops its share of the resolve answer: `<project>.json` (consumed by the scan step)
// and `<project>.compile-tasks.txt` (concatenated by the compile phase). Shell/Java contracts only (no TS
// type). Keep in sync with FlakinessProjectResolvePlugin.TARGETS_DIR on the Java side.
const FLAKINESS_TARGETS_DIR = "build/flakiness/project-targets";
// The per-project files are uploaded as ONE tarball rather than as ~450 individual artifacts: every project
// writes its share (owners and non-owners alike), so a `*.json` glob would mean ~450 uploads per build for
// what is purely post-hoc debugging detail - nothing downstream reads them (resolve, compile and scan all
// share one agent, so the scan step reads them straight off local disk).
const FLAKINESS_TARGETS_ARCHIVE = "flakiness-project-targets.tgz";
// The exact task list the compile phase invoked, flattened out of the per-project files. Uploaded on its own
// because "what did we actually compile?" is the first question when a build_failed is being triaged.
const FLAKINESS_COMPILE_TASKS_ARTIFACT = "flakiness-compile-tasks.txt";

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
 * The orchestration shell: resolve -> compile -> scan, run sequentially on ONE gradle agent. It never runs
 * generate - that is a separate step on a node-capable agent (see toResolvePipeline).
 *
 * Why resolve/compile/scan share one agent: Buildkite steps run on fresh agents with no shared workspace.
 * The scan phase reads the `build/classes` output the compile phase produced; across separate agents nothing
 * ships that output to scan, so `flakinessScan` would find zero compiled classes. One agent keeps the
 * compiled output on local disk for scan and warms the gradle daemon across the invocations. It does NOT
 * change the three gradle invocations or the CC / whole-build-config facts.
 *
 * Failure attribution (P2) is preserved entirely in-shell via markers:
 *  - resolve non-zero -> resolver/infra defect, NOT build_failed: write no marker, exit rc.
 *  - compile non-zero -> the SOLE build_failed signal: write the buildFailed plan.json + the precompile
 *                        marker, then exit rc. The separate generate step (depends_on allow_failure) then
 *                        uploads the analyze-only pipeline that records the single build_failed.
 *  - scan non-zero    -> resolver/infra defect, NOT build_failed: write no marker, exit rc.
 *  - happy path       -> exit 0 after scan; the generate step reads the plan and uploads batch + analyze.
 *
 * `$$rc` / `$$TASKS` defer past Buildkite's pipeline-upload interpolation pass.
 */
function orchestrationCommand(): string {
  return [
    // refs are produced by the bootstrap step on a different agent, so fetch them onto this one.
    `buildkite-agent artifact download "${FLAKINESS_REFS_ARTIFACT}" . || true`,
    "set +e",
    "",
    "# --- resolve ---",
    // Drop anything a reused workspace left behind, so the compile phase only ever concatenates this run's
    // answer (each project overwrites its own files, but a project removed from the build would not).
    `rm -rf ${FLAKINESS_TARGETS_DIR}`,
    // UNQUALIFIED task name: Gradle runs `flakinessResolveProject` in every project that registered it, and
    // each project self-selects on whether a ref lands in its own source sets (FlakinessProjectResolve).
    // The configuration cache is deliberately left ON - the per-project topology carries the model through
    // task inputs, which survive the configuration/execution boundary. See JAVA_RESOLVER_NOTES.md.
    `${innerGradleTimeout(RESOLVE_TIMEOUT_MINUTES)} .ci/scripts/run-gradle.sh -Pflakiness.resolve flakinessResolveProject`,
    "rc=$?",
    `if [ "$$rc" -ne 0 ]; then`,
    `  echo "flakiness resolve failed (rc=$$rc): resolver/infra defect, not a PR build failure."`,
    "  exit $$rc",
    "fi",
    "",
    // Pack the per-project answers into one artifact instead of ~450 individual uploads (debug detail only).
    `tar -czf ${FLAKINESS_TARGETS_ARCHIVE} -C ${FLAKINESS_TARGETS_DIR} . 2>/dev/null || true`,
    "",
    "# --- compile (plain invocation of the resolved task list; empty list = clean skip) ---",
    // The only glue the per-project topology needs: concatenate what the owning projects each wrote. No
    // Gradle task and no JSON parsing in shell - the per-project files are plain newline-terminated lists.
    `TASKS=$(cat ${FLAKINESS_TARGETS_DIR}/*.compile-tasks.txt 2>/dev/null | sort -u)`,
    // Persist the flattened list so a build_failed can be triaged without re-deriving it from the tarball.
    `printf '%s\\n' "$$TASKS" > ${FLAKINESS_COMPILE_TASKS_ARTIFACT}`,
    `if [ -n "$$TASKS" ]; then`,
    `  ${innerGradleTimeout(COMPILE_TIMEOUT_MINUTES)} .ci/scripts/run-gradle.sh $$TASKS`,
    "  rc=$?",
    `  if [ "$$rc" -ne 0 ]; then`,
    // compile is the ONLY build_failed signal. Leave the markers, then propagate the red exit. The separate
    // generate step (depends_on allow_failure) picks up the markers and records the single build_failed.
    `    printf '{"buildFailed":true,"reason":"precompile","entries":[]}' > ${FLAKINESS_PLAN_ARTIFACT}`,
    `    printf '{"outcome":"build_failed","reason":"precompile"}' > ${FLAKINESS_PRECOMPILE_ARTIFACT}`,
    "    exit $$rc",
    "  fi",
    "else",
    `  echo "No compile tasks listed; nothing to compile."`,
    "fi",
    "",
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
 * The generate shell: download the plan (and the precompile marker) the orchestration step produced, then
 * run the node generate entrypoint, which uploads the batch + analyze steps. Runs on the DEFAULT
 * node-capable agent (the gradle-tuned image lacks node). `|| true` on the downloads tolerates the case
 * where orchestration failed before writing them - generate then logs and exits 0 without uploading.
 */
function generateCommand(): string {
  return [
    `buildkite-agent artifact download "${FLAKINESS_PLAN_ARTIFACT}" . || true`,
    `buildkite-agent artifact download "${FLAKINESS_PRECOMPILE_ARTIFACT}" . || true`,
    GENERATE_ENTRYPOINT,
  ].join("\n");
}

/**
 * Pure: the orchestration sub-pipeline the bootstrap step uploads. TWO steps in the group:
 *  1. orchestration (`flakiness-orchestration:run`): resolve/compile/scan on ONE gradle agent.
 *  2. generate (`flakiness-orchestration:generate`): downloads the plan and uploads the batch + analyze
 *     steps, on the DEFAULT node-capable agent (no `agents:` pin - the gradle image lacks node).
 *
 * BOTH keyed under `flakiness-orchestration:` (NOT `flakiness-detection:`) so a red/failed orchestration run
 * is never fallback-recorded as a test batch by the external metric predicate
 * (`step_key.startsWith("flakiness-detection:") && step_key !== "flakiness-detection:analyze"`).
 */
export function toResolvePipeline(cfg: AgentConfig): Pipeline {
  const orchestration: PipelineStep = {
    label: "resolve · compile · scan",
    key: ORCHESTRATION_KEY,
    command: orchestrationCommand(),
    timeout_in_minutes: ORCHESTRATION_TIMEOUT_MINUTES,
    // gradle-tuned image. It does NOT run node (that is the separate generate step below).
    agents: { ...cfg.agents },
    // Everything a later, separate agent needs: the plan, the precompile marker (compile failure), plus the
    // intermediates for debugging. The generate step downloads the plan from here.
    artifact_paths: [
      FLAKINESS_TARGETS_ARCHIVE,
      FLAKINESS_COMPILE_TASKS_ARTIFACT,
      FLAKINESS_PLAN_ARTIFACT,
      FLAKINESS_PRECOMPILE_ARTIFACT,
    ],
    retry: NO_AUTO_RETRY,
  };
  const generate: PipelineStep = {
    label: "generate",
    key: GENERATE_KEY,
    command: generateCommand(),
    timeout_in_minutes: GENERATE_TIMEOUT_MINUTES,
    // No `agents:` pin: use the DEFAULT node-capable image (the gradle-tuned image lacks node, which was the
    // prior residual risk of running generate inline in the orchestration step).
    // allow_failure so a compile-failed (red) orchestration run still triggers generate, which then uploads
    // the analyze-only pipeline that records the single build_failed.
    depends_on: [{ step: ORCHESTRATION_KEY, allow_failure: true }],
    // The plan/precompile marker generate downloads and writes; skipped list generate may write - all
    // consumed by the later analyze step.
    artifact_paths: [FLAKINESS_SKIPPED_ARTIFACT, FLAKINESS_PRECOMPILE_ARTIFACT, FLAKINESS_PLAN_ARTIFACT],
    retry: NO_AUTO_RETRY,
  };
  return { steps: [{ group: cfg.groupName, steps: [orchestration, generate] }] };
}

/**
 * Impure: serialize and upload the [resolve, compile, scan, generate] sub-pipeline. Called by the
 * bootstrap entrypoints after they have gathered refs and written flakiness-refs.json.
 */
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
 * Pure: build the BK pipeline structure. Groups commands by step `key`; if a
 * key produced N > 1 batches, fans them out via BUILDKITE_PARALLEL_JOB env vars.
 */
export function toBuildkitePipeline(
  commands: RunnableCommand[],
  cfg: AgentConfig,
  // `hasNotApplicable`: emit the analyze step even with zero batch steps so BWC
  // `not_applicable` records still reach the outcomes artifact. The compile gate
  // is now a first-class orchestration step (see toResolvePipeline), so this
  // function no longer prepends one.
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
    const step: PipelineStep = {
      label: head.label,
      key,
      command: wrapNeverFail(head.command, key, cfg.timeoutInMinutes, { kind: head.kind, taskPaths: head.taskPaths }),
      timeout_in_minutes: cfg.timeoutInMinutes,
      agents: { ...cfg.agents },
      artifact_paths: [TEST_RESULTS_ARTIFACTS, FLAKINESS_STATUS_ARTIFACTS],
      retry: NO_AUTO_RETRY,
    };

    if (batches.length > 1) {
      const env: Record<string, string> = {};
      for (let i = 0; i < batches.length; i++) {
        env[`BATCH_COMMAND_${i}`] = wrapNeverFail(batches[i].command, key, cfg.timeoutInMinutes, {
          kind: batches[i].kind,
          taskPaths: batches[i].taskPaths,
        });
      }
      // Both `$$` escapes defer interpolation past Buildkite's pipeline-upload
      // pass: `$$BUILDKITE_PARALLEL_JOB` because the variable is set per-job at
      // run time (BK substitutes empty at upload time, breaking the indirect
      // lookup), and `$${!VARNAME}` because BK can't parse `!` as the start of
      // a variable identifier.
      step.command = 'VARNAME="BATCH_COMMAND_$${BUILDKITE_PARALLEL_JOB}"; eval "$${!VARNAME}"';
      step.parallelism = batches.length;
      step.env = env;
    }

    steps.push(step);
  }

  if (steps.length > 0 || opts.hasNotApplicable) {
    // allow_failure: true so the report still runs when a batch fails - it must
    // record the `build_failed`/`not_applicable` outcomes in those cases too.
    const deps = steps.map((s) => ({ step: s.key, allow_failure: true }));
    steps.push({
      label: "flakiness report",
      key: "flakiness-detection:analyze",
      // Download the per-job status files and the skipped-tests list, then run
      // the analyzer. The analyzer reads each status file and downloads that
      // job's JUnit XML per job (`--step <jobId>`) so it can attribute results
      // to a job before classifying, and folds the skipped list in as
      // `not_applicable`. It writes the structured per-job outcomes to
      // FLAKINESS_OUTCOMES_ARTIFACT, which `artifact_paths` below uploads for the
      // observability pipeline to read. `|| true` tolerates a build with no
      // status/skipped artifacts.
      command: wrapNeverFail(
        [
          `buildkite-agent artifact download "${FLAKINESS_STATUS_ARTIFACTS}" . || true`,
          `buildkite-agent artifact download "${FLAKINESS_SKIPPED_ARTIFACT}" . || true`,
          `buildkite-agent artifact download "${FLAKINESS_PRECOMPILE_ARTIFACT}" . || true`,
          "node .buildkite/scripts/flakiness-detection/entrypoints/analyze.ts",
        ].join("\n"),
        "flakiness-detection:analyze",
        10
      ),
      timeout_in_minutes: 10,
      // Intentionally no `agents:` — the analyze step is lightweight markdown
      // rendering and should not pin to the gradle-tuned `cfg.agents` image
      // (that image lacks npm). Letting BK pick the parent pipeline default
      // gives us an agent with the standard Node toolchain available.
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
