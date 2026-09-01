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
function wrapNeverFail(
  command: string,
  contextKey: string,
  outerTimeoutMin: number,
  emitOutcome?: { kind: TestKind }
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
          `printf '{"jobId":"%s","stepKey":"%s","kind":"%s","rc":%s,"durationSec":%s,"infraSubtype":"%s"}' "$$BUILDKITE_JOB_ID" "${contextKey}" "${emitOutcome.kind}" "$$rc" "$(( _fd_end - _fd_start ))" "$$_fd_oom" > "flakiness-status/status-$$BUILDKITE_JOB_ID.json" || true`,
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

// Written by the pre-flight compile step (below) only when compilation fails, so
// the analyze step can record a single `build_failed` outcome instead of the
// batches (which are skipped) producing none. Keep in sync with entrypoints/analyze.ts.
const FLAKINESS_PRECOMPILE_ARTIFACT = "flakiness-precompile.json";

// Pre-flight compile gate. A PR that does not compile otherwise runs every
// re-run batch to a doomed failure (up to 100x fan-out), wasting CI. This step
// compiles the affected source sets once; batch steps hard-depend on it
// (allow_failure: false) so Buildkite skips them when it fails, saving that
// compute. It is intentionally NOT never-fail wrapped: it must exit non-zero to
// make Buildkite skip the batches. That turns the step red, but only ever on a
// PR that genuinely does not compile - which is already red from its main build -
// so it never introduces a false failure on an otherwise-green PR.
//
// Note on the metric: skipping the batches saves CI, but does not by itself
// reduce the `infra_fail` count. A skipped job writes no status file, so the
// external pipeline still records each skipped batch (and this gate job) as
// `infra_fail` from job state. The analyze step folds the gate failure into a
// single `build_failed`; suppressing the skipped-job fallback is an
// external-pipeline concern (see README).
const PRECOMPILE_KEY = "flakiness-detection:precompile";
const PRECOMPILE_TIMEOUT_MINUTES = 30;

function precompileCommand(compileTasks: string[]): string {
  // Fire the inner compile timeout a grace period before the step timeout, so the
  // script can capture the exit code, write the marker, and exit before Buildkite
  // SIGKILLs the step. Reuses the same grace as the never-fail wrapper.
  const innerTimeoutMin = Math.max(1, PRECOMPILE_TIMEOUT_MINUTES - NEVER_FAIL_GRACE_MINUTES);
  return [
    "set +e",
    // Run under an inner `timeout` so a hang (e.g. a develocity build-scan upload
    // stall - the same failure mode the never-fail wrapper guards against) fails
    // with a captured non-zero rc rather than being SIGKILLed by Buildkite's outer
    // timeout. An outer SIGKILL would skip the marker write below and leave the
    // analyze step to render a misleading green summary. `--foreground` keeps the
    // gradle CLI in the parent process group so its scan plugin does not hang.
    `timeout --foreground --signal=TERM --kill-after=30s ${innerTimeoutMin}m .ci/scripts/run-gradle.sh ${compileTasks.join(" ")}`,
    "rc=$?",
    // On any non-zero exit - a compile error, or a timeout (rc 124/137) - leave a
    // marker the analyze step folds into the flakiness summary annotation as
    // `build_failed` (the analyze step owns the single developer-facing
    // annotation). `$$rc` defers past Buildkite's upload-time interpolation.
    `if [ "$$rc" -ne 0 ]; then`,
    `  printf '{"outcome":"build_failed","reason":"precompile"}' > "${FLAKINESS_PRECOMPILE_ARTIFACT}" || true`,
    "fi",
    // Propagate the real exit code so dependent batch steps are skipped on failure.
    "exit $$rc",
  ].join("\n");
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
  // `not_applicable` records still reach the outcomes artifact.
  // `compileTasks`: when non-empty, prepend a pre-flight compile gate the batch
  // steps hard-depend on.
  opts: { hasNotApplicable?: boolean; compileTasks?: string[] } = {}
): Pipeline {
  const byKey = new Map<string, RunnableCommand[]>();
  for (const c of commands) {
    const list = byKey.get(c.key);
    if (list) list.push(c);
    else byKey.set(c.key, [c]);
  }

  // Only gate on compile when there are batches to gate. All-BWC builds (no
  // batches) have nothing to compile.
  const gateOnCompile = (opts.compileTasks?.length ?? 0) > 0 && byKey.size > 0;

  const steps: PipelineStep[] = [];
  for (const [key, batches] of byKey) {
    const head = batches[0];
    const step: PipelineStep = {
      label: head.label,
      key,
      command: wrapNeverFail(head.command, key, cfg.timeoutInMinutes, { kind: head.kind }),
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

    // Hard dependency (allow_failure omitted = false) so a compile failure skips
    // the re-run batches instead of running them all doomed.
    if (gateOnCompile) {
      step.depends_on = [{ step: PRECOMPILE_KEY, allow_failure: false }];
    }
    steps.push(step);
  }

  // Prepend the compile gate ahead of the batch steps it guards.
  if (gateOnCompile) {
    steps.unshift({
      label: "precompile",
      key: PRECOMPILE_KEY,
      command: precompileCommand(opts.compileTasks!),
      timeout_in_minutes: PRECOMPILE_TIMEOUT_MINUTES,
      agents: { ...cfg.agents },
      artifact_paths: FLAKINESS_PRECOMPILE_ARTIFACT,
      retry: NO_AUTO_RETRY,
    });
  }

  if (steps.length > 0 || opts.hasNotApplicable) {
    // allow_failure: true so the report still runs when a batch fails, is
    // skipped (compile gate failed), or the gate itself fails - it must record
    // the `build_failed`/`not_applicable` outcomes in those cases.
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
  opts: { hasNotApplicable?: boolean; compileTasks?: string[]; cwd?: string } = {}
): void {
  const cwd = opts.cwd ?? PROJECT_ROOT;
  const yaml = stringify(
    toBuildkitePipeline(commands, cfg, { hasNotApplicable: opts.hasNotApplicable, compileTasks: opts.compileTasks })
  );
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
