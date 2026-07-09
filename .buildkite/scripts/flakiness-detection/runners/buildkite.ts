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
    // `stepKey`/`kind` are build-time constants; `rc`/duration are runtime, so
    // they are `$$`-escaped to defer past Buildkite's pipeline-upload pass.
    ...(emitOutcome
      ? [
          "_fd_end=$(date +%s)",
          "mkdir -p flakiness-status",
          `printf '{"jobId":"%s","stepKey":"%s","kind":"%s","rc":%s,"durationSec":%s}' "$$BUILDKITE_JOB_ID" "${contextKey}" "${emitOutcome.kind}" "$$rc" "$(( _fd_end - _fd_start ))" > "flakiness-status/status-$$BUILDKITE_JOB_ID.json" || true`,
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
  cfg: AgentConfig
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
    steps.push(step);
  }

  if (steps.length > 0) {
    const deps = steps.map((s) => ({ step: s.key, allow_failure: true }));
    steps.push({
      label: "flakiness report",
      key: "flakiness-detection:analyze",
      // Download the per-job status files, then run the analyzer. The analyzer
      // reads each status file and downloads that job's JUnit XML per job
      // (`--step <jobId>`) so it can attribute results to a job before
      // classifying. It writes the structured per-job outcomes to
      // FLAKINESS_OUTCOMES_ARTIFACT, which `artifact_paths` below uploads for the
      // observability pipeline to read. `|| true` tolerates a build with no
      // status artifacts.
      command: wrapNeverFail(
        [
          `buildkite-agent artifact download "${FLAKINESS_STATUS_ARTIFACTS}" . || true`,
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
  cwd: string = PROJECT_ROOT
): void {
  const yaml = stringify(toBuildkitePipeline(commands, cfg));
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
