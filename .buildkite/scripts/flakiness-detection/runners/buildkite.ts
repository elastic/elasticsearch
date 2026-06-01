import { execSync } from "child_process";
import { resolve } from "path";
import { stringify } from "yaml";

import type { AgentConfig, RunnableCommand } from "../domain.ts";

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
  artifact_paths?: string;
}

// Minutes of headroom kept between the inner `timeout` (which we own) and the
// outer Buildkite `timeout_in_minutes` (which the agent enforces by SIGKILLing
// the whole step). The wrapper needs to win the race so it can annotate and
// exit 0; if the BK agent fires first the step ends up in state "timed_out".
const NEVER_FAIL_GRACE_MINUTES = 2;

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
function wrapNeverFail(command: string, contextKey: string, outerTimeoutMin: number): string {
  const innerTimeoutMin = Math.max(1, outerTimeoutMin - NEVER_FAIL_GRACE_MINUTES);
  return [
    "set +e",
    "WRAPPED_CMD_FILE=$(mktemp)",
    // Quoted heredoc avoids any shell-expansion of the inner command at
    // write time; variables in it are evaluated when bash runs the file.
    "cat > \"$$WRAPPED_CMD_FILE\" <<'__NEVER_FAIL_EOF__'",
    command,
    "__NEVER_FAIL_EOF__",
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
    "exit 0",
  ].join("\n");
}

// Each BK step runs on its own fresh agent — workspaces are not shared. To get
// the JUnit XML written by the batch steps to the analyze step's agent, the
// batch steps upload them as build artifacts and the analyze step downloads
// them. Both ends use the same path so the existing walker in
// `analyzer/analyze.ts` picks the files up at `*/build/test-results/...`.
const TEST_RESULTS_ARTIFACTS = "**/build/test-results/**/TEST-*.xml";

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
      command: wrapNeverFail(head.command, key, cfg.timeoutInMinutes),
      timeout_in_minutes: cfg.timeoutInMinutes,
      agents: { ...cfg.agents },
      artifact_paths: TEST_RESULTS_ARTIFACTS,
    };

    if (batches.length > 1) {
      const env: Record<string, string> = {};
      for (let i = 0; i < batches.length; i++) {
        env[`BATCH_COMMAND_${i}`] = wrapNeverFail(batches[i].command, key, cfg.timeoutInMinutes);
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
      // Download JUnit XML from every preceding batch step,
      // then run the analyzer. The download preserves the upload paths so
      // the analyzer finds files at the same `*/build/test-results/...`
      // locations a local run would see.
      command: wrapNeverFail(
        [
          `buildkite-agent artifact download "${TEST_RESULTS_ARTIFACTS}" .`,
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
      depends_on: deps,
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
