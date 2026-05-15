import { execSync } from "child_process";
import { resolve } from "path";
import { stringify } from "yaml";

import { AgentConfig, RunnableCommand } from "../domain";

const PROJECT_ROOT = resolve(`${import.meta.dir}/../../../..`);

interface PipelineStep {
  label: string;
  key: string;
  command: string;
  timeout_in_minutes: number;
  agents: AgentConfig["agents"];
  soft_fail: boolean;
  parallelism?: number;
  env?: Record<string, string>;
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
      command: head.command,
      timeout_in_minutes: cfg.timeoutInMinutes,
      agents: { ...cfg.agents },
      soft_fail: cfg.softFail,
    };

    if (batches.length > 1) {
      const env: Record<string, string> = {};
      for (let i = 0; i < batches.length; i++) {
        env[`BATCH_COMMAND_${i}`] = batches[i].command;
      }
      step.command = 'VARNAME="BATCH_COMMAND_${BUILDKITE_PARALLEL_JOB}"; eval "$${!VARNAME}"';
      step.parallelism = batches.length;
      step.env = env;
    }
    steps.push(step);
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
