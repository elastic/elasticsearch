import { stringify } from "yaml";
import { execSync } from "child_process";
import { resolve } from "path";
import {
  AGENTS,
  BATCH_CAPS,
  DEFAULT_BATCHING_CONFIG,
  KIND_LABELS,
  KIND_KEYS,
  KIND_ORDER,
  TestKind,
  ClassifiedTest,
} from "./domain";
import {
  collapseYamlSuites,
  dedupeTests,
  deduplicateYamlRunners,
  generateBatchCommand,
} from "./commands";
import { classifyChangedFiles } from "./detectors/changed-files";
import { detectUnmutedTests } from "./detectors/unmutes";

export * from "./domain";
export { classifyChangedFiles } from "./detectors/changed-files";
export {
  parseMutedEntries,
  diffMutedEntries,
  locateUnmutedTest,
  findUnmutedTests,
  detectUnmutedTests,
  type UnmuteDetectionResult,
} from "./detectors/unmutes";
export {
  classifyExplicitList,
  type ExplicitListResult,
  type UnresolvedSpec,
} from "./detectors/explicit-list";
export {
  buildCommands,
  collapseYamlSuites,
  dedupeTests,
  deduplicateYamlRunners,
  generateBatchCommand,
} from "./commands";

const PROJECT_ROOT = resolve(`${import.meta.dir}/../..`);

interface PipelineStep {
  label: string;
  key: string;
  command: string;
  timeout_in_minutes: number;
  agents: typeof AGENTS;
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

type CommandRunner = (command: string, options: { cwd: string; stdio?: "inherit" | "ignore" }) => Buffer;

export function generatePipeline(tests: ClassifiedTest[]): Pipeline {
  const byKind = new Map<TestKind, ClassifiedTest[]>();
  for (const test of tests) {
    if (!byKind.has(test.kind)) {
      byKind.set(test.kind, []);
    }
    byKind.get(test.kind)!.push(test);
  }

  const allSteps: PipelineStep[] = [];

  for (const kind of KIND_ORDER) {
    const kindTests = byKind.get(kind);
    if (!kindTests) continue;

    const cap = BATCH_CAPS[kind];
    const totalBatches = Math.ceil(kindTests.length / cap);
    const typeLabel = KIND_LABELS[kind];

    const batchCommands: string[] = [];
    for (let i = 0; i < kindTests.length; i += cap) {
      const batch = kindTests.slice(i, i + cap);
      batchCommands.push(generateBatchCommand(batch, DEFAULT_BATCHING_CONFIG));
    }

    const step: PipelineStep = {
      label: typeLabel,
      key: KIND_KEYS[kind],
      command: batchCommands[0],
      timeout_in_minutes: 60,
      agents: { ...AGENTS },
      soft_fail: true,
    };

    if (totalBatches > 1) {
      const env: Record<string, string> = {};
      for (let i = 0; i < batchCommands.length; i++) {
        env[`BATCH_COMMAND_${i}`] = batchCommands[i];
      }
      step.command = 'VARNAME="BATCH_COMMAND_${BUILDKITE_PARALLEL_JOB}"; eval "$${!VARNAME}"';
      step.parallelism = totalBatches;
      step.env = env;
    }

    allSteps.push(step);
  }

  return {
    steps: [
      {
        group: "flakiness-detection",
        steps: allSteps,
      },
    ],
  };
}

export function resolveMergeBaseTarget(
  targetBranch: string,
  run: CommandRunner = (command, options) => execSync(command, options),
  projectRoot: string = PROJECT_ROOT
): string {
  try {
    run(`git rev-parse --verify ${targetBranch}^{commit}`, { cwd: projectRoot, stdio: "ignore" });
    return targetBranch;
  } catch {
  // Some target branches aren't present in the local checkout: ghstack synthetic
  // refs (gh/<user>/<n>/base) and serverless patch branches (patch/<name>). Fetch
  // the ref and use FETCH_HEAD so we don't depend on origin/<branch> naming.
    run(`git fetch --no-tags origin ${targetBranch}`, { cwd: projectRoot, stdio: "inherit" });
    return "FETCH_HEAD";
  }
}

function main() {
  console.log("Computing merge base...");
  const targetBranch = process.env.GITHUB_PR_TARGET_BRANCH;
  if (!targetBranch) {
    throw new Error("GITHUB_PR_TARGET_BRANCH environment variable is required");
  }
  const targetRef = resolveMergeBaseTarget(targetBranch);
  const mergeBase = execSync(`git merge-base ${targetRef} HEAD`, { cwd: PROJECT_ROOT }).toString().trim();
  console.log(`Merge base: ${mergeBase}`);

  console.log("Getting changed files...");
  const changedFilesOutput = execSync(`git diff --diff-filter=d --name-only ${mergeBase}`, { cwd: PROJECT_ROOT }).toString().trim();
  const changedFiles = changedFilesOutput
    .split("\n")
    .map((f) => f.trim())
    .filter((f) => f);
  console.log(`Found ${changedFiles.length} changed files`);

  const changedTests = classifyChangedFiles(changedFiles);
  console.log(`Found ${changedTests.length} changed test files`);

  console.log("Detecting unmuted tests...");
  const unmuted = detectUnmutedTests(mergeBase, PROJECT_ROOT);
  console.log(`Found ${unmuted.located.length} unmuted tests`);
  if (unmuted.unlocated.length > 0) {
    console.log(
      `Skipping ${unmuted.unlocated.length} unmuted tests whose class files no longer exist:`
    );
    for (const e of unmuted.unlocated) {
      console.log(`  - ${e.className}${e.method !== undefined ? "." + e.method : ""}`);
    }
  }

  let tests = dedupeTests([...changedTests, ...unmuted.located]);
  console.log(`Total tests to run: ${tests.length} (${changedTests.length} changed, ${unmuted.located.length} unmuted)`);

  if (tests.length === 0) {
    console.log("No test changes or unmutes detected");
    if (process.env.CI) {
      try {
        execSync(
          `buildkite-agent annotate "No test changes or unmutes detected" --style "info" --context "flakiness-detection"`,
          { cwd: PROJECT_ROOT, stdio: "inherit" }
        );
      } catch {
        // Ignore annotation failures
      }
    }
    process.exit(0);
  }

  if (tests.length > 30) {
    console.log(`Warning: ${tests.length} test files to re-run`);
    if (process.env.CI) {
      try {
        execSync(
          `buildkite-agent annotate "Warning: ${tests.length} test files to re-run (${changedTests.length} changed, ${unmuted.located.length} unmuted). This may take a while." --style "warning" --context "flakiness-detection"`,
          { cwd: PROJECT_ROOT, stdio: "inherit" }
        );
      } catch {
        // Ignore annotation failures
      }
    }
  }

  tests = collapseYamlSuites(tests);
  tests = deduplicateYamlRunners(tests);

  const pipeline = generatePipeline(tests);
  const yaml = stringify(pipeline);

  console.log("--- Generated pipeline");
  console.log(yaml);

  if (process.env.CI) {
    console.log("Uploading pipeline...");
    execSync(`buildkite-agent pipeline upload`, {
      input: yaml,
      stdio: ["pipe", "inherit", "inherit"],
      cwd: PROJECT_ROOT,
    });
  }
}

if (import.meta.main) {
  main();
}
