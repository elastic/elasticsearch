import { stringify } from "yaml";
import { execSync } from "child_process";
import { resolve, dirname } from "path";
import {
  AGENTS,
  BATCH_CAPS,
  KIND_LABELS,
  KIND_KEYS,
  TestKind,
  ClassifiedTest,
} from "./domain";
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

export function dedupeTests(tests: ClassifiedTest[]): ClassifiedTest[] {
  const seen = new Set<string>();
  const result: ClassifiedTest[] = [];
  for (const t of tests) {
    const identity = t.yamlTest ?? t.fqcn ?? t.suitePath ?? "";
    const key = `${t.gradleProject}|${t.kind}|${identity}`;
    if (seen.has(key)) continue;
    seen.add(key);
    result.push(t);
  }
  return result;
}

export function collapseYamlSuites(tests: ClassifiedTest[]): ClassifiedTest[] {
  const yamlSuitesByProject = new Map<string, ClassifiedTest[]>();
  const result: ClassifiedTest[] = [];

  for (const test of tests) {
    if (test.kind === "yamlRestTestSuite") {
      const key = test.gradleProject;
      if (!yamlSuitesByProject.has(key)) {
        yamlSuitesByProject.set(key, []);
      }
      yamlSuitesByProject.get(key)!.push(test);
    } else {
      result.push(test);
    }
  }

  for (const [, suites] of yamlSuitesByProject) {
    const byDir = new Map<string, ClassifiedTest[]>();
    for (const suite of suites) {
      const dir = dirname(suite.suitePath!);
      if (!byDir.has(dir)) {
        byDir.set(dir, []);
      }
      byDir.get(dir)!.push(suite);
    }

    for (const [dir, dirSuites] of byDir) {
      if (dirSuites.length > 1 && dir !== ".") {
        // Collapse multiple files in same directory to directory-level targeting
        result.push({
          gradleProject: dirSuites[0].gradleProject,
          kind: "yamlRestTestSuite",
          sourceSet: "yamlRestTest",
          suitePath: dir,
        });
      } else {
        // Single file in directory or root-level files: keep individual paths
        for (const suite of dirSuites) {
          result.push(suite);
        }
      }
    }
  }

  return result;
}

export function deduplicateYamlRunners(tests: ClassifiedTest[]): ClassifiedTest[] {
  const seen = new Set<string>();
  return tests.filter((test) => {
    if (test.kind === "yamlRestTestRunner") {
      const key = `${test.gradleProject}:yamlRestTestRunner`;
      if (seen.has(key)) return false;
      seen.add(key);
    }
    return true;
  });
}

// Gradle task-level options (`--tests`, `--rerun`, ...) bind to the most
// recently named task on the command line; they don't fan out to all listed
// test tasks. So per-task options must be emitted directly after each
// `:project:taskName` they apply to. See
// https://docs.gradle.org/current/userguide/command_line_interface.html#sec:task_options
function tasksWithFilters(
  batch: ClassifiedTest[],
  taskName: string,
  toFilter: (t: ClassifiedTest) => string,
  perTaskSuffix?: string
): string {
  const byTask = new Map<string, string[]>();
  for (const t of batch) {
    const task = `${t.gradleProject}:${taskName}`;
    const filters = byTask.get(task);
    if (filters) {
      filters.push(toFilter(t));
    } else {
      byTask.set(task, [toFilter(t)]);
    }
  }
  return [...byTask.entries()]
    .map(([task, filters]) => [task, ...filters, ...(perTaskSuffix ? [perTaskSuffix] : [])].join(" "))
    .join(" ");
}

export function generateBatchCommand(batch: ClassifiedTest[]): string {
  const kind = batch[0].kind;

  switch (kind) {
    case "test": {
      const tasks = tasksWithFilters(batch, "test", (t) => `--tests ${t.fqcn}`);
      return `.ci/scripts/run-gradle.sh -Dtests.iters=100 -Dtests.timeoutSuite=3600000! ${tasks}`;
    }
    case "internalClusterTest": {
      const tasks = tasksWithFilters(batch, "internalClusterTest", (t) => `--tests ${t.fqcn}`);
      return `.ci/scripts/run-gradle.sh -Dtests.iters=20 -Dtests.timeoutSuite=3600000! ${tasks}`;
    }
    case "javaRestTest": {
      const tasks = tasksWithFilters(batch, "javaRestTest", (t) => `--tests ${t.fqcn}`, "--rerun");
      return `.ci/scripts/repeat-rest-test.sh 10 .ci/scripts/run-gradle.sh ${tasks}`;
    }
    case "yamlRestTestRunner": {
      return `.ci/scripts/repeat-rest-test.sh 10 .ci/scripts/run-gradle.sh ${batch[0].gradleProject}:yamlRestTest --rerun`;
    }
    case "yamlRestTestSuite": {
      // `tests.rest.suite` is a JVM system property, not a Gradle task option,
      // so a single value would apply to every yamlRestTest task in the
      // invocation. ESClientYamlSuiteTestCase recognises a per-task scoped
      // variant `tests.rest.suite.<task path>` so each task can receive only
      // the suites that exist on its classpath.
      const byTask = new Map<string, string[]>();
      for (const t of batch) {
        const task = `${t.gradleProject}:yamlRestTest`;
        const paths = byTask.get(task);
        if (paths) {
          paths.push(t.suitePath!);
        } else {
          byTask.set(task, [t.suitePath!]);
        }
      }
      const tasks = [...byTask.keys()].map((task) => `${task} --rerun`).join(" ");
      const suiteProps = [...byTask.entries()]
        .map(([task, paths]) => `-Dtests.rest.suite.${task}=${paths.join(",")}`)
        .join(" ");
      return `.ci/scripts/repeat-rest-test.sh 10 .ci/scripts/run-gradle.sh ${tasks} ${suiteProps}`;
    }
    case "yamlRestTestCase": {
      // Each parameterized case is addressed by the full `<FQCN>.test {yaml=...}`
      // form, so multiple cases can be batched into one gradle invocation and
      // share agent and cluster setup.
      const tasks = tasksWithFilters(batch, "yamlRestTest", (t) => `--tests "${t.fqcn}.${t.yamlTest}"`, "--rerun");
      return `.ci/scripts/repeat-rest-test.sh 10 .ci/scripts/run-gradle.sh ${tasks}`;
    }
  }
}

export function generatePipeline(tests: ClassifiedTest[]): Pipeline {
  const KIND_ORDER: TestKind[] = [
    "test",
    "internalClusterTest",
    "javaRestTest",
    "yamlRestTestRunner",
    "yamlRestTestSuite",
    "yamlRestTestCase",
  ];

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
      batchCommands.push(generateBatchCommand(batch));
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
        group: "repeat-changed-tests",
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
          `buildkite-agent annotate "No test changes or unmutes detected" --style "info" --context "repeat-changed-tests"`,
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
          `buildkite-agent annotate "Warning: ${tests.length} test files to re-run (${changedTests.length} changed, ${unmuted.located.length} unmuted). This may take a while." --style "warning" --context "repeat-changed-tests"`,
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
