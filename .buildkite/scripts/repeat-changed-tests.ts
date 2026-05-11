import { stringify } from "yaml";
import { execSync } from "child_process";
import { resolve, dirname } from "path";

const PROJECT_ROOT = resolve(`${import.meta.dir}/../..`);

const AGENTS = {
  provider: "gcp",
  image: "family/elasticsearch-ubuntu-2404",
  machineType: "n4-custom-32-98304",
  diskType: "hyperdisk-balanced",
  buildDirectory: "/dev/shm/bk",
};

const SOURCE_SET_PATTERNS = [
  { regex: /^(.+)\/src\/test\/java\/(.+Tests)\.java$/, sourceSet: "test", kind: "test" as const },
  {
    regex: /^(.+)\/src\/internalClusterTest\/java\/(.+IT)\.java$/,
    sourceSet: "internalClusterTest",
    kind: "internalClusterTest" as const,
  },
  { regex: /^(.+)\/src\/javaRestTest\/java\/(.+IT)\.java$/, sourceSet: "javaRestTest", kind: "javaRestTest" as const },
  {
    regex: /^(.+)\/src\/yamlRestTest\/resources\/rest-api-spec\/test\/(.+)\.yml$/,
    sourceSet: "yamlRestTest",
    kind: "yamlRestTestSuite" as const,
  },
  {
    regex: /^(.+)\/src\/yamlRestTest\/java\/(.+IT)\.java$/,
    sourceSet: "yamlRestTest",
    kind: "yamlRestTestRunner" as const,
  },
];

type TestKind = (typeof SOURCE_SET_PATTERNS)[number]["kind"];

export const BATCH_CAPS: Record<TestKind, number> = {
  test: 360,
  internalClusterTest: 36,
  javaRestTest: 4,
  yamlRestTestSuite: 4,
  yamlRestTestRunner: 1,
};

export interface ClassifiedTest {
  gradleProject: string;
  kind: TestKind;
  sourceSet: string;
  fqcn?: string;
  suitePath?: string;
}

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

export function toGradleProject(path: string): string {
  const segments = path.split("/");
  // Mirror the rename in settings.gradle: direct children of :test:external-modules
  // have their project name prefixed with "test-".
  if (segments[0] === "test" && segments[1] === "external-modules" && segments.length >= 3) {
    segments[2] = `test-${segments[2]}`;
  }
  return ":" + segments.join(":");
}

export function toFqcn(javaPath: string): string {
  return javaPath.replace(/\//g, ".");
}

export function classifyChangedFiles(files: string[]): ClassifiedTest[] {
  const tests: ClassifiedTest[] = [];

  for (const file of files) {
    for (const pattern of SOURCE_SET_PATTERNS) {
      const match = file.match(pattern.regex);
      if (match) {
        const test: ClassifiedTest = {
          gradleProject: toGradleProject(match[1]),
          kind: pattern.kind,
          sourceSet: pattern.sourceSet,
        };

        if (pattern.kind === "yamlRestTestSuite") {
          test.suitePath = match[2];
        } else if (pattern.kind !== "yamlRestTestRunner") {
          test.fqcn = toFqcn(match[2]);
        }

        tests.push(test);
        break;
      }
    }
  }

  return tests;
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

const KIND_LABELS: Record<TestKind, string> = {
  test: "unit tests",
  internalClusterTest: "integ tests",
  javaRestTest: "java rest tests",
  yamlRestTestRunner: "yaml rest test runner",
  yamlRestTestSuite: "yaml rest tests",
};

const KIND_KEYS: Record<TestKind, string> = {
  test: "repeat-changed-tests:unit",
  internalClusterTest: "repeat-changed-tests:integ",
  javaRestTest: "repeat-changed-tests:java-rest",
  yamlRestTestRunner: "repeat-changed-tests:yaml-runner",
  yamlRestTestSuite: "repeat-changed-tests:yaml-suite",
};

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
  }
}

export function generatePipeline(tests: ClassifiedTest[]): Pipeline {
  const KIND_ORDER: TestKind[] = ["test", "internalClusterTest", "javaRestTest", "yamlRestTestRunner", "yamlRestTestSuite"];

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

  let tests = classifyChangedFiles(changedFiles);
  console.log(`Found ${tests.length} changed test files`);

  if (tests.length === 0) {
    console.log("No test changes detected");
    if (process.env.CI) {
      try {
        execSync(
          `buildkite-agent annotate "No test changes detected" --style "info" --context "repeat-changed-tests"`,
          { cwd: PROJECT_ROOT, stdio: "inherit" }
        );
      } catch {
        // Ignore annotation failures
      }
    }
    process.exit(0);
  }

  if (tests.length > 30) {
    console.log(`Warning: ${tests.length} changed test files detected`);
    if (process.env.CI) {
      try {
        execSync(
          `buildkite-agent annotate "Warning: ${tests.length} changed test files detected. This may take a while." --style "warning" --context "repeat-changed-tests"`,
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
