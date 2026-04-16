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
  command: string;
  timeout_in_minutes: number;
  agents: typeof AGENTS;
}

interface Pipeline {
  steps: [{ group: string; steps: PipelineStep[] }];
}

export function toGradleProject(path: string): string {
  return ":" + path.replace(/\//g, ":");
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

export function generateBatchCommand(batch: ClassifiedTest[]): string {
  const kind = batch[0].kind;

  switch (kind) {
    case "test": {
      const projects = [...new Set(batch.map((t) => `${t.gradleProject}:test`))];
      const testFilters = batch.map((t) => `--tests ${t.fqcn}`).join(" ");
      return `.ci/scripts/run-gradle.sh -Dtests.iters=100 ${projects.join(" ")} ${testFilters}`;
    }
    case "internalClusterTest": {
      const projects = [...new Set(batch.map((t) => `${t.gradleProject}:internalClusterTest`))];
      const testFilters = batch.map((t) => `--tests ${t.fqcn}`).join(" ");
      return `.ci/scripts/run-gradle.sh -Dtests.iters=20 ${projects.join(" ")} ${testFilters}`;
    }
    case "javaRestTest": {
      const projects = [...new Set(batch.map((t) => `${t.gradleProject}:javaRestTest`))];
      const testFilters = batch.map((t) => `--tests ${t.fqcn}`).join(" ");
      return `.ci/scripts/repeat-rest-test.sh 10 .ci/scripts/run-gradle.sh ${projects.join(" ")} ${testFilters} --rerun`;
    }
    case "yamlRestTestRunner": {
      return `.ci/scripts/repeat-rest-test.sh 10 .ci/scripts/run-gradle.sh ${batch[0].gradleProject}:yamlRestTest --rerun`;
    }
    case "yamlRestTestSuite": {
      const projects = [...new Set(batch.map((t) => `${t.gradleProject}:yamlRestTest`))];
      const suitePaths = batch.map((t) => t.suitePath).join(",");
      return `.ci/scripts/repeat-rest-test.sh 10 .ci/scripts/run-gradle.sh ${projects.join(" ")} -Dtests.rest.suite=${suitePaths} --rerun`;
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

    for (let i = 0; i < kindTests.length; i += cap) {
      const batch = kindTests.slice(i, i + cap);
      const batchNum = Math.floor(i / cap) + 1;
      const label = totalBatches === 1 ? typeLabel : `${typeLabel} (${batchNum})`;
      allSteps.push({
        label,
        command: generateBatchCommand(batch),
        timeout_in_minutes: 60,
        agents: { ...AGENTS },
      });
    }
  }

  return {
    steps: [{ group: "repeat-new-tests", steps: allSteps }],
  };
}

function main() {
  console.log("Computing merge base...");
  const targetBranch = process.env.GITHUB_PR_TARGET_BRANCH;
  if (!targetBranch) {
    throw new Error("GITHUB_PR_TARGET_BRANCH environment variable is required");
  }
  const mergeBase = execSync(`git merge-base ${targetBranch} HEAD`, { cwd: PROJECT_ROOT }).toString().trim();
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
          `buildkite-agent annotate "No test changes detected" --style "info" --context "repeat-new-tests"`,
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
          `buildkite-agent annotate "Warning: ${tests.length} changed test files detected. This may take a while." --style "warning" --context "repeat-new-tests"`,
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
