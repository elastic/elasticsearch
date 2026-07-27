import { dirname } from "path";

import type { BatchingConfig, ClassifiedTest, RunnableCommand, TestKind } from "./domain.ts";

import { KIND_KEYS, KIND_LABELS, KIND_ORDER } from "./domain.ts";

// The REST-loop wrapper: repeats a Gradle invocation `restIters` times and
// preserves each iteration's JUnit XML.
const REPEAT_REST_TEST = ".buildkite/scripts/flakiness-detection/runners/repeat-rest-test.sh";

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
  perTaskSuffix?: string,
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

export function generateBatchCommand(batch: ClassifiedTest[], cfg: BatchingConfig): string {
  const kind = batch[0].kind;
  // .ci/scripts/run-gradle.sh is a BK-agent wrapper (Linux-only, expects $WORKSPACE
  // and $GRADLEW). For local target, invoke gradle directly via ./gradlew so the
  // commands work on a developer laptop.
  const gradle = cfg.target === "local" ? "./gradlew" : ".ci/scripts/run-gradle.sh";

  switch (kind) {
    case "test": {
      const tasks = tasksWithFilters(batch, "test", (t) => `--tests ${t.fqcn}`);
      return `${gradle} -Dtests.iters=${cfg.itersByKind.test} -Dtests.timeoutSuite=${cfg.suiteTimeoutMs}! ${tasks}`;
    }
    case "internalClusterTest": {
      const tasks = tasksWithFilters(batch, "internalClusterTest", (t) => `--tests ${t.fqcn}`);
      return `${gradle} -Dtests.iters=${cfg.itersByKind.internalClusterTest} -Dtests.timeoutSuite=${cfg.suiteTimeoutMs}! ${tasks}`;
    }
    case "javaRestTest": {
      const tasks = tasksWithFilters(batch, "javaRestTest", (t) => `--tests ${t.fqcn}`, "--rerun");
      return `${REPEAT_REST_TEST} ${cfg.restIters} ${gradle} ${tasks}`;
    }
    case "yamlRestTestRunner": {
      return `${REPEAT_REST_TEST} ${cfg.restIters} ${gradle} ${batch[0].gradleProject}:yamlRestTest --rerun`;
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
      return `${REPEAT_REST_TEST} ${cfg.restIters} ${gradle} ${tasks} ${suiteProps}`;
    }
    case "yamlRestTestCase": {
      // Each parameterized case is addressed by the full `<FQCN>.test {yaml=...}`
      // form, so multiple cases can be batched into one gradle invocation and
      // share agent and cluster setup.
      const tasks = tasksWithFilters(batch, "yamlRestTest", (t) => `--tests "${t.fqcn}.${t.yamlTest}"`, "--rerun");
      return `${REPEAT_REST_TEST} ${cfg.restIters} ${gradle} ${tasks}`;
    }
  }
}

export function buildCommands(tests: ClassifiedTest[], cfg: BatchingConfig): RunnableCommand[] {
  let staged = dedupeTests(tests);
  staged = collapseYamlSuites(staged);
  staged = deduplicateYamlRunners(staged);

  const byKind = new Map<TestKind, ClassifiedTest[]>();
  for (const t of staged) {
    const list = byKind.get(t.kind);
    if (list) list.push(t);
    else byKind.set(t.kind, [t]);
  }

  const out: RunnableCommand[] = [];
  for (const kind of KIND_ORDER) {
    const kindTests = byKind.get(kind);
    if (!kindTests) continue;

    const cap = cfg.capByKind[kind];
    for (let i = 0; i < kindTests.length; i += cap) {
      const batch = kindTests.slice(i, i + cap);
      out.push({
        kind,
        label: KIND_LABELS[kind],
        key: KIND_KEYS[kind],
        command: generateBatchCommand(batch, cfg),
      });
    }
  }
  return out;
}
