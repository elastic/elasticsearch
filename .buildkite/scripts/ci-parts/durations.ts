// Turns duration rows exported from the CI stats cluster into a .ci/test-parts durations.json, the input of the
// CI part planner (`./gradlew generateCiPartAssignment` and friends). Everything here is pure, so it is tested
// without the cluster. The file holds only measured values: the planner derives split counts and shard estimates.

export const TEST_TASK_TYPES = new Set([
  "org.gradle.api.tasks.testing.Test",
  "org.elasticsearch.gradle.testclusters.StandaloneRestIntegTestTask",
  "org.elasticsearch.gradle.internal.test.RestIntegTestTask",
]);

/**
 * Non-test tasks that are not a project's own work: every part that runs one of the project's test tasks runs these
 * too (they are prerequisites of the tests), so charging them to the project's home part would count them twice.
 */
export const SHARED_PREREQUISITE_TYPES = new Set([
  "org.gradle.api.tasks.compile.JavaCompile",
  "org.gradle.api.tasks.compile.GroovyCompile",
  "org.gradle.api.tasks.bundling.Jar",
  "org.gradle.language.jvm.tasks.ProcessResources",
]);

/** A fixed cost needs at least this many executions with item durations, or the planner's default is used. */
export const MIN_OVERHEAD_SAMPLES = 3;

/** Same as CiHeavyTestThrottle#isHeavy: test tasks that start clusters (REST, cluster and spec tests). */
export const isClusterTest = (taskPath: string): boolean => {
  const name = taskPath.substring(taskPath.lastIndexOf(":") + 1);
  return (
    name.includes("RestTest") ||
    name.includes("RestCompatTest") ||
    name.includes("ClusterTest") ||
    name.startsWith("csvSpecTests") ||
    name === "integTest" ||
    name === "bcUpgradeTest"
  );
};

/**
 * Whether a task's fixed cost can be measured as its duration minus its items' durations: only in tasks that run their
 * items one after another in one test JVM, which cluster tests do. internalClusterTest and unit test tasks run items
 * in several JVMs at once, so the difference means nothing there and the planner's default applies.
 */
export const measuresFixedCost = (taskPath: string): boolean => isClusterTest(taskPath) && !taskPath.endsWith(":internalClusterTest");

export type SplitBy = "class" | "yamlFile";

export interface Split {
  by: SplitBy;
  count: number;
}

/** Which projects' non-test work a suite's parts run. */
export type ProjectWork = "all" | "withTests" | "none";

/** The parts of config.json the exporter needs. */
export interface PlannerConfig {
  shardTargetSeconds?: number;
  shardOverheadSeconds?: number;
  pinnedSplits?: Record<string, Split>;
}

/** Median executed duration (ms) of one Gradle task path in the suite's jobs. */
export interface TaskRow {
  path: string;
  type: string;
  p50Ms: number;
}

/**
 * One job's execution of a test task. A split task runs once per job that holds one of its shards, and a retried
 * job runs the same shards again, so a build can have several rows for one task.
 *
 * @property notExecuted how often the task came from the cache or was skipped in the job; smart retry skips what
 *                       passed in an earlier attempt, which a preempted attempt never reported
 * @property retry       whether the job is a retry: smart retry may have run only part of the task
 */
export interface ExecutionRow {
  path: string;
  build: string;
  jobName: string;
  jobId: string;
  executedMs: number | null;
  notExecuted: number;
  retry: boolean;
}

/** Median duration (ms) of one test class (suite) or one YAML test of a task. */
export interface ItemRow {
  task: string;
  name: string;
  p50Ms: number;
}

/** The summed duration (ms) of a task's items (classes, or YAML tests) in one job. */
export interface JobItemSum {
  task: string;
  jobId: string;
  ms: number;
}

export interface Items {
  by: SplitBy;
  seconds: Record<string, number>;
}

export interface Durations {
  source: string;
  tests: Record<string, number>;
  projects: Record<string, number>;
  overheads: Record<string, number>;
  items: Record<string, Items>;
}

export const median = (values: number[]): number => {
  if (values.length === 0) {
    throw new Error("median of no values");
  }
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 1 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
};

export const projectOf = (taskPath: string): string => {
  const i = taskPath.lastIndexOf(":");
  return i <= 0 ? ":" : taskPath.substring(0, i);
};

/** Nested classes run with their top-level class, which is what CiShards buckets by. */
export const topLevelClassName = (className: string): string => {
  const nested = className.indexOf("$");
  return nested >= 0 ? className.substring(0, nested) : className;
};

/**
 * The suite path ESClientYamlSuiteTestCase shards by (`<api>/<file name>`) of a YAML test named
 * `test {yaml=<api>/<file name>/<test name>}`, or null for other tests. Test names may contain '/', and docs
 * suites have nested api directories (`reference/mapping/types/nested/line_89`), so: REST API suites, whose file
 * names are numbered (`indices.get_mapping/40_aliases/...`), are the first two segments; anything else drops the
 * last segment.
 */
export const yamlSuitePath = (testName: string): string | null => {
  const m = /^test \{yaml=(.*)\}$/.exec(testName);
  if (!m) {
    return null;
  }
  const segments = m[1].split("/");
  if (segments.length < 3) {
    return null;
  }
  if (/^\d+_/.test(segments[1])) {
    return `${segments[0]}/${segments[1]}`;
  }
  return segments.slice(0, -1).join("/");
};

const sortedRecord = <T>(entries: Iterable<[string, T]>): Record<string, T> =>
  Object.fromEntries([...entries].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)));

/** Median duration (s) of each test task as one execution, before split tasks are corrected. */
export const testSeconds = (rows: TaskRow[]): Map<string, number> => {
  const tests = new Map<string, number>();
  for (const row of rows) {
    if (TEST_TASK_TYPES.has(row.type)) {
      tests.set(row.path, row.p50Ms / 1000);
    }
  }
  return tests;
};

/**
 * Summed median duration (s) of each project's own non-test work. `withTests` keeps only projects that have a test
 * task in the suite, for suites whose jobs also run other projects' tasks that the suite's parts do not own.
 */
export const projectSeconds = (rows: TaskRow[], mode: ProjectWork, tests: Map<string, number>): Map<string, number> => {
  const projects = new Map<string, number>();
  if (mode === "none") {
    return projects;
  }
  const withTests = new Set([...tests.keys()].map(projectOf));
  for (const row of rows) {
    if (TEST_TASK_TYPES.has(row.type) || SHARED_PREREQUISITE_TYPES.has(row.type)) {
      continue;
    }
    const project = projectOf(row.path);
    if (mode === "withTests" && !withTests.has(project)) {
      continue;
    }
    projects.set(project, (projects.get(project) ?? 0) + row.p50Ms / 1000);
  }
  return projects;
};

/**
 * Whether test rows are a YAML suite's tests. Mostly is enough: a YAML suite also reports a few tests that are not
 * YAML files (initialization errors, a suite-level entry), which carry no file to shard by.
 */
export const isYamlSuite = (rows: { name: string }[]): boolean =>
  rows.length > 0 && rows.filter((r) => yamlSuitePath(r.name) !== null).length >= 0.9 * rows.length;

/**
 * Which name each item row belongs to, by task: YAML suite paths for tasks whose rows are YAML tests, top-level
 * class names otherwise. Rows that do not fit the task's kind are dropped.
 */
export const itemsByTask = (rows: ItemRow[]): Map<string, Items> => {
  const byTask = new Map<string, ItemRow[]>();
  for (const row of rows) {
    byTask.set(row.task, [...(byTask.get(row.task) ?? []), row]);
  }
  const items = new Map<string, Items>();
  for (const [task, taskRows] of byTask) {
    const yaml = isYamlSuite(taskRows);
    const seconds = new Map<string, number>();
    for (const row of taskRows) {
      const name = yaml ? yamlSuitePath(row.name) : topLevelClassName(row.name);
      if (name !== null) {
        seconds.set(name, (seconds.get(name) ?? 0) + row.p50Ms / 1000);
      }
    }
    // a single item can't be split, so it tells the planner nothing
    if (seconds.size > 1) {
      items.set(task, {
        by: yaml ? "yamlFile" : "class",
        seconds: sortedRecord([...seconds].map(([name, s]): [string, number] => [name, Math.round(s * 10) / 10])),
      });
    }
  }
  return items;
};

/**
 * The fixed cost (s) of one execution of each task: the median, over jobs where both are known, of the task's
 * duration minus the summed duration of the items it ran. That is the cost every shard pays again (test JVM, cluster
 * start for YAML suites, suite setup outside the items). Only measured where it means that (measuresFixedCost), and
 * with at least MIN_OVERHEAD_SAMPLES executions; otherwise the planner's default applies.
 */
export const overheadSeconds = (executions: ExecutionRow[], itemSums: JobItemSum[]): Map<string, number> => {
  const sums = new Map(itemSums.map((s) => [JSON.stringify([s.task, s.jobId]), s.ms]));
  const samples = new Map<string, number[]>();
  for (const e of executions) {
    const itemMs = sums.get(JSON.stringify([e.path, e.jobId]));
    if (e.retry || !measuresFixedCost(e.path) || e.executedMs === null || itemMs === undefined || itemMs <= 0 || e.executedMs <= itemMs) {
      continue;
    }
    samples.set(e.path, [...(samples.get(e.path) ?? []), e.executedMs - itemMs]);
  }
  const overheads = new Map<string, number>();
  for (const [task, values] of samples) {
    if (values.length >= MIN_OVERHEAD_SAMPLES) {
      overheads.set(task, median(values) / 1000);
    }
  }
  return overheads;
};

/**
 * The duration (s) of each split task as one execution of the whole task: per build where every shard executed (none
 * came from the cache or was skipped, and no job was retried, since a retry may run only part of a shard), the summed
 * shard executions minus the extra fixed costs the split added, then the median over those builds.
 */
export const splitTaskSeconds = (
  executions: ExecutionRow[],
  splitTasks: Set<string>,
  overheadOf: (task: string) => number
): Map<string, number> => {
  const builds = new Map<string, { task: string; jobs: Map<string, number>; incomplete: boolean }>();
  for (const e of executions) {
    if (!splitTasks.has(e.path)) {
      continue;
    }
    const key = JSON.stringify([e.path, e.build]);
    const build = builds.get(key) ?? { task: e.path, jobs: new Map<string, number>(), incomplete: false };
    build.incomplete ||= e.retry || e.notExecuted > 0;
    if (e.executedMs !== null) {
      build.jobs.set(e.jobName, e.executedMs);
    }
    builds.set(key, build);
  }
  const perTask = new Map<string, number[]>();
  for (const build of builds.values()) {
    if (build.incomplete || build.jobs.size === 0) {
      continue;
    }
    const totalSeconds = [...build.jobs.values()].reduce((a, b) => a + b, 0) / 1000;
    const whole = totalSeconds - (build.jobs.size - 1) * overheadOf(build.task);
    perTask.set(build.task, [...(perTask.get(build.task) ?? []), Math.max(whole, totalSeconds / build.jobs.size)]);
  }
  return new Map([...perTask].map(([task, values]) => [task, median(values)]));
};

/** The test tasks long enough that the planner might split them: their item durations are worth exporting. */
export const splitCandidates = (tests: Map<string, number>, config: PlannerConfig, currentSplits: Iterable<string>): string[] => {
  const candidates = new Set<string>([...currentSplits, ...Object.keys(config.pinnedSplits ?? {})]);
  const target = config.shardTargetSeconds ?? 0;
  if (target > 0) {
    for (const [task, seconds] of tests) {
      if (seconds >= target / 2) {
        candidates.add(task);
      }
    }
  }
  return [...candidates].filter((t) => tests.has(t)).sort();
};

export interface DurationInputs {
  source: string;
  config: PlannerConfig;
  projectWork: ProjectWork;
  currentSplits: Set<string>;
  taskRows: TaskRow[];
  executions: ExecutionRow[];
  itemRows: ItemRow[];
  itemSums: JobItemSum[];
}

export const buildDurations = (inputs: DurationInputs): Durations => {
  const tests = testSeconds(inputs.taskRows);
  const overheads = overheadSeconds(inputs.executions, inputs.itemSums);
  const defaultOverhead = inputs.config.shardOverheadSeconds ?? 0;
  const overheadOf = (task: string): number => overheads.get(task) ?? defaultOverhead;
  for (const [task, seconds] of splitTaskSeconds(inputs.executions, inputs.currentSplits, overheadOf)) {
    if (tests.has(task)) {
      tests.set(task, seconds);
    }
  }
  const items = itemsByTask(inputs.itemRows.filter((r) => tests.has(r.task)));
  const round = (m: Map<string, number>): Record<string, number> => sortedRecord([...m].map(([k, v]): [string, number] => [k, Math.round(v)]));
  return {
    source: inputs.source,
    tests: round(tests),
    projects: round(projectSeconds(inputs.taskRows, inputs.projectWork, tests)),
    overheads: round(new Map([...overheads].filter(([task]) => items.has(task) || inputs.currentSplits.has(task)))),
    items: sortedRecord(items),
  };
};

/** Same layout as the checked-in files, so refreshes produce small diffs. */
export const formatDurations = (durations: Durations): string => JSON.stringify(durations, null, 1) + "\n";
