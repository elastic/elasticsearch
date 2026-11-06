import { describe, expect, test } from "vitest";

import {
  buildDurations,
  formatDurations,
  isClusterTest,
  isYamlSuite,
  itemsByTask,
  measuresFixedCost,
  MIN_OVERHEAD_SAMPLES,
  overheadSeconds,
  projectSeconds,
  splitCandidates,
  splitTaskSeconds,
  testSeconds,
  topLevelClassName,
  yamlSuitePath,
  type ExecutionRow,
  type TaskRow,
} from "./durations.ts";

const TEST = "org.gradle.api.tasks.testing.Test";
const REST = "org.elasticsearch.gradle.testclusters.StandaloneRestIntegTestTask";
const COMPILE = "org.gradle.api.tasks.compile.JavaCompile";
const OTHER = "de.thetaphi.forbiddenapis.gradle.CheckForbiddenApis";

const execution = (path: string, build: string, jobName: string, executedMs: number | null, notExecuted = 0, retry = false): ExecutionRow => ({
  path,
  build,
  jobName,
  jobId: `${build}/${jobName}${retry ? "/retry" : ""}`,
  executedMs,
  notExecuted,
  retry,
});

describe("names", () => {
  test("YAML suite paths are what ESClientYamlSuiteTestCase shards by", () => {
    expect(yamlSuitePath("test {yaml=indices.get_mapping/40_aliases/Getting mapping for aliases}")).toBe("indices.get_mapping/40_aliases");
    expect(yamlSuitePath("test {yaml=search/10_source/a/b with slashes}")).toBe("search/10_source");
    expect(yamlSuitePath("test {yaml=reference/mapping/types/nested/line_89}")).toBe("reference/mapping/types/nested");
    expect(yamlSuitePath("testSomething")).toBeNull();
    expect(yamlSuitePath("test {yaml=too/short}")).toBeNull();
  });

  test("nested classes count with their top-level class", () => {
    expect(topLevelClassName("org.example.FooIT$Inner")).toBe("org.example.FooIT");
    expect(topLevelClassName("org.example.FooIT")).toBe("org.example.FooIT");
  });
});

describe("tests and projects", () => {
  const rows: TaskRow[] = [
    { path: ":a:test", type: TEST, p50Ms: 60_000 },
    { path: ":a:javaRestTest", type: REST, p50Ms: 120_000 },
    { path: ":a:compileJava", type: COMPILE, p50Ms: 30_000 },
    { path: ":a:forbiddenApisMain", type: OTHER, p50Ms: 10_000 },
    { path: ":b:forbiddenApisMain", type: OTHER, p50Ms: 5_000 },
  ];

  test("test tasks in seconds", () => {
    expect(testSeconds(rows)).toEqual(new Map([[":a:test", 60], [":a:javaRestTest", 120]]));
  });

  test("project work leaves out the prerequisites every part running the tests pays anyway", () => {
    const tests = testSeconds(rows);
    expect(projectSeconds(rows, "all", tests)).toEqual(new Map([[":a", 10], [":b", 5]]));
    expect(projectSeconds(rows, "withTests", tests)).toEqual(new Map([[":a", 10]]));
    expect(projectSeconds(rows, "none", tests)).toEqual(new Map());
  });
});

describe("items", () => {
  test("classes, with nested classes added to their top-level class", () => {
    const items = itemsByTask([
      { task: ":a:test", name: "org.example.FooTests", p50Ms: 1_000 },
      { task: ":a:test", name: "org.example.FooTests$Nested", p50Ms: 500 },
      { task: ":a:test", name: "org.example.BarTests", p50Ms: 2_000 },
    ]);
    expect(items.get(":a:test")).toEqual({ by: "class", seconds: { "org.example.BarTests": 2, "org.example.FooTests": 1.5 } });
  });

  test("YAML tests, summed per file", () => {
    const items = itemsByTask([
      { task: ":x:yamlRestTest", name: "test {yaml=search/10_basic/one}", p50Ms: 1_000 },
      { task: ":x:yamlRestTest", name: "test {yaml=search/10_basic/two}", p50Ms: 2_000 },
      { task: ":x:yamlRestTest", name: "test {yaml=bulk/20_list/three}", p50Ms: 4_000 },
    ]);
    expect(items.get(":x:yamlRestTest")).toEqual({ by: "yamlFile", seconds: { "bulk/20_list": 4, "search/10_basic": 3 } });
  });

  test("a YAML suite's few non-YAML tests are dropped, not a reason to give up on the suite", () => {
    const rows = [
      ...Array.from({ length: 20 }, (_, i) => ({ task: ":x:yamlRestTest", name: `test {yaml=api${i}/10_basic/t}`, p50Ms: 1_000 })),
      { task: ":x:yamlRestTest", name: "initializationError", p50Ms: 5 },
    ];
    expect(isYamlSuite(rows)).toBe(true);
    const items = itemsByTask(rows).get(":x:yamlRestTest");
    expect(items?.by).toBe("yamlFile");
    expect(Object.keys(items?.seconds ?? {})).toHaveLength(20);
    expect(isYamlSuite([{ name: "testFoo" }, { name: "test {yaml=a/10_b/c}" }])).toBe(false);
  });

  test("a single item tells the planner nothing", () => {
    expect(itemsByTask([{ task: ":a:test", name: "org.example.OnlyTests", p50Ms: 1_000 }]).has(":a:test")).toBe(false);
  });
});

describe("fixed cost per execution", () => {
  test("median of the task's duration minus the items it ran, per job", () => {
    const executions = [execution(":a:javaRestTest", "b1", "part-1", 100_000), execution(":a:javaRestTest", "b2", "part-1", 110_000), execution(":a:javaRestTest", "b3", "part-1", 90_000)];
    const sums = [
      { task: ":a:javaRestTest", jobId: "b1/part-1", ms: 70_000 },
      { task: ":a:javaRestTest", jobId: "b2/part-1", ms: 70_000 },
      { task: ":a:javaRestTest", jobId: "b3/part-1", ms: 70_000 },
    ];
    expect(overheadSeconds(executions, sums)).toEqual(new Map([[":a:javaRestTest", 30]]));
  });

  test("only measured for cluster tests that run their items in one test JVM", () => {
    expect(measuresFixedCost(":a:javaRestTest")).toBe(true);
    expect(measuresFixedCost(":a:yamlRestTest")).toBe(true);
    expect(measuresFixedCost(":x:qa:single-node:csvSpecTests")).toBe(true);
    expect(measuresFixedCost(":a:internalClusterTest")).toBe(false);
    expect(measuresFixedCost(":a:test")).toBe(false);
    expect(isClusterTest(":a:internalClusterTest")).toBe(true);
    const unit = [execution(":a:test", "b1", "part-1", 100_000), execution(":a:test", "b2", "part-1", 100_000), execution(":a:test", "b3", "part-1", 100_000)];
    expect(overheadSeconds(unit, unit.map((e) => ({ task: e.path, jobId: e.jobId, ms: 50_000 }))).has(":a:test")).toBe(false);
  });

  test("items that overlap, retried jobs and too few samples give no measurement", () => {
    const executions = [1, 2, 3].map((b) => execution(":a:javaRestTest", `b${b}`, "part-1", 100_000));
    expect(overheadSeconds(executions, executions.map((e) => ({ task: e.path, jobId: e.jobId, ms: 300_000 }))).has(":a:javaRestTest")).toBe(false);
    const few = executions.slice(0, MIN_OVERHEAD_SAMPLES - 1);
    expect(overheadSeconds(few, few.map((e) => ({ task: e.path, jobId: e.jobId, ms: 50_000 }))).has(":a:javaRestTest")).toBe(false);
    const retried = [...few, execution(":a:javaRestTest", "b3", "part-1", 100_000, 0, true)];
    expect(overheadSeconds(retried, retried.map((e) => ({ task: e.path, jobId: e.jobId, ms: 50_000 }))).has(":a:javaRestTest")).toBe(false);
  });
});

describe("split tasks", () => {
  test("one execution of the whole task: the shards of a build, minus the fixed costs the split added", () => {
    const executions = [
      // build 1: two shards, 400 + 300 s, each with a 60 s fixed cost: the whole task would take 640 s
      execution(":x:test", "b1", "part-1", 400_000),
      execution(":x:test", "b1", "part-2", 300_000),
      // build 2: 410 + 310 s, so 660 s as one execution
      execution(":x:test", "b2", "part-1", 410_000),
      execution(":x:test", "b2", "part-2", 310_000),
      // build 3: one shard came from the cache (or was skipped by smart retry), so the build says nothing about the whole task
      execution(":x:test", "b3", "part-1", 405_000),
      execution(":x:test", "b3", "part-2", null, 1),
      // build 4: part-2 was retried, and a retry may have run only part of its shard
      execution(":x:test", "b4", "part-1", 405_000),
      execution(":x:test", "b4", "part-2", 10_000, 0, true),
    ];
    const seconds = splitTaskSeconds(executions, new Set([":x:test"]), () => 60);
    expect(seconds).toEqual(new Map([[":x:test", 650]]));
  });

  test("candidates are tasks at least half the shard target, current and pinned splits", () => {
    const tests = new Map([[":long:test", 400], [":short:test", 100], [":split:test", 50], [":pinned:test", 10]]);
    const config = { shardTargetSeconds: 720, pinnedSplits: { ":pinned:test": { by: "class" as const, count: 2 } } };
    expect(splitCandidates(tests, config, [":split:test", ":gone:test"])).toEqual([":long:test", ":pinned:test", ":split:test"]);
    expect(splitCandidates(tests, {}, [])).toEqual([]);
  });
});

describe("durations.json", () => {
  test("measured values only, rounded, in a stable order", () => {
    const durations = buildDurations({
      source: "test",
      config: { shardTargetSeconds: 720, shardOverheadSeconds: 90 },
      projectWork: "all",
      currentSplits: new Set([":x:javaRestTest"]),
      taskRows: [
        { path: ":x:javaRestTest", type: REST, p50Ms: 300_000 },
        { path: ":y:test", type: TEST, p50Ms: 10_400 },
        { path: ":y:forbiddenApisMain", type: OTHER, p50Ms: 2_600 },
      ],
      executions: [
        execution(":x:javaRestTest", "b1", "part-1", 400_000),
        execution(":x:javaRestTest", "b1", "part-2", 300_000),
        execution(":x:javaRestTest", "b2", "part-1", 400_000),
        execution(":x:javaRestTest", "b2", "part-2", 300_000),
      ],
      itemRows: [
        { task: ":x:javaRestTest", name: "org.example.AIT", p50Ms: 340_000 },
        { task: ":x:javaRestTest", name: "org.example.BIT", p50Ms: 240_000 },
      ],
      itemSums: [
        { task: ":x:javaRestTest", jobId: "b1/part-1", ms: 340_000 },
        { task: ":x:javaRestTest", jobId: "b1/part-2", ms: 240_000 },
        { task: ":x:javaRestTest", jobId: "b2/part-1", ms: 340_000 },
        { task: ":x:javaRestTest", jobId: "b2/part-2", ms: 240_000 },
      ],
    });
    expect(durations).toEqual({
      source: "test",
      // 700 s over two shards, each paying the measured 60 s fixed cost once: 640 s as one execution
      tests: { ":x:javaRestTest": 640, ":y:test": 10 },
      projects: { ":y": 3 },
      overheads: { ":x:javaRestTest": 60 },
      items: { ":x:javaRestTest": { by: "class", seconds: { "org.example.AIT": 340, "org.example.BIT": 240 } } },
    });
    expect(formatDurations(durations)).toMatch(/^\{\n "source": "test",\n "tests": \{\n  ":x:javaRestTest": 640,/);
  });
});
