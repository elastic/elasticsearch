import { describe, expect, test } from "vitest";
import {
  dedupeTests,
  collapseYamlSuites,
  deduplicateYamlRunners,
  generateBatchCommand,
  buildCommands,
} from "./commands.ts";
import type { ClassifiedTest } from "./domain.ts";
import { DEFAULT_BATCHING_CONFIG } from "./domain.ts";

describe("collapseYamlSuites", () => {
  test("collapses multiple YAML files in same directory", () => {
    const tests: ClassifiedTest[] = [
      {
        gradleProject: ":x-pack:plugin:ml",
        kind: "yamlRestTestSuite",
        sourceSet: "yamlRestTest",
        suitePath: "ml/anomaly_detectors_get",
      },
      {
        gradleProject: ":x-pack:plugin:ml",
        kind: "yamlRestTestSuite",
        sourceSet: "yamlRestTest",
        suitePath: "ml/anomaly_detectors_put",
      },
    ];

    const result = collapseYamlSuites(tests);
    expect(result).toHaveLength(1);
    expect(result[0].suitePath).toBe("ml");
  });

  test("keeps single YAML file in directory uncollapsed", () => {
    const tests: ClassifiedTest[] = [
      {
        gradleProject: ":x-pack:plugin:ml",
        kind: "yamlRestTestSuite",
        sourceSet: "yamlRestTest",
        suitePath: "ml/anomaly_detectors_get",
      },
    ];

    const result = collapseYamlSuites(tests);
    expect(result).toHaveLength(1);
    expect(result[0].suitePath).toBe("ml/anomaly_detectors_get");
  });

  test("collapses per directory independently", () => {
    const tests: ClassifiedTest[] = [
      {
        gradleProject: ":x-pack:plugin:ml",
        kind: "yamlRestTestSuite",
        sourceSet: "yamlRestTest",
        suitePath: "ml/anomaly_detectors_get",
      },
      {
        gradleProject: ":x-pack:plugin:ml",
        kind: "yamlRestTestSuite",
        sourceSet: "yamlRestTest",
        suitePath: "ml/anomaly_detectors_put",
      },
      {
        gradleProject: ":x-pack:plugin:ml",
        kind: "yamlRestTestSuite",
        sourceSet: "yamlRestTest",
        suitePath: "search/search_basic",
      },
    ];

    const result = collapseYamlSuites(tests);
    expect(result).toHaveLength(2);

    const suitePaths = result.map((t) => t.suitePath).sort();
    expect(suitePaths).toEqual(["ml", "search/search_basic"]);
  });

  test("does not collapse across different projects", () => {
    const tests: ClassifiedTest[] = [
      {
        gradleProject: ":x-pack:plugin:ml",
        kind: "yamlRestTestSuite",
        sourceSet: "yamlRestTest",
        suitePath: "ml/test1",
      },
      {
        gradleProject: ":x-pack:plugin:security",
        kind: "yamlRestTestSuite",
        sourceSet: "yamlRestTest",
        suitePath: "ml/test2",
      },
    ];

    const result = collapseYamlSuites(tests);
    expect(result).toHaveLength(2);
  });

  test("preserves non-YAML-suite tests", () => {
    const tests: ClassifiedTest[] = [
      { gradleProject: ":server", kind: "test", sourceSet: "test", fqcn: "org.elasticsearch.SomeTests" },
      {
        gradleProject: ":x-pack:plugin:ml",
        kind: "yamlRestTestSuite",
        sourceSet: "yamlRestTest",
        suitePath: "ml/test1",
      },
    ];

    const result = collapseYamlSuites(tests);
    expect(result).toHaveLength(2);
    expect(result[0].kind).toBe("test");
    expect(result[1].kind).toBe("yamlRestTestSuite");
  });
});

describe("deduplicateYamlRunners", () => {
  test("deduplicates yaml runners for same project", () => {
    const tests: ClassifiedTest[] = [
      { gradleProject: ":x-pack:plugin:ml", kind: "yamlRestTestRunner", sourceSet: "yamlRestTest" },
      { gradleProject: ":x-pack:plugin:ml", kind: "yamlRestTestRunner", sourceSet: "yamlRestTest" },
    ];

    const result = deduplicateYamlRunners(tests);
    expect(result).toHaveLength(1);
  });

  test("keeps yaml runners for different projects", () => {
    const tests: ClassifiedTest[] = [
      { gradleProject: ":x-pack:plugin:ml", kind: "yamlRestTestRunner", sourceSet: "yamlRestTest" },
      { gradleProject: ":x-pack:plugin:security", kind: "yamlRestTestRunner", sourceSet: "yamlRestTest" },
    ];

    const result = deduplicateYamlRunners(tests);
    expect(result).toHaveLength(2);
  });

  test("does not affect other test kinds", () => {
    const tests: ClassifiedTest[] = [
      { gradleProject: ":server", kind: "test", sourceSet: "test", fqcn: "org.elasticsearch.SomeTests" },
      { gradleProject: ":server", kind: "test", sourceSet: "test", fqcn: "org.elasticsearch.OtherTests" },
    ];

    const result = deduplicateYamlRunners(tests);
    expect(result).toHaveLength(2);
  });
});

describe("generateBatchCommand", () => {
  test("unit test", () => {
    const batch: ClassifiedTest[] = [
      { gradleProject: ":server", kind: "test", sourceSet: "test", fqcn: "org.elasticsearch.index.IndexTests" },
    ];
    expect(generateBatchCommand(batch, DEFAULT_BATCHING_CONFIG)).toBe(
      ".ci/scripts/run-gradle.sh -Dtests.iters=100 -Dtests.timeoutSuite=3600000! :server:test --tests org.elasticsearch.index.IndexTests",
    );
  });

  test("unit tests across projects", () => {
    const batch: ClassifiedTest[] = [
      { gradleProject: ":server", kind: "test", sourceSet: "test", fqcn: "org.elasticsearch.index.FooTests" },
      { gradleProject: ":libs:core", kind: "test", sourceSet: "test", fqcn: "org.elasticsearch.core.BarTests" },
    ];
    expect(generateBatchCommand(batch, DEFAULT_BATCHING_CONFIG)).toBe(
      ".ci/scripts/run-gradle.sh -Dtests.iters=100 -Dtests.timeoutSuite=3600000! :server:test --tests org.elasticsearch.index.FooTests :libs:core:test --tests org.elasticsearch.core.BarTests",
    );
  });

  test("deduplicates projects", () => {
    const batch: ClassifiedTest[] = [
      { gradleProject: ":server", kind: "test", sourceSet: "test", fqcn: "org.elasticsearch.FooTests" },
      { gradleProject: ":server", kind: "test", sourceSet: "test", fqcn: "org.elasticsearch.BarTests" },
    ];
    expect(generateBatchCommand(batch, DEFAULT_BATCHING_CONFIG)).toBe(
      ".ci/scripts/run-gradle.sh -Dtests.iters=100 -Dtests.timeoutSuite=3600000! :server:test --tests org.elasticsearch.FooTests --tests org.elasticsearch.BarTests",
    );
  });

  test("internal cluster test", () => {
    const batch: ClassifiedTest[] = [
      {
        gradleProject: ":server",
        kind: "internalClusterTest",
        sourceSet: "internalClusterTest",
        fqcn: "org.elasticsearch.cluster.ClusterIT",
      },
    ];
    expect(generateBatchCommand(batch, DEFAULT_BATCHING_CONFIG)).toBe(
      ".ci/scripts/run-gradle.sh -Dtests.iters=20 -Dtests.timeoutSuite=3600000! :server:internalClusterTest --tests org.elasticsearch.cluster.ClusterIT",
    );
  });

  test("java REST test", () => {
    const batch: ClassifiedTest[] = [
      {
        gradleProject: ":modules:transport-netty4",
        kind: "javaRestTest",
        sourceSet: "javaRestTest",
        fqcn: "org.elasticsearch.rest.RestIT",
      },
    ];
    expect(generateBatchCommand(batch, DEFAULT_BATCHING_CONFIG)).toBe(
      ".buildkite/scripts/flakiness-detection/runners/repeat-rest-test.sh 10 .ci/scripts/run-gradle.sh :modules:transport-netty4:javaRestTest --tests org.elasticsearch.rest.RestIT --rerun",
    );
  });

  test("java REST tests across projects", () => {
    const batch: ClassifiedTest[] = [
      { gradleProject: ":mod:a", kind: "javaRestTest", sourceSet: "javaRestTest", fqcn: "org.es.FooIT" },
      { gradleProject: ":mod:b", kind: "javaRestTest", sourceSet: "javaRestTest", fqcn: "org.es.BarIT" },
    ];
    expect(generateBatchCommand(batch, DEFAULT_BATCHING_CONFIG)).toBe(
      ".buildkite/scripts/flakiness-detection/runners/repeat-rest-test.sh 10 .ci/scripts/run-gradle.sh :mod:a:javaRestTest --tests org.es.FooIT --rerun :mod:b:javaRestTest --tests org.es.BarIT --rerun",
    );
  });

  test("YAML REST test runner", () => {
    const batch: ClassifiedTest[] = [
      { gradleProject: ":x-pack:plugin:ml", kind: "yamlRestTestRunner", sourceSet: "yamlRestTest" },
    ];
    expect(generateBatchCommand(batch, DEFAULT_BATCHING_CONFIG)).toBe(
      ".buildkite/scripts/flakiness-detection/runners/repeat-rest-test.sh 10 .ci/scripts/run-gradle.sh :x-pack:plugin:ml:yamlRestTest --rerun",
    );
  });

  test("YAML REST test suite", () => {
    const batch: ClassifiedTest[] = [
      {
        gradleProject: ":x-pack:plugin:ml",
        kind: "yamlRestTestSuite",
        sourceSet: "yamlRestTest",
        suitePath: "ml/anomaly_detectors_get",
      },
    ];
    expect(generateBatchCommand(batch, DEFAULT_BATCHING_CONFIG)).toBe(
      ".buildkite/scripts/flakiness-detection/runners/repeat-rest-test.sh 10 .ci/scripts/run-gradle.sh :x-pack:plugin:ml:yamlRestTest --rerun -Dtests.rest.suite.:x-pack:plugin:ml:yamlRestTest=ml/anomaly_detectors_get",
    );
  });

  test("YAML REST test suites batched", () => {
    const batch: ClassifiedTest[] = [
      {
        gradleProject: ":x-pack:plugin:ml",
        kind: "yamlRestTestSuite",
        sourceSet: "yamlRestTest",
        suitePath: "ml/test1",
      },
      {
        gradleProject: ":x-pack:plugin:ml",
        kind: "yamlRestTestSuite",
        sourceSet: "yamlRestTest",
        suitePath: "ml/test2",
      },
    ];
    expect(generateBatchCommand(batch, DEFAULT_BATCHING_CONFIG)).toBe(
      ".buildkite/scripts/flakiness-detection/runners/repeat-rest-test.sh 10 .ci/scripts/run-gradle.sh :x-pack:plugin:ml:yamlRestTest --rerun -Dtests.rest.suite.:x-pack:plugin:ml:yamlRestTest=ml/test1,ml/test2",
    );
  });

  test("YAML REST test suites across projects", () => {
    const batch: ClassifiedTest[] = [
      {
        gradleProject: ":x-pack:plugin:ml",
        kind: "yamlRestTestSuite",
        sourceSet: "yamlRestTest",
        suitePath: "ml/test1",
      },
      {
        gradleProject: ":x-pack:plugin:security",
        kind: "yamlRestTestSuite",
        sourceSet: "yamlRestTest",
        suitePath: "security/test1",
      },
    ];
    expect(generateBatchCommand(batch, DEFAULT_BATCHING_CONFIG)).toBe(
      ".buildkite/scripts/flakiness-detection/runners/repeat-rest-test.sh 10 .ci/scripts/run-gradle.sh :x-pack:plugin:ml:yamlRestTest --rerun :x-pack:plugin:security:yamlRestTest --rerun -Dtests.rest.suite.:x-pack:plugin:ml:yamlRestTest=ml/test1 -Dtests.rest.suite.:x-pack:plugin:security:yamlRestTest=security/test1",
    );
  });

  test("YAML REST test case targets the exact parameterized test", () => {
    const batch: ClassifiedTest[] = [
      {
        gradleProject: ":x-pack:plugin:apm-data",
        kind: "yamlRestTestCase",
        sourceSet: "yamlRestTest",
        fqcn: "org.elasticsearch.xpack.apmdata.APMYamlTestSuiteIT",
        yamlTest: "test {yaml=/10_apm/Test template reinstallation}",
      },
    ];
    expect(generateBatchCommand(batch, DEFAULT_BATCHING_CONFIG)).toBe(
      '.buildkite/scripts/flakiness-detection/runners/repeat-rest-test.sh 10 .ci/scripts/run-gradle.sh :x-pack:plugin:apm-data:yamlRestTest --tests "org.elasticsearch.xpack.apmdata.APMYamlTestSuiteIT.test {yaml=/10_apm/Test template reinstallation}" --rerun',
    );
  });

  test("YAML REST test cases batched across projects", () => {
    const batch: ClassifiedTest[] = [
      {
        gradleProject: ":x-pack:plugin:apm-data",
        kind: "yamlRestTestCase",
        sourceSet: "yamlRestTest",
        fqcn: "org.elasticsearch.xpack.apmdata.APMYamlTestSuiteIT",
        yamlTest: "test {yaml=/10_apm/Test template reinstallation}",
      },
      {
        gradleProject: ":x-pack:plugin:ml",
        kind: "yamlRestTestCase",
        sourceSet: "yamlRestTest",
        fqcn: "org.elasticsearch.xpack.ml.MlYamlIT",
        yamlTest: "test {yaml=ml/anomaly_detectors_get/basic}",
      },
    ];
    expect(generateBatchCommand(batch, DEFAULT_BATCHING_CONFIG)).toBe(
      '.buildkite/scripts/flakiness-detection/runners/repeat-rest-test.sh 10 .ci/scripts/run-gradle.sh :x-pack:plugin:apm-data:yamlRestTest --tests "org.elasticsearch.xpack.apmdata.APMYamlTestSuiteIT.test {yaml=/10_apm/Test template reinstallation}" --rerun :x-pack:plugin:ml:yamlRestTest --tests "org.elasticsearch.xpack.ml.MlYamlIT.test {yaml=ml/anomaly_detectors_get/basic}" --rerun',
    );
  });

  test("local target emits ./gradlew for unit tests", () => {
    const batch: ClassifiedTest[] = [
      { gradleProject: ":server", kind: "test", sourceSet: "test", fqcn: "org.elasticsearch.FooTests" },
    ];
    expect(generateBatchCommand(batch, { ...DEFAULT_BATCHING_CONFIG, target: "local" })).toBe(
      "./gradlew -Dtests.iters=100 -Dtests.timeoutSuite=3600000! :server:test --tests org.elasticsearch.FooTests",
    );
  });

  test("local target emits ./gradlew inside repeat-rest-test for REST kinds", () => {
    const batch: ClassifiedTest[] = [
      { gradleProject: ":x-pack:plugin:ml", kind: "yamlRestTestRunner", sourceSet: "yamlRestTest" },
    ];
    expect(generateBatchCommand(batch, { ...DEFAULT_BATCHING_CONFIG, target: "local" })).toBe(
      ".buildkite/scripts/flakiness-detection/runners/repeat-rest-test.sh 10 ./gradlew :x-pack:plugin:ml:yamlRestTest --rerun",
    );
  });

  test("threads restIters override into the rest-test gradle command", () => {
    const batch: ClassifiedTest[] = [
      { gradleProject: ":m:a", kind: "javaRestTest", sourceSet: "javaRestTest", fqcn: "Foo" },
    ];
    expect(generateBatchCommand(batch, { ...DEFAULT_BATCHING_CONFIG, restIters: 3 })).toContain(
      "repeat-rest-test.sh 3 ",
    );
  });

  test("YAML REST test cases from the same project dedupe the task list", () => {
    const batch: ClassifiedTest[] = [
      {
        gradleProject: ":x-pack:plugin:ml",
        kind: "yamlRestTestCase",
        sourceSet: "yamlRestTest",
        fqcn: "org.elasticsearch.xpack.ml.MlYamlIT",
        yamlTest: "test {yaml=ml/a}",
      },
      {
        gradleProject: ":x-pack:plugin:ml",
        kind: "yamlRestTestCase",
        sourceSet: "yamlRestTest",
        fqcn: "org.elasticsearch.xpack.ml.MlYamlIT",
        yamlTest: "test {yaml=ml/b}",
      },
    ];
    expect(generateBatchCommand(batch, DEFAULT_BATCHING_CONFIG)).toBe(
      '.buildkite/scripts/flakiness-detection/runners/repeat-rest-test.sh 10 .ci/scripts/run-gradle.sh :x-pack:plugin:ml:yamlRestTest --tests "org.elasticsearch.xpack.ml.MlYamlIT.test {yaml=ml/a}" --tests "org.elasticsearch.xpack.ml.MlYamlIT.test {yaml=ml/b}" --rerun',
    );
  });
});

describe("dedupeTests", () => {
  test("removes duplicate unit test", () => {
    const tests: ClassifiedTest[] = [
      { gradleProject: ":server", kind: "test", sourceSet: "test", fqcn: "org.elasticsearch.FooTests" },
      { gradleProject: ":server", kind: "test", sourceSet: "test", fqcn: "org.elasticsearch.FooTests" },
    ];
    expect(dedupeTests(tests)).toHaveLength(1);
  });

  test("keeps tests with different fqcn", () => {
    const tests: ClassifiedTest[] = [
      { gradleProject: ":server", kind: "test", sourceSet: "test", fqcn: "org.elasticsearch.FooTests" },
      { gradleProject: ":server", kind: "test", sourceSet: "test", fqcn: "org.elasticsearch.BarTests" },
    ];
    expect(dedupeTests(tests)).toHaveLength(2);
  });

  test("keeps yaml runner and suite for the same project as distinct", () => {
    const tests: ClassifiedTest[] = [
      { gradleProject: ":x-pack:plugin:ml", kind: "yamlRestTestRunner", sourceSet: "yamlRestTest" },
      {
        gradleProject: ":x-pack:plugin:ml",
        kind: "yamlRestTestSuite",
        sourceSet: "yamlRestTest",
        suitePath: "ml/foo",
      },
    ];
    expect(dedupeTests(tests)).toHaveLength(2);
  });

  test("removes duplicate yaml suite", () => {
    const tests: ClassifiedTest[] = [
      {
        gradleProject: ":x-pack:plugin:ml",
        kind: "yamlRestTestSuite",
        sourceSet: "yamlRestTest",
        suitePath: "ml/foo",
      },
      {
        gradleProject: ":x-pack:plugin:ml",
        kind: "yamlRestTestSuite",
        sourceSet: "yamlRestTest",
        suitePath: "ml/foo",
      },
    ];
    expect(dedupeTests(tests)).toHaveLength(1);
  });

  test("distinguishes yaml cases by their yamlTest descriptor", () => {
    const tests: ClassifiedTest[] = [
      {
        gradleProject: ":x-pack:plugin:ml",
        kind: "yamlRestTestCase",
        sourceSet: "yamlRestTest",
        fqcn: "org.elasticsearch.xpack.ml.MlYamlIT",
        yamlTest: "test {yaml=/10_foo/case A}",
      },
      {
        gradleProject: ":x-pack:plugin:ml",
        kind: "yamlRestTestCase",
        sourceSet: "yamlRestTest",
        fqcn: "org.elasticsearch.xpack.ml.MlYamlIT",
        yamlTest: "test {yaml=/10_foo/case A}",
      },
      {
        gradleProject: ":x-pack:plugin:ml",
        kind: "yamlRestTestCase",
        sourceSet: "yamlRestTest",
        fqcn: "org.elasticsearch.xpack.ml.MlYamlIT",
        yamlTest: "test {yaml=/10_foo/case B}",
      },
    ];
    const result = dedupeTests(tests);
    expect(result).toHaveLength(2);
    expect(result.map((t) => t.yamlTest)).toEqual(["test {yaml=/10_foo/case A}", "test {yaml=/10_foo/case B}"]);
  });

  test("preserves input order", () => {
    const tests: ClassifiedTest[] = [
      { gradleProject: ":a", kind: "test", sourceSet: "test", fqcn: "A" },
      { gradleProject: ":b", kind: "test", sourceSet: "test", fqcn: "B" },
      { gradleProject: ":a", kind: "test", sourceSet: "test", fqcn: "A" },
    ];
    const result = dedupeTests(tests);
    expect(result.map((t) => t.fqcn)).toEqual(["A", "B"]);
  });
});

describe("buildCommands", () => {
  test("emits one RunnableCommand per batch, grouped by kind", () => {
    const tests: ClassifiedTest[] = [
      { gradleProject: ":server", kind: "test", sourceSet: "test", fqcn: "A" },
      { gradleProject: ":server", kind: "test", sourceSet: "test", fqcn: "B" },
      { gradleProject: ":server", kind: "internalClusterTest", sourceSet: "internalClusterTest", fqcn: "C" },
    ];
    const cmds = buildCommands(tests, DEFAULT_BATCHING_CONFIG);
    expect(cmds.map((c) => c.kind)).toEqual(["test", "internalClusterTest"]);
    expect(cmds[0].key).toBe("flakiness-detection:unit");
    expect(cmds[1].key).toBe("flakiness-detection:integ");
    expect(cmds[0].command).toContain("--tests A");
    expect(cmds[0].command).toContain("--tests B");
  });

  test("uses default capByKind values when batching all test kinds", () => {
    const tests: ClassifiedTest[] = [
      { gradleProject: ":server", kind: "test", sourceSet: "test", fqcn: "Unit1Tests" },
      { gradleProject: ":server", kind: "test", sourceSet: "test", fqcn: "Unit2Tests" },
      { gradleProject: ":server", kind: "test", sourceSet: "test", fqcn: "Unit3Tests" },
      { gradleProject: ":server", kind: "test", sourceSet: "test", fqcn: "Unit4Tests" },
      { gradleProject: ":server", kind: "internalClusterTest", sourceSet: "internalClusterTest", fqcn: "Integ1IT" },
      { gradleProject: ":server", kind: "internalClusterTest", sourceSet: "internalClusterTest", fqcn: "Integ2IT" },
      { gradleProject: ":server", kind: "internalClusterTest", sourceSet: "internalClusterTest", fqcn: "Integ3IT" },
      { gradleProject: ":modules:rest", kind: "javaRestTest", sourceSet: "javaRestTest", fqcn: "Rest1IT" },
      { gradleProject: ":modules:rest", kind: "javaRestTest", sourceSet: "javaRestTest", fqcn: "Rest2IT" },
      { gradleProject: ":x-pack:plugin:ml", kind: "yamlRestTestSuite", sourceSet: "yamlRestTest", suitePath: "ml/a/test" },
      { gradleProject: ":x-pack:plugin:ml", kind: "yamlRestTestSuite", sourceSet: "yamlRestTest", suitePath: "ml/b/test" },
      { gradleProject: ":x-pack:plugin:ml", kind: "yamlRestTestSuite", sourceSet: "yamlRestTest", suitePath: "ml/c/test" },
      { gradleProject: ":x-pack:plugin:ml", kind: "yamlRestTestSuite", sourceSet: "yamlRestTest", suitePath: "ml/d/test" },
      { gradleProject: ":x-pack:plugin:ml", kind: "yamlRestTestSuite", sourceSet: "yamlRestTest", suitePath: "ml/e/test" },
      { gradleProject: ":x-pack:plugin:ml", kind: "yamlRestTestRunner", sourceSet: "yamlRestTest" },
      { gradleProject: ":x-pack:plugin:security", kind: "yamlRestTestRunner", sourceSet: "yamlRestTest" },
      {
        gradleProject: ":x-pack:plugin:esql",
        kind: "yamlRestTestCase",
        sourceSet: "yamlRestTest",
        yamlTest: "test {yaml=esql/0}",
      },
      {
        gradleProject: ":x-pack:plugin:esql",
        kind: "yamlRestTestCase",
        sourceSet: "yamlRestTest",
        yamlTest: "test {yaml=esql/1}",
      },
      {
        gradleProject: ":x-pack:plugin:esql",
        kind: "yamlRestTestCase",
        sourceSet: "yamlRestTest",
        yamlTest: "test {yaml=esql/2}",
      },
      {
        gradleProject: ":x-pack:plugin:esql",
        kind: "yamlRestTestCase",
        sourceSet: "yamlRestTest",
        yamlTest: "test {yaml=esql/3}",
      },
      {
        gradleProject: ":x-pack:plugin:esql",
        kind: "yamlRestTestCase",
        sourceSet: "yamlRestTest",
        yamlTest: "test {yaml=esql/4}",
      },
    ];

    const cmds = buildCommands(tests, DEFAULT_BATCHING_CONFIG);

    expect(cmds.filter((c) => c.kind === "test")).toHaveLength(2); // 3 + 1
    expect(cmds.filter((c) => c.kind === "internalClusterTest")).toHaveLength(2); // 2 + 1
    expect(cmds.filter((c) => c.kind === "javaRestTest")).toHaveLength(2); // 1 + 1
    expect(cmds.filter((c) => c.kind === "yamlRestTestSuite")).toHaveLength(2); // 4 + 1
    expect(cmds.filter((c) => c.kind === "yamlRestTestRunner")).toHaveLength(2); // 1 + 1
    expect(cmds.filter((c) => c.kind === "yamlRestTestCase")).toHaveLength(2); // 4 + 1
  });

  test("respects capByKind to produce multiple batches per kind", () => {
    const tests: ClassifiedTest[] = Array.from({ length: 5 }, (_, i) => ({
      gradleProject: ":server",
      kind: "test",
      sourceSet: "test",
      fqcn: `Test${i}`,
    }));
    const cmds = buildCommands(tests, {
      ...DEFAULT_BATCHING_CONFIG,
      capByKind: { ...DEFAULT_BATCHING_CONFIG.capByKind, test: 2 },
    });
    expect(cmds.filter((c) => c.kind === "test")).toHaveLength(3); // 2 + 2 + 1
  });

  test("threads itersByKind override into the gradle command", () => {
    const tests: ClassifiedTest[] = [{ gradleProject: ":server", kind: "test", sourceSet: "test", fqcn: "A" }];
    const cmds = buildCommands(tests, {
      ...DEFAULT_BATCHING_CONFIG,
      itersByKind: { test: 5, internalClusterTest: 20 },
    });
    expect(cmds[0].command).toContain("-Dtests.iters=5");
  });

  test("threads suiteTimeoutMs override into the gradle command", () => {
    const tests: ClassifiedTest[] = [{ gradleProject: ":server", kind: "test", sourceSet: "test", fqcn: "A" }];
    const cmds = buildCommands(tests, {
      ...DEFAULT_BATCHING_CONFIG,
      suiteTimeoutMs: 60_000,
    });
    expect(cmds[0].command).toContain("-Dtests.timeoutSuite=60000!");
  });

  test("populates the RunnableCommand.label from KIND_LABELS", () => {
    const tests: ClassifiedTest[] = [
      { gradleProject: ":server", kind: "test", sourceSet: "test", fqcn: "A" },
      { gradleProject: ":server", kind: "internalClusterTest", sourceSet: "internalClusterTest", fqcn: "B" },
    ];
    const cmds = buildCommands(tests, DEFAULT_BATCHING_CONFIG);
    // Labels come from KIND_LABELS in domain.ts; assert the contract field is populated.
    expect(cmds.find((c) => c.kind === "test")?.label).toBe("unit tests");
    expect(cmds.find((c) => c.kind === "internalClusterTest")?.label).toBe("integ tests");
  });

  test("dedupes identical ClassifiedTests before batching", () => {
    // Two identical entries should collapse to one --tests filter, not two.
    const tests: ClassifiedTest[] = [
      { gradleProject: ":server", kind: "test", sourceSet: "test", fqcn: "Same" },
      { gradleProject: ":server", kind: "test", sourceSet: "test", fqcn: "Same" },
    ];
    const cmds = buildCommands(tests, DEFAULT_BATCHING_CONFIG);
    expect(cmds).toHaveLength(1);
    // The literal "--tests Same" should appear exactly once.
    const occurrences = cmds[0].command.split("--tests Same").length - 1;
    expect(occurrences).toBe(1);
  });

  test("collapses multiple yaml suites in same directory to one --tests entry", () => {
    // Two yamlRestTestSuite paths in the same dir should collapse to a directory-level target.
    const tests: ClassifiedTest[] = [
      {
        gradleProject: ":x-pack:plugin:ml",
        kind: "yamlRestTestSuite",
        sourceSet: "yamlRestTest",
        suitePath: "ml/foo/a",
      },
      {
        gradleProject: ":x-pack:plugin:ml",
        kind: "yamlRestTestSuite",
        sourceSet: "yamlRestTest",
        suitePath: "ml/foo/b",
      },
    ];
    const cmds = buildCommands(tests, DEFAULT_BATCHING_CONFIG);
    // The collapsed command should reference the parent directory "ml/foo", not both individual paths.
    expect(cmds[0].command).toContain("ml/foo");
    expect(cmds[0].command).not.toContain("ml/foo/a");
    expect(cmds[0].command).not.toContain("ml/foo/b");
  });

  test("threads restIters override into rest-test RunnableCommands", () => {
    const tests: ClassifiedTest[] = [
      { gradleProject: ":x:p:ml", kind: "yamlRestTestRunner", sourceSet: "yamlRestTest" },
    ];
    const cmds = buildCommands(tests, { ...DEFAULT_BATCHING_CONFIG, restIters: 2 });
    expect(cmds[0].command).toContain("repeat-rest-test.sh 2 ");
  });

  test("dedupes yaml runners per project", () => {
    // Two yamlRestTestRunner entries for the same project should collapse to a single batch
    // (the runner runs the whole source set anyway).
    const tests: ClassifiedTest[] = [
      { gradleProject: ":x-pack:plugin:ml", kind: "yamlRestTestRunner", sourceSet: "yamlRestTest" },
      { gradleProject: ":x-pack:plugin:ml", kind: "yamlRestTestRunner", sourceSet: "yamlRestTest" },
    ];
    const cmds = buildCommands(tests, DEFAULT_BATCHING_CONFIG);
    const runners = cmds.filter((c) => c.kind === "yamlRestTestRunner");
    expect(runners).toHaveLength(1);
  });
});
