import { describe, expect, test } from "bun:test";

import {
  classifyChangedFiles,
  collapseYamlSuites,
  deduplicateYamlRunners,
  generateBatchCommand,
  generatePipeline,
  resolveMergeBaseTarget,
  toGradleProject,
  toFqcn,
  ClassifiedTest,
  BATCH_CAPS,
} from "./repeat-changed-tests";

describe("toGradleProject", () => {
  test("converts simple path", () => {
    expect(toGradleProject("server")).toBe(":server");
  });

  test("converts nested path", () => {
    expect(toGradleProject("x-pack/plugin/core")).toBe(":x-pack:plugin:core");
  });

  test("converts modules path", () => {
    expect(toGradleProject("modules/transport-netty4")).toBe(":modules:transport-netty4");
  });

  test("converts deeply nested qa path", () => {
    expect(toGradleProject("x-pack/plugin/ml/qa/native-multi-node-tests")).toBe(
      ":x-pack:plugin:ml:qa:native-multi-node-tests"
    );
  });

  test("prefixes test- on children of test/external-modules", () => {
    expect(toGradleProject("test/external-modules/apm-integration")).toBe(
      ":test:external-modules:test-apm-integration"
    );
  });

  test("leaves sibling test paths unchanged", () => {
    expect(toGradleProject("test/fixtures/some-fixture")).toBe(":test:fixtures:some-fixture");
  });
});

describe("toFqcn", () => {
  test("converts java package path to FQCN", () => {
    expect(toFqcn("org/elasticsearch/index/IndexTests")).toBe("org.elasticsearch.index.IndexTests");
  });

  test("converts deeply nested path", () => {
    expect(toFqcn("org/elasticsearch/xpack/core/security/AuthTests")).toBe(
      "org.elasticsearch.xpack.core.security.AuthTests"
    );
  });
});

describe("classifyChangedFiles", () => {
  test("classifies unit test files", () => {
    const result = classifyChangedFiles(["server/src/test/java/org/elasticsearch/index/IndexTests.java"]);

    expect(result).toEqual([
      {
        gradleProject: ":server",
        kind: "test",
        sourceSet: "test",
        fqcn: "org.elasticsearch.index.IndexTests",
      },
    ]);
  });

  test("classifies internal cluster test files", () => {
    const result = classifyChangedFiles([
      "server/src/internalClusterTest/java/org/elasticsearch/cluster/ClusterIT.java",
    ]);

    expect(result).toEqual([
      {
        gradleProject: ":server",
        kind: "internalClusterTest",
        sourceSet: "internalClusterTest",
        fqcn: "org.elasticsearch.cluster.ClusterIT",
      },
    ]);
  });

  test("classifies Java REST test files", () => {
    const result = classifyChangedFiles([
      "modules/transport-netty4/src/javaRestTest/java/org/elasticsearch/rest/RestIT.java",
    ]);

    expect(result).toEqual([
      {
        gradleProject: ":modules:transport-netty4",
        kind: "javaRestTest",
        sourceSet: "javaRestTest",
        fqcn: "org.elasticsearch.rest.RestIT",
      },
    ]);
  });

  test("classifies YAML REST test runner files", () => {
    const result = classifyChangedFiles([
      "x-pack/plugin/ml/src/yamlRestTest/java/org/elasticsearch/xpack/ml/MlYamlIT.java",
    ]);

    expect(result).toEqual([
      {
        gradleProject: ":x-pack:plugin:ml",
        kind: "yamlRestTestRunner",
        sourceSet: "yamlRestTest",
      },
    ]);
  });

  test("classifies YAML REST test suite files", () => {
    const result = classifyChangedFiles([
      "x-pack/plugin/ml/src/yamlRestTest/resources/rest-api-spec/test/ml/anomaly_detectors_get.yml",
    ]);

    expect(result).toEqual([
      {
        gradleProject: ":x-pack:plugin:ml",
        kind: "yamlRestTestSuite",
        sourceSet: "yamlRestTest",
        suitePath: "ml/anomaly_detectors_get",
      },
    ]);
  });

  test("ignores non-test files", () => {
    const result = classifyChangedFiles([
      "server/src/main/java/org/elasticsearch/index/Index.java",
      "docs/README.asciidoc",
      "build.gradle",
    ]);

    expect(result).toEqual([]);
  });

  test("skips abstract base classes and helpers", () => {
    const result = classifyChangedFiles([
      "server/src/test/java/org/elasticsearch/test/AbstractIndexTest.java",
      "server/src/test/java/org/elasticsearch/test/IndexTestCase.java",
      "server/src/test/java/org/elasticsearch/test/TestUtils.java",
      "server/src/internalClusterTest/java/org/elasticsearch/test/AbstractClusterTestCase.java",
      "server/src/javaRestTest/java/org/elasticsearch/rest/AbstractRestTest.java",
      "server/src/yamlRestTest/java/org/elasticsearch/rest/YamlTestHelper.java",
    ]);

    expect(result).toEqual([]);
  });

  test("classifies x-pack nested project paths", () => {
    const result = classifyChangedFiles([
      "x-pack/plugin/core/src/test/java/org/elasticsearch/xpack/core/SomeTests.java",
    ]);

    expect(result).toEqual([
      {
        gradleProject: ":x-pack:plugin:core",
        kind: "test",
        sourceSet: "test",
        fqcn: "org.elasticsearch.xpack.core.SomeTests",
      },
    ]);
  });

  test("classifies external-modules javaRestTest with test- prefix", () => {
    const result = classifyChangedFiles([
      "test/external-modules/apm-integration/src/javaRestTest/java/org/elasticsearch/test/apmintegration/ApmAgentTracesIT.java",
    ]);

    expect(result).toEqual([
      {
        gradleProject: ":test:external-modules:test-apm-integration",
        kind: "javaRestTest",
        sourceSet: "javaRestTest",
        fqcn: "org.elasticsearch.test.apmintegration.ApmAgentTracesIT",
      },
    ]);
  });

  test("classifies mixed file types", () => {
    const result = classifyChangedFiles([
      "server/src/test/java/org/elasticsearch/index/IndexTests.java",
      "server/src/main/java/org/elasticsearch/index/Index.java",
      "x-pack/plugin/ml/src/javaRestTest/java/org/elasticsearch/xpack/ml/MlRestIT.java",
      "docs/README.asciidoc",
    ]);

    expect(result).toHaveLength(2);
    expect(result[0].kind).toBe("test");
    expect(result[1].kind).toBe("javaRestTest");
  });
});

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
    expect(generateBatchCommand(batch)).toBe(
      ".ci/scripts/run-gradle.sh -Dtests.iters=100 -Dtests.timeoutSuite=3600000! :server:test --tests org.elasticsearch.index.IndexTests"
    );
  });

  test("unit tests across projects", () => {
    const batch: ClassifiedTest[] = [
      { gradleProject: ":server", kind: "test", sourceSet: "test", fqcn: "org.elasticsearch.index.FooTests" },
      { gradleProject: ":libs:core", kind: "test", sourceSet: "test", fqcn: "org.elasticsearch.core.BarTests" },
    ];
    expect(generateBatchCommand(batch)).toBe(
      ".ci/scripts/run-gradle.sh -Dtests.iters=100 -Dtests.timeoutSuite=3600000! :server:test :libs:core:test --tests org.elasticsearch.index.FooTests --tests org.elasticsearch.core.BarTests"
    );
  });

  test("deduplicates projects", () => {
    const batch: ClassifiedTest[] = [
      { gradleProject: ":server", kind: "test", sourceSet: "test", fqcn: "org.elasticsearch.FooTests" },
      { gradleProject: ":server", kind: "test", sourceSet: "test", fqcn: "org.elasticsearch.BarTests" },
    ];
    expect(generateBatchCommand(batch)).toBe(
      ".ci/scripts/run-gradle.sh -Dtests.iters=100 -Dtests.timeoutSuite=3600000! :server:test --tests org.elasticsearch.FooTests --tests org.elasticsearch.BarTests"
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
    expect(generateBatchCommand(batch)).toBe(
      ".ci/scripts/run-gradle.sh -Dtests.iters=20 -Dtests.timeoutSuite=3600000! :server:internalClusterTest --tests org.elasticsearch.cluster.ClusterIT"
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
    expect(generateBatchCommand(batch)).toBe(
      ".ci/scripts/repeat-rest-test.sh 10 .ci/scripts/run-gradle.sh :modules:transport-netty4:javaRestTest --tests org.elasticsearch.rest.RestIT --rerun"
    );
  });

  test("java REST tests across projects", () => {
    const batch: ClassifiedTest[] = [
      { gradleProject: ":mod:a", kind: "javaRestTest", sourceSet: "javaRestTest", fqcn: "org.es.FooIT" },
      { gradleProject: ":mod:b", kind: "javaRestTest", sourceSet: "javaRestTest", fqcn: "org.es.BarIT" },
    ];
    expect(generateBatchCommand(batch)).toBe(
      ".ci/scripts/repeat-rest-test.sh 10 .ci/scripts/run-gradle.sh :mod:a:javaRestTest :mod:b:javaRestTest --tests org.es.FooIT --tests org.es.BarIT --rerun"
    );
  });

  test("YAML REST test runner", () => {
    const batch: ClassifiedTest[] = [
      { gradleProject: ":x-pack:plugin:ml", kind: "yamlRestTestRunner", sourceSet: "yamlRestTest" },
    ];
    expect(generateBatchCommand(batch)).toBe(
      ".ci/scripts/repeat-rest-test.sh 10 .ci/scripts/run-gradle.sh :x-pack:plugin:ml:yamlRestTest --rerun"
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
    expect(generateBatchCommand(batch)).toBe(
      ".ci/scripts/repeat-rest-test.sh 10 .ci/scripts/run-gradle.sh :x-pack:plugin:ml:yamlRestTest -Dtests.rest.suite=ml/anomaly_detectors_get --rerun"
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
    expect(generateBatchCommand(batch)).toBe(
      ".ci/scripts/repeat-rest-test.sh 10 .ci/scripts/run-gradle.sh :x-pack:plugin:ml:yamlRestTest -Dtests.rest.suite=ml/test1,ml/test2 --rerun"
    );
  });
});

describe("generatePipeline", () => {
  test("single batch has no parallelism", () => {
    const tests: ClassifiedTest[] = [
      { gradleProject: ":server", kind: "test", sourceSet: "test", fqcn: "org.elasticsearch.index.IndexTests" },
    ];

    const pipeline = generatePipeline(tests);
    expect(pipeline.steps).toHaveLength(1);
    expect(pipeline.steps[0].group).toBe("repeat-changed-tests");

    const step = pipeline.steps[0].steps[0];
    expect(step.label).toBe("unit tests");
    expect(step.key).toBe("repeat-changed-tests:unit");
    expect(step.parallelism).toBeUndefined();
    expect(step.env).toBeUndefined();
    expect(step.command).toBe(
      ".ci/scripts/run-gradle.sh -Dtests.iters=100 -Dtests.timeoutSuite=3600000! :server:test --tests org.elasticsearch.index.IndexTests"
    );
    expect(step.timeout_in_minutes).toBe(60);
    expect(step.agents.provider).toBe("gcp");
    expect(step.agents.machineType).toBe("n4-custom-32-98304");
  });

  test("multiple batches use parallelism with env dispatch", () => {
    const tests: ClassifiedTest[] = [];
    for (let i = 0; i < 5; i++) {
      tests.push({
        gradleProject: `:mod:${i}`,
        kind: "javaRestTest",
        sourceSet: "javaRestTest",
        fqcn: `org.elasticsearch.Rest${i}IT`,
      });
    }

    const pipeline = generatePipeline(tests);
    expect(pipeline.steps).toHaveLength(1);

    const group = pipeline.steps[0];
    expect(group.group).toBe("repeat-changed-tests");
    expect(group.steps).toHaveLength(1);

    const step = group.steps[0];
    expect(step.label).toBe("java rest tests");
    expect(step.key).toBe("repeat-changed-tests:java-rest");
    expect(step.parallelism).toBe(2);
    expect(step.env).toBeDefined();
    expect(step.env!["BATCH_COMMAND_0"]).toContain("repeat-rest-test.sh");
    expect(step.env!["BATCH_COMMAND_1"]).toContain("repeat-rest-test.sh");
    expect(step.command).toContain("BUILDKITE_PARALLEL_JOB");
    // The `$$` escape prevents Buildkite pipeline interpolation from trying to
    // parse `${!VARNAME}` (bash indirect expansion) as a Buildkite variable,
    // which fails with "Expected identifier to start with a letter, got !".
    expect(step.command).toContain('$${!VARNAME}');
    expect(step.command).not.toMatch(/[^$]\$\{!VARNAME\}/);
  });

  test("all test kinds appear in single group with unique keys", () => {
    const tests: ClassifiedTest[] = [
      { gradleProject: ":server", kind: "test", sourceSet: "test", fqcn: "org.elasticsearch.SomeTests" },
      {
        gradleProject: ":server",
        kind: "internalClusterTest",
        sourceSet: "internalClusterTest",
        fqcn: "org.elasticsearch.ClusterIT",
      },
    ];

    const pipeline = generatePipeline(tests);
    expect(pipeline.steps).toHaveLength(1);
    expect(pipeline.steps[0].group).toBe("repeat-changed-tests");
    expect(pipeline.steps[0].steps).toHaveLength(2);
    expect(pipeline.steps[0].steps[0].label).toBe("unit tests");
    expect(pipeline.steps[0].steps[0].key).toBe("repeat-changed-tests:unit");
    expect(pipeline.steps[0].steps[1].label).toBe("integ tests");
    expect(pipeline.steps[0].steps[1].key).toBe("repeat-changed-tests:integ");
  });

  test("yaml runners and suites get separate labels", () => {
    const tests: ClassifiedTest[] = [
      { gradleProject: ":x-pack:plugin:ml", kind: "yamlRestTestRunner", sourceSet: "yamlRestTest" },
      {
        gradleProject: ":x-pack:plugin:ml",
        kind: "yamlRestTestSuite",
        sourceSet: "yamlRestTest",
        suitePath: "ml/test",
      },
    ];

    const pipeline = generatePipeline(tests);
    expect(pipeline.steps).toHaveLength(1);
    expect(pipeline.steps[0].steps).toHaveLength(2);
    expect(pipeline.steps[0].steps[0].label).toBe("yaml rest test runner");
    expect(pipeline.steps[0].steps[1].label).toBe("yaml rest tests");
  });

  test("returns empty group for empty input", () => {
    const pipeline = generatePipeline([]);
    expect(pipeline.steps).toHaveLength(1);
    expect(pipeline.steps[0].group).toBe("repeat-changed-tests");
    expect(pipeline.steps[0].steps).toEqual([]);
  });
});

describe("resolveMergeBaseTarget", () => {
  test("uses target branch directly when ref exists locally", () => {
    const commands: string[] = [];
    const runner = (command: string): Buffer => {
      commands.push(command);
      return Buffer.from("");
    };

    const result = resolveMergeBaseTarget("main", runner, "/repo");

    expect(result).toBe("main");
    expect(commands).toEqual(["git rev-parse --verify main^{commit}"]);
  });

  test("fetches remote target and falls back to FETCH_HEAD when ref is missing", () => {
    const commands: string[] = [];
    const runner = (command: string): Buffer => {
      commands.push(command);
      if (command.startsWith("git rev-parse")) {
        throw new Error("missing ref");
      }
      return Buffer.from("");
    };

    const result = resolveMergeBaseTarget("gh/MattAlp/1/base", runner, "/repo");

    expect(result).toBe("FETCH_HEAD");
    expect(commands).toEqual([
      "git rev-parse --verify gh/MattAlp/1/base^{commit}",
      "git fetch --no-tags origin gh/MattAlp/1/base",
    ]);
  });
});
