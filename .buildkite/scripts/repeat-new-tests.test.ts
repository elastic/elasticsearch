import { describe, expect, test } from "bun:test";

import {
  classifyChangedFiles,
  collapseYamlSuites,
  deduplicateYamlRunners,
  generatePipeline,
  toGradleProject,
  toFqcn,
  ClassifiedTest,
} from "./repeat-new-tests";

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
});

describe("toFqcn", () => {
  test("converts java package path to FQCN", () => {
    expect(toFqcn("org/elasticsearch/index/IndexTest")).toBe("org.elasticsearch.index.IndexTest");
  });

  test("converts deeply nested path", () => {
    expect(toFqcn("org/elasticsearch/xpack/core/security/AuthTest")).toBe(
      "org.elasticsearch.xpack.core.security.AuthTest"
    );
  });
});

describe("classifyChangedFiles", () => {
  test("classifies unit test files", () => {
    const result = classifyChangedFiles(["server/src/test/java/org/elasticsearch/index/IndexTest.java"]);

    expect(result).toEqual([
      {
        gradleProject: ":server",
        kind: "test",
        sourceSet: "test",
        fqcn: "org.elasticsearch.index.IndexTest",
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

  test("classifies x-pack nested project paths", () => {
    const result = classifyChangedFiles([
      "x-pack/plugin/core/src/test/java/org/elasticsearch/xpack/core/SomeTest.java",
    ]);

    expect(result).toEqual([
      {
        gradleProject: ":x-pack:plugin:core",
        kind: "test",
        sourceSet: "test",
        fqcn: "org.elasticsearch.xpack.core.SomeTest",
      },
    ]);
  });

  test("classifies mixed file types", () => {
    const result = classifyChangedFiles([
      "server/src/test/java/org/elasticsearch/index/IndexTest.java",
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
      { gradleProject: ":server", kind: "test", sourceSet: "test", fqcn: "org.elasticsearch.SomeTest" },
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
      { gradleProject: ":server", kind: "test", sourceSet: "test", fqcn: "org.elasticsearch.SomeTest" },
      { gradleProject: ":server", kind: "test", sourceSet: "test", fqcn: "org.elasticsearch.OtherTest" },
    ];

    const result = deduplicateYamlRunners(tests);
    expect(result).toHaveLength(2);
  });
});

describe("generatePipeline", () => {
  test("generates correct pipeline for unit tests", () => {
    const tests: ClassifiedTest[] = [
      { gradleProject: ":server", kind: "test", sourceSet: "test", fqcn: "org.elasticsearch.index.IndexTest" },
    ];

    const pipeline = generatePipeline(tests);
    expect(pipeline.steps).toHaveLength(1);
    expect(pipeline.steps[0].group).toBe("Unit Tests (tests.iters=100)");
    expect(pipeline.steps[0].steps).toHaveLength(1);

    const step = pipeline.steps[0].steps[0];
    expect(step.label).toBe(":server:test - IndexTest (x100)");
    expect(step.command).toBe(
      ".ci/scripts/run-gradle.sh -Dtests.iters=100 :server:test --tests org.elasticsearch.index.IndexTest"
    );
    expect(step.timeout_in_minutes).toBe(60);
    expect(step.agents.provider).toBe("gcp");
    expect(step.agents.machineType).toBe("n4-custom-32-98304");
  });

  test("generates correct pipeline for internal cluster tests", () => {
    const tests: ClassifiedTest[] = [
      {
        gradleProject: ":server",
        kind: "internalClusterTest",
        sourceSet: "internalClusterTest",
        fqcn: "org.elasticsearch.cluster.ClusterIT",
      },
    ];

    const pipeline = generatePipeline(tests);
    const step = pipeline.steps[0].steps[0];
    expect(step.label).toBe(":server:internalClusterTest - ClusterIT (x20)");
    expect(step.command).toBe(
      ".ci/scripts/run-gradle.sh -Dtests.iters=20 :server:internalClusterTest --tests org.elasticsearch.cluster.ClusterIT"
    );
  });

  test("generates correct pipeline for Java REST tests", () => {
    const tests: ClassifiedTest[] = [
      {
        gradleProject: ":modules:transport-netty4",
        kind: "javaRestTest",
        sourceSet: "javaRestTest",
        fqcn: "org.elasticsearch.rest.RestIT",
      },
    ];

    const pipeline = generatePipeline(tests);
    const step = pipeline.steps[0].steps[0];
    expect(step.label).toBe(":modules:transport-netty4:javaRestTest - RestIT (x10)");
    expect(step.command).toBe(
      ".ci/scripts/repeat-rest-test.sh 10 .ci/scripts/run-gradle.sh :modules:transport-netty4:javaRestTest --tests org.elasticsearch.rest.RestIT"
    );
  });

  test("generates correct pipeline for YAML REST test runners", () => {
    const tests: ClassifiedTest[] = [
      { gradleProject: ":x-pack:plugin:ml", kind: "yamlRestTestRunner", sourceSet: "yamlRestTest" },
    ];

    const pipeline = generatePipeline(tests);
    const step = pipeline.steps[0].steps[0];
    expect(step.label).toBe(":x-pack:plugin:ml:yamlRestTest (x10)");
    expect(step.command).toBe(
      ".ci/scripts/repeat-rest-test.sh 10 .ci/scripts/run-gradle.sh :x-pack:plugin:ml:yamlRestTest"
    );
  });

  test("generates correct pipeline for YAML REST test suites", () => {
    const tests: ClassifiedTest[] = [
      {
        gradleProject: ":x-pack:plugin:ml",
        kind: "yamlRestTestSuite",
        sourceSet: "yamlRestTest",
        suitePath: "ml/anomaly_detectors_get",
      },
    ];

    const pipeline = generatePipeline(tests);
    const step = pipeline.steps[0].steps[0];
    expect(step.label).toBe(":x-pack:plugin:ml:yamlRestTest - ml/anomaly_detectors_get (x10)");
    expect(step.command).toBe(
      ".ci/scripts/repeat-rest-test.sh 10 .ci/scripts/run-gradle.sh :x-pack:plugin:ml:yamlRestTest -Dtests.rest.suite=ml/anomaly_detectors_get"
    );
  });

  test("groups tests by source set type", () => {
    const tests: ClassifiedTest[] = [
      { gradleProject: ":server", kind: "test", sourceSet: "test", fqcn: "org.elasticsearch.SomeTest" },
      { gradleProject: ":server", kind: "test", sourceSet: "test", fqcn: "org.elasticsearch.OtherTest" },
      {
        gradleProject: ":server",
        kind: "internalClusterTest",
        sourceSet: "internalClusterTest",
        fqcn: "org.elasticsearch.ClusterIT",
      },
    ];

    const pipeline = generatePipeline(tests);
    expect(pipeline.steps).toHaveLength(2);
    expect(pipeline.steps[0].group).toBe("Unit Tests (tests.iters=100)");
    expect(pipeline.steps[0].steps).toHaveLength(2);
    expect(pipeline.steps[1].group).toBe("Internal Cluster Tests (tests.iters=20)");
    expect(pipeline.steps[1].steps).toHaveLength(1);
  });

  test("combines YAML REST runners and suites in same group", () => {
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
    expect(pipeline.steps[0].group).toBe("YAML REST Tests (x10)");
    expect(pipeline.steps[0].steps).toHaveLength(2);
  });

  test("returns empty steps for empty input", () => {
    const pipeline = generatePipeline([]);
    expect(pipeline.steps).toEqual([]);
  });
});
