import { describe, expect, test } from "vitest";
import { classifyChangedFiles } from "./changed-files.ts";

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
