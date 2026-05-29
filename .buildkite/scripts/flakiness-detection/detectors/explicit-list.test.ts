import { describe, expect, test } from "vitest";
import { classifyExplicitList } from "./explicit-list.ts";

describe("classifyExplicitList", () => {
  const repoFiles = [
    "server/src/test/java/org/elasticsearch/index/IndexTests.java",
    "server/src/internalClusterTest/java/org/elasticsearch/cluster/ClusterIT.java",
    "modules/transport-netty4/src/javaRestTest/java/org/elasticsearch/rest/RestIT.java",
    "x-pack/plugin/ml/src/yamlRestTest/java/org/elasticsearch/xpack/ml/MlYamlIT.java",
  ];

  test("resolves a bare FQCN to a unit test", () => {
    const { located, unlocated } = classifyExplicitList(
      ["org.elasticsearch.index.IndexTests"],
      repoFiles
    );
    expect(unlocated).toEqual([]);
    expect(located).toEqual([
      {
        gradleProject: ":server",
        kind: "test",
        sourceSet: "test",
        fqcn: "org.elasticsearch.index.IndexTests",
      },
    ]);
  });

  test("resolves FQCN.method into a class-level ClassifiedTest", () => {
    // Method-level --tests filtering is not part of v1 (see plan non-goals).
    // The returned ClassifiedTest is class-level; method filtering would need
    // command-creator changes that are explicitly deferred.
    const { located } = classifyExplicitList(
      ["org.elasticsearch.index.IndexTests.testFoo"],
      repoFiles
    );
    expect(located).toEqual([
      {
        gradleProject: ":server",
        kind: "test",
        sourceSet: "test",
        fqcn: "org.elasticsearch.index.IndexTests",
      },
    ]);
  });

  test("resolves a yaml-test-case spec into yamlRestTestCase kind", () => {
    const { located } = classifyExplicitList(
      ['org.elasticsearch.xpack.ml.MlYamlIT.test {yaml=/10_ml/Basic stats}'],
      repoFiles
    );
    expect(located).toEqual([
      {
        gradleProject: ":x-pack:plugin:ml",
        kind: "yamlRestTestCase",
        sourceSet: "yamlRestTest",
        fqcn: "org.elasticsearch.xpack.ml.MlYamlIT",
        yamlTest: "test {yaml=/10_ml/Basic stats}",
      },
    ]);
  });

  test("classifies an internal cluster test", () => {
    const { located } = classifyExplicitList(
      ["org.elasticsearch.cluster.ClusterIT"],
      repoFiles
    );
    expect(located[0].kind).toBe("internalClusterTest");
  });

  test("reports unresolved specs in unlocated", () => {
    const { located, unlocated } = classifyExplicitList(
      [
        "org.elasticsearch.index.IndexTests",
        "org.elasticsearch.does.not.Exist",
      ],
      repoFiles
    );
    expect(located).toHaveLength(1);
    expect(unlocated).toEqual([
      { spec: "org.elasticsearch.does.not.Exist" },
    ]);
  });

  test("trims whitespace and ignores empty entries", () => {
    const { located, unlocated } = classifyExplicitList(
      ["  org.elasticsearch.index.IndexTests  ", "", "   "],
      repoFiles
    );
    expect(located).toHaveLength(1);
    expect(unlocated).toEqual([]);
  });
});
