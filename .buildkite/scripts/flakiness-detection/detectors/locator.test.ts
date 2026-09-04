import { describe, expect, test } from "vitest";
import { locateTest } from "./locator.ts";
import type { TestRef } from "../domain.ts";

describe("locateTest", () => {
  const repoFiles = [
    "server/src/test/java/org/elasticsearch/index/IndexTests.java",
    "server/src/internalClusterTest/java/org/elasticsearch/cluster/ClusterIT.java",
    "modules/transport-netty4/src/javaRestTest/java/org/elasticsearch/rest/RestIT.java",
    "x-pack/plugin/ml/src/yamlRestTest/java/org/elasticsearch/xpack/ml/MlYamlIT.java",
  ];

  test("locates unit test by fqcn", () => {
    const ref: TestRef = {
      className: "org.elasticsearch.index.IndexTests",
      method: "testFoo",
    };
    expect(locateTest(ref, repoFiles)).toEqual({
      gradleProject: ":server",
      kind: "test",
      sourceSet: "test",
      fqcn: "org.elasticsearch.index.IndexTests",
    });
  });

  test("locates internal cluster test by fqcn", () => {
    const ref: TestRef = {
      className: "org.elasticsearch.cluster.ClusterIT",
    };
    expect(locateTest(ref, repoFiles)).toEqual({
      gradleProject: ":server",
      kind: "internalClusterTest",
      sourceSet: "internalClusterTest",
      fqcn: "org.elasticsearch.cluster.ClusterIT",
    });
  });

  test("locates java rest test by fqcn", () => {
    const ref: TestRef = { className: "org.elasticsearch.rest.RestIT" };
    expect(locateTest(ref, repoFiles)).toEqual({
      gradleProject: ":modules:transport-netty4",
      kind: "javaRestTest",
      sourceSet: "javaRestTest",
      fqcn: "org.elasticsearch.rest.RestIT",
    });
  });

  test("classifies yaml runner class as yamlRestTestRunner without a yaml method", () => {
    const ref: TestRef = {
      className: "org.elasticsearch.xpack.ml.MlYamlIT",
    };
    expect(locateTest(ref, repoFiles)).toEqual({
      gradleProject: ":x-pack:plugin:ml",
      kind: "yamlRestTestRunner",
      sourceSet: "yamlRestTest",
    });
  });

  test("classifies yaml parameterized method as yamlRestTestCase carrying the full descriptor", () => {
    const ref: TestRef = {
      className: "org.elasticsearch.xpack.ml.MlYamlIT",
      method: "test {yaml=/10_apm/Test template reinstallation}",
    };
    expect(locateTest(ref, repoFiles)).toEqual({
      gradleProject: ":x-pack:plugin:ml",
      kind: "yamlRestTestCase",
      sourceSet: "yamlRestTest",
      fqcn: "org.elasticsearch.xpack.ml.MlYamlIT",
      yamlTest: "test {yaml=/10_apm/Test template reinstallation}",
    });
  });

  test("preserves descriptor verbatim when yaml path has no leading slash", () => {
    const ref: TestRef = {
      className: "org.elasticsearch.xpack.ml.MlYamlIT",
      method: "test {yaml=ml/anomaly_detectors_get/basic}",
    };
    expect(locateTest(ref, repoFiles)).toEqual({
      gradleProject: ":x-pack:plugin:ml",
      kind: "yamlRestTestCase",
      sourceSet: "yamlRestTest",
      fqcn: "org.elasticsearch.xpack.ml.MlYamlIT",
      yamlTest: "test {yaml=ml/anomaly_detectors_get/basic}",
    });
  });

  test("returns null when class file no longer exists", () => {
    const ref: TestRef = {
      className: "org.elasticsearch.deleted.GoneTests",
    };
    expect(locateTest(ref, repoFiles)).toBeNull();
  });

  test("returns null when file path doesn't match any source set pattern", () => {
    const ref: TestRef = { className: "org.elasticsearch.NotATest" };
    expect(
      locateTest(ref, ["server/src/main/java/org/elasticsearch/NotATest.java"])
    ).toBeNull();
  });
});
