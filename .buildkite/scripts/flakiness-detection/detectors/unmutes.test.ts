import { describe, expect, test } from "bun:test";
import {
  parseMutedEntries,
  diffMutedEntries,
  locateUnmutedTest,
  findUnmutedTests,
} from "./unmutes";
import type { MutedEntry } from "../domain";

describe("parseMutedEntries", () => {
  test("returns empty array for empty input", () => {
    expect(parseMutedEntries("")).toEqual([]);
  });

  test("returns empty array when tests key missing", () => {
    expect(parseMutedEntries("foo: bar\n")).toEqual([]);
  });

  test("parses entry with single method", () => {
    const yaml = `tests:
- class: org.elasticsearch.Foo
  method: testBar
  issue: https://example.com/1
`;
    expect(parseMutedEntries(yaml)).toEqual([
      { className: "org.elasticsearch.Foo", method: "testBar" },
    ]);
  });

  test("parses entry with methods list", () => {
    const yaml = `tests:
- class: org.elasticsearch.Foo
  methods:
    - testA
    - testB
  issue: https://example.com/2
`;
    expect(parseMutedEntries(yaml)).toEqual([
      { className: "org.elasticsearch.Foo", method: "testA" },
      { className: "org.elasticsearch.Foo", method: "testB" },
    ]);
  });

  test("parses whole-class mute when no method is given", () => {
    const yaml = `tests:
- class: org.elasticsearch.Foo
  issue: https://example.com/3
`;
    expect(parseMutedEntries(yaml)).toEqual([
      { className: "org.elasticsearch.Foo" },
    ]);
  });

  test("parses entry with both method and methods", () => {
    const yaml = `tests:
- class: org.elasticsearch.Foo
  method: testX
  methods:
    - testA
    - testB
`;
    expect(parseMutedEntries(yaml)).toEqual([
      { className: "org.elasticsearch.Foo", method: "testA" },
      { className: "org.elasticsearch.Foo", method: "testB" },
      { className: "org.elasticsearch.Foo", method: "testX" },
    ]);
  });

  test("preserves yaml parameterized method strings verbatim", () => {
    const yaml = `tests:
- class: org.elasticsearch.xpack.apmdata.APMYamlTestSuiteIT
  method: "test {yaml=/10_apm/Test template reinstallation}"
`;
    expect(parseMutedEntries(yaml)).toEqual([
      {
        className: "org.elasticsearch.xpack.apmdata.APMYamlTestSuiteIT",
        method: "test {yaml=/10_apm/Test template reinstallation}",
      },
    ]);
  });

  test("skips entries without a class field", () => {
    const yaml = `tests:
- method: testOrphan
- class: org.elasticsearch.Foo
  method: testBar
`;
    expect(parseMutedEntries(yaml)).toEqual([
      { className: "org.elasticsearch.Foo", method: "testBar" },
    ]);
  });
});

describe("diffMutedEntries", () => {
  test("returns empty when before and after match", () => {
    const entries: MutedEntry[] = [
      { className: "org.elasticsearch.Foo", method: "testBar" },
    ];
    expect(diffMutedEntries(entries, entries)).toEqual([]);
  });

  test("reports entries present in before but missing in after", () => {
    const before: MutedEntry[] = [
      { className: "org.elasticsearch.Foo", method: "testBar" },
      { className: "org.elasticsearch.Baz", method: "testQux" },
    ];
    const after: MutedEntry[] = [
      { className: "org.elasticsearch.Foo", method: "testBar" },
    ];
    expect(diffMutedEntries(before, after)).toEqual([
      { className: "org.elasticsearch.Baz", method: "testQux" },
    ]);
  });

  test("ignores entries only present in after (newly muted)", () => {
    const before: MutedEntry[] = [];
    const after: MutedEntry[] = [
      { className: "org.elasticsearch.Foo", method: "testBar" },
    ];
    expect(diffMutedEntries(before, after)).toEqual([]);
  });

  test("treats whole-class mute and method-level mute as distinct", () => {
    const before: MutedEntry[] = [{ className: "org.elasticsearch.Foo" }];
    const after: MutedEntry[] = [
      { className: "org.elasticsearch.Foo", method: "testBar" },
    ];
    expect(diffMutedEntries(before, after)).toEqual([
      { className: "org.elasticsearch.Foo" },
    ]);
  });

  test("ignores reordering", () => {
    const before: MutedEntry[] = [
      { className: "org.elasticsearch.A", method: "testX" },
      { className: "org.elasticsearch.B", method: "testY" },
    ];
    const after: MutedEntry[] = [
      { className: "org.elasticsearch.B", method: "testY" },
      { className: "org.elasticsearch.A", method: "testX" },
    ];
    expect(diffMutedEntries(before, after)).toEqual([]);
  });
});

describe("locateUnmutedTest", () => {
  const repoFiles = [
    "server/src/test/java/org/elasticsearch/index/IndexTests.java",
    "server/src/internalClusterTest/java/org/elasticsearch/cluster/ClusterIT.java",
    "modules/transport-netty4/src/javaRestTest/java/org/elasticsearch/rest/RestIT.java",
    "x-pack/plugin/ml/src/yamlRestTest/java/org/elasticsearch/xpack/ml/MlYamlIT.java",
  ];

  test("locates unit test by fqcn", () => {
    const entry: MutedEntry = {
      className: "org.elasticsearch.index.IndexTests",
      method: "testFoo",
    };
    expect(locateUnmutedTest(entry, repoFiles)).toEqual({
      gradleProject: ":server",
      kind: "test",
      sourceSet: "test",
      fqcn: "org.elasticsearch.index.IndexTests",
    });
  });

  test("locates internal cluster test by fqcn", () => {
    const entry: MutedEntry = {
      className: "org.elasticsearch.cluster.ClusterIT",
    };
    expect(locateUnmutedTest(entry, repoFiles)).toEqual({
      gradleProject: ":server",
      kind: "internalClusterTest",
      sourceSet: "internalClusterTest",
      fqcn: "org.elasticsearch.cluster.ClusterIT",
    });
  });

  test("locates java rest test by fqcn", () => {
    const entry: MutedEntry = { className: "org.elasticsearch.rest.RestIT" };
    expect(locateUnmutedTest(entry, repoFiles)).toEqual({
      gradleProject: ":modules:transport-netty4",
      kind: "javaRestTest",
      sourceSet: "javaRestTest",
      fqcn: "org.elasticsearch.rest.RestIT",
    });
  });

  test("classifies yaml runner class as yamlRestTestRunner without a yaml method", () => {
    const entry: MutedEntry = {
      className: "org.elasticsearch.xpack.ml.MlYamlIT",
    };
    expect(locateUnmutedTest(entry, repoFiles)).toEqual({
      gradleProject: ":x-pack:plugin:ml",
      kind: "yamlRestTestRunner",
      sourceSet: "yamlRestTest",
    });
  });

  test("classifies yaml parameterized method as yamlRestTestCase carrying the full descriptor", () => {
    const entry: MutedEntry = {
      className: "org.elasticsearch.xpack.ml.MlYamlIT",
      method: "test {yaml=/10_apm/Test template reinstallation}",
    };
    expect(locateUnmutedTest(entry, repoFiles)).toEqual({
      gradleProject: ":x-pack:plugin:ml",
      kind: "yamlRestTestCase",
      sourceSet: "yamlRestTest",
      fqcn: "org.elasticsearch.xpack.ml.MlYamlIT",
      yamlTest: "test {yaml=/10_apm/Test template reinstallation}",
    });
  });

  test("preserves descriptor verbatim when yaml path has no leading slash", () => {
    const entry: MutedEntry = {
      className: "org.elasticsearch.xpack.ml.MlYamlIT",
      method: "test {yaml=ml/anomaly_detectors_get/basic}",
    };
    expect(locateUnmutedTest(entry, repoFiles)).toEqual({
      gradleProject: ":x-pack:plugin:ml",
      kind: "yamlRestTestCase",
      sourceSet: "yamlRestTest",
      fqcn: "org.elasticsearch.xpack.ml.MlYamlIT",
      yamlTest: "test {yaml=ml/anomaly_detectors_get/basic}",
    });
  });

  test("returns null when class file no longer exists", () => {
    const entry: MutedEntry = {
      className: "org.elasticsearch.deleted.GoneTests",
    };
    expect(locateUnmutedTest(entry, repoFiles)).toBeNull();
  });

  test("returns null when file path doesn't match any source set pattern", () => {
    const entry: MutedEntry = { className: "org.elasticsearch.NotATest" };
    expect(
      locateUnmutedTest(entry, ["server/src/main/java/org/elasticsearch/NotATest.java"])
    ).toBeNull();
  });
});

describe("findUnmutedTests", () => {
  const repoFiles = [
    "server/src/test/java/org/elasticsearch/index/IndexTests.java",
  ];

  test("returns empty when nothing changed", () => {
    const yaml = `tests:
- class: org.elasticsearch.index.IndexTests
  method: testFoo
`;
    expect(findUnmutedTests(yaml, yaml, repoFiles)).toEqual({
      located: [],
      unlocated: [],
    });
  });

  test("locates an unmuted test that still exists", () => {
    const before = `tests:
- class: org.elasticsearch.index.IndexTests
  method: testFoo
`;
    const after = "tests:\n";
    expect(findUnmutedTests(before, after, repoFiles)).toEqual({
      located: [
        {
          gradleProject: ":server",
          kind: "test",
          sourceSet: "test",
          fqcn: "org.elasticsearch.index.IndexTests",
        },
      ],
      unlocated: [],
    });
  });

  test("reports unlocated when the class file was removed", () => {
    const before = `tests:
- class: org.elasticsearch.deleted.GoneTests
  method: testFoo
`;
    const after = "tests:\n";
    expect(findUnmutedTests(before, after, repoFiles)).toEqual({
      located: [],
      unlocated: [
        { className: "org.elasticsearch.deleted.GoneTests", method: "testFoo" },
      ],
    });
  });

  test("handles empty before yaml (file did not exist at merge base)", () => {
    const after = `tests:
- class: org.elasticsearch.index.IndexTests
  method: testFoo
`;
    expect(findUnmutedTests("", after, repoFiles)).toEqual({
      located: [],
      unlocated: [],
    });
  });
});
