import { describe, expect, test } from "vitest";
import {
  parseMutedEntries,
  diffMutedEntries,
  findUnmutedTests,
} from "./unmutes.ts";
import type { TestRef } from "../domain.ts";

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
    const entries: TestRef[] = [
      { className: "org.elasticsearch.Foo", method: "testBar" },
    ];
    expect(diffMutedEntries(entries, entries)).toEqual([]);
  });

  test("reports entries present in before but missing in after", () => {
    const before: TestRef[] = [
      { className: "org.elasticsearch.Foo", method: "testBar" },
      { className: "org.elasticsearch.Baz", method: "testQux" },
    ];
    const after: TestRef[] = [
      { className: "org.elasticsearch.Foo", method: "testBar" },
    ];
    expect(diffMutedEntries(before, after)).toEqual([
      { className: "org.elasticsearch.Baz", method: "testQux" },
    ]);
  });

  test("ignores entries only present in after (newly muted)", () => {
    const before: TestRef[] = [];
    const after: TestRef[] = [
      { className: "org.elasticsearch.Foo", method: "testBar" },
    ];
    expect(diffMutedEntries(before, after)).toEqual([]);
  });

  test("treats whole-class mute and method-level mute as distinct", () => {
    const before: TestRef[] = [{ className: "org.elasticsearch.Foo" }];
    const after: TestRef[] = [
      { className: "org.elasticsearch.Foo", method: "testBar" },
    ];
    expect(diffMutedEntries(before, after)).toEqual([
      { className: "org.elasticsearch.Foo" },
    ]);
  });

  test("ignores reordering", () => {
    const before: TestRef[] = [
      { className: "org.elasticsearch.A", method: "testX" },
      { className: "org.elasticsearch.B", method: "testY" },
    ];
    const after: TestRef[] = [
      { className: "org.elasticsearch.B", method: "testY" },
      { className: "org.elasticsearch.A", method: "testX" },
    ];
    expect(diffMutedEntries(before, after)).toEqual([]);
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
