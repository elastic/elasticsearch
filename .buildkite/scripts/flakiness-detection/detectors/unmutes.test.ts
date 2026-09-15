import { describe, expect, test } from "vitest";
import { parseMutedEntries, diffMutedEntries, findUnmutedRefs } from "./unmutes.ts";

type MutedEntry = { className: string; method?: string };

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
    const entries: MutedEntry[] = [{ className: "org.elasticsearch.Foo", method: "testBar" }];
    expect(diffMutedEntries(entries, entries)).toEqual([]);
  });

  test("reports entries present in before but missing in after", () => {
    const before: MutedEntry[] = [
      { className: "org.elasticsearch.Foo", method: "testBar" },
      { className: "org.elasticsearch.Baz", method: "testQux" },
    ];
    const after: MutedEntry[] = [{ className: "org.elasticsearch.Foo", method: "testBar" }];
    expect(diffMutedEntries(before, after)).toEqual([
      { className: "org.elasticsearch.Baz", method: "testQux" },
    ]);
  });

  test("ignores entries only present in after (newly muted)", () => {
    const before: MutedEntry[] = [];
    const after: MutedEntry[] = [{ className: "org.elasticsearch.Foo", method: "testBar" }];
    expect(diffMutedEntries(before, after)).toEqual([]);
  });

  test("treats whole-class mute and method-level mute as distinct", () => {
    const before: MutedEntry[] = [{ className: "org.elasticsearch.Foo" }];
    const after: MutedEntry[] = [{ className: "org.elasticsearch.Foo", method: "testBar" }];
    expect(diffMutedEntries(before, after)).toEqual([{ className: "org.elasticsearch.Foo" }]);
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

describe("findUnmutedRefs", () => {
  test("returns empty when nothing changed", () => {
    const yaml = `tests:
- class: org.elasticsearch.index.IndexTests
  method: testFoo
`;
    expect(findUnmutedRefs(yaml, yaml)).toEqual([]);
  });

  test("emits an unmute ref (class + method) for a removed entry", () => {
    const before = `tests:
- class: org.elasticsearch.index.IndexTests
  method: testFoo
`;
    const after = "tests:\n";
    // Note: resolution to project/sourceSet/kind is now the Java resolver's job; the gatherer just emits
    // the ref verbatim, whether or not the class still exists.
    expect(findUnmutedRefs(before, after)).toEqual([
      { source: "unmute", className: "org.elasticsearch.index.IndexTests", method: "testFoo" },
    ]);
  });

  test("emits a whole-class unmute ref with no method", () => {
    const before = `tests:
- class: org.elasticsearch.deleted.GoneTests
`;
    const after = "tests:\n";
    expect(findUnmutedRefs(before, after)).toEqual([
      { source: "unmute", className: "org.elasticsearch.deleted.GoneTests" },
    ]);
  });

  test("handles empty before yaml (file did not exist at merge base)", () => {
    const after = `tests:
- class: org.elasticsearch.index.IndexTests
  method: testFoo
`;
    expect(findUnmutedRefs("", after)).toEqual([]);
  });
});
