import { describe, expect, test } from "vitest";

import { diffMutedEntries, findUnmutedRefs, parseMutedEntries } from "./unmutes.ts";

type MutedEntry = { className: string; method?: string; tasks?: string[] };

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
    expect(parseMutedEntries(yaml)).toEqual([{ className: "org.elasticsearch.Foo", method: "testBar" }]);
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
    expect(parseMutedEntries(yaml)).toEqual([{ className: "org.elasticsearch.Foo" }]);
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

  test("normalizes and carries task scope", () => {
    const yaml = `tests:
- class: org.elasticsearch.Foo
  method: testBar
  tasks:
    - :server:internalClusterTest
    - :server:test
    - :server:test
`;
    expect(parseMutedEntries(yaml)).toEqual([
      {
        className: "org.elasticsearch.Foo",
        method: "testBar",
        tasks: [":server:internalClusterTest", ":server:test"],
      },
    ]);
  });

  test("skips entries without a class field", () => {
    const yaml = `tests:
- method: testOrphan
- class: org.elasticsearch.Foo
  method: testBar
`;
    expect(parseMutedEntries(yaml)).toEqual([{ className: "org.elasticsearch.Foo", method: "testBar" }]);
  });
});

describe("diffMutedEntries", () => {
  test("returns empty when before and after match", () => {
    const entries: MutedEntry[] = [{ className: "org.elasticsearch.Foo", method: "testBar" }];
    expect(diffMutedEntries(entries, entries)).toEqual([]);
  });

  test("reports a fully removed global mute as an unmute ref", () => {
    const before: MutedEntry[] = [{ className: "org.elasticsearch.Baz", method: "testQux" }];
    const after: MutedEntry[] = [];
    expect(diffMutedEntries(before, after)).toEqual([
      { source: "unmute", className: "org.elasticsearch.Baz", method: "testQux" },
    ]);
  });

  test("reports a removed scoped mute with includeTasks", () => {
    const before: MutedEntry[] = [{ className: "org.elasticsearch.Foo", method: "testBar", tasks: [":server:test"] }];
    const after: MutedEntry[] = [];
    expect(diffMutedEntries(before, after)).toEqual([
      {
        source: "unmute",
        className: "org.elasticsearch.Foo",
        method: "testBar",
        includeTasks: [":server:test"],
      },
    ]);
  });

  test("treats whole-class mute and method-level mute as distinct", () => {
    const before: MutedEntry[] = [{ className: "org.elasticsearch.Foo" }];
    const after: MutedEntry[] = [{ className: "org.elasticsearch.Foo", method: "testBar" }];
    expect(diffMutedEntries(before, after)).toEqual([{ source: "unmute", className: "org.elasticsearch.Foo" }]);
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

  test("global to scoped emits excludeTasks", () => {
    const before: MutedEntry[] = [{ className: "org.elasticsearch.Foo", method: "testBar" }];
    const after: MutedEntry[] = [{ className: "org.elasticsearch.Foo", method: "testBar", tasks: [":server:test"] }];
    expect(diffMutedEntries(before, after)).toEqual([
      {
        source: "unmute",
        className: "org.elasticsearch.Foo",
        method: "testBar",
        excludeTasks: [":server:test"],
      },
    ]);
  });

  test("scoped shrink emits includeTasks for the removed tasks only", () => {
    const before: MutedEntry[] = [
      { className: "org.elasticsearch.Foo", method: "testBar", tasks: [":a", ":b", ":c"] },
    ];
    const after: MutedEntry[] = [{ className: "org.elasticsearch.Foo", method: "testBar", tasks: [":b"] }];
    expect(diffMutedEntries(before, after)).toEqual([
      {
        source: "unmute",
        className: "org.elasticsearch.Foo",
        method: "testBar",
        includeTasks: [":a", ":c"],
      },
    ]);
  });

  test("scoped expansion is not an unmute", () => {
    const before: MutedEntry[] = [{ className: "org.elasticsearch.Foo", method: "testBar", tasks: [":a"] }];
    const after: MutedEntry[] = [{ className: "org.elasticsearch.Foo", method: "testBar", tasks: [":a", ":b"] }];
    expect(diffMutedEntries(before, after)).toEqual([]);
  });

  test("scoped to global is not an unmute", () => {
    const before: MutedEntry[] = [{ className: "org.elasticsearch.Foo", method: "testBar", tasks: [":a"] }];
    const after: MutedEntry[] = [{ className: "org.elasticsearch.Foo", method: "testBar" }];
    expect(diffMutedEntries(before, after)).toEqual([]);
  });

  test("merges duplicate scoped entries by unioning their tasks", () => {
    const before: MutedEntry[] = [
      { className: "org.elasticsearch.Foo", method: "testBar", tasks: [":a"] },
      { className: "org.elasticsearch.Foo", method: "testBar", tasks: [":b"] },
    ];
    const after: MutedEntry[] = [{ className: "org.elasticsearch.Foo", method: "testBar", tasks: [":b"] }];
    expect(diffMutedEntries(before, after)).toEqual([
      {
        source: "unmute",
        className: "org.elasticsearch.Foo",
        method: "testBar",
        includeTasks: [":a"],
      },
    ]);
  });

  test("a global duplicate keeps the mute global", () => {
    const before: MutedEntry[] = [
      { className: "org.elasticsearch.Foo", method: "testBar", tasks: [":a"] },
      { className: "org.elasticsearch.Foo", method: "testBar" },
    ];
    const after: MutedEntry[] = [{ className: "org.elasticsearch.Foo", method: "testBar", tasks: [":a"] }];
    expect(diffMutedEntries(before, after)).toEqual([
      {
        source: "unmute",
        className: "org.elasticsearch.Foo",
        method: "testBar",
        excludeTasks: [":a"],
      },
    ]);
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
    expect(findUnmutedRefs(before, after)).toEqual([
      { source: "unmute", className: "org.elasticsearch.index.IndexTests", method: "testFoo" },
    ]);
  });

  test("emits a task-scoped unmute ref when a mute becomes scoped", () => {
    const before = `tests:
- class: org.elasticsearch.index.IndexTests
  method: testFoo
`;
    const after = `tests:
- class: org.elasticsearch.index.IndexTests
  method: testFoo
  tasks:
    - :server:test
`;
    expect(findUnmutedRefs(before, after)).toEqual([
      {
        source: "unmute",
        className: "org.elasticsearch.index.IndexTests",
        method: "testFoo",
        excludeTasks: [":server:test"],
      },
    ]);
  });

  test("emits a whole-class unmute ref with no method", () => {
    const before = `tests:
- class: org.elasticsearch.deleted.GoneTests
`;
    const after = "tests:\n";
    expect(findUnmutedRefs(before, after)).toEqual([{ source: "unmute", className: "org.elasticsearch.deleted.GoneTests" }]);
  });

  test("handles empty before yaml (file did not exist at merge base)", () => {
    const after = `tests:
- class: org.elasticsearch.index.IndexTests
  method: testFoo
`;
    expect(findUnmutedRefs("", after)).toEqual([]);
  });
});
