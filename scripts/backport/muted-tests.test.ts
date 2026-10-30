import { readFileSync } from "fs";
import { describe, expect, it } from "vitest";
import {
  diffRemovedEntries,
  parseMutedTests,
  removeEntries,
} from "./muted-tests.ts";

// FILE intentionally mixes quoted and unquoted scalars to prove the
// line-splice preserves the original style verbatim.
const FILE = `tests:
- class: org.elasticsearch.Foo
  method: "testA"
  issue: https://github.com/elastic/elasticsearch/issues/1
- class: "org.elasticsearch.Bar"
  method: testB
  issue: https://github.com/elastic/elasticsearch/issues/2
- class: org.elasticsearch.Baz
  method: testC
  issue: https://github.com/elastic/elasticsearch/issues/3
`;

// ─── parseMutedTests ─────────────────────────────────────────────────────────

describe("parseMutedTests", () => {
  it("parses class, method, and issue", () => {
    expect(parseMutedTests(FILE)).toEqual([
      {
        class: "org.elasticsearch.Foo",
        method: "testA",
        issue: "https://github.com/elastic/elasticsearch/issues/1",
      },
      {
        class: "org.elasticsearch.Bar",
        method: "testB",
        issue: "https://github.com/elastic/elasticsearch/issues/2",
      },
      {
        class: "org.elasticsearch.Baz",
        method: "testC",
        issue: "https://github.com/elastic/elasticsearch/issues/3",
      },
    ]);
  });

  it("expands a methods: entry into one entry per method", () => {
    const src = `tests:
- class: org.elasticsearch.CharArraysTests
  methods:
    - testCharsBeginsWith
    - testCharsToBytes
    - testConstantTimeEquals
  issue: https://github.com/elastic/elasticsearch/issues/99
`;
    expect(parseMutedTests(src)).toEqual([
      {
        class: "org.elasticsearch.CharArraysTests",
        method: "testCharsBeginsWith",
        issue: "https://github.com/elastic/elasticsearch/issues/99",
      },
      {
        class: "org.elasticsearch.CharArraysTests",
        method: "testCharsToBytes",
        issue: "https://github.com/elastic/elasticsearch/issues/99",
      },
      {
        class: "org.elasticsearch.CharArraysTests",
        method: "testConstantTimeEquals",
        issue: "https://github.com/elastic/elasticsearch/issues/99",
      },
    ]);
  });

  it("parses entries with no method (whole-class mute)", () => {
    const src = `tests:
- class: org.elasticsearch.Foo
  issue: https://github.com/elastic/elasticsearch/issues/1
`;
    expect(parseMutedTests(src)).toEqual([
      {
        class: "org.elasticsearch.Foo",
        method: undefined,
        issue: "https://github.com/elastic/elasticsearch/issues/1",
      },
    ]);
  });

  it("parses quoted methods containing colons and braces", () => {
    const src = `tests:
- class: org.elasticsearch.MvPercentileTests
  method: "testEvaluate {TestCase=field: <random mv doubles>, percentile: <int>}"
  issue: https://github.com/elastic/elasticsearch/issues/145886
`;
    expect(parseMutedTests(src)).toEqual([
      {
        class: "org.elasticsearch.MvPercentileTests",
        method:
          "testEvaluate {TestCase=field: <random mv doubles>, percentile: <int>}",
        issue: "https://github.com/elastic/elasticsearch/issues/145886",
      },
    ]);
  });

  it("treats an empty tests key as an empty list", () => {
    expect(parseMutedTests("tests:\n")).toEqual([]);
  });

  it("throws on invalid YAML", () => {
    expect(() => parseMutedTests("tests:\n- [unclosed\n")).toThrow();
  });

  it("throws when an entry has no class", () => {
    expect(() => parseMutedTests("tests:\n- method: testA\n")).toThrow(
      /no `class`/,
    );
  });
});

// ─── diffRemovedEntries ───────────────────────────────────────────────────────

describe("diffRemovedEntries", () => {
  it("returns the removed entry", () => {
    const after = `tests:
- class: org.elasticsearch.Foo
  method: testA
  issue: https://github.com/elastic/elasticsearch/issues/1
- class: org.elasticsearch.Baz
  method: testC
  issue: https://github.com/elastic/elasticsearch/issues/3
`;
    expect(diffRemovedEntries(FILE, after)).toEqual([
      {
        class: "org.elasticsearch.Bar",
        method: "testB",
        issue: "https://github.com/elastic/elasticsearch/issues/2",
      },
    ]);
  });

  it("returns multiple removed entries", () => {
    const after = `tests:
- class: org.elasticsearch.Bar
  method: testB
  issue: https://github.com/elastic/elasticsearch/issues/2
`;
    expect(diffRemovedEntries(FILE, after)).toEqual([
      {
        class: "org.elasticsearch.Foo",
        method: "testA",
        issue: "https://github.com/elastic/elasticsearch/issues/1",
      },
      {
        class: "org.elasticsearch.Baz",
        method: "testC",
        issue: "https://github.com/elastic/elasticsearch/issues/3",
      },
    ]);
  });

  it("treats removal of some methods: items as individual removals", () => {
    const before = `tests:
- class: org.elasticsearch.CharArraysTests
  methods:
    - testCharsBeginsWith
    - testCharsToBytes
    - testConstantTimeEquals
  issue: https://github.com/elastic/elasticsearch/issues/99
`;
    const after = `tests:
- class: org.elasticsearch.CharArraysTests
  methods:
    - testCharsToBytes
  issue: https://github.com/elastic/elasticsearch/issues/99
`;
    expect(diffRemovedEntries(before, after)).toEqual([
      {
        class: "org.elasticsearch.CharArraysTests",
        method: "testCharsBeginsWith",
        issue: "https://github.com/elastic/elasticsearch/issues/99",
      },
      {
        class: "org.elasticsearch.CharArraysTests",
        method: "testConstantTimeEquals",
        issue: "https://github.com/elastic/elasticsearch/issues/99",
      },
    ]);
  });

  it("returns null when the commit adds an entry (a muting commit)", () => {
    const after = `${FILE}- class: org.elasticsearch.New
  method: testD
  issue: https://github.com/elastic/elasticsearch/issues/4
`;
    expect(diffRemovedEntries(FILE, after)).toBeNull();
  });

  it("returns null when the commit both adds and removes", () => {
    const after = `tests:
- class: org.elasticsearch.Foo
  method: testA
  issue: https://github.com/elastic/elasticsearch/issues/1
- class: org.elasticsearch.New
  method: testD
  issue: https://github.com/elastic/elasticsearch/issues/4
`;
    expect(diffRemovedEntries(FILE, after)).toBeNull();
  });

  it("returns an empty array when nothing changed", () => {
    expect(diffRemovedEntries(FILE, FILE)).toEqual([]);
  });

  it("ignores a changed issue link, since issue is not part of identity", () => {
    const after = FILE.replace("issues/2", "issues/999");
    expect(diffRemovedEntries(FILE, after)).toEqual([]);
  });

  it("detects removal even when the file is reordered", () => {
    const after = `tests:
- class: org.elasticsearch.Baz
  method: testC
  issue: https://github.com/elastic/elasticsearch/issues/3
- class: org.elasticsearch.Foo
  method: testA
  issue: https://github.com/elastic/elasticsearch/issues/1
`;
    expect(diffRemovedEntries(FILE, after)).toEqual([
      {
        class: "org.elasticsearch.Bar",
        method: "testB",
        issue: "https://github.com/elastic/elasticsearch/issues/2",
      },
    ]);
  });
});

// ─── removeEntries ────────────────────────────────────────────────────────────

describe("removeEntries", () => {
  it("removes a single entry and leaves everything else byte-identical", () => {
    const result = removeEntries(FILE, [
      { class: "org.elasticsearch.Bar", method: "testB" },
    ]);
    expect(result).toBe(`tests:
- class: org.elasticsearch.Foo
  method: "testA"
  issue: https://github.com/elastic/elasticsearch/issues/1
- class: org.elasticsearch.Baz
  method: testC
  issue: https://github.com/elastic/elasticsearch/issues/3
`);
  });

  it("removes multiple entries", () => {
    const result = removeEntries(FILE, [
      { class: "org.elasticsearch.Foo", method: "testA" },
      { class: "org.elasticsearch.Baz", method: "testC" },
    ]);
    expect(result).toBe(`tests:
- class: "org.elasticsearch.Bar"
  method: testB
  issue: https://github.com/elastic/elasticsearch/issues/2
`);
  });

  it("removes all methods from a methods: block, deleting the whole entry", () => {
    const src = `tests:
- class: org.elasticsearch.CharArraysTests
  methods:
    - testCharsBeginsWith
    - testCharsToBytes
  issue: https://github.com/elastic/elasticsearch/issues/99
- class: org.elasticsearch.Keep
  method: testKeep
  issue: https://github.com/elastic/elasticsearch/issues/2
`;
    const result = removeEntries(src, [
      { class: "org.elasticsearch.CharArraysTests", method: "testCharsBeginsWith" },
      { class: "org.elasticsearch.CharArraysTests", method: "testCharsToBytes" },
    ]);
    expect(result).toBe(`tests:
- class: org.elasticsearch.Keep
  method: testKeep
  issue: https://github.com/elastic/elasticsearch/issues/2
`);
  });

  it("removes a subset of methods from a methods: block, keeping the rest", () => {
    const src = `tests:
- class: org.elasticsearch.CharArraysTests
  methods:
    - testCharsBeginsWith
    - testCharsToBytes
    - testConstantTimeEquals
  issue: https://github.com/elastic/elasticsearch/issues/99
`;
    const result = removeEntries(src, [
      { class: "org.elasticsearch.CharArraysTests", method: "testCharsBeginsWith" },
      { class: "org.elasticsearch.CharArraysTests", method: "testConstantTimeEquals" },
    ]);
    expect(result).toBe(`tests:
- class: org.elasticsearch.CharArraysTests
  methods:
    - testCharsToBytes
  issue: https://github.com/elastic/elasticsearch/issues/99
`);
  });

  it("throws when the issue link differs between the cherry-pick and target branch", () => {
    const target = FILE.replace("issues/2", "issues/12345");
    expect(() =>
      removeEntries(target, [
        {
          class: "org.elasticsearch.Bar",
          method: "testB",
          issue: "https://github.com/elastic/elasticsearch/issues/2",
        },
      ]),
    ).toThrow(/issue mismatch/);
  });

  it("throws on issue mismatch for a methods: block", () => {
    const src = `tests:
- class: org.elasticsearch.CharArraysTests
  methods:
    - testCharsBeginsWith
    - testCharsToBytes
  issue: https://github.com/elastic/elasticsearch/issues/99
`;
    expect(() =>
      removeEntries(src, [
        {
          class: "org.elasticsearch.CharArraysTests",
          method: "testCharsBeginsWith",
          issue: "https://github.com/elastic/elasticsearch/issues/WRONG",
        },
      ]),
    ).toThrow(/issue mismatch/);
  });

  it("removes the entry when the issue link matches", () => {
    const result = removeEntries(FILE, [
      {
        class: "org.elasticsearch.Bar",
        method: "testB",
        issue: "https://github.com/elastic/elasticsearch/issues/2",
      },
    ]);
    expect(result).not.toContain("org.elasticsearch.Bar");
    expect(result).toContain("org.elasticsearch.Foo");
  });

  it("is a no-op when the entry is already absent on the target branch", () => {
    const result = removeEntries(FILE, [
      { class: "org.elasticsearch.NotHere", method: "testX" },
    ]);
    expect(result).toBe(FILE);
  });

  it("does not confuse a whole-class mute with a method mute", () => {
    const src = `tests:
- class: org.elasticsearch.Foo
  issue: https://github.com/elastic/elasticsearch/issues/1
- class: org.elasticsearch.Foo
  method: testA
  issue: https://github.com/elastic/elasticsearch/issues/2
`;
    const result = removeEntries(src, [{ class: "org.elasticsearch.Foo" }]);
    expect(result).toBe(`tests:
- class: org.elasticsearch.Foo
  method: testA
  issue: https://github.com/elastic/elasticsearch/issues/2
`);
  });

  it("removes an entry whose method is a quoted multi-token string", () => {
    const src = `tests:
- class: org.elasticsearch.MvPercentileTests
  method: "testEvaluate {TestCase=field: <random mv doubles>, percentile: <int>}"
  issue: https://github.com/elastic/elasticsearch/issues/145886
- class: org.elasticsearch.Keep
  method: testKeep
  issue: https://github.com/elastic/elasticsearch/issues/2
`;
    const result = removeEntries(src, [
      {
        class: "org.elasticsearch.MvPercentileTests",
        method:
          "testEvaluate {TestCase=field: <random mv doubles>, percentile: <int>}",
      },
    ]);
    expect(result).toBe(`tests:
- class: org.elasticsearch.Keep
  method: testKeep
  issue: https://github.com/elastic/elasticsearch/issues/2
`);
  });

  it("preserves surrounding comments and blank lines", () => {
    const src = `# top comment
tests:
- class: org.elasticsearch.Foo
  method: testA
  issue: https://github.com/elastic/elasticsearch/issues/1

- class: org.elasticsearch.Bar
  method: testB
  issue: https://github.com/elastic/elasticsearch/issues/2
`;
    const result = removeEntries(src, [
      { class: "org.elasticsearch.Bar", method: "testB" },
    ]);
    expect(result).toBe(`# top comment
tests:
- class: org.elasticsearch.Foo
  method: testA
  issue: https://github.com/elastic/elasticsearch/issues/1

`);
  });

  it("preserves the trailing newline", () => {
    const result = removeEntries(FILE, [
      { class: "org.elasticsearch.Foo", method: "testA" },
    ]);
    expect(result.endsWith("\n")).toBe(true);
  });

  it("preserves CRLF line endings", () => {
    const crlf = FILE.replace(/\n/g, "\r\n");
    const result = removeEntries(crlf, [
      { class: "org.elasticsearch.Bar", method: "testB" },
    ]);
    expect(result).toBe(
      `tests:
- class: org.elasticsearch.Foo
  method: "testA"
  issue: https://github.com/elastic/elasticsearch/issues/1
- class: org.elasticsearch.Baz
  method: testC
  issue: https://github.com/elastic/elasticsearch/issues/3
`.replace(/\n/g, "\r\n"),
    );
  });

  it("is a no-op when given no entries", () => {
    expect(removeEntries(FILE, [])).toBe(FILE);
  });

  it("throws rather than corrupting a flow-style sequence", () => {
    const src = `tests: [{class: org.elasticsearch.Foo, method: testA}]\n`;
    expect(() =>
      removeEntries(src, [{ class: "org.elasticsearch.Foo", method: "testA" }]),
    ).toThrow(/unexpected entry layout/);
  });
});

// ─── end-to-end ───────────────────────────────────────────────────────────────

describe("end-to-end against the real muted-tests.yml", () => {
  it("removes an entry from the checked-in file, changing only those lines", () => {
    const real = readFileSync(
      new URL("../../muted-tests.yml", import.meta.url),
      "utf8",
    );
    const entries = parseMutedTests(real);
    expect(entries.length).toBeGreaterThan(0);

    const victim = entries[0]!;
    const result = removeEntries(real, [victim]);

    const before = real.split("\n");
    const after = result.split("\n");

    // Every remaining entry is untouched, and the victim is gone.
    expect(parseMutedTests(result)).toEqual(entries.slice(1));

    // `after` must be `before` with whole lines deleted — no line added,
    // reworded, or reordered. Verified by walking it as a subsequence.
    let i = 0;
    for (const line of after) {
      while (i < before.length && before[i] !== line) i++;
      expect(i, `line not found in original: ${line}`).toBeLessThan(
        before.length,
      );
      i++;
    }
    expect(before.length - after.length).toBeGreaterThan(0);
  });
});
