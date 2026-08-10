import { describe, expect, it } from "vitest";
import { parseRemovedBlocks, removeBlocks } from "./muted-tests.ts";

// Realistic diff header shared by all test diffs.
const DIFF_HEADER = `diff --git a/muted-tests.yml b/muted-tests.yml
index abc1234..def5678 100644
--- a/muted-tests.yml
+++ b/muted-tests.yml`;

function makeDiff(hunk: string): string {
  return `${DIFF_HEADER}\n${hunk}`;
}

describe("parseRemovedBlocks", () => {
  it("returns one block for a single removed entry", () => {
    const diff = makeDiff(`@@ -1,7 +1,4 @@
 tests:
-- class: org.elasticsearch.xpack.esql.CsvIT
-  method: test {csv-spec:k8s-misc.by_four_dims}
-  issue: https://github.com/elastic/elasticsearch/issues/156313
 - class: org.elasticsearch.SomeOtherTest
   method: testFoo
   issue: https://github.com/elastic/elasticsearch/issues/1`);

    expect(parseRemovedBlocks(diff)).toEqual([
      [
        "- class: org.elasticsearch.xpack.esql.CsvIT",
        "  method: test {csv-spec:k8s-misc.by_four_dims}",
        "  issue: https://github.com/elastic/elasticsearch/issues/156313",
      ],
    ]);
  });

  it("returns multiple blocks when several entries are removed", () => {
    const diff = makeDiff(`@@ -1,10 +1,4 @@
 tests:
-- class: org.elasticsearch.Foo
-  method: testA
-  issue: https://github.com/elastic/elasticsearch/issues/1
-- class: org.elasticsearch.Bar
-  method: testB
-  issue: https://github.com/elastic/elasticsearch/issues/2
 - class: org.elasticsearch.Remaining
   method: testC
   issue: https://github.com/elastic/elasticsearch/issues/3`);

    expect(parseRemovedBlocks(diff)).toEqual([
      [
        "- class: org.elasticsearch.Foo",
        "  method: testA",
        "  issue: https://github.com/elastic/elasticsearch/issues/1",
      ],
      [
        "- class: org.elasticsearch.Bar",
        "  method: testB",
        "  issue: https://github.com/elastic/elasticsearch/issues/2",
      ],
    ]);
  });

  it("returns null when the diff adds lines (a muting commit, not unmuting)", () => {
    const diff = makeDiff(`@@ -1,4 +1,7 @@
 tests:
+- class: org.elasticsearch.xpack.esql.CsvIT
+  method: test {csv-spec:k8s-misc.four_rates_by_four_dims}
+  issue: https://github.com/elastic/elasticsearch/issues/156314
 - class: org.elasticsearch.SomeOtherTest
   method: testFoo
   issue: https://github.com/elastic/elasticsearch/issues/1`);

    expect(parseRemovedBlocks(diff)).toBeNull();
  });

  it("returns null when the diff both removes and adds lines", () => {
    const diff = makeDiff(`@@ -1,7 +1,7 @@
 tests:
-- class: org.elasticsearch.Foo
-  method: testA
-  issue: https://github.com/elastic/elasticsearch/issues/1
+- class: org.elasticsearch.Bar
+  method: testB
+  issue: https://github.com/elastic/elasticsearch/issues/2`);

    expect(parseRemovedBlocks(diff)).toBeNull();
  });

  it("returns an empty array when there are no removals in the hunk", () => {
    const diff = makeDiff(`@@ -1,4 +1,4 @@
 tests:
 - class: org.elasticsearch.SomeOtherTest
   method: testFoo
   issue: https://github.com/elastic/elasticsearch/issues/1`);

    expect(parseRemovedBlocks(diff)).toEqual([]);
  });

  it("handles quoted method names containing special characters", () => {
    const diff = makeDiff(`@@ -1,4 +1,1 @@
 tests:
-- class: org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvPercentileTests
-  method: "testEvaluateBlockWithNulls {TestCase=field: <random mv DEDUPLICATED_UNORDERED doubles>, percentile: <positive int>}"
-  issue: https://github.com/elastic/elasticsearch/issues/145886`);

    expect(parseRemovedBlocks(diff)).toEqual([
      [
        "- class: org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvPercentileTests",
        '  method: "testEvaluateBlockWithNulls {TestCase=field: <random mv DEDUPLICATED_UNORDERED doubles>, percentile: <positive int>}"',
        "  issue: https://github.com/elastic/elasticsearch/issues/145886",
      ],
    ]);
  });
});

describe("removeBlocks", () => {
  const FILE = `tests:
- class: org.elasticsearch.Foo
  method: testA
  issue: https://github.com/elastic/elasticsearch/issues/1
- class: org.elasticsearch.Bar
  method: testB
  issue: https://github.com/elastic/elasticsearch/issues/2
- class: org.elasticsearch.Baz
  method: testC
  issue: https://github.com/elastic/elasticsearch/issues/3
`;

  it("removes a single block", () => {
    const block = [
      "- class: org.elasticsearch.Bar",
      "  method: testB",
      "  issue: https://github.com/elastic/elasticsearch/issues/2",
    ];
    const result = removeBlocks(FILE, [block]);
    expect(result).toBe(`tests:
- class: org.elasticsearch.Foo
  method: testA
  issue: https://github.com/elastic/elasticsearch/issues/1
- class: org.elasticsearch.Baz
  method: testC
  issue: https://github.com/elastic/elasticsearch/issues/3
`);
  });

  it("removes multiple blocks", () => {
    const blocks = [
      [
        "- class: org.elasticsearch.Foo",
        "  method: testA",
        "  issue: https://github.com/elastic/elasticsearch/issues/1",
      ],
      [
        "- class: org.elasticsearch.Baz",
        "  method: testC",
        "  issue: https://github.com/elastic/elasticsearch/issues/3",
      ],
    ];
    const result = removeBlocks(FILE, blocks);
    expect(result).toBe(`tests:
- class: org.elasticsearch.Bar
  method: testB
  issue: https://github.com/elastic/elasticsearch/issues/2
`);
  });

  it("leaves the file unchanged when the block is not found", () => {
    const block = [
      "- class: org.elasticsearch.Missing",
      "  method: testX",
      "  issue: https://github.com/elastic/elasticsearch/issues/99",
    ];
    expect(removeBlocks(FILE, [block])).toBe(FILE);
  });

  it("preserves the trailing newline", () => {
    const block = [
      "- class: org.elasticsearch.Foo",
      "  method: testA",
      "  issue: https://github.com/elastic/elasticsearch/issues/1",
    ];
    const result = removeBlocks(FILE, [block]);
    expect(result.endsWith("\n")).toBe(true);
  });

  it("preserves CRLF line endings", () => {
    const crlf = FILE.replace(/\n/g, "\r\n");
    const block = [
      "- class: org.elasticsearch.Bar",
      "  method: testB",
      "  issue: https://github.com/elastic/elasticsearch/issues/2",
    ];
    const result = removeBlocks(crlf, [block]);
    expect(result).not.toContain("\r\n- class: org.elasticsearch.Bar");
    expect(result.split("\r\n").length).toBeGreaterThan(1);
  });
});
