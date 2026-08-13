import { describe, expect, test } from "vitest";
import { mkdtemp, mkdir, writeFile } from "fs/promises";
import { tmpdir } from "os";
import { join, dirname } from "path";

import { analyzeReports, classifyFailure, stripSeedSuffix, StripSystemStreams } from "./analyze.ts";

describe("stripSeedSuffix", () => {
  test("strips a trailing randomized-runner seed suffix", () => {
    expect(stripSeedSuffix("testFoo {seed=[24918B24CBE01DE3:A940852AB5443E5]}")).toBe("testFoo");
  });

  test("leaves plain names and non-seed parameterization suffixes intact", () => {
    expect(stripSeedSuffix("testFoo")).toBe("testFoo");
    expect(stripSeedSuffix("testFoo {p0=x}")).toBe("testFoo {p0=x}");
  });
});

describe("classifyFailure", () => {
  test("classifies AssertionError messages as 'assertion'", () => {
    expect(classifyFailure({ type: "java.lang.AssertionError", message: "expected:<1> but was:<2>" })).toBe("assertion");
  });

  test("classifies suite-timeout markers as 'suite-timeout'", () => {
    expect(classifyFailure({ type: "java.lang.Exception", message: "Test abandoned because suite timeout was reached." })).toBe("suite-timeout");
    expect(classifyFailure({ type: "java.lang.Exception", message: "Suite timeout exceeded (>= 3600000 msec)." })).toBe("suite-timeout");
  });

  test("classifies other Exception entries as 'error'", () => {
    expect(classifyFailure({ type: "java.lang.RuntimeException", message: "kaboom" })).toBe("error");
  });

  test("classifies unmatched shapes as 'other'", () => {
    expect(classifyFailure({ type: "", message: "" })).toBe("other");
  });
});

describe("analyzeReports", () => {
  test("aggregates pass/fail counts per (class, method)", async () => {
    const root = await mkTmpReports([
      {
        path: "server/build/test-results/test/TEST-org.example.FooTests.xml",
        body: `<?xml version="1.0" encoding="UTF-8"?>
<testsuite name="org.example.FooTests" tests="3" failures="1" errors="0">
  <testcase classname="org.example.FooTests" name="testBar"/>
  <testcase classname="org.example.FooTests" name="testBar"/>
  <testcase classname="org.example.FooTests" name="testBar">
    <failure type="java.lang.AssertionError" message="boom"/>
  </testcase>
</testsuite>`,
      },
    ]);
    const report = await analyzeReports([root]);
    expect(report.totals.successfulCases).toBe(2);
    expect(report.totals.realFailures).toBe(1);
    expect(report.perTest).toEqual([
      expect.objectContaining({
        className: "org.example.FooTests",
        method: "testBar",
        passes: 2,
        failures: 1,
        failureKinds: ["assertion"],
      }),
    ]);
  });

  test("collapses seeded iterations of a method into one row, keeping failing seeds", async () => {
    // A randomized `tests.iters` run emits one testcase per seed. All iterations
    // of a method must aggregate into a single row with real pass/fail counts,
    // and the seed-bearing name of a failure must survive for reproduction.
    const root = await mkTmpReports([
      {
        path: "server/build/test-results/test/TEST-org.example.SeedTests.xml",
        body: `<?xml version="1.0" encoding="UTF-8"?>
<testsuite name="org.example.SeedTests" tests="2" failures="1" errors="0">
  <testcase classname="org.example.SeedTests" name="testFlaky {seed=[AAAA:1111]}"/>
  <testcase classname="org.example.SeedTests" name="testFlaky {seed=[AAAA:2222]}">
    <failure type="java.lang.AssertionError" message="boom"/>
  </testcase>
</testsuite>`,
      },
    ]);
    const report = await analyzeReports([root]);
    expect(report.perTest).toHaveLength(1);
    const flaky = report.perTest[0];
    expect(flaky.method).toBe("testFlaky");
    expect(flaky.passes).toBe(1);
    expect(flaky.failures).toBe(1);
    expect(flaky.exampleFailures).toEqual([
      { name: "testFlaky {seed=[AAAA:2222]}", message: "boom" },
    ]);
  });

  test("processes every <testsuite> inside a <testsuites> wrapper", async () => {
    // Some emitters wrap multiple suites in a single file. The analyzer must
    // process all of them, not just the first.
    const root = await mkTmpReports([
      {
        path: "server/build/test-results/test/TEST-org.example.MultiTests.xml",
        body: `<?xml version="1.0" encoding="UTF-8"?>
<testsuites>
  <testsuite name="org.example.AlphaTests" tests="1" failures="0">
    <testcase classname="org.example.AlphaTests" name="testA"/>
  </testsuite>
  <testsuite name="org.example.BetaTests" tests="2" failures="1">
    <testcase classname="org.example.BetaTests" name="testB"/>
    <testcase classname="org.example.BetaTests" name="testB">
      <failure type="java.lang.AssertionError" message="boom"/>
    </testcase>
  </testsuite>
</testsuites>`,
      },
    ]);
    const report = await analyzeReports([root]);
    expect(report.totals.successfulCases).toBe(2); // testA + first testB
    expect(report.totals.realFailures).toBe(1);    // second testB
    const summaries = report.perTest.map((t) => `${t.className}.${t.method}`).sort();
    expect(summaries).toEqual([
      "org.example.AlphaTests.testA",
      "org.example.BetaTests.testB",
    ]);
    // Single BatchSummary aggregates both suites in the one file.
    expect(report.batches).toHaveLength(1);
    expect(report.batches[0].totalCases).toBe(3);
    expect(report.batches[0].failed).toBe(1);
  });

  test("falls back to <failure> body text when no `message` attribute is set", async () => {
    // Many emitters omit the message attribute and inline the stack trace as
    // the element body — both as plain text and inside a CDATA section. The
    // streaming parser has to coalesce text/cdata chunks back into a single
    // message string so classification still works.
    const root = await mkTmpReports([
      {
        path: "server/build/test-results/test/TEST-org.example.BodyTests.xml",
        body: `<?xml version="1.0" encoding="UTF-8"?>
<testsuite name="org.example.BodyTests" tests="2" failures="2" errors="0">
  <testcase classname="org.example.BodyTests" name="testPlain">
    <failure type="java.lang.AssertionError">expected:&lt;1&gt; but was:&lt;2&gt;</failure>
  </testcase>
  <testcase classname="org.example.BodyTests" name="testCdata">
    <failure type="java.lang.RuntimeException"><![CDATA[boom\nat Foo.bar(Foo.java:42)]]></failure>
  </testcase>
</testsuite>`,
      },
    ]);
    const report = await analyzeReports([root]);
    expect(report.totals.realFailures).toBe(2);
    const plain = report.perTest.find((t) => t.method === "testPlain")!;
    const cdata = report.perTest.find((t) => t.method === "testCdata")!;
    expect(plain.failureKinds).toEqual(["assertion"]);
    expect(plain.exampleFailures[0].message).toContain("expected:<1> but was:<2>");
    expect(cdata.failureKinds).toEqual(["error"]);
    expect(cdata.exampleFailures[0].message).toContain("boom");
    expect(cdata.exampleFailures[0].message).toContain("Foo.bar(Foo.java:42)");
  });

  test("keeps memory bounded when a single <failure> body is huge", async () => {
    // The whole point of the streaming refactor is that we no longer load
    // entire report files into memory. Drop a multi-MiB stack trace into a
    // single failure body and assert the analyzer still classifies it
    // correctly without blowing up.
    const longBody = "A".repeat(4 * 1024 * 1024); // 4 MiB
    const root = await mkTmpReports([
      {
        path: "server/build/test-results/test/TEST-org.example.HugeTests.xml",
        body: `<?xml version="1.0" encoding="UTF-8"?>
<testsuite name="org.example.HugeTests" tests="1" failures="1">
  <testcase classname="org.example.HugeTests" name="testHuge">
    <failure type="java.lang.AssertionError"><![CDATA[${longBody}]]></failure>
  </testcase>
</testsuite>`,
      },
    ]);
    const report = await analyzeReports([root]);
    expect(report.totals.realFailures).toBe(1);
    const huge = report.perTest.find((t) => t.method === "testHuge")!;
    expect(huge.failureKinds).toEqual(["assertion"]);
    // The cached example message must be truncated, not the original 4 MiB.
    // The cap inside analyze.ts is 16 KiB per failure body.
    expect(huge.exampleFailures[0].message.length).toBeLessThanOrEqual(16 * 1024);
  });

  test("uses body text when `message` attribute is empty or absent", async () => {
    // A non-empty `message` attribute wins over the body; an empty
    // `message=""` falls through to the body just as a missing attribute
    // would. Empty messages aren't useful to humans reading the report.
    const root = await mkTmpReports([
      {
        path: "server/build/test-results/test/TEST-org.example.AttrTests.xml",
        body: `<?xml version="1.0" encoding="UTF-8"?>
<testsuite name="org.example.AttrTests" tests="2" failures="2">
  <testcase classname="org.example.AttrTests" name="testWins">
    <failure type="java.lang.AssertionError" message="attr wins">body text loses</failure>
  </testcase>
  <testcase classname="org.example.AttrTests" name="testEmptyAttr">
    <failure type="java.lang.AssertionError" message="">body text must be used</failure>
  </testcase>
</testsuite>`,
      },
    ]);
    const report = await analyzeReports([root]);
    const wins = report.perTest.find((t) => t.method === "testWins")!;
    const empty = report.perTest.find((t) => t.method === "testEmptyAttr")!;
    expect(wins.exampleFailures.map((e) => e.message)).toEqual(["attr wins"]);
    expect(empty.exampleFailures.map((e) => e.message)).toEqual(["body text must be used"]);
  });

  test("self-closing <failure message='..'/> still classifies and records", async () => {
    const root = await mkTmpReports([
      {
        path: "server/build/test-results/test/TEST-org.example.SelfTests.xml",
        body: `<?xml version="1.0" encoding="UTF-8"?>
<testsuite name="org.example.SelfTests" tests="1" failures="1">
  <testcase classname="org.example.SelfTests" name="testSelf">
    <failure type="java.lang.AssertionError" message="boom"/>
  </testcase>
</testsuite>`,
      },
    ]);
    const report = await analyzeReports([root]);
    expect(report.totals.realFailures).toBe(1);
    expect(report.perTest[0].exampleFailures.map((e) => e.message)).toEqual(["boom"]);
  });

  test("strips huge <system-out>/<system-err> sections before parsing", async () => {
    // Mirrors the production OOM shape: thousands of self-closing testcases
    // followed by a multi-MiB <system-out> sibling. The byte-stream filter
    // must replace the sibling with <system-out/> so the XML parser never
    // sees the body. We also include CDATA inside <system-out> to confirm
    // the close-tag scanner isn't fooled by content boundaries.
    // Same method name across iterations — mirrors a randomized `tests.iters`
    // run where one method is exercised many times. The analyzer aggregates
    // those into a single TestSummary with passes = N.
    const cases = Array.from({ length: 200 }, () =>
      `  <testcase classname="org.example.HugeRunTests" name="testIter" time="0.01"/>`
    ).join("\n");
    const stdout = "X".repeat(2 * 1024 * 1024); // 2 MiB of stdout
    const stderr = "Y".repeat(1 * 1024 * 1024); // 1 MiB of stderr
    const root = await mkTmpReports([
      {
        path: "server/build/test-results/test/TEST-org.example.HugeRunTests.xml",
        body: `<?xml version="1.0" encoding="UTF-8"?>
<testsuite name="org.example.HugeRunTests" tests="200" failures="0" errors="0">
${cases}
  <system-out><![CDATA[${stdout}]]></system-out>
  <system-err>${stderr}</system-err>
</testsuite>`,
      },
    ]);
    const report = await analyzeReports([root]);
    expect(report.totals.successfulCases).toBe(200);
    expect(report.totals.realFailures).toBe(0);
    expect(report.perTest).toHaveLength(1);
    expect(report.perTest[0].passes).toBe(200);
  });

  test("strips <system-out> nested inside <testcase> (per JUnit schema)", async () => {
    // The Surefire JUnit schemas allow <system-out>/<system-err> as
    // children of <testcase> too, not just <testsuite>. The filter must strip
    // both placements without confusing the testcase boundary or losing the
    // failure attached to the same case.
    const root = await mkTmpReports([
      {
        path: "server/build/test-results/test/TEST-org.example.NestedTests.xml",
        body: `<?xml version="1.0" encoding="UTF-8"?>
<testsuite name="org.example.NestedTests" tests="2" failures="1">
  <testcase classname="org.example.NestedTests" name="testWithStdout">
    <system-out>captured stdout for this test</system-out>
    <system-err>captured stderr for this test</system-err>
  </testcase>
  <testcase classname="org.example.NestedTests" name="testWithFailureAndStdout">
    <failure type="java.lang.AssertionError" message="boom"/>
    <system-out>stdout that follows the failure</system-out>
  </testcase>
</testsuite>`,
      },
    ]);
    const report = await analyzeReports([root]);
    expect(report.totals.successfulCases).toBe(1);
    expect(report.totals.realFailures).toBe(1);
    const failing = report.perTest.find((t) => t.method === "testWithFailureAndStdout")!;
    expect(failing.exampleFailures.map((e) => e.message)).toEqual(["boom"]);
  });

  test("does NOT strip a literal '<system-out' embedded inside a CDATA failure body", async () => {
    // The filter scans bytes for system-stream tags, so it must skip past
    // CDATA sections to avoid corrupting failure bodies that happen to
    // contain the literal sequence '<system-out' (e.g. a stack trace that
    // mentions the tag by name). Such body content is harmless to keep,
    // because nothing in classification looks at it beyond the first 16 KiB.
    const root = await mkTmpReports([
      {
        path: "server/build/test-results/test/TEST-org.example.CdataTests.xml",
        body: `<?xml version="1.0" encoding="UTF-8"?>
<testsuite name="org.example.CdataTests" tests="1" failures="1">
  <testcase classname="org.example.CdataTests" name="testCdataLeak">
    <failure type="java.lang.AssertionError"><![CDATA[oops <system-out> leaked into the message]]></failure>
  </testcase>
</testsuite>`,
      },
    ]);
    const report = await analyzeReports([root]);
    expect(report.totals.realFailures).toBe(1);
    expect(report.perTest[0].exampleFailures[0].message).toContain("<system-out>");
    expect(report.perTest[0].exampleFailures[0].message).toContain("leaked into the message");
  });

  test("rejects on malformed XML rather than swallowing it silently", async () => {
    // Unbalanced tag — sax in strict mode must surface the parse error so the
    // analyze step fails loudly instead of producing a misleading green report.
    const root = await mkTmpReports([
      {
        path: "server/build/test-results/test/TEST-org.example.BrokenTests.xml",
        body: `<?xml version="1.0" encoding="UTF-8"?>
<testsuite name="org.example.BrokenTests" tests="1">
  <testcase classname="org.example.BrokenTests" name="testBroken"
</testsuite>`,
      },
    ]);
    await expect(analyzeReports([root])).rejects.toThrow();
  });

  test("treats suite-timeout markers as informational, not real failures", async () => {
    const root = await mkTmpReports([
      {
        path: "server/build/test-results/test/TEST-org.example.BarTests.xml",
        body: `<?xml version="1.0" encoding="UTF-8"?>
<testsuite name="org.example.BarTests" tests="3" failures="1" errors="1">
  <testcase classname="org.example.BarTests" name="testBaz"/>
  <testcase classname="org.example.BarTests" name="testBaz">
    <failure type="java.lang.Exception" message="Test abandoned because suite timeout was reached."/>
  </testcase>
  <testcase classname="org.example.BarTests" name="@@suite@@">
    <error type="java.lang.Exception" message="Suite timeout exceeded (>= 3600000 msec)."/>
  </testcase>
</testsuite>`,
      },
    ]);
    const report = await analyzeReports([root]);
    expect(report.totals.realFailures).toBe(0);
    expect(report.totals.suiteTimeoutMarkers).toBe(2);
    expect(report.totals.successfulCases).toBe(1);
  });
});

describe("StripSystemStreams", () => {
  test("reassembles a system-out tag split across many tiny chunks", async () => {
    // The integration tests feed whole files via writeFile and never exercise
    // the cross-chunk carry logic. Here we explicitly feed the filter one byte
    // at a time to make sure sentinels straddling chunk boundaries are still
    // detected, and that the body is stripped down to a self-closing tag.
    const input =
      `<root>` +
      `<testcase name="a"/>` +
      `<system-out>hello world</system-out>` +
      `<testcase name="b"/>` +
      `</root>`;
    const filter = new StripSystemStreams();
    const out: string[] = [];
    filter.on("data", (b: Buffer | string) => out.push(b.toString()));
    for (const ch of input) filter.write(ch);
    filter.end();
    await new Promise<void>((resolve) => filter.on("end", () => resolve()));
    const result = out.join("");
    expect(result).toContain(`<system-out/>`);
    expect(result).not.toContain(`hello world`);
    expect(result).toContain(`<testcase name="a"/>`);
    expect(result).toContain(`<testcase name="b"/>`);
  });

  test("preserves multi-byte UTF-8 characters split across chunk boundaries", async () => {
    // Non-ASCII characters in classname / test name must survive the byte-stream
    // filter even when their UTF-8 bytes land on either side of a chunk break.
    // "é" is 0xC3 0xA9 in UTF-8; we split between the two bytes.
    const utf8 = Buffer.from("<testcase name=\"caf\u00e9\"/>", "utf8");
    const splitAt = utf8.indexOf(0xa9);
    const filter = new StripSystemStreams();
    const out: Buffer[] = [];
    filter.on("data", (b: Buffer) => out.push(Buffer.isBuffer(b) ? b : Buffer.from(b)));
    filter.write(utf8.subarray(0, splitAt));
    filter.write(utf8.subarray(splitAt));
    filter.end();
    await new Promise<void>((resolve) => filter.on("end", () => resolve()));
    const result = Buffer.concat(out).toString("utf8");
    expect(result).toBe(`<testcase name="caf\u00e9"/>`);
  });

  test("strips multiple system-out and system-err sections in one stream", async () => {
    const input =
      `<root>` +
      `<system-out>FIRST</system-out>` +
      `<keep/>` +
      `<system-err>SECOND</system-err>` +
      `<system-out>THIRD</system-out>` +
      `</root>`;
    const filter = new StripSystemStreams();
    const out: string[] = [];
    filter.on("data", (b: Buffer | string) => out.push(b.toString()));
    filter.write(input);
    filter.end();
    await new Promise<void>((resolve) => filter.on("end", () => resolve()));
    const result = out.join("");
    expect(result).toBe(`<root><system-out/><keep/><system-err/><system-out/></root>`);
  });

  test("passes already self-closing <system-out/> through unchanged", async () => {
    const input = `<root><system-out/><system-err /></root>`;
    const filter = new StripSystemStreams();
    const out: string[] = [];
    filter.on("data", (b: Buffer | string) => out.push(b.toString()));
    filter.write(input);
    filter.end();
    await new Promise<void>((resolve) => filter.on("end", () => resolve()));
    expect(out.join("")).toBe(input);
  });
});

// Helper — keep alongside the analyzer describes.
async function mkTmpReports(files: { path: string; body: string }[]): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), "analyze-"));
  for (const f of files) {
    const full = join(root, f.path);
    await mkdir(dirname(full), { recursive: true });
    await writeFile(full, f.body, "utf8");
  }
  return root;
}
