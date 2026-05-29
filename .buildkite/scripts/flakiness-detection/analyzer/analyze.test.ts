import { describe, expect, test } from "vitest";
import { mkdtemp, mkdir, writeFile } from "fs/promises";
import { tmpdir } from "os";
import { join, dirname } from "path";

import { analyzeReports, classifyFailure } from "./analyze.ts";

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
