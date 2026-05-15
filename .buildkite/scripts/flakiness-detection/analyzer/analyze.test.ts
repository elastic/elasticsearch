import { describe, expect, test } from "bun:test";
import { mkdtemp, mkdir, writeFile } from "fs/promises";
import { tmpdir } from "os";
import { join, dirname } from "path";

import { analyzeReports, classifyFailure } from "./analyze";

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
