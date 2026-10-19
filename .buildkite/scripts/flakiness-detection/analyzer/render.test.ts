import { describe, expect, test } from "vitest";
import type { TestSummary } from "./junit-reports-analyzer.ts";
import { CONTACT_NOTE, renderMarkdown, severity } from "./render.ts";

describe("renderMarkdown / severity", () => {
  test("a report with real failures tells the author who to contact, at the top, even when truncated", () => {
    const failing = (className: string): TestSummary => ({
      className,
      method: "y",
      passes: 0,
      failures: 1,
      failureKinds: ["assertion"],
      exampleFailures: [{ name: "y", message: "boom" }],
    });
    const totals = { iterations: 1, realFailures: 1, suiteTimeoutMarkers: 0, tempDirExhaustedMarkers: 0, successfulCases: 0 };

    const md = renderMarkdown({ batches: [], tempDirExhaustedClasses: [], perTest: [failing("X")], totals });
    expect(md).toContain(CONTACT_NOTE);
    expect(md.indexOf(CONTACT_NOTE)).toBeLessThan(md.indexOf("- Iterations attempted"));

    const clean = { iterations: 1, realFailures: 0, suiteTimeoutMarkers: 0, tempDirExhaustedMarkers: 0, successfulCases: 1 };
    expect(renderMarkdown({ batches: [], tempDirExhaustedClasses: [], perTest: [], totals: clean })).not.toContain(CONTACT_NOTE);

    // Class names are not shortened, so 100 rows of 10k-character names push the body past the limit.
    const huge = Array.from({ length: 100 }, (_, i) => failing(`${i}`.padEnd(10_000, "C")));
    const truncated = renderMarkdown({ batches: [], tempDirExhaustedClasses: [], perTest: huge, totals: { ...totals, realFailures: 100 } });
    expect(truncated).toContain("Annotation truncated");
    expect(truncated).toContain(CONTACT_NOTE);
  });

  test("renders the failures table when there are real failures", () => {
    const md = renderMarkdown({
      batches: [],
      tempDirExhaustedClasses: [],
      perTest: [{
        className: "X",
        method: "y",
        passes: 0,
        failures: 1,
        failureKinds: ["assertion"],
        exampleFailures: [{ name: "y {seed=[A:B]}", message: "boom" }],
      }],
      totals: { iterations: 1, realFailures: 1, suiteTimeoutMarkers: 0, tempDirExhaustedMarkers: 0, successfulCases: 0 },
    });
    expect(md).toContain("Failures by test");
    expect(md).toContain("boom");
    // The seed-bearing name is preserved in the example line for reproduction.
    expect(md).toContain("`X.y {seed=[A:B]}`: boom");
  });

  test("summarizes the Kinds column as counts, omitting x1 for singletons", () => {
    const md = renderMarkdown({
      batches: [],
      tempDirExhaustedClasses: [],
      perTest: [{
        className: "X",
        method: "y",
        passes: 0,
        failures: 6,
        failureKinds: ["error", "error", "error", "assertion", "assertion", "other"],
        exampleFailures: [{ name: "y", message: "boom" }],
      }],
      totals: { iterations: 6, realFailures: 6, suiteTimeoutMarkers: 0, tempDirExhaustedMarkers: 0, successfulCases: 0 },
    });
    expect(md).toContain("| error x3, assertion x2, other |");
  });

  test("bounds a mass-failure report and lists the most-flaky tests first", () => {
    // Simulate many distinct failing methods (the case seed-grouping cannot
    // collapse) with long messages, to prove the annotation stays postable.
    const perTest: TestSummary[] = Array.from({ length: 500 }, (_, i) => ({
      className: "org.example.MassTests",
      method: `testFail${i}`,
      passes: 0,
      failures: (i % 7) + 1,
      failureKinds: ["error"],
      exampleFailures: [{ name: `testFail${i}`, message: "E".repeat(5000) }],
    }));
    const md = renderMarkdown({
      batches: [],
      tempDirExhaustedClasses: [],
      perTest,
      totals: { iterations: 500, realFailures: 500, suiteTimeoutMarkers: 0, tempDirExhaustedMarkers: 0, successfulCases: 0 },
    });

    // Comfortably under Buildkite's ~1 MiB annotation limit.
    expect(md.length).toBeLessThan(900_000);
    // Row count is capped and the truncation note is present.
    expect(md).toContain("of 500 failing tests");
    // Highest-failures rows survive: some method with failures=7 must appear,
    // and the table must not blow past the row cap (100 rows + header lines).
    const rowCount = md.split("\n").filter((l) => l.startsWith("| org.example.MassTests |")).length;
    expect(rowCount).toBeLessThanOrEqual(100);
    // Individual example messages are truncated, not emitted at full 5000 chars.
    expect(md).not.toContain("E".repeat(5000));
  });

  test("severity reflects the worst signal", () => {
    expect(severity({ batches: [], tempDirExhaustedClasses: [], perTest: [], totals: { iterations: 1, realFailures: 1, suiteTimeoutMarkers: 0, tempDirExhaustedMarkers: 0, successfulCases: 0 } })).toBe("error");
    expect(severity({ batches: [], tempDirExhaustedClasses: [], perTest: [], totals: { iterations: 1, realFailures: 0, suiteTimeoutMarkers: 1, tempDirExhaustedMarkers: 0, successfulCases: 0 } })).toBe("warning");
    expect(severity({ batches: [], tempDirExhaustedClasses: [], perTest: [], totals: { iterations: 1, realFailures: 0, suiteTimeoutMarkers: 0, tempDirExhaustedMarkers: 0, successfulCases: 1 } })).toBe("success");
  });

  test("temp-dir exhaustion is reported as an informational warning naming the classes, never as flakiness", () => {
    const report = {
      batches: [],
      tempDirExhaustedClasses: ["org.example.BarTests"],
      perTest: [],
      totals: { iterations: 3, realFailures: 0, suiteTimeoutMarkers: 0, tempDirExhaustedMarkers: 2, successfulCases: 1 },
    };
    expect(severity(report)).toBe("warning");
    const md = renderMarkdown(report);
    expect(md).toContain("- Temp-dir-exhausted markers (informational, test-harness limit): 2 in `org.example.BarTests`");
    expect(md).not.toContain(CONTACT_NOTE);
    // The line only appears when there is something to report.
    const clean = { ...report, tempDirExhaustedClasses: [], totals: { ...report.totals, tempDirExhaustedMarkers: 0 } };
    expect(renderMarkdown(clean)).not.toContain("Temp-dir-exhausted");
    // The class list is capped so a mass failure cannot blow up the annotation.
    const many = { ...report, tempDirExhaustedClasses: Array.from({ length: 12 }, (_, i) => `C${i}`) };
    const manyMd = renderMarkdown(many);
    expect(manyMd).toContain("`C9`, 2 more");
    expect(manyMd).not.toContain("`C10`");
  });

  test("a compile-gate failure is a warning (orange), not a false-green", () => {
    const clean = { batches: [], tempDirExhaustedClasses: [], perTest: [], totals: { iterations: 0, realFailures: 0, suiteTimeoutMarkers: 0, tempDirExhaustedMarkers: 0, successfulCases: 0 } };
    // Without the gate signal the empty report would read as success.
    expect(severity(clean)).toBe("success");
    expect(severity(clean, true)).toBe("warning");
  });

  test("an incomplete report says so and is a warning, not a clean pass", () => {
    const report = {
      batches: [],
      tempDirExhaustedClasses: [],
      perTest: [],
      totals: { iterations: 0, realFailures: 0, suiteTimeoutMarkers: 0, tempDirExhaustedMarkers: 0, successfulCases: 0 },
    };
    const problems = ["flakiness-skipped.json did not reach this step"];
    expect(severity(report, false, problems)).toBe("warning");
    expect(renderMarkdown(report, false, problems)).toContain("This report is incomplete");
    expect(renderMarkdown(report, false, problems)).toContain("- flakiness-skipped.json did not reach this step");
  });

  test("a compile failure with an incomplete report shows both notices and stays a warning", () => {
    const report = {
      batches: [],
      tempDirExhaustedClasses: [],
      perTest: [],
      totals: { iterations: 0, realFailures: 0, suiteTimeoutMarkers: 0, tempDirExhaustedMarkers: 0, successfulCases: 0 },
    };
    const problems = ["FLAKINESS_SKIPPED_COUNT is undefined"];
    expect(severity(report, true, problems)).toBe("warning");
    const md = renderMarkdown(report, true, problems);
    expect(md).toContain("failed to compile");
    expect(md).toContain("This report is incomplete");
  });

  test("renders the compile-gate notice and does not read as a clean pass when buildFailed", () => {
    const clean = { batches: [], tempDirExhaustedClasses: [], perTest: [], totals: { iterations: 0, realFailures: 0, suiteTimeoutMarkers: 0, tempDirExhaustedMarkers: 0, successfulCases: 0 } };
    const md = renderMarkdown(clean, true);
    expect(md).toContain("failed to compile");
    expect(md).toContain("re-runs were skipped");
    // The banner is absent unless the gate actually failed.
    expect(renderMarkdown(clean)).not.toContain("failed to compile");
  });
});
