import { describe, expect, test } from "vitest";
import type { TestSummary } from "./analyze.ts";
import { renderMarkdown, severity } from "./render.ts";

describe("renderMarkdown / severity", () => {
  test("renders the failures table when there are real failures", () => {
    const md = renderMarkdown({
      batches: [],
      perTest: [{
        className: "X",
        method: "y",
        passes: 0,
        failures: 1,
        failureKinds: ["assertion"],
        exampleFailures: [{ name: "y {seed=[A:B]}", message: "boom" }],
      }],
      totals: { iterations: 1, realFailures: 1, suiteTimeoutMarkers: 0, successfulCases: 0 },
    });
    expect(md).toContain("Failures by test");
    expect(md).toContain("boom");
    // The seed-bearing name is preserved in the example line for reproduction.
    expect(md).toContain("`X.y {seed=[A:B]}`: boom");
  });

  test("summarizes the Kinds column as counts, omitting x1 for singletons", () => {
    const md = renderMarkdown({
      batches: [],
      perTest: [{
        className: "X",
        method: "y",
        passes: 0,
        failures: 6,
        failureKinds: ["error", "error", "error", "assertion", "assertion", "other"],
        exampleFailures: [{ name: "y", message: "boom" }],
      }],
      totals: { iterations: 6, realFailures: 6, suiteTimeoutMarkers: 0, successfulCases: 0 },
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
      perTest,
      totals: { iterations: 500, realFailures: 500, suiteTimeoutMarkers: 0, successfulCases: 0 },
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
    expect(severity({ batches: [], perTest: [], totals: { iterations: 1, realFailures: 1, suiteTimeoutMarkers: 0, successfulCases: 0 } })).toBe("error");
    expect(severity({ batches: [], perTest: [], totals: { iterations: 1, realFailures: 0, suiteTimeoutMarkers: 1, successfulCases: 0 } })).toBe("warning");
    expect(severity({ batches: [], perTest: [], totals: { iterations: 1, realFailures: 0, suiteTimeoutMarkers: 0, successfulCases: 1 } })).toBe("success");
  });
});
