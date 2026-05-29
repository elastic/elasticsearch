import { describe, expect, test } from "vitest";
import { renderMarkdown, severity } from "./render.ts";

describe("renderMarkdown / severity", () => {
  test("renders the failures table when there are real failures", () => {
    const md = renderMarkdown({
      batches: [],
      perTest: [{ className: "X", method: "y", passes: 0, failures: 1, failureKinds: ["assertion"], exampleMessages: ["boom"] }],
      totals: { iterations: 1, realFailures: 1, suiteTimeoutMarkers: 0, successfulCases: 0 },
    });
    expect(md).toContain("Failures by test");
    expect(md).toContain("boom");
  });

  test("severity reflects the worst signal", () => {
    expect(severity({ batches: [], perTest: [], totals: { iterations: 1, realFailures: 1, suiteTimeoutMarkers: 0, successfulCases: 0 } })).toBe("error");
    expect(severity({ batches: [], perTest: [], totals: { iterations: 1, realFailures: 0, suiteTimeoutMarkers: 1, successfulCases: 0 } })).toBe("warning");
    expect(severity({ batches: [], perTest: [], totals: { iterations: 1, realFailures: 0, suiteTimeoutMarkers: 0, successfulCases: 1 } })).toBe("success");
  });
});
