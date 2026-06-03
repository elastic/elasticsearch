import type { FlakinessReport } from "./analyze.ts";

export function renderMarkdown(report: FlakinessReport): string {
  const { totals } = report;
  const lines: string[] = [];
  lines.push("## Flakiness summary");
  lines.push("");
  lines.push(`- Iterations attempted: ${totals.iterations}`);
  lines.push(`- Successful cases: ${totals.successfulCases}`);
  lines.push(`- Real failures: ${totals.realFailures}`);
  lines.push(`- Suite-timeout markers (informational): ${totals.suiteTimeoutMarkers}`);
  lines.push("");

  const failing = report.perTest.filter((t) => t.failures > 0);
  if (failing.length > 0) {
    lines.push("### Failures by test");
    lines.push("| Class | Method | Pass | Fail | Kinds |");
    lines.push("| --- | --- | ---: | ---: | --- |");
    for (const t of failing) {
      lines.push(`| ${t.className} | ${t.method} | ${t.passes} | ${t.failures} | ${t.failureKinds.join(", ")} |`);
    }
    lines.push("");

    lines.push("### Example failure messages");
    for (const t of failing) {
      for (const msg of t.exampleMessages) {
        lines.push(`- \`${t.className}.${t.method}\`: ${msg}`);
      }
    }
  } else {
    lines.push("_No real failures recorded._");
  }
  return lines.join("\n");
}

export function severity(report: FlakinessReport): "error" | "warning" | "success" {
  if (report.totals.realFailures > 0) return "error";
  if (report.totals.suiteTimeoutMarkers > 0) return "warning";
  return "success";
}
