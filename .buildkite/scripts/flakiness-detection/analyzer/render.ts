import type { FailureKind, FlakinessReport } from "./analyze.ts";

// Buildkite rejects annotation bodies larger than ~1 MiB.
const MAX_FAILING_ROWS = 100;
const MAX_EXAMPLE_MESSAGES = 25;
const MAX_MESSAGE_CHARS = 400;
const MAX_ANNOTATION_BYTES = 900_000;
const TRUNCATION_MARKER = "\n\n_Annotation truncated to stay under the Buildkite size limit._";

function truncateMessage(msg: string): string {
  return msg.length <= MAX_MESSAGE_CHARS ? msg : `${msg.slice(0, MAX_MESSAGE_CHARS)}…`;
}

// Collapse the per-occurrence failure-kind list into counts, preserving
// first-seen order, e.g. ["error","error","assertion"] -> "error x2, assertion".
// The " xN" suffix is omitted for singletons.
function summarizeKinds(kinds: FailureKind[]): string {
  const counts = new Map<FailureKind, number>();
  for (const k of kinds) {
    counts.set(k, (counts.get(k) ?? 0) + 1);
  }
  return [...counts.entries()].map(([k, n]) => (n > 1 ? `${k} x${n}` : k)).join(", ");
}

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

  // Most-flaky first, so truncation drops the least-failing tests.
  const failing = report.perTest.filter((t) => t.failures > 0).sort((a, b) => b.failures - a.failures);
  if (failing.length > 0) {
    const shownRows = failing.slice(0, MAX_FAILING_ROWS);

    lines.push("### Failures by test");
    lines.push("| Class | Method | Pass | Fail | Kinds |");
    lines.push("| --- | --- | ---: | ---: | --- |");
    for (const t of shownRows) {
      lines.push(`| ${t.className} | ${t.method} | ${t.passes} | ${t.failures} | ${summarizeKinds(t.failureKinds)} |`);
    }
    if (failing.length > shownRows.length) {
      lines.push("");
      lines.push(
        `_Showing ${shownRows.length} of ${failing.length} failing tests; full data in the flakiness-outcomes.json artifact and job logs._`
      );
    }
    lines.push("");

    lines.push("### Example failure messages");
    let shownMessages = 0;
    let omittedMessages = 0;
    for (const t of shownRows) {
      for (const ex of t.exampleFailures) {
        if (shownMessages >= MAX_EXAMPLE_MESSAGES) {
          omittedMessages += 1;
          continue;
        }
        lines.push(`- \`${t.className}.${ex.name}\`: ${truncateMessage(ex.message)}`);
        shownMessages += 1;
      }
    }
    if (omittedMessages > 0) {
      lines.push("");
      lines.push(`_${omittedMessages} further example message(s) omitted; see job logs._`);
    }
  } else {
    lines.push("_No real failures recorded._");
  }

  const body = lines.join("\n");
  if (body.length <= MAX_ANNOTATION_BYTES) return body;
  return body.slice(0, MAX_ANNOTATION_BYTES - TRUNCATION_MARKER.length) + TRUNCATION_MARKER;
}

export function severity(report: FlakinessReport): "error" | "warning" | "success" {
  if (report.totals.realFailures > 0) return "error";
  if (report.totals.suiteTimeoutMarkers > 0) return "warning";
  return "success";
}
