import { readdir, stat } from "fs/promises";
import { createReadStream } from "fs";
import { Transform } from "stream";
import { StringDecoder } from "string_decoder";
import { join } from "path";
import sax from "sax";

export type FailureKind = "assertion" | "suite-timeout" | "error" | "other";

export interface FailureEntry {
  type: string;
  message: string;
}

export interface TestSummary {
  className: string;
  method: string;
  passes: number;
  failures: number;
  failureKinds: FailureKind[];
  exampleMessages: string[];
}

export interface BatchSummary {
  reportPath: string;
  totalCases: number;
  failed: number;
  suiteTimeoutMarkers: number;
}

export interface FlakinessReport {
  batches: BatchSummary[];
  perTest: TestSummary[];
  totals: {
    iterations: number;
    realFailures: number;
    suiteTimeoutMarkers: number;
    successfulCases: number;
  };
}

const SUITE_TIMEOUT_PATTERNS = [
  "Test abandoned because suite timeout was reached.",
  "Suite timeout exceeded (>= ",
];

const SKIP_DIRS = new Set([
  "node_modules", ".git", ".gradle", "dist", "out",
  ".idea", ".vscode", ".cache",
]);

// Cap how much of a <failure>/<error> text body we keep in memory while
// streaming. Only attribute `message` and the first ~16 KiB of body text are
// retained per failure, which is more than enough to classify and to populate
// the (max 3) example messages — but bounds the worst case when a single
// stack trace runs to multiple megabytes.
const MAX_FAILURE_TEXT_BYTES = 16 * 1024;

export function classifyFailure(f: FailureEntry): FailureKind {
  const msg = f.message ?? "";
  for (const pat of SUITE_TIMEOUT_PATTERNS) {
    if (msg.includes(pat)) return "suite-timeout";
  }
  const type = f.type ?? "";
  if (type.endsWith("AssertionError") || type.includes(".AssertionError")) {
    return "assertion";
  }
  if (type.endsWith("Exception") || type.endsWith("Error")) {
    return "error";
  }
  return "other";
}

async function findXmlReports(root: string): Promise<string[]> {
  const out: string[] = [];
  async function walk(dir: string) {
    let entries;
    try {
      entries = await readdir(dir, { withFileTypes: true });
    } catch {
      return;
    }
    for (const e of entries) {
      const p = join(dir, e.name);
      if (e.isDirectory()) {
        if (SKIP_DIRS.has(e.name)) continue;
        await walk(p);
      } else if (e.isFile() && /^TEST-.*\.xml$/.test(e.name) && p.includes("/build/test-results/")) {
        out.push(p);
      }
    }
  }
  await walk(root);
  return out;
}

interface AggregateState {
  perTestMap: Map<string, TestSummary>;
  realFailures: number;
  suiteTimeoutMarkers: number;
  successfulCases: number;
}

interface PendingFailure {
  type: string;
  attrMessage: string;
  text: string;
}

interface CurrentCase {
  entry: TestSummary;
  // First `<failure>` or `<error>` child of the testcase wins; subsequent ones
  // are ignored. Text/CDATA chunks append to `pending.text` while inside the
  // corresponding element body (gated by `insideFailureBody`).
  pending: PendingFailure | null;
  insideFailureBody: boolean;
  skipped: boolean;
}

function ensureEntry(state: AggregateState, className: string, method: string): TestSummary {
  const key = `${className}|${method}`;
  let entry = state.perTestMap.get(key);
  if (!entry) {
    entry = {
      className,
      method,
      passes: 0,
      failures: 0,
      failureKinds: [],
      exampleMessages: [],
    };
    state.perTestMap.set(key, entry);
  }
  return entry;
}

// `<system-out>` and `<system-err>` can hold hundreds of MiB of stdout/stderr
// from a randomized run, and sax-js buffers the complete text node in memory
// before emitting it. The analyzer never reads system-out/err, so we strip those
// sections at the byte stream level — substituting a synthetic `<system-out/>`
// self-close — before the XML parser sees them. This keeps peak memory
// bounded by the count of testcases, not by the total stdout volume of the
// run.
//
// Per the JUnit XML schemas, `<system-out>` and `<system-err>` may appear as
// children of `<testsuite>`, of `<testcase>`, or nested inside Surefire rerun elements.
// This filter is location-agnostic:
// it scans the byte stream for any opening `<system-out>` / `<system-err>` in
// NORMAL state and strips the body up to the matching close tag, so every
// valid placement is handled. XML text bodies cannot contain a raw `<`
// outside of CDATA, so the only places this scanner can falsely match are
// inside CDATA — which it explicitly skips — and inside XML comments, which
// the Gradle/Ant emitters used by Elasticsearch do not produce.
export class StripSystemStreams extends Transform {
  // Longest sentinel we look ahead for: `</system-out>` / `</system-err>` are
  // 13 chars; `<system-out` / `<system-err` are 11; `<![CDATA[` is 9. 16 gives
  // a small safety margin and bounds the per-chunk carry size.
  private static readonly MAX_LOOKAHEAD = 16;

  // StringDecoder holds back any partial multi-byte UTF-8 sequence at the
  // tail of a chunk, so non-ASCII content (test names, failure messages) is
  // never corrupted by a chunk boundary.
  private readonly decoder = new StringDecoder("utf8");
  private carry = "";
  private state: "NORMAL" | "INSIDE_SYSTEM_OUT" | "INSIDE_SYSTEM_ERR" = "NORMAL";
  private closeTag = "";

  override _transform(chunk: Buffer | string, _enc: BufferEncoding, cb: () => void): void {
    const text = typeof chunk === "string" ? chunk : this.decoder.write(chunk);
    this.process(this.carry + text, false);
    cb();
  }
  override _flush(cb: () => void): void {
    this.process(this.carry + this.decoder.end(), true);
    cb();
  }

  private process(buf: string, isFinal: boolean): void {
    this.carry = "";
    while (buf.length > 0) {
      if (this.state === "NORMAL") {
        const sysOut = buf.indexOf("<system-out");
        const sysErr = buf.indexOf("<system-err");
        const cdata = buf.indexOf("<![CDATA[");
        let next = -1;
        let kind: "system-out" | "system-err" | "cdata" | null = null;
        for (const cand of [
          { i: sysOut, k: "system-out" as const },
          { i: sysErr, k: "system-err" as const },
          { i: cdata, k: "cdata" as const },
        ]) {
          if (cand.i !== -1 && (next === -1 || cand.i < next)) {
            next = cand.i;
            kind = cand.k;
          }
        }
        if (next === -1) {
          if (isFinal) { this.push(buf); return; }
          // Hold back enough bytes to recover from a sentinel split across the
          // chunk boundary.
          const safe = Math.max(0, buf.length - StripSystemStreams.MAX_LOOKAHEAD);
          this.push(buf.slice(0, safe));
          this.carry = buf.slice(safe);
          return;
        }
        this.push(buf.slice(0, next));
        buf = buf.slice(next);

        if (kind === "cdata") {
          const end = buf.indexOf("]]>");
          if (end === -1) {
            if (isFinal) { this.push(buf); return; }
            this.carry = buf; return;
          }
          this.push(buf.slice(0, end + 3));
          buf = buf.slice(end + 3);
          continue;
        }

        const gt = buf.indexOf(">");
        if (gt === -1) {
          if (isFinal) { this.push(buf); return; }
          this.carry = buf; return;
        }
        if (buf[gt - 1] === "/") {
          // Already self-closing — pass through unchanged.
          this.push(buf.slice(0, gt + 1));
          buf = buf.slice(gt + 1);
          continue;
        }
        // Replace `<system-out ...>BODY</system-out>` with `<system-out/>`
        // and switch to body-skipping state.
        this.push(`<${kind}/>`);
        buf = buf.slice(gt + 1);
        this.state = kind === "system-out" ? "INSIDE_SYSTEM_OUT" : "INSIDE_SYSTEM_ERR";
        this.closeTag = `</${kind}>`;
        continue;
      }
      const idx = buf.indexOf(this.closeTag);
      if (idx === -1) {
        // Drop everything we definitely can't be inside a close tag.
        if (isFinal) return;
        const safe = Math.max(0, buf.length - this.closeTag.length);
        this.carry = buf.slice(safe);
        return;
      }
      buf = buf.slice(idx + this.closeTag.length);
      this.state = "NORMAL";
      this.closeTag = "";
    }
  }
}

// Streaming SAX parser for a single JUnit XML report. Reads the file
// chunk by chunk — with system-out/err pre-stripped — so peak memory stays
// bounded regardless of file size. The analyzer used to OOM on CI when
// a single report grew into the hundreds of MiB.
function streamParseReport(file: string, state: AggregateState): Promise<BatchSummary> {
  return new Promise((resolve, reject) => {
    const parser = sax.createStream(true, { trim: false, position: false });
    const fileStream = createReadStream(file);
    const filter = new StripSystemStreams();

    let batchFailed = 0;
    let batchTimeout = 0;
    let batchTotalCases = 0;
    let curCase: CurrentCase | null = null;

    const onError = (err: Error) => {
      // sax keeps emitting after an error unless we tear the stream down,
      // and the underlying file descriptor would otherwise be held until GC. A malformed
      // report fails the analyze step today, so the cleanup is purely for hygiene.
      fileStream.destroy();
      filter.destroy();
      parser.destroy?.();
      reject(err);
    };

    parser.on("opentag", (node) => {
      const tag = node.name;
      const attrs = node.attributes as Record<string, string>;

      if (tag === "testcase") {
        curCase = {
          entry: ensureEntry(state, attrs.classname ?? "", attrs.name ?? ""),
          pending: null,
          insideFailureBody: false,
          skipped: false,
        };
        batchTotalCases += 1;
        return;
      }

      if (!curCase) return;

      if (tag === "skipped") {
        curCase.skipped = true;
        return;
      }

      if (tag === "failure" || tag === "error") {
        if (curCase.pending) return;
        curCase.pending = {
          type: attrs.type ?? "",
          attrMessage: attrs.message ?? "",
          text: "",
        };
        curCase.insideFailureBody = true;
      }
    });

    const appendFailureText = (chunk: string) => {
      const slot = curCase?.pending;
      if (!slot || !curCase?.insideFailureBody) return;
      if (slot.text.length >= MAX_FAILURE_TEXT_BYTES) return;
      const remaining = MAX_FAILURE_TEXT_BYTES - slot.text.length;
      slot.text += chunk.length <= remaining ? chunk : chunk.slice(0, remaining);
    };
    parser.on("text", appendFailureText);
    parser.on("cdata", appendFailureText);

    parser.on("closetag", (name) => {
      if (!curCase) return;

      if (name === "failure" || name === "error") {
        curCase.insideFailureBody = false;
        return;
      }

      if (name === "testcase") {
        const pending = curCase.pending;
        if (pending) {
          // Prefer a non-empty `message` attribute; otherwise fall back to the
          // body text. An explicit empty `message=""` falls through too —
          // empty messages aren't useful to humans reading the report.
          const message = pending.attrMessage || pending.text;
          const kind = classifyFailure({ type: pending.type, message });
          if (kind === "suite-timeout") {
            state.suiteTimeoutMarkers += 1;
            batchTimeout += 1;
          } else {
            state.realFailures += 1;
            curCase.entry.failures += 1;
            curCase.entry.failureKinds.push(kind);
            if (
              curCase.entry.exampleMessages.length < 3 &&
              !curCase.entry.exampleMessages.includes(message)
            ) {
              curCase.entry.exampleMessages.push(message);
            }
            batchFailed += 1;
          }
        } else if (!curCase.skipped) {
          curCase.entry.passes += 1;
          state.successfulCases += 1;
        }
        curCase = null;
      }
    });

    parser.on("error", onError);
    parser.on("end", () => {
      resolve({
        reportPath: file,
        totalCases: batchTotalCases,
        failed: batchFailed,
        suiteTimeoutMarkers: batchTimeout,
      });
    });

    fileStream.on("error", onError);
    filter.on("error", onError);
    fileStream.pipe(filter).pipe(parser);
  });
}

export async function analyzeReports(
  roots: string[],
  minMtimeMs?: number
): Promise<FlakinessReport> {
  const state: AggregateState = {
    perTestMap: new Map(),
    realFailures: 0,
    suiteTimeoutMarkers: 0,
    successfulCases: 0,
  };
  const batches: BatchSummary[] = [];

  for (const root of roots) {
    const files = await findXmlReports(root);
    for (const file of files) {
      if (minMtimeMs !== undefined) {
        const s = await stat(file);
        if (s.mtimeMs < minMtimeMs) continue;
      }
      batches.push(await streamParseReport(file, state));
    }
  }

  return {
    batches,
    perTest: [...state.perTestMap.values()],
    totals: {
      iterations: state.successfulCases + state.realFailures + state.suiteTimeoutMarkers,
      realFailures: state.realFailures,
      suiteTimeoutMarkers: state.suiteTimeoutMarkers,
      successfulCases: state.successfulCases,
    },
  };
}
