import { readdir, readFile } from "fs/promises";
import { join } from "path";
import { XMLParser } from "fast-xml-parser";

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
      if (e.isDirectory()) await walk(p);
      else if (e.isFile() && /^TEST-.*\.xml$/.test(e.name) && p.includes("/build/test-results/")) {
        out.push(p);
      }
    }
  }
  await walk(root);
  return out;
}

export async function analyzeReports(roots: string[]): Promise<FlakinessReport> {
  const parser = new XMLParser({ ignoreAttributes: false, attributeNamePrefix: "" });
  const perTestMap = new Map<string, TestSummary>();
  const batches: BatchSummary[] = [];
  let realFailures = 0;
  let suiteTimeoutMarkers = 0;
  let successfulCases = 0;

  for (const root of roots) {
    const files = await findXmlReports(root);
    for (const file of files) {
      const xml = parser.parse(await readFile(file, "utf8"));
      const suite = xml.testsuite ?? xml.testsuites?.testsuite;
      if (!suite) continue;
      const rawCases = suite.testcase;
      const cases: any[] = Array.isArray(rawCases) ? rawCases : rawCases ? [rawCases] : [];

      let batchFailed = 0;
      let batchTimeout = 0;
      for (const c of cases) {
        const key = `${c.classname}|${c.name}`;
        let entry = perTestMap.get(key);
        if (!entry) {
          entry = {
            className: c.classname,
            method: c.name,
            passes: 0,
            failures: 0,
            failureKinds: [],
            exampleMessages: [],
          };
          perTestMap.set(key, entry);
        }
        const fail = c.failure ?? c.error;
        if (fail) {
          // fail-parser may return string body OR { type, message, "#text" }
          const f: FailureEntry = typeof fail === "string"
            ? { type: "", message: fail }
            : { type: fail.type ?? "", message: fail.message ?? fail["#text"] ?? "" };
          const kind = classifyFailure(f);
          if (kind === "suite-timeout") {
            suiteTimeoutMarkers += 1;
            batchTimeout += 1;
          } else {
            realFailures += 1;
            entry.failures += 1;
            entry.failureKinds.push(kind);
            if (entry.exampleMessages.length < 3 && !entry.exampleMessages.includes(f.message)) {
              entry.exampleMessages.push(f.message);
            }
            batchFailed += 1;
          }
        } else if (c.skipped === undefined) {
          entry.passes += 1;
          successfulCases += 1;
        }
      }

      batches.push({ reportPath: file, totalCases: cases.length, failed: batchFailed, suiteTimeoutMarkers: batchTimeout });
    }
  }

  return {
    batches,
    perTest: [...perTestMap.values()],
    totals: {
      iterations: successfulCases + realFailures + suiteTimeoutMarkers,
      realFailures,
      suiteTimeoutMarkers,
      successfulCases,
    },
  };
}
