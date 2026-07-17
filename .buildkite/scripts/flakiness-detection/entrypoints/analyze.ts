import { execSync } from "child_process";
import { mkdirSync } from "fs";
import { readdir, readFile, writeFile } from "fs/promises";
import { join, resolve } from "path";

import { analyzeReports } from "../analyzer/analyze.ts";
import { deriveOutcome } from "../analyzer/outcome.ts";
import { renderMarkdown, severity } from "../analyzer/render.ts";
import { DEFAULT_AGENT_CONFIG } from "../domain.ts";
import { NEVER_FAIL_GRACE_MINUTES } from "../runners/buildkite.ts";

const PROJECT_ROOT = resolve(`${import.meta.dirname}/../../../..`);

// The inner `timeout` SIGTERM deadline (seconds) for a batch step, derived from
// the same config the wrapper uses. An rc-137 at/after this is our kill-after
// (a real timeout); earlier is an OOM-kill. Passed into deriveOutcome so the
// classification tracks the configured timeout.
const INNER_TIMEOUT_SEC = (DEFAULT_AGENT_CONFIG.timeoutInMinutes - NEVER_FAIL_GRACE_MINUTES) * 60;

// Per-job status files (rc + duration) the batch wrappers uploaded; downloaded
// flat into this dir by the analyze step command before this script runs.
const STATUS_DIR = join(PROJECT_ROOT, "flakiness-status");

// Per-job JUnit XML is downloaded under here, one subdir per job id, so the
// analyzer can attribute results to a single job (a flat download would mix
// every parallel job together).
const JOBS_DIR = join(PROJECT_ROOT, "flakiness-jobs");

const TEST_RESULTS_GLOB = "**/build/test-results/**/TEST-*.xml";

// Bounds the payload size; failingClasses is human drill-down only.
const MAX_FAILING_CLASSES = 50;

// File the structured per-job outcomes are written to. It is uploaded as a build
// artifact (via the analyze step's `artifact_paths`) and read back by the
// external observability pipeline.
// Keep this filename in sync with FLAKINESS_OUTCOMES_ARTIFACT in runners/buildkite.ts.
const OUTCOMES_ARTIFACT_FILE = "flakiness-outcomes.json";

// Self-reported by each batch job's never-fail wrapper (runners/buildkite.ts).
interface JobStatus {
  jobId: string;
  stepKey: string;
  kind: string;
  rc: number;
  durationSec: number;
}

// One element of the `data-flakiness` annotation array.
interface FlakinessPayload extends JobStatus {
  realFailures: number;
  suiteTimeouts: number;
  totalCases: number;
  outcome: string;
  timedOut: boolean;
  infraSubtype?: string;
  failingClasses: string[];
}

async function readJobStatuses(): Promise<JobStatus[]> {
  let entries;
  try {
    entries = await readdir(STATUS_DIR, { withFileTypes: true });
  } catch {
    return [];
  }
  const statuses: JobStatus[] = [];
  for (const e of entries) {
    if (!e.isFile() || !e.name.endsWith(".json")) continue;
    try {
      const parsed = JSON.parse(await readFile(join(STATUS_DIR, e.name), "utf8"));
      if (parsed && typeof parsed.jobId === "string") {
        statuses.push({
          jobId: parsed.jobId,
          stepKey: String(parsed.stepKey ?? ""),
          kind: String(parsed.kind ?? ""),
          rc: Number(parsed.rc ?? 0),
          durationSec: Number(parsed.durationSec ?? 0),
        });
      }
    } catch (err) {
      console.error(`Failed to read status file ${e.name}:`, err);
    }
  }
  return statuses;
}

// Download one job's JUnit XML into its own dir. Returns the dir, which may be
// absent/empty when the job uploaded no reports (e.g. a hang or infra failure).
function downloadJobReports(jobId: string): string {
  const dest = join(JOBS_DIR, jobId);
  // buildkite-agent does not create the destination directory; without this the
  // download fails with ENOENT for every job, leaving zero test counts and
  // mis-classifying real outcomes (e.g. flaky_detected -> infra_fail).
  mkdirSync(dest, { recursive: true });
  try {
    execSync(`buildkite-agent artifact download "${TEST_RESULTS_GLOB}" "${dest}" --step "${jobId}"`, {
      cwd: PROJECT_ROOT,
      stdio: ["pipe", "inherit", "inherit"],
    });
  } catch (err) {
    // A non-zero exit can mean "no matching artifacts" (expected for jobs that
    // wrote no XML) or a genuine download failure. Surface the underlying error
    // so the latter is not silently swallowed as the former.
    const message = err instanceof Error ? err.message : String(err);
    console.error(`No JUnit reports downloaded for job ${jobId} (none uploaded, or download failed): ${message}`);
  }
  return dest;
}

// Build one job's payload. Resilient by design: if downloading or parsing this
// job's XML fails, fall back to classifying from rc/duration alone (zero test
// counts) so a single bad job can never block the whole annotation. The status
// file's rc is the most important signal anyway.
async function buildPayload(status: JobStatus): Promise<FlakinessPayload> {
  let realFailures = 0;
  let suiteTimeouts = 0;
  let totalCases = 0;
  let failingClasses: string[] = [];
  try {
    const dir = process.env.CI ? downloadJobReports(status.jobId) : join(JOBS_DIR, status.jobId);
    const report = await analyzeReports([dir]);
    realFailures = report.totals.realFailures;
    suiteTimeouts = report.totals.suiteTimeoutMarkers;
    totalCases = report.batches.reduce((acc, b) => acc + b.totalCases, 0);
    failingClasses = [...new Set(report.perTest.filter((t) => t.failures > 0).map((t) => t.className))].slice(0, MAX_FAILING_CLASSES);
  } catch (err) {
    console.error(`Failed to analyze reports for job ${status.jobId}; classifying from rc/duration only:`, err);
  }
  const derived = deriveOutcome({ rc: status.rc, durationSec: status.durationSec, realFailures, totalCases, timeoutThresholdSec: INNER_TIMEOUT_SEC });
  const payload: FlakinessPayload = {
    ...status,
    realFailures,
    suiteTimeouts,
    totalCases,
    outcome: derived.outcome,
    timedOut: derived.timedOut,
    failingClasses,
  };
  if (derived.infraSubtype) {
    payload.infraSubtype = derived.infraSubtype;
  }
  return payload;
}

function annotate(context: string, style: string, body: string): void {
  try {
    execSync(`buildkite-agent annotate --style "${style}" --context "${context}"`, {
      input: body,
      cwd: PROJECT_ROOT,
      stdio: ["pipe", "inherit", "inherit"],
    });
  } catch (err) {
    console.error(`Failed to post annotation ${context}:`, err);
  }
}

async function run(): Promise<void> {
  // Build the per-job outcome payloads first and write them as one structured
  // artifact, so a later render error still leaves the data persisted.
  const statuses = await readJobStatuses();
  const payloads: FlakinessPayload[] = [];
  for (const status of statuses) {
    try {
      payloads.push(await buildPayload(status));
    } catch (err) {
      // buildPayload is internally guarded, so this is belt-and-braces: never
      // let one job drop the rest of the array.
      console.error(`Failed to build payload for job ${status.jobId}:`, err);
    }
  }
  if (process.env.CI && payloads.length > 0) {
    // Written at PROJECT_ROOT (the step cwd / checkout root) so the analyze
    // step's `artifact_paths` glob picks it up for upload.
    try {
      await writeFile(join(PROJECT_ROOT, OUTCOMES_ARTIFACT_FILE), JSON.stringify(payloads));
    } catch (err) {
      console.error("Failed to write flakiness outcomes artifact:", err);
    }
  }

  // Human-readable report aggregated across every downloaded job.
  const report = await analyzeReports([JOBS_DIR]);
  const md = renderMarkdown(report);
  console.log(md);
  if (process.env.CI) {
    annotate("flakiness-detection-report", severity(report), md);
  }
}

if (import.meta.main) run();
