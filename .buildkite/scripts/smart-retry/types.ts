/** Shape of a single run within task-status.json, produced by GradleRunner's TaskTracker. */
export interface TaskStatusReport {
  tasks: TaskEntry[];
  suites?: SuiteEntry[];
  tests: TestEntry[];
  cancelled: boolean;
  preemptedAt?: string | null;
  buildScanId?: string | null;
  buildScanUrl?: string | null;
}

export interface TaskEntry {
  path: string;
  outcome: "SUCCESS" | "UP_TO_DATE" | "FROM_CACHE" | "FAILED" | "SKIPPED" | "INTERRUPTED" | "NOT_RUN";
}

export interface SuiteEntry {
  taskPath: string;
  className: string;
  result: "SUCCESS" | "FAILURE" | "SKIPPED" | "INTERRUPTED";
}

export interface TestEntry {
  taskPath: string;
  className: string;
  methodName: string;
  result: "SUCCESS" | "FAILURE" | "SKIPPED" | "INTERRUPTED";
}

/**
 * Multi-run wrapper for task-status.json. Each retry appends its run.
 * The first run (no previous artifact) produces a single-element array.
 * Runs are ordered oldest-first: runs[0] is the original attempt.
 */
export interface MultiRunTaskStatus {
  runs: TaskStatusReport[];
}

/**
 * Shape of .failed-test-history.json consumed by InternalTestRerunPlugin.
 *
 * The plugin skips any test task in successfulTasks entirely. For tasks not
 * in successfulTasks but present in successfulTests, individual passing tests
 * are excluded so only the failures re-run. Everything else runs normally.
 */
export interface FailedTestsReport {
  successfulTasks: string[];
  successfulSuites: Record<string, string[]>;
  successfulTests: Record<string, string[]>;
  testseed: string;
}

/** Environment variables read by the smart-retry orchestrator. */
export interface SmartRetryEnv {
  buildkiteApiToken: string;
  buildkiteJobId: string;
  buildkitePipelineSlug: string;
  buildkiteBuildNumber: string;
  originJobId?: string;
  testsSeed?: string;
}

export type SmartRetryStatus = "enabled" | "disabled" | "failed";

/** Everything the orchestrator decides — no side effects, just data. */
export interface SmartRetryResult {
  status: SmartRetryStatus;
  details: string;
  failedTestHistory: FailedTestsReport | null;
  annotation: string | null;
  metadata: Record<string, string>;
}

/** Injectable I/O operations so tests can replace them. */
export interface SmartRetryDeps {
  fetchBuildJson: (apiToken: string, pipelineSlug: string, buildNumber: string) => Promise<BuildkiteBuildJson | null>;
  downloadArtifact: (originJobId: string) => Promise<MultiRunTaskStatus | null>;
}

/** Minimal subset of the Buildkite build JSON we actually use. */
export interface BuildkiteBuildJson {
  jobs: BuildkiteJob[];
  meta_data: Record<string, string>;
}

export interface BuildkiteJob {
  id: string;
  name?: string;
  retry_source?: { job_id: string };
}
