import type { ExecutionRow, ItemRow, JobItemSum, TaskRow } from "./durations.ts";

// Queries the CI stats cluster (gradle-tasks and gradle-tests indices, fed from the Develocity build scans of every
// CI job) for the durations the CI part planner balances.

export interface SearchClient {
  search(index: string, body: object): Promise<unknown>;
}

export const fetchClient = (url: string, apiKey: string): SearchClient => ({
  async search(index: string, body: object): Promise<unknown> {
    const response = await fetch(`${url.replace(/\/$/, "")}/${index}/_search`, {
      method: "POST",
      headers: { Authorization: `ApiKey ${apiKey}`, "Content-Type": "application/json" },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(300_000),
    });
    if (!response.ok) {
      throw new Error(`${index}/_search failed: HTTP ${response.status} ${(await response.text()).substring(0, 1000)}`);
    }
    return response.json();
  },
});

/** The CI jobs whose durations one suite's assignment balances, and the window to read. */
export interface JobSelection {
  /** Regular expression (Elasticsearch regexp syntax) the Buildkite job name must match in full. */
  jobName: string;
  days: number;
  /** Only these build numbers (within the window), for example builds that ran a part layout main doesn't run yet. */
  builds?: string[];
}

/**
 * Jobs of pull requests against main in the pull request pipeline: the code the assignment applies to. Retried jobs
 * are left out unless asked for: smart retry skips the tests that passed in the attempt before, so a retry's
 * durations cover only part of a task.
 */
export const jobFilter = (jobs: JobSelection, excludeRetries = true): object[] => [
  { range: { "@timestamp": { gte: `now-${jobs.days}d` } } },
  { term: { "build.values.Pipeline.keyword": "elasticsearch-pull-request" } },
  { regexp: { "build.values.Job Name.keyword": jobs.jobName } },
  { term: { "build.values.PR Target Branch.keyword": "main" } },
  ...(jobs.builds ? [{ terms: { "build.values.Job Number.keyword": jobs.builds } }] : []),
  ...(excludeRetries ? [{ bool: { must_not: [{ term: { "build.tags.keyword": "RETRY" } }] } }] : []),
];

const EXECUTED = ["SUCCESS", "FAILED"];
const NOT_EXECUTED = ["FROM_CACHE", "UP_TO_DATE", "SKIPPED"];

interface CompositeBucket {
  key: Record<string, string>;
  doc_count: number;
  [agg: string]: unknown;
}

const isRecord = (value: unknown): value is Record<string, unknown> => typeof value === "object" && value !== null;

const compositeBuckets = (response: unknown): { buckets: CompositeBucket[]; afterKey?: Record<string, string> } => {
  const aggs = isRecord(response) ? response.aggregations : undefined;
  const c = isRecord(aggs) ? aggs.c : undefined;
  if (!isRecord(c)) {
    throw new Error(`unexpected search response: ${JSON.stringify(response).substring(0, 500)}`);
  }
  // the bucket and after_key shapes are fixed by the composite request built in compositeSearch; metric values are
  // still read defensively through numberAt
  const buckets = Array.isArray(c.buckets) ? (c.buckets as CompositeBucket[]) : [];
  const afterKey = isRecord(c.after_key) ? (c.after_key as Record<string, string>) : undefined;
  return { buckets, afterKey };
};

/** Pages through a composite aggregation named `c`. */
export const compositeSearch = async (
  client: SearchClient,
  index: string,
  filter: object[],
  sources: Record<string, string>,
  aggs: object,
  pageSize = 5000
): Promise<CompositeBucket[]> => {
  const all: CompositeBucket[] = [];
  let after: Record<string, string> | undefined;
  for (;;) {
    const composite = {
      size: pageSize,
      sources: Object.entries(sources).map(([name, field]) => ({ [name]: { terms: { field } } })),
      ...(after ? { after } : {}),
    };
    const response = await client.search(index, { size: 0, query: { bool: { filter } }, aggs: { c: { composite, aggs } } });
    const { buckets, afterKey } = compositeBuckets(response);
    all.push(...buckets);
    if (buckets.length < pageSize || !afterKey) {
      return all;
    }
    after = afterKey;
  }
};

/** The number at `path` inside an aggregation response, or null. */
export const numberAt = (value: unknown, ...path: string[]): number | null => {
  let current: unknown = value;
  for (const key of path) {
    if (!isRecord(current) || !(key in current)) {
      return null;
    }
    current = current[key];
  }
  return typeof current === "number" ? current : null;
};

const P50_AGG = { d: { percentiles: { field: "duration", percents: [50] } } };
const p50 = (bucket: CompositeBucket): number | null => numberAt(bucket, "d", "values", "50.0");

/** Median executed duration of every task path (test or not) in the selected jobs. */
export const taskRows = async (client: SearchClient, jobs: JobSelection): Promise<TaskRow[]> => {
  const buckets = await compositeSearch(
    client,
    "gradle-tasks",
    [...jobFilter(jobs), { terms: { "outcome.keyword": EXECUTED } }],
    { p: "path.keyword", t: "type.keyword" },
    P50_AGG
  );
  return buckets.flatMap((b) => {
    const v = p50(b);
    return v === null ? [] : [{ path: b.key.p, type: b.key.t, p50Ms: v }];
  });
};

/** Every job's execution, cache hit or skip of the given test tasks, retries included and marked. */
export const executionRows = async (client: SearchClient, jobs: JobSelection, tasks: string[]): Promise<ExecutionRow[]> => {
  if (tasks.length === 0) {
    return [];
  }
  const buckets = await compositeSearch(
    client,
    "gradle-tasks",
    [...jobFilter(jobs, false), { terms: { "path.keyword": tasks } }],
    { p: "path.keyword", b: "build.values.Build ID.keyword", n: "build.values.Job Name.keyword", j: "build.values.Job ID.keyword" },
    {
      executed: { filter: { terms: { "outcome.keyword": EXECUTED } }, aggs: { ms: { max: { field: "duration" } } } },
      notExecuted: { filter: { terms: { "outcome.keyword": NOT_EXECUTED } } },
      retry: { filter: { term: { "build.tags.keyword": "RETRY" } } },
    }
  );
  return buckets.map((b) => ({
    path: b.key.p,
    build: b.key.b,
    jobName: b.key.n,
    jobId: b.key.j,
    executedMs: (numberAt(b, "executed", "doc_count") ?? 0) > 0 ? numberAt(b, "executed", "ms", "value") : null,
    notExecuted: numberAt(b, "notExecuted", "doc_count") ?? 0,
    retry: (numberAt(b, "retry", "doc_count") ?? 0) > 0,
  }));
};

/**
 * Median duration of each test class of the given tasks (`suite: true`) or of each test (`suite: false`, for YAML
 * suites, whose tests are what files group).
 */
export const itemRows = async (client: SearchClient, jobs: JobSelection, tasks: string[], suite: boolean): Promise<ItemRow[]> => {
  if (tasks.length === 0) {
    return [];
  }
  const buckets = await compositeSearch(
    client,
    "gradle-tests",
    [...jobFilter(jobs), { term: { suite } }, { terms: { "task.keyword": tasks } }],
    { t: "task.keyword", n: suite ? "className.keyword" : "name.keyword" },
    P50_AGG
  );
  return buckets.flatMap((b) => {
    const v = p50(b);
    return v === null ? [] : [{ task: b.key.t, name: b.key.n, p50Ms: v }];
  });
};

/** The summed duration of the given tasks' classes (`suite: true`) or tests (`suite: false`) in each job. */
export const jobItemSums = async (client: SearchClient, jobs: JobSelection, tasks: string[], suite: boolean): Promise<JobItemSum[]> => {
  if (tasks.length === 0) {
    return [];
  }
  const buckets = await compositeSearch(
    client,
    "gradle-tests",
    [...jobFilter(jobs), { term: { suite } }, { terms: { "task.keyword": tasks } }],
    { t: "task.keyword", j: "build.values.Job ID.keyword" },
    { ms: { sum: { field: "duration" } } }
  );
  return buckets.flatMap((b) => {
    const ms = numberAt(b, "ms", "value");
    return ms === null ? [] : [{ task: b.key.t, jobId: b.key.j, ms }];
  });
};
