import { describe, expect, test } from "vitest";

import { compositeSearch, executionRows, itemRows, jobFilter, jobItemSums, numberAt, type SearchClient } from "./stats.ts";

// A search client that answers each composite request with the next page and records the requests, standing in for
// the CI stats cluster.
const fakeClient = (pages: object[][]): SearchClient & { requests: { index: string; body: Record<string, unknown> }[] } => {
  const requests: { index: string; body: Record<string, unknown> }[] = [];
  return {
    requests,
    async search(index: string, body: object): Promise<unknown> {
      requests.push({ index, body: body as Record<string, unknown> });
      const buckets = pages[requests.length - 1] ?? [];
      return { aggregations: { c: { buckets, after_key: { page: String(requests.length) } } } };
    },
  };
};

const jobs = { jobName: "part-[0-9]+", days: 14 };

describe("stats", () => {
  test("composite searches page until a short page", async () => {
    const client = fakeClient([[{ key: { p: "a" } }, { key: { p: "b" } }], [{ key: { p: "c" } }]]);
    const buckets = await compositeSearch(client, "gradle-tasks", [], { p: "path.keyword" }, {}, 2);
    expect(buckets.map((b) => b.key.p)).toEqual(["a", "b", "c"]);
    expect(client.requests).toHaveLength(2);
    expect(JSON.stringify(client.requests[1].body)).toContain('"after":{"page":"1"}');
  });

  test("an unexpected response fails loudly", async () => {
    const client: SearchClient = { search: async () => ({ error: "nope" }) };
    await expect(compositeSearch(client, "gradle-tasks", [], {}, {})).rejects.toThrow(/unexpected search response/);
  });

  test("the job filter selects the suite's jobs of PRs against main, without retries", () => {
    const filter = JSON.stringify(jobFilter(jobs));
    expect(filter).toContain('"build.values.Job Name.keyword":"part-[0-9]+"');
    expect(filter).toContain('"build.values.PR Target Branch.keyword":"main"');
    expect(filter).toContain('"gte":"now-14d"');
    expect(filter).toContain('"must_not":[{"term":{"build.tags.keyword":"RETRY"}}]');
    expect(filter).not.toContain("Job Number");
    expect(JSON.stringify(jobFilter(jobs, false))).not.toContain("RETRY");
    expect(JSON.stringify(jobFilter({ ...jobs, builds: ["197107", "197162"] }))).toContain('"build.values.Job Number.keyword":["197107","197162"]');
  });

  test("executions keep cache hits, skips and retries apart from executed durations", async () => {
    const client = fakeClient([
      [
        { key: { p: ":x:test", b: "b1", n: "part-1", j: "j1" }, doc_count: 1, executed: { doc_count: 1, ms: { value: 1000 } }, notExecuted: { doc_count: 0 }, retry: { doc_count: 0 } },
        { key: { p: ":x:test", b: "b1", n: "part-2", j: "j2" }, doc_count: 1, executed: { doc_count: 0, ms: { value: null } }, notExecuted: { doc_count: 1 }, retry: { doc_count: 1 } },
      ],
    ]);
    expect(await executionRows(client, jobs, [":x:test"])).toEqual([
      { path: ":x:test", build: "b1", jobName: "part-1", jobId: "j1", executedMs: 1000, notExecuted: 0, retry: false },
      { path: ":x:test", build: "b1", jobName: "part-2", jobId: "j2", executedMs: null, notExecuted: 1, retry: true },
    ]);
    // retries are kept here, marked, so that split tasks can tell an incomplete build
    expect(JSON.stringify(client.requests[0].body)).not.toContain("must_not");
  });

  test("items and per-job sums read classes or tests", async () => {
    const items = fakeClient([[{ key: { t: ":x:test", n: "org.example.FooTests" }, doc_count: 3, d: { values: { "50.0": 1500 } } }]]);
    expect(await itemRows(items, jobs, [":x:test"], true)).toEqual([{ task: ":x:test", name: "org.example.FooTests", p50Ms: 1500 }]);
    expect(JSON.stringify(items.requests[0].body)).toContain('"className.keyword"');

    const sums = fakeClient([[{ key: { t: ":x:yamlRestTest", j: "j1" }, doc_count: 9, ms: { value: 4200 } }]]);
    expect(await jobItemSums(sums, jobs, [":x:yamlRestTest"], false)).toEqual([{ task: ":x:yamlRestTest", jobId: "j1", ms: 4200 }]);
    expect(JSON.stringify(sums.requests[0].body)).toContain('{"term":{"suite":false}}');
  });

  test("no tasks means no queries", async () => {
    const client = fakeClient([]);
    expect(await executionRows(client, jobs, [])).toEqual([]);
    expect(await itemRows(client, jobs, [], true)).toEqual([]);
    expect(await jobItemSums(client, jobs, [], true)).toEqual([]);
    expect(client.requests).toHaveLength(0);
  });

  test("numberAt reads nested numbers and nothing else", () => {
    expect(numberAt({ a: { b: 3 } }, "a", "b")).toBe(3);
    expect(numberAt({ a: { b: "3" } }, "a", "b")).toBeNull();
    expect(numberAt({ a: null }, "a", "b")).toBeNull();
  });
});
