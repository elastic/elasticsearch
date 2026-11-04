import { beforeEach, describe, expect, test } from "vitest";
import { existsSync, readFileSync } from "fs";
import { parse, stringify } from "yaml";

import { generateMergeQueuePipeline, getBaseBranch, getPullRequestNumber } from "./pipeline.ts";
import type { BuildkiteStep } from "../pull-request/types.ts";
import { setBwcVersionsPath, setSnapshotBwcVersionsPath } from "../pull-request/bwc-versions.ts";
import { setBranchesJson } from "../pull-request/later-branches.ts";

const MOCKS = `${import.meta.dirname}/../pull-request/mocks`;
const PULL_REQUEST_PIPELINES = `${import.meta.dirname}/../../pipelines/pull-request`;

const leafSteps = (steps: BuildkiteStep[]): BuildkiteStep[] =>
  steps.flatMap((step) => (step.steps?.length ? leafSteps(step.steps) : [step]));

const groupNames = (steps: BuildkiteStep[]) => steps.map((step) => step.group ?? step.label);

// A change to production code, which every default pull request pipeline runs for
const SERVER_CHANGE = ["server/src/main/java/org/elasticsearch/Foo.java"];

describe("merge queue pipeline", () => {
  beforeEach(() => {
    setBwcVersionsPath(`${MOCKS}/bwcVersions`);
    setSnapshotBwcVersionsPath(`${MOCKS}/snapshotBwcVersions`);

    setBranchesJson({
      branches: [
        { branch: "main", version: "9.6.0" },
        { branch: "9.5", version: "9.5.5" },
        { branch: "9.4", version: "9.4.8" },
        { branch: "8.19", version: "8.19.22" },
      ],
    });

    // Pull request state must not affect the merge queue, which selects as for a pull request with no labels
    process.env["GITHUB_PR_LABELS"] = "";
    process.env["GITHUB_PR_TRIGGER_COMMENT"] = "";
    process.env["GITHUB_PR_TARGET_BRANCH"] = "";
  });

  test.each([
    ["gh-readonly-queue/main/pr-164485-33c63bc9e80b2a0e1f3c4d5e6f7a8b9c0d1e2f3a", "main"],
    ["gh-readonly-queue/9.4/pr-1-deadbeef", "9.4"],
    ["gh-readonly-queue/patch/serverless-fix/pr-12-abc123", "patch/serverless-fix"],
    ["9.5", "9.5"],
  ])("should merge %s into %s", (branch, baseBranch) => {
    expect(getBaseBranch(branch)).toBe(baseBranch);
  });

  test("should read the queued pull request's number from the branch", () => {
    expect(getPullRequestNumber("gh-readonly-queue/main/pr-164485-33c63bc9e80b2a0e")).toBe("164485");
    expect(getPullRequestNumber("9.5")).toBeUndefined();
  });

  test("should run everything a pull request ran before suites moved off the pull request gate", () => {
    const pipeline = generateMergeQueuePipeline("main", "164485", SERVER_CHANGE);
    const names = groupNames(pipeline.steps!);

    // every default pull request pipeline, and the suites that moved here (later-branch-bwc: nothing is ahead of main)
    for (const name of [
      "build-logic",
      "bwc-snapshots",
      "eql-correctness",
      "flakiness-detection",
      "packaging-tests-unix-sample",
      "packaging-tests-windows-sample",
      "part-1",
      "pr-upgrade",
      "rest-compatibility",
      "validate-changelogs",
      "validate-transport-version-backport",
    ]) {
      expect(names).toContain(name);
    }
    // pipelines a pull request only runs with a label, or for other paths, stay out
    for (const name of ["release-tests", "part-1-arm", "part-1-fips", "part-1-windows", "gpu", "ipv6", "docs-check", "esql-consistency"]) {
      expect(names).not.toContain(name);
    }

    expect(pipeline.env?.["GITHUB_PR_TARGET_BRANCH"]).toBe("main");
    // for scripts written for pull requests: pr-upgrade, the backport checks, flakiness detection
    expect(pipeline.env?.["MERGE_QUEUE_BASE_BRANCH"]).toBe("main");
    expect(pipeline.env?.["MERGE_QUEUE_PULL_REQUEST"]).toBe("164485");
    // env from .defaults.yml, as on pull requests
    expect(pipeline.env?.["JACOCO_REPORTING_ENABLED"]).toBe("true");
  });

  test("should run path-gated pipelines when the entry touches their paths", () => {
    const names = groupNames(generateMergeQueuePipeline("main", "1", ["x-pack/plugin/esql/src/main/java/Foo.java"]).steps!);

    expect(names).toContain("esql-consistency");
  });

  test("should select the moved suites with the rules they had before they moved", () => {
    // docs-only changes skipped them on pull requests before, and still skip them here
    expect(groupNames(generateMergeQueuePipeline("main", "1", ["docs/reference/foo.md"]).steps!)).toEqual(["docs-check"]);
    // packaging no longer depends on touching packaging paths, as on pull requests before
    expect(groupNames(generateMergeQueuePipeline("main", "1", SERVER_CHANGE).steps!)).toContain("packaging-tests-unix-sample");
  });

  test("should split bwc-snapshots into the same six parts as the pull request version", () => {
    const bwcSnapshots = generateMergeQueuePipeline("main", "1", SERVER_CHANGE).steps!.find((step) => step.group === "bwc-snapshots")!;

    const parts = [1, 2, 3, 4, 5, 6].map((part) => `bwc-snapshots-part${part}`);
    expect(bwcSnapshots.steps!.map((step) => step.key)).toEqual(parts);
    for (const step of bwcSnapshots.steps!) {
      expect(step.matrix).toEqual({ setup: { BWC_VERSION: ["7.17.14", "8.10.3", "8.11.0"] } });
    }
  });

  test("should run later-branch bwc from every branch ahead of the base branch", () => {
    const pipeline = generateMergeQueuePipeline("9.4", "1", SERVER_CHANGE);
    const laterBranchBwc = pipeline.steps!.find((step) => step.group === "later-branch-bwc")!;

    expect(laterBranchBwc.steps![0].matrix).toEqual({
      setup: { LATER_BRANCH: ["9.5", "main"], PART: ["1", "2", "3", "4", "5", "6"] },
    });
    // later-branch-bwc.sh reads the base branch from here
    expect(pipeline.env?.["GITHUB_PR_TARGET_BRANCH"]).toBe("9.4");
  });

  test("should honor skip-target-branches", () => {
    const names = groupNames(generateMergeQueuePipeline("patch/serverless-fix", "1", SERVER_CHANGE).steps!);

    expect(names).not.toContain("bwc-snapshots");
    expect(names).not.toContain("later-branch-bwc");
    expect(names).toContain("rest-compatibility");
  });

  test("should not push to the build cache or trigger other pipelines", () => {
    const pipeline = generateMergeQueuePipeline("9.4", "1", SERVER_CHANGE);
    const leaves = leafSteps(pipeline.steps!);

    expect(leaves.length).toBeGreaterThan(0);
    for (const step of leaves) {
      expect(step.command).toBeDefined();
      expect(step.command).not.toContain("build.cache.push");
      expect(step.trigger).toBeUndefined();
    }
  });

  test("should add auto-retry to every step, except where a pipeline turns it off as on pull requests", () => {
    const steps = generateMergeQueuePipeline("9.4", "1", SERVER_CHANGE).steps!;
    // flakiness-detection and the validations set auto-retry: false in their own config
    const turnsRetryOff = (step: BuildkiteStep) => {
      const file = `${PULL_REQUEST_PIPELINES}/${step.group ?? step.label}.yml`;
      return existsSync(file) && parse(readFileSync(file, "utf-8")).config?.["auto-retry"] === false;
    };
    const retried = steps.filter((step) => !turnsRetryOff(step));

    expect(retried.length).toBeLessThan(steps.length);
    for (const step of leafSteps(retried)) {
      expect(step.retry?.automatic?.length).toBeGreaterThan(0);
    }
  });

  test("should produce a pipeline that round-trips through YAML", () => {
    const pipeline = generateMergeQueuePipeline("9.4", "1", SERVER_CHANGE);

    expect(parse(stringify(pipeline))).toEqual(pipeline);
    // No pull request generator config may leak into the uploaded pipeline
    expect(stringify(pipeline)).not.toMatch(/allow-labels|any-included-regions|trigger-phrase|bwc_template/);
  });

  test("should bootstrap from the generator without cache push or DRA", () => {
    const bootstrap = parse(readFileSync(`${import.meta.dirname}/../../pipelines/merge-queue.yml`, "utf-8"));

    expect(bootstrap.steps).toEqual([
      { label: ":pipeline: Generate merge queue steps", command: ".buildkite/scripts/merge-queue/pipeline.sh" },
    ]);
  });
});
