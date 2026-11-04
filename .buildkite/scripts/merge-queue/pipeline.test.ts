import { beforeEach, describe, expect, test } from "vitest";
import { readFileSync } from "fs";
import { parse, stringify } from "yaml";

import { generateMergeQueuePipeline, getBaseBranch } from "./pipeline.ts";
import type { BuildkiteStep } from "../pull-request/types.ts";
import { setBwcVersionsPath, setSnapshotBwcVersionsPath } from "../pull-request/bwc-versions.ts";
import { setBranchesJson } from "../pull-request/later-branches.ts";

const MOCKS = `${import.meta.dirname}/../pull-request/mocks`;

const leafSteps = (steps: BuildkiteStep[]): BuildkiteStep[] =>
  steps.flatMap((step) => (step.steps?.length ? leafSteps(step.steps) : [step]));

const groupNames = (steps: BuildkiteStep[]) => steps.map((step) => step.group ?? step.label);

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

    // Pull request state must not affect the merge queue, which always runs every suite
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

  test("should run the moved suites against main", () => {
    const pipeline = generateMergeQueuePipeline("main");

    expect(groupNames(pipeline.steps!)).toEqual([
      "bwc-snapshots",
      // later-branch-bwc: nothing is ahead of main
      "rest-compatibility",
      "eql-correctness",
      "packaging-tests-unix-sample",
      "packaging-tests-windows-sample",
    ]);
    expect(pipeline.env?.["GITHUB_PR_TARGET_BRANCH"]).toBe("main");
    // env from .defaults.yml, as on pull requests
    expect(pipeline.env?.["JACOCO_REPORTING_ENABLED"]).toBe("true");
  });

  test("should split bwc-snapshots into the same six parts as the pull request version", () => {
    const bwcSnapshots = generateMergeQueuePipeline("main").steps!.find((step) => step.group === "bwc-snapshots")!;

    const parts = [1, 2, 3, 4, 5, 6].map((part) => `bwc-snapshots-part${part}`);
    expect(bwcSnapshots.steps!.map((step) => step.key)).toEqual(parts);
    for (const step of bwcSnapshots.steps!) {
      expect(step.matrix).toEqual({ setup: { BWC_VERSION: ["7.17.14", "8.10.3", "8.11.0"] } });
    }
  });

  test("should run later-branch bwc from every branch ahead of the base branch", () => {
    const pipeline = generateMergeQueuePipeline("9.4");
    const laterBranchBwc = pipeline.steps!.find((step) => step.group === "later-branch-bwc")!;

    expect(laterBranchBwc.steps![0].matrix).toEqual({
      setup: { LATER_BRANCH: ["9.5", "main"], PART: ["1", "2", "3", "4", "5", "6"] },
    });
    // later-branch-bwc.sh reads the base branch from here
    expect(pipeline.env?.["GITHUB_PR_TARGET_BRANCH"]).toBe("9.4");
  });

  test("should honor skip-target-branches", () => {
    const names = groupNames(generateMergeQueuePipeline("patch/serverless-fix").steps!);

    expect(names).not.toContain("bwc-snapshots");
    expect(names).not.toContain("later-branch-bwc");
    expect(names).toContain("rest-compatibility");
  });

  test("should not push to the build cache or trigger other pipelines", () => {
    const pipeline = generateMergeQueuePipeline("9.4");
    const leaves = leafSteps(pipeline.steps!);

    expect(leaves.length).toBeGreaterThan(0);
    for (const step of leaves) {
      expect(step.command).toBeDefined();
      expect(step.command).not.toContain("build.cache.push");
      expect(step.trigger).toBeUndefined();
    }
  });

  test("should add auto-retry to every step", () => {
    for (const step of leafSteps(generateMergeQueuePipeline("9.4").steps!)) {
      expect(step.retry?.automatic?.length).toBeGreaterThan(0);
    }
  });

  test("should produce a pipeline that round-trips through YAML", () => {
    const pipeline = generateMergeQueuePipeline("9.4");

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
