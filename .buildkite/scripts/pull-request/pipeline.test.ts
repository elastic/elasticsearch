import { beforeEach, describe, expect, test } from "vitest";

import { generatePipelines } from "./pipeline.ts";
import { setBwcVersionsPath, setSnapshotBwcVersionsPath } from "./bwc-versions.ts";
import { setBranchesJson } from "./later-branches.ts";

describe("generatePipelines", () => {
  beforeEach(() => {
    setBwcVersionsPath(`${import.meta.dirname}/mocks/bwcVersions`);
    setSnapshotBwcVersionsPath(`${import.meta.dirname}/mocks/snapshotBwcVersions`);

    setBranchesJson({
      branches: [
        { branch: "main", version: "9.6.0" },
        { branch: "9.5", version: "9.5.5" },
        { branch: "9.4", version: "9.4.8" },
        { branch: "8.19", version: "8.19.22" },
      ],
    });

    process.env["GITHUB_PR_TARGET_BRANCH"] = "test-branch";
    process.env["GITHUB_PR_LABELS"] = "test-label-1,test-label-2";
    process.env["GITHUB_PR_TRIGGER_COMMENT"] = "";
  });

  // Helper for testing pipeline generations that should be the same when using the overall ci trigger comment "buildkite test this"
  const testWithTriggerCheck = (directory: string, changedFiles?: string[], comment = "buildkite test this") => {
    const pipelines = generatePipelines(directory, changedFiles);
    expect(pipelines).toMatchSnapshot();

    process.env["GITHUB_PR_TRIGGER_COMMENT"] = comment;
    const pipelinesWithTriggerComment = generatePipelines(directory, changedFiles);
    expect(pipelinesWithTriggerComment).toEqual(pipelines);
  };

  test("should generate correct pipelines with a non-docs change", () => {
    testWithTriggerCheck(`${import.meta.dirname}/mocks/pipelines`, ["build.gradle", "docs/README.asciidoc"]);
  });

  test("should generate correct pipelines with only docs changes", () => {
    testWithTriggerCheck(`${import.meta.dirname}/mocks/pipelines`, ["docs/README.asciidoc"]);
  });

  test("should generate correct pipelines with full BWC expansion", () => {
    process.env["GITHUB_PR_LABELS"] = "test-full-bwc";

    testWithTriggerCheck(`${import.meta.dirname}/mocks/pipelines`, ["build.gradle"]);
  });

  test("should generate correct pipelines with a different branch that is not skipped", () => {
    process.env["GITHUB_PR_TARGET_BRANCH"] = "main";

    testWithTriggerCheck(`${import.meta.dirname}/mocks/pipelines`, ["build.gradle"]);
  });

  test("should generate correct pipeline when using a trigger comment for it", () => {
    process.env["GITHUB_PR_TRIGGER_COMMENT"] = "run elasticsearch-ci/using-defaults";

    const pipelines = generatePipelines(`${import.meta.dirname}/mocks/pipelines`, ["build.gradle"]);
    expect(pipelines).toMatchSnapshot();
  });

  test("should generate correct pipelines with a non-docs change and @elasticmachine", () => {
    testWithTriggerCheck(
      `${import.meta.dirname}/mocks/pipelines`,
      ["build.gradle", "docs/README.asciidoc"],
      "@elasticmachine test this please",
    );
  });

  test("should generate correct pipelines with a non-docs change and @elasticsearchmachine", () => {
    testWithTriggerCheck(
      `${import.meta.dirname}/mocks/pipelines`,
      ["build.gradle", "docs/README.asciidoc"],
      "@elasticsearchmachine test this please",
    );
  });

  test("should not inject auto-retry when auto-retry config is false", () => {
    const pipelines = generatePipelines(`${import.meta.dirname}/mocks/pipelines`, ["build.gradle"]);
    const noAutoRetry = pipelines.find((p) => p.name === "no-auto-retry");

    expect(noAutoRetry).toBeDefined();
    expect((noAutoRetry!.pipeline.steps![0] as any).retry).toBeUndefined();
  });

  test("should inject auto-retry by default", () => {
    const pipelines = generatePipelines(`${import.meta.dirname}/mocks/pipelines`, ["build.gradle"]);
    const usingDefaults = pipelines.find((p) => p.name === "using-defaults");

    expect(usingDefaults).toBeDefined();
    expect((usingDefaults!.pipeline.steps![0] as any).retry).toBeDefined();
    expect((usingDefaults!.pipeline.steps![0] as any).retry.automatic).toHaveLength(4);
  });

  test("should inject retry into nested steps within groups", () => {
    const pipelines = generatePipelines(`${import.meta.dirname}/mocks/pipelines`, ["build.gradle"]);
    const bwcSnapshots = pipelines.find((p) => p.name === "bwc-snapshots");

    expect(bwcSnapshots).toBeDefined();
    // The group's nested step should have retry injected
    const group = bwcSnapshots!.pipeline.steps![0] as any;
    expect(group.group).toBe("bwc-snapshots");
    expect(group.steps[0].retry).toBeDefined();
    expect(group.steps[0].retry.automatic).toHaveLength(4);
  });

  test("should not overwrite pre-existing retry config on steps", () => {
    const pipelines = generatePipelines(`${import.meta.dirname}/mocks/pipelines`, ["build.gradle"]);
    const existingRetry = pipelines.find((p) => p.name === "existing-retry");

    expect(existingRetry).toBeDefined();
    // The step already has a retry config — it should be preserved, not overwritten
    const step = existingRetry!.pipeline.steps![0] as any;
    expect(step.retry.automatic).toHaveLength(1);
    expect(step.retry.automatic[0].exit_status).toBe("2");
    expect(step.retry.automatic[0].limit).toBe(5);
  });

  test("should preserve existing env vars when injecting auto-retry", () => {
    const pipelines = generatePipelines(`${import.meta.dirname}/mocks/pipelines`, ["build.gradle"]);
    const usingDefaults = pipelines.find((p) => p.name === "using-defaults");

    expect(usingDefaults).toBeDefined();
    expect(usingDefaults!.pipeline.env?.["CUSTOM_ENV_VAR"]).toBe("value");
  });

  const fwcSteps = (targetBranch: string) => {
    process.env["GITHUB_PR_TARGET_BRANCH"] = targetBranch;

    const pipelines = generatePipelines(`${import.meta.dirname}/mocks/pipelines`, ["build.gradle"]);
    return pipelines.find((pipeline) => pipeline.name === "fwc-snapshots");
  };

  test("should run forward compatibility against the one branch ahead of 9.5", () => {
    const fwc = fwcSteps("9.5");

    expect(fwc?.pipeline.steps?.[0].steps?.[0].matrix).toEqual({
      setup: { LATER_BRANCH: ["main"], PART: ["1", "2", "3", "4", "5", "6"] },
    });
  });

  test("should run forward compatibility against every branch ahead of 9.4, oldest first", () => {
    const fwc = fwcSteps("9.4");

    expect(fwc?.pipeline.steps?.[0].steps?.[0].matrix).toEqual({
      setup: { LATER_BRANCH: ["9.5", "main"], PART: ["1", "2", "3", "4", "5", "6"] },
    });
  });

  test("should not run forward compatibility on main, where nothing is ahead", () => {
    expect(fwcSteps("main")).toBeUndefined();
  });

  test("should not run forward compatibility on the excluded maintenance branch", () => {
    expect(fwcSteps("8.19")).toBeUndefined();
  });

  test("should not run forward compatibility on a branch that is not a development branch", () => {
    expect(fwcSteps("patch/serverless-fix")).toBeUndefined();
  });
});
