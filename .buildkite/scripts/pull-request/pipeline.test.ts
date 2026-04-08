import { beforeEach, describe, expect, test } from "bun:test";

import { generatePipelines } from "./pipeline";
import { setBwcVersionsPath, setSnapshotBwcVersionsPath } from "./bwc-versions";

describe("generatePipelines", () => {
  beforeEach(() => {
    setBwcVersionsPath(`${import.meta.dir}/mocks/bwcVersions`);
    setSnapshotBwcVersionsPath(`${import.meta.dir}/mocks/snapshotBwcVersions`);

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
    testWithTriggerCheck(`${import.meta.dir}/mocks/pipelines`, ["build.gradle", "docs/README.asciidoc"]);
  });

  test("should generate correct pipelines with only docs changes", () => {
    testWithTriggerCheck(`${import.meta.dir}/mocks/pipelines`, ["docs/README.asciidoc"]);
  });

  test("should generate correct pipelines with full BWC expansion", () => {
    process.env["GITHUB_PR_LABELS"] = "test-full-bwc";

    testWithTriggerCheck(`${import.meta.dir}/mocks/pipelines`, ["build.gradle"]);
  });

  test("should generate correct pipelines with a different branch that is not skipped", () => {
    process.env["GITHUB_PR_TARGET_BRANCH"] = "main";

    testWithTriggerCheck(`${import.meta.dir}/mocks/pipelines`, ["build.gradle"]);
  });

  test("should generate correct pipeline when using a trigger comment for it", () => {
    process.env["GITHUB_PR_TRIGGER_COMMENT"] = "run elasticsearch-ci/using-defaults";

    const pipelines = generatePipelines(`${import.meta.dir}/mocks/pipelines`, ["build.gradle"]);
    expect(pipelines).toMatchSnapshot();
  });

  test("should generate correct pipelines with a non-docs change and @elasticmachine", () => {
    testWithTriggerCheck(
      `${import.meta.dir}/mocks/pipelines`,
      ["build.gradle", "docs/README.asciidoc"],
      "@elasticmachine test this please"
    );
  });

  test("should generate correct pipelines with a non-docs change and @elasticsearchmachine", () => {
    testWithTriggerCheck(
      `${import.meta.dir}/mocks/pipelines`,
      ["build.gradle", "docs/README.asciidoc"],
      "@elasticsearchmachine test this please"
    );
  });

  test("should not inject auto-retry when auto-retry config is false", () => {
    const pipelines = generatePipelines(`${import.meta.dir}/mocks/pipelines`, ["build.gradle"]);
    const noAutoRetry = pipelines.find((p) => p.name === "no-auto-retry");

    expect(noAutoRetry).toBeDefined();
    expect((noAutoRetry!.pipeline.steps![0] as any).retry).toBeUndefined();
  });

  test("should inject auto-retry by default", () => {
    const pipelines = generatePipelines(`${import.meta.dir}/mocks/pipelines`, ["build.gradle"]);
    const usingDefaults = pipelines.find((p) => p.name === "using-defaults");

    expect(usingDefaults).toBeDefined();
    expect((usingDefaults!.pipeline.steps![0] as any).retry).toBeDefined();
    expect((usingDefaults!.pipeline.steps![0] as any).retry.automatic).toHaveLength(3);
  });

  test("should inject retry into nested steps within groups", () => {
    const pipelines = generatePipelines(`${import.meta.dir}/mocks/pipelines`, ["build.gradle"]);
    const bwcSnapshots = pipelines.find((p) => p.name === "bwc-snapshots");

    expect(bwcSnapshots).toBeDefined();
    // The group's nested step should have retry injected
    const group = bwcSnapshots!.pipeline.steps![0] as any;
    expect(group.group).toBe("bwc-snapshots");
    expect(group.steps[0].retry).toBeDefined();
    expect(group.steps[0].retry.automatic).toHaveLength(3);
  });

  test("should not overwrite pre-existing retry config on steps", () => {
    const pipelines = generatePipelines(`${import.meta.dir}/mocks/pipelines`, ["build.gradle"]);
    const existingRetry = pipelines.find((p) => p.name === "existing-retry");

    expect(existingRetry).toBeDefined();
    // The step already has a retry config — it should be preserved, not overwritten
    const step = existingRetry!.pipeline.steps![0] as any;
    expect(step.retry.automatic).toHaveLength(1);
    expect(step.retry.automatic[0].exit_status).toBe("2");
    expect(step.retry.automatic[0].limit).toBe(5);
  });

  test("should preserve existing env vars when injecting auto-retry", () => {
    const pipelines = generatePipelines(`${import.meta.dir}/mocks/pipelines`, ["build.gradle"]);
    const usingDefaults = pipelines.find((p) => p.name === "using-defaults");

    expect(usingDefaults).toBeDefined();
    expect(usingDefaults!.pipeline.env?.["CUSTOM_ENV_VAR"]).toBe("value");
  });
});
