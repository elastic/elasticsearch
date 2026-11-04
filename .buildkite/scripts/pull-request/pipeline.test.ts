import { beforeEach, describe, expect, test } from "vitest";

import { generatePipelines } from "./pipeline.ts";
import {
  setBwcVersionsPath,
  setIncludedSnapshotBwcVersions,
  setSnapshotBwcVersionsPath,
} from "./bwc-versions.ts";
import { setBranchesJson } from "./later-branches.ts";

describe("generatePipelines", () => {
  beforeEach(() => {
    setBwcVersionsPath(`${import.meta.dirname}/mocks/bwcVersions`);
    setSnapshotBwcVersionsPath(`${import.meta.dirname}/mocks/snapshotBwcVersions`);
    setIncludedSnapshotBwcVersions(undefined);

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

  describe("env defaults", () => {
    const envDefaultsPipeline = (name: string) =>
      generatePipelines(`${import.meta.dirname}/mocks/pipelines-env-defaults`, ["build.gradle"]).find(
        (pipeline) => pipeline.name === name,
      );

    test("should apply env from .defaults.yml to pipelines that do not define env", () => {
      expect(envDefaultsPipeline("inheriting-env")?.pipeline.env).toEqual({
        DEFAULT_ENV_VAR: "default",
        OVERRIDDEN_ENV_VAR: "default",
      });
    });

    test("should let pipeline env override and extend env from .defaults.yml", () => {
      expect(envDefaultsPipeline("overriding-env")?.pipeline.env).toEqual({
        DEFAULT_ENV_VAR: "default",
        OVERRIDDEN_ENV_VAR: "pipeline",
        PIPELINE_ENV_VAR: "pipeline",
      });
    });

    test("should not add env to pipelines when .defaults.yml defines none", () => {
      const pipelines = generatePipelines(`${import.meta.dirname}/mocks/pipelines`, ["build.gradle"]);
      const noAutoRetry = pipelines.find((p) => p.name === "no-auto-retry");

      expect(noAutoRetry).toBeDefined();
      expect(noAutoRetry!.pipeline.env).toBeUndefined();
    });
  });

  const laterBranchBwcSteps = (targetBranch: string) => {
    process.env["GITHUB_PR_TARGET_BRANCH"] = targetBranch;

    const pipelines = generatePipelines(`${import.meta.dirname}/mocks/pipelines`, ["build.gradle"]);
    return pipelines.find((pipeline) => pipeline.name === "later-branch-bwc");
  };

  test("should run later-branch bwc from the one branch ahead of 9.5", () => {
    const steps = laterBranchBwcSteps("9.5");

    expect(steps?.pipeline.steps?.[0].steps?.[0].matrix).toEqual({
      setup: { LATER_BRANCH: ["main"], PART: ["1", "2", "3", "4", "5", "6"] },
    });
  });

  test("should run later-branch bwc from every branch ahead of 9.4, oldest first", () => {
    const steps = laterBranchBwcSteps("9.4");

    expect(steps?.pipeline.steps?.[0].steps?.[0].matrix).toEqual({
      setup: { LATER_BRANCH: ["9.5", "main"], PART: ["1", "2", "3", "4", "5", "6"] },
    });
  });

  test("should not run later-branch bwc on main, where nothing is ahead", () => {
    expect(laterBranchBwcSteps("main")).toBeUndefined();
  });

  test("should not run later-branch bwc on the excluded maintenance branch", () => {
    expect(laterBranchBwcSteps("8.19")).toBeUndefined();
  });

  test("should not run later-branch bwc on a branch that is not a development branch", () => {
    expect(laterBranchBwcSteps("patch/serverless-fix")).toBeUndefined();
  });

  const anyIncludedRegionPipeline = (changedFiles: string[]) =>
    generatePipelines(`${import.meta.dirname}/mocks/pipelines`, changedFiles).find(
      (pipeline) => pipeline.name === "any-included-region",
    );

  test("should include a pipeline when any changed file is in an included region", () => {
    expect(anyIncludedRegionPipeline(["build.gradle", "x-pack/plugin/esql/build.gradle"])).toBeDefined();
  });

  test("should not include a pipeline when no changed file is in an included region", () => {
    expect(anyIncludedRegionPipeline(["build.gradle"])).toBeUndefined();
  });

  test("should run bwc snapshot jobs in dra mode, which reads the known-good builds itself", () => {
    const pipelines = generatePipelines(`${import.meta.dirname}/mocks/pipelines`, ["build.gradle"]);
    const bwc = pipelines.find((p) => p.name === "bwc-snapshots");

    expect(bwc?.pipeline.steps?.[0]?.steps?.[0]?.command).toBe(
      ".ci/scripts/run-gradle.sh -Dignore.tests.seed -Dtests.bwc.mode=dra v{{matrix.BWC_VERSION}}#bwcTest",
    );
  });

  test("should leave bwc snapshot versions without a known-good build out of the matrix", () => {
    setIncludedSnapshotBwcVersions(["7.17.14", "8.11.0"]);

    const pipelines = generatePipelines(`${import.meta.dirname}/mocks/pipelines`, ["build.gradle"]);
    const bwc = pipelines.find((p) => p.name === "bwc-snapshots");

    expect(bwc?.pipeline.steps?.[0]?.steps?.[0]?.matrix).toEqual({ setup: { BWC_VERSION: ["7.17.14", "8.11.0"] } });
  });

  test("should drop the bwc snapshot pipeline when no version is included", () => {
    setIncludedSnapshotBwcVersions([]);

    const pipelines = generatePipelines(`${import.meta.dirname}/mocks/pipelines`, ["build.gradle"]);

    expect(pipelines.map((p) => p.name)).not.toContain("bwc-snapshots");
  });

  describe("real pull request pipelines", () => {
    const PIPELINES_DIR = `${import.meta.dirname}/../../pipelines/pull-request`;
    const MOVED_TO_MERGE_QUEUE = ["bwc-snapshots", "later-branch-bwc", "rest-compatibility", "eql-correctness"];
    const PACKAGING_SAMPLES = ["packaging-tests-unix-sample", "packaging-tests-windows-sample"];

    // 9.4 has later branches, so later-branch-bwc is not dropped for lack of them
    const pipelineNames = (changedFiles: string[], labels = "", targetBranch = "9.4") => {
      process.env["GITHUB_PR_TARGET_BRANCH"] = targetBranch;
      process.env["GITHUB_PR_LABELS"] = labels;
      return generatePipelines(PIPELINES_DIR, changedFiles).map((pipeline) => pipeline.name);
    };

    test("should not run the merge queue suites by default", () => {
      const names = pipelineNames(["server/src/main/java/org/elasticsearch/Build.java"]);

      expect(names.filter((name) => MOVED_TO_MERGE_QUEUE.includes(name!))).toEqual([]);
      // pr-upgrade and the core parts stay on the gate
      expect(names).toEqual(expect.arrayContaining(["part-1", "part-6", "pr-upgrade"]));
    });

    test("should run the merge queue suites with the test-merge-queue label", () => {
      const names = pipelineNames(["server/src/main/java/org/elasticsearch/Build.java"], "test-merge-queue");

      expect(names).toEqual(expect.arrayContaining(MOVED_TO_MERGE_QUEUE));
    });

    test("should still replace bwc-snapshots with full-bwc under test-full-bwc", () => {
      const names = pipelineNames(["build.gradle"], "test-merge-queue,test-full-bwc");

      expect(names).toContain("full-bwc");
      expect(names).not.toContain("bwc-snapshots");
    });

    test.each([
      ["run elasticsearch-ci/bwc", "bwc-snapshots"],
      ["run elasticsearch-ci/later-branch-bwc", "later-branch-bwc"],
      ["run elasticsearch-ci/rest-compatibility", "rest-compatibility"],
      ["run elasticsearch-ci/eql-correctness", "eql-correctness"],
      ["run elasticsearch-ci/packaging-tests-unix-sample", "packaging-tests-unix-sample"],
    ])("should run a moved suite without the label when triggered by '%s'", (comment, name) => {
      process.env["GITHUB_PR_TRIGGER_COMMENT"] = comment;

      expect(pipelineNames(["build.gradle"])).toContain(name);
    });

    test("should not run the packaging samples for a change outside the packaging regions", () => {
      const names = pipelineNames(["server/src/main/java/org/elasticsearch/Build.java", "build.gradle"]);

      expect(names.filter((name) => PACKAGING_SAMPLES.includes(name!))).toEqual([]);
    });

    test.each([
      "distribution/docker/src/docker/dockerfiles/default/Dockerfile",
      "distribution/src/bin/elasticsearch-env",
      "qa/packaging/src/test/java/org/elasticsearch/packaging/test/DockerTests.java",
      ".ci/scripts/packaging-test.sh",
      ".ci/scripts/packaging-test.ps1",
      "build-tools-internal/version.properties",
      "build-tools-internal/src/main/java/org/elasticsearch/gradle/internal/test/DistroTestPlugin.java",
      "build-tools-internal/src/main/java/org/elasticsearch/gradle/internal/docker/DockerBuildTask.java",
      "build-tools/src/main/java/org/elasticsearch/gradle/DistributionDownloadPlugin.java",
      "x-pack/plugin/security/cli/src/main/java/org/elasticsearch/xpack/security/cli/AutoConfigureNode.java",
    ])("should run the packaging samples when %s changes", (file) => {
      const names = pipelineNames(["server/src/main/java/org/elasticsearch/Build.java", file]);

      expect(names).toEqual(expect.arrayContaining(PACKAGING_SAMPLES));
    });

    test("should run the packaging samples for the FIPS packaging change in #161768", () => {
      // A subset of the files changed by https://github.com/elastic/elasticsearch/pull/161768
      const names = pipelineNames([
        "build-tools-internal/src/main/java/org/elasticsearch/gradle/internal/test/DistroTestPlugin.java",
        "distribution/build.gradle",
        "distribution/src/fips/fips_java.security.overrides",
        "libs/ssl-config/src/main/java/org/elasticsearch/common/ssl/KeyStoreUtil.java",
        "qa/packaging/src/test/java/org/elasticsearch/packaging/test/ArchiveSecurityCliWithBcFipsTests.java",
        "server/src/main/java/org/elasticsearch/common/settings/KeyStoreWrapper.java",
      ]);

      expect(names).toEqual(expect.arrayContaining(PACKAGING_SAMPLES));
    });
  });
});
