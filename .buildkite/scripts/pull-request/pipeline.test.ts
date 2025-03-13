import { beforeEach, describe, expect, test } from "bun:test";

import { generatePipelines } from "./pipeline";
import { setBwcVersionsPath, setSnapshotBwcVersionsPath } from "./bwc-versions";

describe("generatePipelines", () => {
  beforeEach(() => {
    setBwcVersionsPath(`${import.meta.dir}/mocks/bwcVersions`);
    setSnapshotBwcVersionsPath(`${import.meta.dir}/mocks/snapshotBwcVersions`);

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
});
