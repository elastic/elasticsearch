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

  test("should generate correct pipelines with a non-docs change", () => {
    const pipelines = generatePipelines(`${import.meta.dir}/mocks/pipelines`, ["build.gradle", "docs/README.asciidoc"]);
    expect(pipelines).toMatchSnapshot();
  });

  test("should generate correct pipelines with only docs changes", () => {
    const pipelines = generatePipelines(`${import.meta.dir}/mocks/pipelines`, ["docs/README.asciidoc"]);
    expect(pipelines).toMatchSnapshot();
  });

  test("should generate correct pipelines with full BWC expansion", () => {
    process.env["GITHUB_PR_LABELS"] = "test-full-bwc";

    const pipelines = generatePipelines(`${import.meta.dir}/mocks/pipelines`, ["build.gradle"]);
    expect(pipelines).toMatchSnapshot();
  });

  test("should generate correct pipeline when using a trigger comment for it", () => {
    process.env["GITHUB_PR_TRIGGER_COMMENT"] = "run elasticsearch-ci/using-defaults";

    const pipelines = generatePipelines(`${import.meta.dir}/mocks/pipelines`, ["build.gradle"]);
    expect(pipelines).toMatchSnapshot();
  });
});
