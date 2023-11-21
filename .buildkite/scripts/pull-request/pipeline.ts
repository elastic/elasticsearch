import { parse } from "yaml";
import { readFileSync, readdirSync } from "fs";
import { basename, resolve } from "path";
import { execSync } from "child_process";

import { BuildkitePipeline, BuildkiteStep, EsPipeline, EsPipelineConfig } from "./types";
import { getBwcVersions, getSnapshotBwcVersions } from "./bwc-versions";

const PROJECT_ROOT = resolve(`${import.meta.dir}/../../..`);

const getArray = (strOrArray: string | string[] | undefined): string[] => {
  if (typeof strOrArray === "undefined") {
    return [];
  }

  return typeof strOrArray === "string" ? [strOrArray] : strOrArray;
};

const labelCheckAllow = (pipeline: EsPipeline, labels: string[]): boolean => {
  if (pipeline.config?.["allow-labels"]) {
    return getArray(pipeline.config["allow-labels"]).some((label) => labels.includes(label));
  }
  return true;
};

const labelCheckSkip = (pipeline: EsPipeline, labels: string[]): boolean => {
  if (pipeline.config?.["skip-labels"]) {
    return !getArray(pipeline.config["skip-labels"]).some((label) => labels.includes(label));
  }
  return true;
};

// Exclude the pipeline if all of the changed files in the PR are in at least one excluded region
const changedFilesExcludedCheck = (pipeline: EsPipeline, changedFiles: string[]): boolean => {
  if (pipeline.config?.["excluded-regions"]) {
    return !changedFiles.every((file) =>
      getArray(pipeline.config?.["excluded-regions"]).some((region) => file.match(region))
    );
  }
  return true;
};

// Include the pipeline if all of the changed files in the PR are in at least one included region
const changedFilesIncludedCheck = (pipeline: EsPipeline, changedFiles: string[]): boolean => {
  if (pipeline.config?.["included-regions"]) {
    return changedFiles.every((file) =>
      getArray(pipeline.config?.["included-regions"]).some((region) => file.match(region))
    );
  }
  return true;
};

const triggerCommentCheck = (pipeline: EsPipeline): boolean => {
  if (process.env["GITHUB_PR_TRIGGER_COMMENT"] && pipeline.config?.["trigger-phrase"]) {
    return !!process.env["GITHUB_PR_TRIGGER_COMMENT"].match(pipeline.config["trigger-phrase"]);
  }
  return false;
};

// There are so many BWC versions that we can't use the matrix feature in Buildkite, as it's limited to 20 elements per dimension
// So we need to duplicate the steps instead
// Recursively check for any steps that have a bwc_template attribute and expand them out into multiple steps, one for each BWC_VERSION
const doBwcTransforms = (step: BuildkitePipeline | BuildkiteStep) => {
  const stepsToExpand = (step.steps || []).filter((s) => s.bwc_template);
  step.steps = (step.steps || []).filter((s) => !s.bwc_template);

  for (const s of step.steps) {
    if (s.steps?.length) {
      doBwcTransforms(s);
    }
  }

  for (const stepToExpand of stepsToExpand) {
    for (const bwcVersion of getBwcVersions()) {
      let newStepJson = JSON.stringify(stepToExpand).replaceAll("$BWC_VERSION_SNAKE", bwcVersion.replaceAll(".", "_"));
      newStepJson = newStepJson.replaceAll("$BWC_VERSION", bwcVersion);
      const newStep = JSON.parse(newStepJson);
      delete newStep.bwc_template;
      step.steps.push(newStep);
    }
  }
};

export const generatePipelines = (
  directory: string = `${PROJECT_ROOT}/.buildkite/pipelines/pull-request`,
  changedFiles: string[] = []
) => {
  let defaults: EsPipelineConfig = { config: {} };
  defaults = parse(readFileSync(`${directory}/.defaults.yml`, "utf-8"));
  defaults.config = defaults.config || {};

  let pipelines: EsPipeline[] = [];
  const files = readdirSync(directory);
  for (const file of files) {
    if (!file.endsWith(".yml") || file.endsWith(".defaults.yml")) {
      continue;
    }

    let yaml = readFileSync(`${directory}/${file}`, "utf-8");
    yaml = yaml.replaceAll("$SNAPSHOT_BWC_VERSIONS", JSON.stringify(getSnapshotBwcVersions()));
    const pipeline: EsPipeline = parse(yaml) || {};

    pipeline.config = { ...defaults.config, ...(pipeline.config || {}) };

    // '.../build-benchmark.yml' => 'build-benchmark'
    const name = basename(file).split(".", 2)[0];
    pipeline.name = name;
    pipeline.config["trigger-phrase"] = pipeline.config["trigger-phrase"] || `.*run\\W+elasticsearch-ci/${name}.*`;

    pipelines.push(pipeline);
  }

  const labels = (process.env["GITHUB_PR_LABELS"] || "")
    .split(",")
    .map((x) => x.trim())
    .filter((x) => x);

  if (!changedFiles?.length) {
    console.log("Doing git fetch and getting merge-base");
    const mergeBase = execSync(
      `git fetch origin ${process.env["GITHUB_PR_TARGET_BRANCH"]}; git merge-base origin/${process.env["GITHUB_PR_TARGET_BRANCH"]} HEAD`,
      { cwd: PROJECT_ROOT }
    )
      .toString()
      .trim();

    console.log(`Merge base: ${mergeBase}`);

    const changedFilesOutput = execSync(`git diff --name-only ${mergeBase}`, { cwd: PROJECT_ROOT }).toString().trim();

    changedFiles = changedFilesOutput
      .split("\n")
      .map((x) => x.trim())
      .filter((x) => x);

    console.log("Changed files (first 50):");
    console.log(changedFiles.slice(0, 50).join("\n"));
  }

  let filters: ((pipeline: EsPipeline) => boolean)[] = [
    (pipeline) => labelCheckAllow(pipeline, labels),
    (pipeline) => labelCheckSkip(pipeline, labels),
    (pipeline) => changedFilesExcludedCheck(pipeline, changedFiles),
    (pipeline) => changedFilesIncludedCheck(pipeline, changedFiles),
  ];

  // When triggering via the "run elasticsearch-ci/step-name" comment, we ONLY want to run pipelines that match the trigger phrase, regardless of labels, etc
  // However, if we're using the overall CI trigger "[buildkite] test this [please]", we should use the regular filters above
  if (
    process.env["GITHUB_PR_TRIGGER_COMMENT"] &&
    !process.env["GITHUB_PR_TRIGGER_COMMENT"].match(/^\s*(buildkite\s*)?test\s+this(\s+please)?/i)
  ) {
    filters = [triggerCommentCheck];
  }

  for (const filter of filters) {
    pipelines = pipelines.filter(filter);
  }

  for (const pipeline of pipelines) {
    doBwcTransforms(pipeline);
  }

  pipelines.sort((a, b) => (a.name ?? "").localeCompare(b.name ?? ""));

  const finalPipelines = pipelines.map((pipeline) => {
    const finalPipeline = { name: pipeline.name, pipeline: { ...pipeline } };
    delete finalPipeline.pipeline.config;
    delete finalPipeline.pipeline.name;

    return finalPipeline;
  });

  return finalPipelines;
};
