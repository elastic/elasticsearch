import { parse, stringify } from "yaml";
import { readFileSync, readdirSync } from "fs";
import { basename } from "path";
import { execSync } from "child_process";

process.env["GITHUB_PR_LABELS"] =
  "release_note:skip,:Delivery/Packaging,v8.11.0";
process.env["GITHUB_PR_TARGET_BRANCH"] = "main";
// process.env["GITHUB_PR_TRIGGER_COMMENT"] =
//   "hey run elasticsearch-ci/build-benchmarks please and run elasticsearch-ci/part-2";

let defaults: any = {};
defaults = parse(
  readFileSync(".buildkite/pipelines/pull-request/.defaults.yml").toString()
);
defaults["config"] = defaults.config || {};

let pipelines: any[] = [];
const files = readdirSync(".buildkite/pipelines/pull-request");
for (const file of files) {
  if (!file.endsWith(".yml") || file.endsWith(".defaults.yml")) {
    continue;
  }

  const yaml = readFileSync(
    `.buildkite/pipelines/pull-request/${file}`,
    "utf-8"
  );
  const pipeline: any = parse(yaml) || {};
  pipeline["config"] = { ...defaults["config"], ...(pipeline["config"] || {}) };
  pipelines.push(pipeline);
  const name = basename(file).split(".", 2)[0];
  pipeline["name"] = name;
  pipeline["config"]["trigger-phrase"] =
    pipeline["config"]["trigger-phrase"] ||
    `.*run\\W+elasticsearch-ci/${name}.*`;
}

const labels = (process.env["GITHUB_PR_LABELS"] || "")
  .split(",")
  .map((x) => x.trim())
  .filter((x) => x);

const mergeBase = execSync(
  `git merge-base ${process.env["GITHUB_PR_TARGET_BRANCH"]} HEAD`
)
  .toString()
  .trim();

const changedFilesOutput = execSync(`git diff --name-only ${mergeBase}`)
  .toString()
  .trim();

const changedFiles = changedFilesOutput
  .split("\n")
  .map((x) => x.trim())
  .filter((x) => x);

const getArray = (strOrArray: string | string[]): string[] => {
  return typeof strOrArray === "string" ? [strOrArray] : strOrArray;
};

const labelCheckAllow = (pipeline: any): boolean => {
  if (pipeline["config"]["allow-labels"]) {
    return getArray(pipeline["config"]["allow-labels"]).some((label) =>
      labels.includes(label)
    );
  }
  return true;
};

const labelCheckSkip = (pipeline: any): boolean => {
  if (pipeline["config"]["skip-labels"]) {
    return !getArray(pipeline["config"]["skip-labels"]).some((label) =>
      labels.includes(label)
    );
  }
  return true;
};

// Exclude the pipeline if all of the changed files in the PR are in at least one excluded region
const changedFilesExcludedCheck = (pipeline: any): boolean => {
  if (pipeline["config"]["excluded-regions"]) {
    return !changedFiles.every((file) =>
      getArray(pipeline["config"]["excluded-regions"]).some((region) =>
        file.match(region)
      )
    );
  }
  return true;
};

// Include the pipeline if all of the changed files in the PR are in at least one included region
const changedFilesIncludedCheck = (pipeline: any): boolean => {
  if (pipeline["config"]["included-regions"]) {
    return changedFiles.every((file) =>
      getArray(pipeline["config"]["included-regions"]).some((region) =>
        file.match(region)
      )
    );
  }
  return true;
};

const triggerCommentCheck = (pipeline: any): boolean => {
  if (
    process.env["GITHUB_PR_TRIGGER_COMMENT"] &&
    pipeline["config"]["trigger-phrase"]
  ) {
    return !!process.env["GITHUB_PR_TRIGGER_COMMENT"].match(
      pipeline["config"]["trigger-phrase"]
    );
  }
  return false;
};

let filters: ((pipeline: any) => boolean)[] = [
  labelCheckAllow,
  labelCheckSkip,
  changedFilesExcludedCheck,
  changedFilesIncludedCheck,
];

// When triggering via comment, we ONLY want to run pipelines that match the trigger phrase, regardless of labels, etc
if (process.env["GITHUB_PR_TRIGGER_COMMENT"]) {
  filters = [triggerCommentCheck];
}

for (const filter of filters) {
  pipelines = pipelines.filter(filter);
}

const finalPipeline: any = { steps: [] };

// TODO should we just do a pipeline upload on each individual yaml? so that they are isolated, can use env:, etc?
// Remove our custom attributes before outputting the Buildkite YAML
for (const pipeline of pipelines) {
  delete pipeline["config"];
  delete pipeline["name"];
  finalPipeline["steps"] = [
    ...finalPipeline["steps"],
    ...(pipeline["steps"] || []),
  ];
}

console.log(stringify(finalPipeline));
