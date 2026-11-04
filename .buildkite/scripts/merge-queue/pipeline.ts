import { resolve } from "path";

import type { BuildkitePipeline } from "../pull-request/types.ts";
import {
  checkTargetBranch,
  doBwcTransforms,
  injectAutoRetry,
  loadPipeline,
  readDefaults,
} from "../pull-request/pipeline.ts";

const PROJECT_ROOT = resolve(`${import.meta.dirname}/../../..`);

/**
 * The pull request suites that run in the merge queue instead of on every pull request.
 *
 * They rarely catch breakage caused by the pull request itself, so they run once against the queued merge commit
 * rather than on every push. Each one is still available on a pull request on demand, through the
 * `test-merge-queue` label or its `run elasticsearch-ci/<name>` comment. The definitions live in
 * `.buildkite/pipelines/pull-request` so both places run exactly the same steps.
 */
export const MERGE_QUEUE_PIPELINES = [
  "bwc-snapshots",
  "later-branch-bwc",
  "rest-compatibility",
  "eql-correctness",
  "packaging-tests-unix-sample",
  "packaging-tests-windows-sample",
];

const MERGE_QUEUE_BRANCH = /^gh-readonly-queue\/(.+)\/pr-\d+-[0-9a-f]+$/;

/**
 * The branch a merge queue build will merge into.
 *
 * GitHub builds each queue entry on a `gh-readonly-queue/<base>/pr-<number>-<sha>` branch. Any other branch is treated
 * as its own base, so the pipeline can also be started by hand against a development branch.
 */
export const getBaseBranch = (branch: string): string => branch.match(MERGE_QUEUE_BRANCH)?.[1] ?? branch;

export const generateMergeQueuePipeline = (
  baseBranch: string,
  directory: string = `${PROJECT_ROOT}/.buildkite/pipelines/pull-request`,
  pipelineNames: string[] = MERGE_QUEUE_PIPELINES,
): BuildkitePipeline => {
  const defaults = readDefaults(directory);

  // later-branch-bwc.sh reads the base branch from the pull request variables, which a merge queue build doesn't have
  let env: Record<string, string> = { GITHUB_PR_TARGET_BRANCH: baseBranch };
  const steps = pipelineNames.flatMap((name) => {
    // Labels, changed files and trigger phrases only make sense for pull requests: the queue runs every suite.
    const pipeline = loadPipeline(directory, `${name}.yml`, defaults, baseBranch);
    if (!pipeline || !checkTargetBranch(pipeline, baseBranch)) {
      return [];
    }

    doBwcTransforms(pipeline);
    injectAutoRetry(pipeline);
    env = { ...pipeline.env, ...env };
    return pipeline.steps ?? [];
  });

  return { env, steps };
};
