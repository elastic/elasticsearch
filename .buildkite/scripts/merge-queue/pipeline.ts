import { resolve } from "path";

import type { BuildkitePipeline } from "../pull-request/types.ts";
import {
  changedFilesAgainst,
  doBwcTransforms,
  generatePipelines,
  injectAutoRetry,
  loadPipeline,
  readDefaults,
  selectionFilters,
  type PipelineSelection,
} from "../pull-request/pipeline.ts";

const PROJECT_ROOT = resolve(`${import.meta.dirname}/../../..`);

/**
 * The pull request suites that moved off the pull request gate and into the merge queue.
 *
 * They rarely catch breakage caused by the pull request itself, so they run once against the queued merge commit
 * rather than on every push. On a pull request each one only runs on demand (the `test-merge-queue` label or its
 * `run elasticsearch-ci/<name>` comment) or, for the packaging samples, when the pull request touches packaging. The
 * queue ignores that gating (`allow-labels` and `any-included-regions`) and selects them with the rules they had
 * before they moved, such as skipping docs-only changes. The definitions live in `.buildkite/pipelines/pull-request`
 * so both places run exactly the same steps.
 */
export const MOVED_OFF_PULL_REQUESTS = [
  "bwc-snapshots",
  "later-branch-bwc",
  "rest-compatibility",
  "eql-correctness",
  "packaging-tests-unix-sample",
  "packaging-tests-windows-sample",
];

/**
 * Set on every merge queue step for scripts written for pull requests, which read the pull request variables a queue
 * build doesn't have: the branch the queue entry merges into, and the number of the queued pull request.
 */
export const MERGE_QUEUE_BASE_BRANCH_ENV = "MERGE_QUEUE_BASE_BRANCH";
export const MERGE_QUEUE_PULL_REQUEST_ENV = "MERGE_QUEUE_PULL_REQUEST";

const MERGE_QUEUE_BRANCH = /^gh-readonly-queue\/(.+)\/pr-(\d+)-[0-9a-f]+$/;

/**
 * The branch a merge queue build will merge into.
 *
 * GitHub builds each queue entry on a `gh-readonly-queue/<base>/pr-<number>-<sha>` branch. Any other branch is treated
 * as its own base, so the pipeline can also be started by hand against a development branch.
 */
export const getBaseBranch = (branch: string): string => branch.match(MERGE_QUEUE_BRANCH)?.[1] ?? branch;

/** The number of the queued pull request, or undefined for a branch that isn't a merge queue entry. */
export const getPullRequestNumber = (branch: string): string | undefined => branch.match(MERGE_QUEUE_BRANCH)?.[2];

/**
 * Everything pull request CI ran before suites moved off the pull request gate: every pipeline a pull request with no
 * labels runs, selected with the pull request rules against the queue entry's changes (so path-gated pipelines run
 * when the entry touches their paths), plus the suites in MOVED_OFF_PULL_REQUESTS without their pull request gating.
 *
 * @param changedFiles the queue entry's changes against its base branch; empty means compute them with git
 */
export const generateMergeQueuePipeline = (
  baseBranch: string,
  pullRequest: string | undefined,
  changedFiles: string[] = [],
  directory: string = `${PROJECT_ROOT}/.buildkite/pipelines/pull-request`,
  movedOffPullRequests: string[] = MOVED_OFF_PULL_REQUESTS,
): BuildkitePipeline => {
  const files = changedFiles.length > 0 ? changedFiles : changedFilesAgainst(baseBranch);
  const selection: PipelineSelection = { targetBranch: baseBranch, labels: [], triggerComment: undefined };
  const pullRequestDefault = generatePipelines(directory, files, selection);
  const selected = new Set(pullRequestDefault.map((p) => p.name));

  const defaults = readDefaults(directory);
  const filters = selectionFilters(files, selection);
  const moved = movedOffPullRequests.flatMap((name) => {
    if (selected.has(name)) {
      return [];
    }
    const pipeline = loadPipeline(directory, `${name}.yml`, defaults, baseBranch);
    if (!pipeline) {
      return [];
    }
    // the gating that moved the suite off pull requests; what remains is how pull requests selected it before
    delete pipeline.config?.["allow-labels"];
    delete pipeline.config?.["any-included-regions"];
    if (!filters.every((filter) => filter(pipeline))) {
      return [];
    }
    doBwcTransforms(pipeline);
    injectAutoRetry(pipeline);
    const { config: _config, name: _name, ...uploaded } = pipeline;
    return [{ name, pipeline: uploaded }];
  });

  const pipelines = [...pullRequestDefault, ...moved].sort((a, b) => (a.name ?? "").localeCompare(b.name ?? ""));

  // The scripts pull requests run read the target branch from the pull request variables, which a queue build lacks
  let env: Record<string, string> = {
    GITHUB_PR_TARGET_BRANCH: baseBranch,
    [MERGE_QUEUE_BASE_BRANCH_ENV]: baseBranch,
    ...(pullRequest ? { [MERGE_QUEUE_PULL_REQUEST_ENV]: pullRequest } : {}),
  };
  for (const { pipeline } of pipelines) {
    env = { ...(pipeline.env ?? {}), ...env };
  }
  return { env, steps: pipelines.flatMap(({ pipeline }) => pipeline.steps ?? []) };
};
