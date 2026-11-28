# Pull Request pipeline generator

## Overview

Each time a pull request build is triggered, such as via commit or comment, we use this generator to dynamically create the steps that are needed to run.

The generator handles the following:

  - Various configurations for filtering/activating steps based on labels, changed files, etc. See below.
  - Replacing `$SNAPSHOT_BWC_VERSIONS` in pipelines with an array of versions from `.ci/snapshotBwcVersions`
  - Duplicating any step with `bwc_template: true` for each BWC version in `.ci/bwcVersions`
  - Replacing `$LATER_BRANCHES` in pipelines with the development branches ahead of the pull
    request's target branch, read from `branches.json` on `main`. A pipeline using this placeholder is
    dropped entirely when nothing is ahead of the target branch, since an empty matrix dimension is invalid.

[Bun](https://bun.sh/) is used to test and run the TypeScript. It's an alternative JavaScript runtime that natively handles TypeScript.

### Merge queue suites

Some suites rarely catch breakage caused by the pull request itself, so they are not part of the default pull request set: `bwc-snapshots`, `later-branch-bwc`, `rest-compatibility` and `eql-correctness`, and the packaging samples only run when a pull request touches packaging. They are meant to run in the merge queue instead, through [`.buildkite/pipelines/merge-queue.yml`](../../pipelines/merge-queue.yml). The merge queue runs everything a pull request with no labels runs, selected with the same rules against the queue entry's changes, plus those suites with the rules they had before they moved, so it covers what pull request CI covered before. Its steps are generated from the same pull request pipeline files (see [`../merge-queue/pipeline.ts`](../merge-queue/pipeline.ts)). The merge queue pipeline is not registered in Buildkite yet.

Scripts written for pull requests get `MERGE_QUEUE_BASE_BRANCH` (the branch the entry merges into) and `MERGE_QUEUE_PULL_REQUEST` (the queued pull request's number) in a merge queue build, which has no pull request variables.

To run them on a pull request, add the `test-merge-queue` label, or comment `run elasticsearch-ci/<name>` for a single suite.

### Pipelines Location

Pipelines are in [`.buildkite/pipelines`](../../pipelines/pull-request). They are automatically picked up and given a name based on their filename.

## Setup

- [Install bun](https://bun.sh/)
  - `npm install -g bun` will work if you already have `npm`
- `cd .buildkite; bun install` to install dependencies

## Testing

Testing the pipeline generator is done mostly using snapshot tests, which generate pipeline objects using the pipeline configurations in `mocks/pipelines` and then compare them to previously-generated snapshots in `__snapshots__` to confirm that they are correct.

The mock pipeline configurations should, therefore, try to cover all of the various features of the generator (allow-labels, skip-labels, etc).

Snapshots are generated/managed automatically whenever you create a new test that has a snapshot test condition. These are very similar to Jest snapshots.

### Run tests

```bash
cd .buildkite
bun test
```

If you need to regenerate the snapshots, run `bun test --update-snapshots`.

## Pipeline Configuration

The `config:` property at the top of pipelines inside `.buildkite/pipelines/pull-request` is a custom property used by our pipeline generator. It is not used by Buildkite.

All of the pipelines in this directory are evaluated whenever CI for a pull request is started, and the steps are filtered and combined into one pipeline based on the properties in `config:` and the state of the pull request.

The various configurations available mirror what we were using in our Jenkins pipelines.

### Config Properties

#### `allow-labels`

- Type: `string|string[]`
- Example: `["test-full-bwc"]`

Only trigger a step if the PR has one of these labels.

#### `skip-labels`

- Type: `string|string[]`
- Example: `>test-mute`

Don't trigger the step if the PR has one of these labels.

#### `skip-target-branches`

- Type: `string|string[]`
- Example: `["main", "patch/serverless-fix"]`

Don't trigger the step if the PR targets one of these branches.

#### `excluded-regions`

- Type: `string|string[]` - must be JavaScript regexes
- Example: `["^docs/.*", "^x-pack/docs/.*"]`

Exclude the pipeline if all of the changed files in the PR match at least one regex. E.g. for the example above, don't run the step if all of the changed files are docs changes.

#### `included-regions`

- Type: `string|string[]` - must be JavaScript regexes
- Example: `["^docs/.*", "^x-pack/docs/.*"]`

Only include the pipeline if all of the changed files in the PR match at least one regex. E.g. for the example above, only run the step if all of the changed files are docs changes.

This is particularly useful for having a step that only runs, for example, when all of the other steps get filtered out because of the `excluded-regions` property.

Note that this is "all", not "any". Use `any-included-regions` if you want a step that runs whenever a PR touches certain files, even if it changes other files too.

#### `any-included-regions`

- Type: `string|string[]` - must be JavaScript regexes
- Example: `["^x-pack/plugin/esql/.*"]`

Only include the pipeline if at least one of the changed files in the PR matches at least one regex. E.g. for the example above, run the step for any PR that touches ES|QL, whether or not it also changes other files.

#### `trigger-phrase`

- Type: `string` - must be a JavaScript regex
- Example: `"^run\\W+elasticsearch-ci/test-full-bwc.*"`
- Default: `.*run\\W+elasticsearch-ci/<step-name>.*` (`<step-name>` is generated from the filename of the yml file).

Trigger this step, and ignore all other steps, if the build was triggered by a comment and that comment matches this regex.

Note that the entire build itself is triggered via [`.buildkite/pull-requests.json`](../pull-requests.json). So, a comment has to first match the trigger configured there.
