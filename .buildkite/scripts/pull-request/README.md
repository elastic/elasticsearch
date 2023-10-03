# Pull Request pipeline generator

## Overview

Each time a pull request build is triggered, such as via commit or comment, we use this generator to dynamically create the steps that are needed to run.

The generator handles the following:

  - `allow-labels` - only trigger a step if the PR has one of these labels
  - `skip-labels` - don't trigger the step if the PR has one of these labels
  - `excluded-regions` - don't trigger the step if **all** of the changes in the PR match these paths/regexes
  - `included-regions` - trigger the step if **all** of the changes in the PR match these paths/regexes
  - `trigger-phrase` - trigger this step, and ignore all other steps, if the build was triggered by a comment and that comment matches this regex
    - Note that each step has an automatic phrase of `.*run\\W+elasticsearch-ci/<step-name>.*`
  - Replacing `$SNAPSHOT_BWC_VERSIONS` in pipelines with an array of versions from `.ci/snapshotBwcVersions`
  - Duplicating any step with `bwc_template: true` for each BWC version in `.ci/bwcVersions`

[Bun](https://bun.sh/) is used to test and run the TypeScript. It's an alternative JavaScript runtime that natively handles TypeScript.

### Pipelines Location

Pipelines are in [`.buildkite/pipelines`](../../pipelines/pull-request). They are automatically picked up and given a name based on their filename.


## Setup

- [Install bun](https://bun.sh/)
  - `npm install -g bun` will work if you already have `npm`
- `cd .buildkite; bun install` to install dependencies

## Run tests

```bash
cd .buildkite
bun test
```

If you need to regenerate the snapshots, run `bun test --update-snapshots`.
