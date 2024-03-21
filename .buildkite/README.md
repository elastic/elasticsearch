# Elasticsearch CI Pipelines

This directory contains pipeline definitions and scripts for running Elasticsearch CI on Buildkite.

## Directory Structure

- [pipelines](pipelines/) - pipeline definitions/yml
- [scripts](scripts/) - scripts used by pipelines, inside steps
- [hooks](hooks/) - [Buildkite hooks](https://buildkite.com/docs/agent/v3/hooks), where global env vars and secrets are set

## Pipeline Definitions

Pipelines are defined using YAML files residing in [pipelines](pipelines/). These are mostly static definitions that are used as-is, but there are a few dynamically-generated exceptions (see below).

### Dynamically Generated Pipelines

Pull request pipelines are generated dynamically based on labels, files changed, and other properties of pull requests.

Non-pull request pipelines that include BWC version matrices must also be generated whenever the [list of BWC versions](../.ci/bwcVersions) is updated.

#### Pull Request Pipelines

Pull request pipelines are generated dynamically at CI time based on numerous properties of the pull request. See [scripts/pull-request](scripts/pull-request) for details.

#### BWC Version Matrices

For pipelines that include BWC version matrices, you will see one or more template files (e.g. [periodic.template.yml](pipelines/periodic.template.yml)) and a corresponding generated file (e.g. [periodic.yml](pipelines/periodic.yml)). The generated file is the one that is actually used by Buildkite.

These files are updated by running:

```bash
./gradlew updateCIBwcVersions
```

This also runs automatically during release procedures.

You should always make changes to the template files, and run the above command to update the generated files.

## Node / TypeScript

Node (technically `bun`), TypeScript, and related files are currently used to generate pipelines for pull request CI. See [scripts/pull-request](scripts/pull-request) for details.
