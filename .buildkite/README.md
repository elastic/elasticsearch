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

## Periodic promcheck

[`periodic-promcheck.yml`](pipelines/periodic-promcheck.yml) runs the pinned promcheck
release against the checked-out Elasticsearch revision using the `grafanalabs-top100`
release corpus. The pipeline and its daily `main` schedule are declared in
[`catalog-info.yaml`](../catalog-info.yaml); the schedule becomes active when the
CI pipeline configuration is reconciled from that catalog.

Before enabling the pipeline, provision the Vault secret
`secret/ci/elastic-elasticsearch/promcheck-results` with `url` (the remote cluster's
HTTPS base URL) and `api_key` (an encoded Elasticsearch API key), and grant the
pipeline access to it. The destination index defaults to `promcheck-results`.
Grant the key `index` and `auto_configure` privileges on that index, or pre-create
the index and its mappings and grant only `index`. No cluster administration
privileges are needed.

The step accepts `PROMCHECK_RESULTS_SECRET` to select another Vault secret, or
`PROMCHECK_RESULTS_URL` and `PROMCHECK_RESULTS_API_KEY` to supply the connection
through the CI secret environment. Keep credentials out of committed YAML.
`PROMCHECK_RESULTS_INDEX`, `PROMCHECK_VER`, `PROMCHECK_CORPUS`, and
`PROMCHECK_TEST_INSTANCE_TIMEOUT` configure the index, release, corpus asset name,
and Elasticsearch startup timeout in seconds. GitHub release access uses the
existing `GH_TOKEN` supplied by the agent hook.

Each completed run writes one document containing `@timestamp`, `commit`, `branch`,
`promcheck_version`, `corpus`, Buildkite build/job identifiers and build URL,
`duration_seconds`, `exit_code`, and numeric `counts` (`total`, `match`, `mismatch`,
`http4xx`, `http5xx`, `error`, `response_filtered`, `flaky`). A stable build/corpus
document ID prevents duplicate documents if an upload is retried. Query failures
(exit code 1) are indexed normally; an incomplete run, missing or inconsistent
verdict, or failed upload fails the step. Full query diagnostics remain in the
`promcheck.log` artifact alongside `result.json` and `index-response.json`.

To verify the runner without building Elasticsearch or contacting remote services:

```bash
cd .buildkite
node node_modules/vitest/vitest.mjs run scripts/periodic-promcheck.test.ts
```
