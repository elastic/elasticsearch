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

## Shared promcheck pipeline

The existing
[`pull-request-validate-prometheus-compliance.yml`](pipelines/pull-request-validate-prometheus-compliance.yml)
defines PR comparisons and nightly metrics in one YAML pipeline. Its top-level
`env` block is the single source for `PROMCHECK_VER`,
`PROMCHECK_TEST_INSTANCE_TIMEOUT`, and `PROMCHECK_CORPUS`. YAML anchors share the
worker agents and job timeout. Bash contains execution and reporting routines,
without configuration defaults.

The existing Buildkite pipeline and catalog resource retain their identity,
teams, and permissions. The daily schedule in
[`catalog-info.yaml`](../catalog-info.yaml) runs on `main` and selects the
`elastic / prometheus / nightly` step through `build.source == "schedule"`.
Ordinary PR builds run the early notification and merge-base comparison instead.
For a manual nightly build, set `PROMCHECK_MODE=nightly` on that same pipeline.
The conditions are complementary, so nightly builds skip PR notifications and
comparisons. The schedule becomes active when CI reconciles the catalog change.

Both job commands use
[`scripts/promcheck-common.sh`](scripts/promcheck-common.sh) for release downloads,
CI Gradle cache/scan options, agent-sized Elasticsearch/query concurrency, JSONL
execution, and summary validation. Each command retains its reporting policy:
PRs publish a regression comparison; nightly records one run in Overview.

## Prometheus nightly

The nightly step runs the YAML-pinned release and `grafanalabs-top100` corpus
against the checked-out Elasticsearch revision.

The destination is the existing Overview Elasticsearch cluster at
`https://overview-elastic-cloud-com.es.us-east-1.aws.found.io`, data stream
`synthetics-promql.support-experimental`. The namespace defaults to `experimental`
to keep trial measurements out of `synthetics-promql.support-default`. This is
separate from the performance-test cluster configured as `ES_URL` in the local shell profile. The runner uses only
`PROMCHECK_RESULTS_*` settings for uploads.

Before enabling the pipeline, provision the Vault secret
`secret/ci/elastic-elasticsearch/promcheck-results` with `api_key` (an encoded
Elasticsearch API key for Overview), and grant the pipeline access to it. The key
needs `create_doc` and `auto_configure` on `synthetics-promql.support-experimental`
to create the stream using the existing `synthetics-*-*` template. If the stream
and mappings are provisioned separately, only `create_doc` is needed. Use a CI
credential; keep local credentials such as `~/.config/elastic-benchmarks/overview-api-key` out of the repo.
No cluster administration privileges are needed.

The step accepts `PROMCHECK_RESULTS_SECRET` to select another Vault secret, or
`PROMCHECK_RESULTS_API_KEY` to supply the key through the CI secret environment.
`PROMCHECK_RESULTS_URL` overrides the Overview base URL; if it is unset, the secret
must also contain `url`. `PROMCHECK_RESULTS_NAMESPACE` selects
`synthetics-promql.support-<namespace>` and the matching `data_stream.namespace` field together; it is required and
never falls back to `default`. `PROMCHECK_VER`, `PROMCHECK_CORPUS`, and
`PROMCHECK_TEST_INSTANCE_TIMEOUT` configure the release, corpus asset name, and
Elasticsearch startup timeout in seconds. GitHub release access uses the existing `GH_TOKEN` supplied by the agent hook.

Each completed run follows the existing Overview document schema: `@timestamp`,
`status`, `dataset`, `git`, `promcheck`, `buildkite`, `data_stream`, and
`compliance`. Compliance contains `input_total`, `executed_total`, `executed_success`, `executed_failure`,
`executed_error`, `executed_skip`, and `success_pct` (matches divided by input
queries, rounded to one decimal). Input and inspected totals come directly from
the JSON summary, including request-filtered queries when present. HTTP errors
count as executed errors; flaky and response-filtered queries count as skips. `status: ok` means the run completed, including runs with
query failures. The `counts.json` artifact retains the detailed verdict breakdown.
Uploaded documents use the existing mapped fields.

This reuses the schema and create semantics of the earlier Overview submission
helper. That temporary backfill helper generated synthetic datapoints; this job
reports only the actual promcheck verdict. `dataset` is the release corpus name
(`grafanalabs-top100`), so it can be distinguished from the older
`promql_test_queries-grafana` series when comparing coverage.

For dashboard filtering, use the existing mapped field `data_stream.namespace`:

- Only experimental runs: `data_stream.namespace: "experimental"`
- Exclude experimental runs: `NOT data_stream.namespace: "experimental"`
- Narrow to this source: `buildkite.pipeline: "elasticsearch-pull-request-validate-prometheus-compliance"`

To compare namespaces, the Kibana data view must include
`synthetics-promql.support-*`. A data view restricted to
`synthetics-promql.support-default` continues to exclude the experimental stream.
The exclusion filter also preserves historical documents without a namespace
field. Moving to production requires explicitly changing the pipeline's
`PROMCHECK_RESULTS_NAMESPACE` to `default` and granting the corresponding access.

Uploads use `PUT /<data-stream>/_create/<build-and-corpus-id>`, as required for
writing to a data stream. A version conflict on retry means that result was
already created. Query failures (exit code 1) are indexed normally; an incomplete
run, missing or inconsistent verdict, or failed upload fails the step. Full query
results in JSONL remain in `promcheck.log`; diagnostics are saved separately in
`promcheck.stderr.log`. The artifacts also include `counts.json`, `result.json`,
`index-response.json`, and the Elasticsearch node log when available.

To verify both entry points without building Elasticsearch or contacting remote
services, use a Linux host with Bash 4.4+ (the PR reporter uses Bash namerefs):

```bash
cd .buildkite
node node_modules/vitest/vitest.mjs run scripts/promcheck-pipeline.test.ts scripts/prometheus-nightly.test.ts scripts/validate-prometheus-compliance.test.ts
```
