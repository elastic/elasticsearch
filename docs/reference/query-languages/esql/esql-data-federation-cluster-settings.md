---
navigation_title: "Cluster settings"
description: "Reference for ES|QL Data Federation cluster settings. Tune object limits, control request concurrency, and adjust file discovery and caching."
applies_to:
  stack: experimental 9.5+
  serverless: unavailable
products:
  - id: elasticsearch
---

# Configure cluster settings for {{esql}} Data Federation

Use these cluster settings to configure {{esql}} Data Federation, including its limits, concurrency, authentication, caching, and query defaults. For general guidance on how to apply cluster settings across deployment types, refer to [configure Elasticsearch](docs-content://deploy-manage/stack-settings.md).

Each setting is labeled Static or Dynamic. You can update a dynamic setting on a running cluster. A static setting takes effect after a node restart.

On {{ech}}, you can update dynamic settings with the cluster settings API, unless a setting can be updated only by operator users. You can't change static settings on {{ech}}, except `esql.federation.enabled`, which you add to your deployment's [user settings](docs-content://deploy-manage/deploy/elastic-cloud/edit-stack-settings.md).

:::{note}
Some of these settings were renamed. Where an entry lists a previous name, the badge on each name shows which versions accept it. Unless a note says otherwise, the previous name is no longer registered, and a node that still sets it in `elasticsearch.yml` fails to start.
:::

## Turn the feature on or off

This setting turns {{esql}} Data Federation on for a node.

$$$esql-federation-enabled$$$

`esql.federation.enabled`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) Enables {{esql}} Data Federation, including the data source and dataset APIs, on the node. On {{ech}}, add this setting to your deployment's [user settings](docs-content://deploy-manage/deploy/elastic-cloud/edit-stack-settings.md).

    - **Default:** `false`
    - **Valid values:** `true`, `false`

    {applies_to}`stack: experimental 9.6+` {{esql}} Data Federation isn't available on Windows. If this setting is `true` on a Windows node, {{es}} ignores it and logs an error at startup. The node still starts.

## Object limits

These settings cap the number of data sources and datasets a cluster can hold.

$$$esql-data-sources-max-count$$$

`esql.data_sources.max_count`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Maximum number of data sources that can be defined. When the {{operator-feature}} is enabled, only operator users can update this setting.

    - **Default:** `100`
    - **Valid values:** `0` to `1000`

$$$esql-datasets-max-count$$$

`esql.datasets.max_count`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Maximum number of datasets that can be defined. When the {{operator-feature}} is enabled, only operator users can update this setting.

    - **Default:** `1000`
    - **Valid values:** `0` to `10000`

## Request concurrency and throttling

These settings control how many concurrent requests each node sends to external storage and how long it retries throttled requests.

$$$esql-external-max-concurrent-requests$$$

`esql.external.max_concurrent_requests`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) Maximum concurrent cloud API requests per storage scheme, per node. A positive value is still capped by the memory term in the default, so an old explicit `16` can't skip the budget. On tiny heaps the parse floor of 4 can exceed that memory term. Tightening the request breaker also takes effect after a restart.

    - **Default:** `allocated processors * 3`, minimum 4 and maximum 100, further limited so concurrent 8 MiB reads stay within a quarter of heap (or half of `indices.breaker.request.limit` when tighter)
    - **Valid values:** `0` to `500`. `0` removes the permit limit.

$$$esql-external-throttle-max-retry-duration$$$

`esql.external.throttle_max_retry_duration`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) Maximum total time, in seconds, spent retrying throttled cloud API requests before failing the query.

    - **Default:** `30`
    - **Valid values:** `0` to `300`. `0` removes the budget.

$$$esql-external-max-concurrent-segmenters$$$

`esql.external.max_concurrent_segmenters` {applies_to}`stack: experimental 9.6+`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) Maximum number of file segmentation tasks that run concurrently.

    - {applies_to}`stack: experimental =9.5` **Previous name:** `esql.external.max_concurrent_segmentators`
    - **Default:** `0`, which derives the value automatically
    - **Valid values:** `0` to `4096`

$$$esql-external-admission-rescue-enabled$$$

`esql.external.admission.rescue.enabled` {applies_to}`stack: experimental 9.6+`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) When a byte-budget FIFO head has waited without a grant, admit it over the cap as a plain hold so the queue can progress. At most one rescued hold is live. Disable to observe a hang for diagnosis.

    - **Default:** `true`
    - **Valid values:** `true`, `false`

## Glob and file-discovery limits

These settings limit how many files and objects glob patterns can discover. Not all caps behave the same way when exceeded. Some abort the query, while others fall back to a slower discovery path.

$$$esql-external-max-listed-objects$$$

`esql.external.max_listed_objects` {applies_to}`stack: experimental 9.6+`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Hard cap on objects visited while listing a glob, including keys that don't match the pattern and keys dropped by exclusion. Applied independently to each glob listing. A comma-separated resource of N globs therefore does N listings, and a rewrite-empty fallback can list the same glob again. The kept-files cap (`esql.external.max_discovered_files`) is shared across that list.

    - **Default:** `1000000`
    - **Valid values:** `1` to `10000000`

$$$esql-external-max-discovered-files$$$

`esql.external.max_discovered_files`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Hard cap on files kept after listing filters (`_file.*`) during glob expansion before the query aborts. Protects against degenerate globs.

    - **Default:** `25000`
    - **Valid values:** `1` to `1000000`

$$$esql-external-max-glob-expansion$$$

`esql.external.max_glob_expansion`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Cap on concrete paths generated by brace expansion before falling back to listing.

    - **Default:** `100`
    - **Valid values:** `1` to `10000`

## Read safety

These settings limit how much CPU and read-thread time a single compressed object can consume. The first 1 MB of decompressed data is always allowed, and the ratio limit applies after that.

$$$esql-external-max-decompression-ratio$$$

`esql.external.max_decompression_ratio` {applies_to}`stack: experimental 9.6+`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Maximum ratio of decompressed to compressed bytes for gzip and other stream-only codecs. A read fails with a 400 error if the object expands beyond this multiple of its compressed size.

    - **Default:** `200`
    - **Valid values:** `0` to `100000`. `0` disables the check.

$$$esql-external-max-decompression-ratio-zstd$$$

`esql.external.max_decompression_ratio.zstd` {applies_to}`stack: experimental 9.6+`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Maximum decompression ratio for zstd-compressed objects. Overrides `esql.external.max_decompression_ratio` for zstd, which can legitimately reach higher ratios than gzip.

    - **Default:** `2000`
    - **Valid values:** `0` to `100000`. `0` disables the check for zstd.

$$$esql-external-schema-max-fields$$$

`esql.external.schema_max_fields` {applies_to}`stack: experimental 9.6+`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) Default maximum number of fields or columns a file's schema can have. For NDJSON, it counts objects as well as leaf fields. For CSV, TSV, and Parquet, it counts columns. If a file's schema exceeds this limit, the query fails with an HTTP 400 error. A dataset overrides it with its `schema_max_fields` setting.

    - **Default:** `1000`
    - **Valid values:** `1` to `100000`

    The same limit applies to the number of columns a dataset declares with `mappings`, and to the width of the schema merged from all files under `schema_resolution: union_by_name`. With `dynamic: false`, a declared schema is held to the limit by its number of declared columns rather than by the width of the files it reads, since only the declared columns are read. Parquet files wider than 100,000 columns can still be refused, because the planner reads a footer at that limit to check the declared types. With `dynamic: true`, each file's inferred schema is held to the limit as well.

## Authentication

These settings control which authentication modes data sources can use, and which endpoint hosts they can reach.

$$$esql-external-managed-identity-enabled$$$

`esql.external.managed_identity.enabled` {applies_to}`stack: experimental 9.6+`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Enables `auth: "managed_identity"`, which uses the node's own cloud identity through the instance metadata service (IMDS). Intended for single-cloud, single-tenant deployments. Never enable it in multi-tenant clusters. When the {{operator-feature}} is enabled, only operator users can update this setting. Refer to the managed identity row in [authentication models](esql-data-federation-sources.md#authentication) for guidance.

    - {applies_to}`stack: deprecated 9.6+, experimental =9.5` **Previous name:** `esql.datasource.managed_identity.enabled`, still accepted with a deprecation warning
    - {applies_to}`stack: deprecated 9.5+` **Previous name:** `esql.datasource.workload_identity.enabled`, still accepted with a deprecation warning
    - **Default:** `false`
    - **Valid values:** `true`, `false`

$$$esql-external-allowed-endpoint-hosts$$$

`esql.external.allowed_endpoint_hosts` {applies_to}`stack: experimental 9.6+`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) `host:port` patterns for endpoint hosts that data sources can use in addition to the supported AWS endpoints. A listed host is also accepted over plain `http` for `endpoint`. The `sts_endpoint` setting always requires `https`, because its host receives the node's OIDC token, so only list hosts on trusted network paths. For the endpoint rules, refer to [S3 endpoint requirements](esql-data-federation-data-source-settings.md#s3-endpoint-requirements).

    - **Default:** Empty
    - **Valid values:** A list of `host:port` patterns

## Caching

These settings control the external-source cache, which stores inferred schemas, file listings, and the footers of columnar files.

$$$esql-external-cache-enabled$$$

`esql.external.cache.enabled` {applies_to}`stack: experimental 9.6+`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Enables the external-source cache for inferred schemas and file listings.

    - {applies_to}`stack: deprecated 9.6+, experimental =9.5` **Previous name:** `esql.source.cache.enabled`, still accepted with a deprecation warning
    - **Default:** `true`
    - **Valid values:** `true`, `false`

$$$esql-external-cache-size$$$

`esql.external.cache.size` {applies_to}`stack: experimental 9.6+`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) Memory budget for the cache.

    - {applies_to}`stack: deprecated 9.6+, experimental =9.5` **Previous name:** `esql.source.cache.size`, still accepted with a deprecation warning
    - **Default:**
      - {applies_to}`stack: experimental 9.6+` 0.5% of heap
      - {applies_to}`stack: experimental =9.5` 0.4% of heap

$$$esql-source-cache-schema-ttl$$$

`esql.source.cache.schema.ttl` {applies_to}`stack: deprecated 9.5+`
:   Deprecated and ignored.

    {applies_to}`stack: experimental 9.6+` Use `esql.external.cache.schema.ttl` instead. It doesn't inherit this key's value.

$$$esql-external-cache-listing-ttl$$$

`esql.external.cache.listing.ttl` {applies_to}`stack: experimental 9.6+`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) How long a file listing stays fresh after it's written. A file added or removed becomes visible on the next query once this elapses. Lower it when changes must be visible sooner.

    - {applies_to}`stack: deprecated 9.6+, experimental =9.5` **Previous name:** `esql.source.cache.listing.ttl`, still accepted with a deprecation warning
    - **Default:**
      - {applies_to}`stack: experimental 9.6+` `5m`
      - {applies_to}`stack: experimental =9.5` `30s`

    {applies_to}`stack: experimental 9.6+` A file overwritten in place is also noticed then. A cached dataset row count also expires on this interval.

$$$esql-external-cache-schema-ttl$$$

`esql.external.cache.schema.ttl` {applies_to}`stack: experimental 9.6+`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) How long a schema inferred from a file is reused before it's inferred again. Statistics measured by reading a text file aren't covered: they're reused while their entry is held.

    - **Default:** `20m`
    - **Valid values:** A time value. `0` removes the limit on schemas, and the memoized dataset total stays on `esql.external.cache.listing.ttl`.

$$$esql-external-cache-footer-size$$$

`esql.external.cache.footer.size` {applies_to}`stack: experimental 9.6+`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) Memory budget for cached raw footer bytes (for example, Parquet footers), which are reused across the resolution, split discovery, and execution phases of a query and across back-to-back queries. The budget applies per columnar format reader.

    - **Default:** 0.5% of heap
    - **Valid values:** A percentage of heap or an absolute size, greater than zero

$$$esql-external-cache-footer-parsed-size$$$

`esql.external.cache.footer.parsed.size` {applies_to}`stack: experimental 9.6+`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) Memory budget for cached deserialized footers, which avoid re-parsing a footer in every query phase. A parsed footer costs several times its serialized form and grows with column count rather than file size, so raise this when querying wide schemas across large file sets. Applies per columnar format reader, like `esql.external.cache.footer.size`.

    - **Default:** 1% of heap
    - **Valid values:** A percentage of heap or an absolute size

$$$esql-external-cache-footer-ttl$$$

`esql.external.cache.footer.ttl` {applies_to}`stack: experimental 9.6+`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) How long a cached footer is kept after it was last read. Shared by the raw and parsed footer caches.

    - **Default:** `5m`
    - **Valid values:** A positive time value

    A footer that every dashboard refresh reads stays cached, within the budgets above. One that stops being read is released this long afterwards, and every entry is released at most six times this long after it was cached. This setting bounds memory, not freshness: footer entries are keyed by the object's version, so a rewritten object is read with its new footer. An object without a version isn't footer-cached.

$$$esql-external-cache-footer-coalesce$$$

`esql.external.cache.footer.coalesce` {applies_to}`stack: experimental 9.6+`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) When `true`, concurrent queries that load the same Parquet footer share one object-store GET.

    - **Default:** `true`
    - **Valid values:** `true`, `false`

:::{note}
:applies_to: stack: experimental 9.6+
The `esql.source.cache.*` keys are accepted as deprecated fallbacks and emit a deprecation warning. You can set them in `elasticsearch.yml`, but you can't update them through the cluster settings API. Use the `esql.external.cache.*` keys for new configuration.
:::

## Query defaults

These settings supply the default for an {{esql}} query setting when a query doesn't specify one.

$$$esql-query-settings-wildcards-match-datasets$$$

`esql.query.settings.wildcards_match_datasets` {applies_to}`stack: experimental 9.6+`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Whether a wildcard in `FROM` also matches registered datasets. When `false`, a wildcard resolves to indices, data streams, aliases, and views, and a dataset is reached by its exact name. A query overrides this in the `_query` request body or with `SET wildcards_match_datasets`. Refer to [query across datasets and indices](esql-data-federation-querying.md#query-across-datasets-and-indices).

    - **Default:** `false`
    - **Valid values:** `true`, `false`
