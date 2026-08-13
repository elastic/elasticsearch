---
navigation_title: "Query datasets"
description: "Query external data with ES|QL Data Federation. Learn how the engine reduces storage reads, query external and indexed data together, and troubleshoot common issues."
applies_to:
  stack: experimental =9.5
  serverless: unavailable
products:
  - id: elasticsearch
---

# Query external datasets with {{esql}} Data Federation

A dataset is a read source for the standard {{esql}} pipeline. You query it with `FROM` like an index, and every processing command works the same way it does for an index.

```esql
FROM my_dataset
```

For a hands-on example, refer to [get started with {{esql}} Data Federation](esql-data-federation-quickstart.md).

## How external data is read

When you query a dataset, {{es}} reads data from object storage (such as Amazon S3) rather than from a local index. This means every column and every row that a query touches results in network I/O. The query engine applies several optimizations automatically, and there are things you can do to help it read less data.

### Column selection

Use [`KEEP`](/reference/query-languages/esql/commands/keep.md) or [`DROP`](/reference/query-languages/esql/commands/drop.md) to select only the columns your query needs. For Parquet files, column selection pushes down to the reader so that unrequested columns are never fetched from storage. For CSV and NDJSON, the full row is read but unrequested columns are discarded early.

In practice, this can make a significant difference. A filtered query over a Parquet dataset that selects three columns reads roughly a third of the bytes that the same query reads without column selection.

### Partition pruning

When a dataset's resource path uses Hive-style partitioning (for example, `year=2024/month=3/`), the engine detects partition keys automatically and promotes them to queryable columns. A `WHERE` condition on a partition column evaluates during file discovery, before any data is read. On a two-year monthly-partitioned dataset, `WHERE year = 2024 AND month = 3` skips 23 out of 24 partitions at zero I/O cost.

Pruning applies when the partition filter comes before any `LIMIT`, `SORT`, or `STATS` in the query. If one of those commands sits between `FROM` and the `WHERE` on a partition column, pruning is silently skipped and every partition is read. Try to put partition filters first.

For details on partition detection modes, refer to [dataset settings](esql-data-federation-datasets.md#common-settings).

### Filter and limit pushdown

[`WHERE`](/reference/query-languages/esql/commands/where.md) conditions and [`LIMIT`](/reference/query-languages/esql/commands/limit.md) reduce how much data a query reads, but how far they push down depends on the format. For Parquet files, filters push down into the reader itself: the engine uses row-group statistics and page indexes to skip data that cannot match the filter. Only row groups whose statistics overlap the filter condition are read, and within those row groups, late materialization reads predicate columns first and materializes other columns only for rows that survive the filter.

For CSV and NDJSON, filters do not reach the reader: every row must be read and parsed, but rows that fail the filter are discarded before further processing.

```esql
FROM access_logs
| WHERE status_code >= 500
| KEEP @timestamp, status_code, request_path
| LIMIT 100
```

The general query performance advice in [optimize {{esql}} query performance](esql-query-performance.md) applies to datasets too. In particular, adding a `WHERE`, a `KEEP`, and a `LIMIT` are the three most effective ways to reduce how much data a query reads from storage.

### Caching

{{es}} caches file metadata (schemas and file listings) so that repeated queries against the same dataset do not re-discover files each time. Cached schemas are invalidated when the underlying files change, so a schema stays cached for as long as it stays correct. There is no schema TTL. Only the file-listing cache uses a TTL (30 seconds by default) configurable through [cluster settings](esql-data-federation-cluster-settings.md).

### File discovery limits

A dataset's resource path can use glob patterns to match many files. Two cluster settings bound file discovery:

- `esql.external.max_discovered_files` (default 10,000): the maximum number of files a single dataset can resolve to.
- `esql.external.max_glob_expansion` (default 100): the maximum number of concrete paths a brace pattern (`{a,b,c}`) expands to. Past this cap, the engine falls back to listing the storage instead of failing.

If your dataset exceeds these limits, narrow the resource path or adjust the settings. Refer to [cluster settings](esql-data-federation-cluster-settings.md) for details.

## Query across datasets and indices

Datasets share the same namespace as indices, data streams, aliases, and [{{esql}} views](esql-views.md), so `FROM` resolves each name independently.

```esql
FROM speedtest_data, network_incidents METADATA _index
| KEEP _index, category, severity, avg_d_kbps, avg_lat_ms
| LIMIT 10
```

When sources have different schemas, columns that do not exist in a given source return `null` for rows from that source. Use `METADATA _index` to see which source each row came from. The `_index` column returns the dataset name for dataset rows and the index name for index rows.

## Use metadata columns

[Metadata columns](/reference/query-languages/esql/esql-metadata-fields.md) are available using the `METADATA` directive:

| Column | Returned for a dataset |
|---|---|
| `_index` | The dataset name. |
| `_id` | A stable per-row identifier. |
| `_version` | The source file's modification time as a `long` in epoch milliseconds, or null when storage reports no modification time. |
| `_source` | The row as a JSON object. |
| `_file.path`, `_file.name`, `_file.directory`, `_file.size`, `_file.modified` | The object each row was read from. |
| `_score` | null |
| `_ignored` | null |
| `_index_mode`, `_tsid`, `_size` | null |

For example, this query returns file-level metadata for each matching row:

```esql
FROM access_logs METADATA _file.path, _file.name, _file.size
| KEEP _file.path, _file.name, _file.size, status_code
| LIMIT 10
```

## Use search functions

[Search functions](/reference/query-languages/esql/functions-operators/search-functions.md) can filter dataset rows by evaluating the query against values read from the files. This runtime search does not use an inverted index. When using `METADATA _score`, `MATCH` and `MATCH_PHRASE` on dataset rows contribute to the relevance score based on the `boost` option and the query terms matched — not BM25, as there are no index statistics for a dataset. {applies_to}`stack: preview 9.6` In earlier versions, dataset rows do not contribute to `_score`.

Because there is no inverted index, search functions on a dataset evaluate by scanning values row by row. For large datasets where search is the primary access pattern, consider ingesting the data into {{es}} for indexed search performance.

Runtime `MATCH` on a dataset requires the query value's type to match the field's type. Text is analyzed with the standard analyzer unless a values analyzer is declared through [`TO_TEXT`](functions-operators/type-conversion-functions/to_text.md)'s `analyzer` option; `MATCH`'s own `analyzer` option applies to the query string only, defaulting to the values analyzer. {applies_to}`stack: preview 9.6`

The following search functions are available for datasets:

| Function | Stack |
|---|---|
| [`MATCH`](functions-operators/search-functions/match.md) | {applies_to}`stack: experimental 9.5` |
| [`MATCH_PHRASE`](functions-operators/search-functions/match_phrase.md) | {applies_to}`stack: experimental 9.6` |
| `_score` for dataset rows | Not yet available |

## Limitations

:::{include} _snippets/data-federation/experimental-warning.md
:::

The operations below require structures that only exist in an {{es}} index, such as the inverted index, doc values, or time series metadata. Each fails with a clear error rather than wrong results.

| Operation | Reason | Error |
|---|---|---|
| `LOOKUP JOIN`, with a dataset as the lookup target | A dataset works as the left (source) side of the join. The lookup target must be an {{es}} index. | `LOOKUP JOIN against a dataset is not supported; dataset(s) requested: [...]` |
| `TS` (time series) | A time-series source must be an {{es}} index. | `TS command is not supported for datasets; dataset(s) requested: [...]` |
| Search functions | Search functions work on datasets as runtime search functions, scanning values row by row without an inverted index. Availability varies by version and deployment type. Refer to the [availability table](#use-search-functions). | `… cannot operate on [<field>], which is not a field from an index mapping (the source is a federated data source, not an index)` |
| `KNN` | `KNN` requires a vector field from an index mapping, which a dataset does not have. | `… cannot operate on [<field>], which is not a field from an index mapping (the source is a federated data source, not an index)` |
| More than 8 sources resolved in one `FROM` | A `FROM` that includes datasets runs one execution branch per resolved source, up to a limit of 8 branches. Query fewer sources together. | |
| A column with conflicting types across sources | When you query a dataset together with other sources and the same column has types that cannot be reconciled, the query fails rather than returning mixed types. | `Column [<name>] has conflicting data types in subqueries` |
| Document-level security (DLS) and field-level security (FLS) | A dataset's `read` grant cannot carry document- or field-level security. Queries where DLS or FLS applies to a dataset are rejected during authorization. The same check covers [{{esql}} views](esql-views.md). | `Datasets with document or field level security restrictions are not supported. Remove DLS/FLS restrictions from the affected datasets in the role definition, or exclude them from the request.` |
| [Cross-cluster search](/reference/query-languages/esql/esql-cross-clusters.md) | Datasets on a remote cluster cannot be queried. Only local datasets are supported. | `ES\|QL queries with remote datasets are not supported. Matched [...]` |
| Snapshot and restore | Data sources and datasets cannot be snapshotted or restored. | |
| Parquet MAP and nested LIST | These complex types are not currently supported and return null. STRUCT is supported and flattened to dot-notation column names (for example, `address.city`). | |

## Troubleshooting

If a query against a dataset returns unexpected results or errors, check the following common causes.

Unexpected nulls in query results
:   If you query a dataset and an index together with `FROM`, columns that do not exist in one source return null for rows from that source. Use `METADATA _index` to check which source each row came from. Separately, complex Parquet types MAP and nested LIST return null because they are not currently supported.

Slow queries
:   Add [`KEEP`](/reference/query-languages/esql/commands/keep.md) to select only the columns you need, add a [`WHERE`](/reference/query-languages/esql/commands/where.md) filter, and add a [`LIMIT`](/reference/query-languages/esql/commands/limit.md). For Parquet datasets, these push down to the reader and can significantly reduce the amount of data read from storage. Check the number of files your dataset's resource path resolves to. Large file counts increase query planning time.

503 error when creating a data source with credentials
:   {{es}} encrypts credentials before storing them. If the cluster state encryption key is not available, the request returns `503 SERVICE_UNAVAILABLE`. Refer to [credential encryption](esql-data-federation-security.md#credential-encryption) for details.

New files not appearing in query results
:   {{es}} caches file listings for each dataset. If you recently added files to your bucket, they might not appear until the listing cache expires. The default listing cache TTL is 30 seconds. Refer to [cluster settings](esql-data-federation-cluster-settings.md) to adjust it.

Columns with unexpected types or missing values
:   When {{es}} infers a dataset's schema from its files, it might infer types differently than you expect. For example, a date column might appear as a keyword if the values do not match the default datetime format. To inspect the inferred field mappings, refer to [check field mappings](esql-data-federation-quickstart.md#check-field-mappings) in the quickstart. Use dataset [mappings](esql-data-federation-datasets.md#declare-a-dataset-mapping) to declare column types explicitly, or adjust the [`datetime_format`](esql-data-federation-datasets.md#csv-and-tsv-settings) setting. If some rows have null values for a column that exists in other files, check the dataset's [`schema_resolution`](esql-data-federation-datasets.md#schema-merge-strategies) setting.

Access denied or connection errors
:   Credential and permission errors appear at query time, not when the data source is created. If a query returns an access denied error, verify that the credentials in the data source have the required permissions (such as `s3:ListBucket` and `s3:GetObject`) and that the region is correct.

## Next steps

- To adjust caching TTLs, file-discovery limits, or request concurrency, refer to [cluster settings](esql-data-federation-cluster-settings.md).
- To control column types or rename columns, declare [dataset mappings](esql-data-federation-datasets.md#declare-a-dataset-mapping).
- For general {{esql}} tuning advice that also applies to datasets, refer to [optimize {{esql}} query performance](esql-query-performance.md).
