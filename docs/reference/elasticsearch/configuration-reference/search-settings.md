---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-settings.html
applies_to:
  deployment:
    self:
---

# Search settings [search-settings]

The following expert settings can be set to manage global search and aggregation limits.

$$$indices-query-bool-max-clause-count$$$

`indices.query.bool.max_clause_count`
:   :::{admonition} Deprecated in 8.0.0
    This deprecated setting has no effect.
    :::

    ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), integer) {{es}} will now dynamically set the maximum number of allowed clauses in a query, using a heuristic based on the size of the search thread pool and the size of the heap allocated to the JVM. This limit has a minimum value of 1024 and will in most cases be larger (for example, a node with 30Gb RAM and 48 CPUs will have a maximum clause count of around 27,000). Larger heaps lead to higher values, and larger thread pools result in lower values.

    Queries with many clauses should be avoided whenever possible. If you previously bumped this setting to accommodate heavy queries, you might need to increase the amount of memory available to {{es}}, or to reduce the size of your search thread pool so that more memory is available to each concurrent search.

    In previous versions of Lucene you could get around this limit by nesting boolean queries within each other, but the limit is now based on the total number of leaf queries within the query as a whole and this workaround will no longer help.


$$$search-settings-max-buckets$$$

`search.max_buckets`
:   ([Dynamic](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-put-settings), integer) Maximum number of [aggregation buckets](/reference/aggregations/bucket.md) allowed in a single response. Defaults to 65,536.

    Requests that attempt to return more than this limit will return an error.


$$$search-settings-only-allowed-scripts$$$

`search.aggs.only_allowed_metric_scripts`
:   ([Dynamic](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-put-settings), boolean) Configures whether only explicitly allowed scripts can be used in [scripted metrics aggregations](/reference/aggregations/search-aggregations-metrics-scripted-metric-aggregation.md). Defaults to `false`.

    Requests using scripts not contained in either [`search.aggs.allowed_inline_metric_scripts`](/reference/elasticsearch/configuration-reference/search-settings.md#search-settings-allowed-inline-scripts) or [`search.aggs.allowed_stored_metric_scripts`](/reference/elasticsearch/configuration-reference/search-settings.md#search-settings-allowed-stored-scripts) will return an error.


$$$search-settings-allowed-inline-scripts$$$

`search.aggs.allowed_inline_metric_scripts`
:   ([Dynamic](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-put-settings), list of strings) List of inline scripts that can be used in scripted metrics aggregations when [`search.aggs.only_allowed_metric_scripts`](#search-settings-only-allowed-scripts) is set to `true`. Defaults to an empty list.

    Requests using other inline scripts will return an error.


$$$search-settings-allowed-stored-scripts$$$

`search.aggs.allowed_stored_metric_scripts`
:   ([Dynamic](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-put-settings), list of strings) List of ids of stored scripts that can be used in scripted metrics aggregations when [`search.aggs.only_allowed_metric_scripts`](#search-settings-only-allowed-scripts) is set to `true`. Defaults to an empty list.

    Requests using other stored scripts will return an error.


$$$indices-query-bool-max-nested-depth$$$

`indices.query.bool.max_nested_depth`
:   ([Static](docs-content://deploy-manage/deploy/self-managed/configure-elasticsearch.md#static-cluster-setting), integer) Maximum nested depth of queries. Defaults to `30`.

    This setting limits the nesting depth of queries. Deep nesting of queries may lead to stack overflow errors.

The following search settings are supported:

* `search.aggs.rewrite_to_filter_by_filter`


