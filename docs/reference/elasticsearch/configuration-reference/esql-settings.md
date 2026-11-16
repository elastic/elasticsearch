---
navigation_title: "ES|QL"
applies_to:
  deployment:
    self:
---

# {{esql}} settings [esql-settings]

The following settings apply to {{esql}} queries. The spilling settings take effect only when a spill store is
installed; the `esql-spill-disk` module, bundled with {{es}}, provides one for local disk. Spilling is not available
on {{serverless-full}}.

Except where noted otherwise, these settings can be dynamically updated on a live cluster with the
[cluster-update-settings](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-put-settings)
API. A change to a dynamic spilling setting applies to queries started after it.

## Spilling settings [esql-spill-settings]

$$$esql-spill-enabled$$$

`esql.spill.enabled`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) Whether {{esql}} operators
    may write intermediate state to the spill store when a query's memory on a node reaches its budget, instead of
    failing the query. Applies to grouped aggregations, `LIMIT ... BY`, `DEDUP`, `SORT` with a `LIMIT`, and the
    collection of a query's results on the coordinating node. Defaults to `false`. A query that started with
    spilling enabled finishes its spills and reads them back even if the setting is turned off while it runs; a
    query that started with it disabled never spills.

$$$esql-spill-store$$$

`esql.spill.store`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) The name of the spill store
    to use. Defaults to `disk`, provided by the `esql-spill-disk` module. A node configured with a name no
    installed store answers to fails to start.

$$$esql-spill-query-budget$$$

`esql.spill.query_budget`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) The share of the request
    circuit breaker's limit, `indices.breaker.request.limit`, that one query may use on a node before its operators
    spill, as a percentage. Defaults to `50%`, so that two queries at their budget fit under the breaker together.
    The breaker remains the limit for all requests on the node, so more queries than that can still be stopped by
    it. An operator holding less than 16 MB of state never spills.

$$$esql-spill-result-budget$$$

`esql.spill.result_budget`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) The share of the request
    circuit breaker's limit that a query's result may occupy in memory on the coordinating node before the rest of
    it goes to the spill store, from where the response is read as it is sent, as a percentage. Defaults to `25%`.
    Applies to synchronous queries; an asynchronous query kept on completion cannot store a result that went to
    the spill store.

$$$esql-spill-disk-path$$$

`esql.spill.disk.path`
:   ([Static](docs-content://deploy-manage/stack-settings.md#static-cluster-setting)) The directory the disk spill
    store writes to. Defaults to `esql_spill` under the first data path. The node writes into a subdirectory named
    after its node id and deletes that subdirectory's contents when it starts. The directory must not be a data
    path, a directory under one, or a symbolic link. Spilled data is not encrypted; use a volume whose protection
    matches the data path's.

$$$esql-spill-disk-cap$$$

`esql.spill.disk.cap`
:   ([Dynamic](docs-content://deploy-manage/stack-settings.md#dynamic-cluster-setting)) The most spilled data the
    node keeps at once, as an absolute size such as `50gb` or a percentage of the spill path's file system such as
    `10%`. A query whose spill would exceed it fails with an error naming this setting; other queries are not
    affected. Lowering it below the bytes in use fails no running query, it only refuses further spills until space
    is returned. Defaults to `10%`.
