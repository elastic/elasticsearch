---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/index-modules-slowlog.html
navigation_title: Slow log
applies_to:
  stack: all
---

# Slow log settings [index-modules-slowlog]

:::{include} _snippets/serverless-availability.md
:::

The slow log in {{es}} helps you capture search or indexing operations that exceed certain durations, which can be useful when diagnosing slow performance or indexing bottlenecks. For more information about slow log formats and how to enable slow logs, refer to [Slow query and index logging](docs-content://deploy-manage/monitor/logging-configuration.md).

Slow logs are emitted under `fileset.name = "slowlog"`, logger names `index.search.slowlog` or `index.indexing.slowlog`.

By default, all thresholds default to `-1`, which disables slow logging.


## Settings for search operations

Search slow logs emit per shard. They must be enabled separately for the shard’s [query and fetch search phases](https://www.elastic.co/blog/understanding-query-then-fetch-vs-dfs-query-then-fetch).


`index.search.slowlog.threshold.query.<level>`
:   Sets the minimum threshold for logging slow query-phase operations. `<level>` can be `warn`, `info`, `debug`, and `trace`. When a query takes longer than the configured value, a slow log entry is emitted. 
:::{dropdown} Example 
The following request enables slow logs by configuring thresholds:

  ```console
  PUT /my-index-000001/_settings
  {
    "index.search.slowlog.threshold.query.warn": "10s",
    "index.search.slowlog.threshold.query.info": "5s",
    "index.search.slowlog.threshold.query.debug": "2s",
    "index.search.slowlog.threshold.query.trace": "500ms"
  }
  ```
:::



`index.search.slowlog.threshold.fetch.<level>`
:    Sets the minimum threshold for logging slow fetch-phase operations (retrieving documents after query hits). `<level>` can be `warn`, `info`, `debug`, and `trace`. When fetching takes longer than the configured value, a slow log entry is emitted.
:::{dropdown} Example
The following request enables slow logs by configuring thresholds:

  ```console
  PUT /my-index-000001/_settings
  {
    "index.search.slowlog.threshold.fetch.warn": "1s",
    "index.search.slowlog.threshold.fetch.info": "800ms",
    "index.search.slowlog.threshold.fetch.debug": "500ms",
    "index.search.slowlog.threshold.fetch.trace": "200ms"
  }
  ```
:::


`index.search.slowlog.include.user`
:   This setting accepts a boolean value. If set to `true`, it includes `user.*` and `auth.type` metadata in the log entries. These fields contain information about the user who triggered the request.
:::{dropdown} Example
The following request enables slow logs and includes information about who triggered the request:

  ```console
  PUT /my-index-000001/_settings
  {
    "index.search.slowlog.threshold.query.warn": "10s",
    "index.search.slowlog.threshold.query.info": "5s",
    "index.search.slowlog.threshold.query.debug": "2s",
    "index.search.slowlog.threshold.query.trace": "500ms",
    "index.search.slowlog.threshold.fetch.warn": "1s",
    "index.search.slowlog.threshold.fetch.info": "800ms",
    "index.search.slowlog.threshold.fetch.debug": "500ms",
    "index.search.slowlog.threshold.fetch.trace": "200ms",
    "index.search.slowlog.include.user": true
  }
  ```
:::




## Settings for indexing operations 

Indexing slow logs emit per index document.

`index.indexing.slowlog.threshold.index.<level>`
:   Sets the minimum threshold for logging slow indexing operations. `<level>` can be `warn`, `info`, `debug`, and `trace`. When an indexing operation takes longer than the configured value, a slow log entry is emitted. 
:::{dropdown} Example 
The following request enables slow logs by configuring thresholds:

  ```console
  PUT /my-index-000001/_settings
  {
    "index.indexing.slowlog.threshold.index.warn": "10s",
    "index.indexing.slowlog.threshold.index.info": "5s",
    "index.indexing.slowlog.threshold.index.debug": "2s",
    "index.indexing.slowlog.threshold.index.trace": "500ms"
  }
  ```
:::


`index.indexing.slowlog.include.user`
:   This setting accepts a boolean value. If set to `true`, it includes `user.*` and `auth.type` metadata in the log entries. These fields contain information about the user who initiated the request.
:::{dropdown} Example 
The following request enables slow logs by configuring thresholds and includes information about who triggered the request:

  ```console
  PUT /my-index-000001/_settings
  {
    "index.indexing.slowlog.threshold.index.warn": "10s",
    "index.indexing.slowlog.threshold.index.info": "5s",
    "index.indexing.slowlog.threshold.index.debug": "2s",
    "index.indexing.slowlog.threshold.index.trace": "500ms",
    "index.indexing.slowlog.include.user": true
  }
  ```
:::

`index.indexing.slowlog.source`
:   Sets the number of `_source` characters to include. By default, {{es}} logs the first 1000 characters in the slow log. Set to `false`or `0` to disable source logging. Set to `true` to log the entire source regardless of size.
:::{dropdown} Example 
The following request enables slow logs by configuring thresholds and setting the number of `_source` characters to include to 1000:

  ```console
  PUT /my-index-000001/_settings
  {
    "index.indexing.slowlog.threshold.index.warn": "10s",
    "index.indexing.slowlog.threshold.index.info": "5s",
    "index.indexing.slowlog.threshold.index.debug": "2s",
    "index.indexing.slowlog.threshold.index.trace": "500ms",
    "index.indexing.slowlog.include.user": true,
    "index.indexing.slowlog.source": "1000"
  }
  ```
:::

`index.indexing.slowlog.reformat`
:   This setting accepts a boolean value. It's set to `true` by default and reformats the original `_source` into a single-line log entry. If you want to preserve the original document format, then you can turn off reformatting by setting it to `false`. This causes source to be logged with the original formatting intact, potentially spanning multiple log lines.
:::{dropdown} Example 
The following request enables slow logs by configuring thresholds and includes reformatting the `_source` into a single-line entry:

  ```console
  PUT /my-index-000001/_settings
  {
    "index.indexing.slowlog.threshold.index.warn": "10s",
    "index.indexing.slowlog.threshold.index.info": "5s",
    "index.indexing.slowlog.threshold.index.debug": "2s",
    "index.indexing.slowlog.threshold.index.trace": "500ms",
    "index.indexing.slowlog.include.user": true,
    "index.indexing.slowlog.source": "1000",
    "index.indexing.slowlog.reformat": true
  }
  ```
:::
