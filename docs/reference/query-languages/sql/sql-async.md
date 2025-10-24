---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-async.html
applies_to:
  stack: ga
  serverless: ga
products:
  - id: elasticsearch
---

# Run an async SQL search [sql-async]

By default, SQL searches are synchronous. They wait for complete results before returning a response. However, results can take longer for searches across large data sets or [frozen data](docs-content://manage-data/lifecycle/data-tiers.md).

To avoid long waits, run an async SQL search. Set `wait_for_completion_timeout` to a duration you’d like to wait for synchronous results.

```console
POST _sql?format=json
{
  "wait_for_completion_timeout": "2s",
  "query": "SELECT * FROM library ORDER BY page_count DESC",
  "fetch_size": 5
}
```
% TEST[setup:library]

If the search doesn’t finish within this period, the search becomes async. The API returns:

* An `id` for the search.
* An `is_partial` value of `true`, indicating the search results are incomplete.
* An `is_running` value of `true`, indicating the search is still running in the background.

For CSV, TSV, and TXT responses, the API returns these values in the respective `Async-ID`, `Async-partial`, and `Async-running` HTTP headers instead.

```console-result
{
  "id": "FnR0TDhyWUVmUmVtWXRWZER4MXZiNFEad2F5UDk2ZVdTVHV1S0xDUy00SklUdzozMTU=",
  "is_partial": true,
  "is_running": true,
  "rows": [ ]
}
```
% TESTRESPONSE[skip:waiting on https://github.com/elastic/elasticsearch/issues/106158]
% TESTRESPONSE[s/FnR0TDhyWUVmUmVtWXRWZER4MXZiNFEad2F5UDk2ZVdTVHV1S0xDUy00SklUdzozMTU=/$body.id/]
% TESTRESPONSE[s/"is_partial": true/"is_partial": $body.is_partial/]
% TESTRESPONSE[s/"is_running": true/"is_running": $body.is_running/]

To check the progress of an async search, use the search ID with the [get async SQL search status API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-sql-get-async-status).

```console
GET _sql/async/status/FnR0TDhyWUVmUmVtWXRWZER4MXZiNFEad2F5UDk2ZVdTVHV1S0xDUy00SklUdzozMTU=
```
% TEST[skip: no access to search ID]

If `is_running` and `is_partial` are `false`, the async search has finished with complete results.

```console-result
{
  "id": "FnR0TDhyWUVmUmVtWXRWZER4MXZiNFEad2F5UDk2ZVdTVHV1S0xDUy00SklUdzozMTU=",
  "is_running": false,
  "is_partial": false,
  "expiration_time_in_millis": 1611690295000,
  "completion_status": 200
}
```
% TESTRESPONSE[skip:waiting on https://github.com/elastic/elasticsearch/issues/106158]
% TESTRESPONSE[s/FnR0TDhyWUVmUmVtWXRWZER4MXZiNFEad2F5UDk2ZVdTVHV1S0xDUy00SklUdzozMTU=/$body.id/]
% TESTRESPONSE[s/"expiration_time_in_millis": 1611690295000/"expiration_time_in_millis": $body.expiration_time_in_millis/]

To get the results, use the search ID with the [get async SQL search API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-sql-get-async). If the search is still running, specify how long you’d like to wait using `wait_for_completion_timeout`. You can also specify the response `format`.

```console
GET _sql/async/FnR0TDhyWUVmUmVtWXRWZER4MXZiNFEad2F5UDk2ZVdTVHV1S0xDUy00SklUdzozMTU=?wait_for_completion_timeout=2s&format=json
```
% TEST[skip: no access to search ID]

## Change the search retention period [sql-async-retention]

By default, {{es}} stores async SQL searches for five days. After this period, {{es}} deletes the search and its results, even if the search is still running. To change this retention period, use the `keep_alive` parameter.

```console
POST _sql?format=json
{
  "keep_alive": "2d",
  "wait_for_completion_timeout": "2s",
  "query": "SELECT * FROM library ORDER BY page_count DESC",
  "fetch_size": 5
}
```
% TEST[setup:library]

You can use the get async SQL search API’s `keep_alive` parameter to later change the retention period. The new period starts after the request runs.

```console
GET _sql/async/FmdMX2pIang3UWhLRU5QS0lqdlppYncaMUpYQ05oSkpTc3kwZ21EdC1tbFJXQToxOTI=?keep_alive=5d&wait_for_completion_timeout=2s&format=json
```
% TEST[skip: no access to search ID]

Use the [delete async SQL search API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-sql-delete-async) to delete an async search before the `keep_alive` period ends. If the search is still running, {{es}} cancels it.

```console
DELETE _sql/async/delete/FmdMX2pIang3UWhLRU5QS0lqdlppYncaMUpYQ05oSkpTc3kwZ21EdC1tbFJXQToxOTI=
```
% TEST[skip: no access to search ID]

## Store synchronous SQL searches [sql-store-searches]

By default, {{es}} only stores async SQL searches. To save a synchronous search, specify `wait_for_completion_timeout` and set `keep_on_completion` to `true`.

```console
POST _sql?format=json
{
  "keep_on_completion": true,
  "wait_for_completion_timeout": "2s",
  "query": "SELECT * FROM library ORDER BY page_count DESC",
  "fetch_size": 5
}
```
% TEST[skip:waiting on https://github.com/elastic/elasticsearch/issues/106158]
% TEST[setup:library]

If `is_partial` and `is_running` are `false`, the search was synchronous and returned complete results.

```console-result
{
  "id": "Fnc5UllQdUVWU0NxRFNMbWxNYXplaFEaMUpYQ05oSkpTc3kwZ21EdC1tbFJXQTo0NzA=",
  "is_partial": false,
  "is_running": false,
  "rows": ...,
  "columns": ...,
  "cursor": ...
}
```
% TESTRESPONSE[skip:waiting on https://github.com/elastic/elasticsearch/issues/106158]
% TESTRESPONSE[s/Fnc5UllQdUVWU0NxRFNMbWxNYXplaFEaMUpYQ05oSkpTc3kwZ21EdC1tbFJXQTo0NzA=/$body.id/]
% TESTRESPONSE[s/"rows": \.\.\./"rows": $body.rows/]
% TESTRESPONSE[s/"columns": \.\.\./"columns": $body.columns/]
% TESTRESPONSE[s/"cursor": \.\.\./"cursor": $body.cursor/]

You can get the same results later using the search ID with the [get async SQL search API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-sql-get-async).

Saved synchronous searches are still subject to the `keep_alive` retention period. When this period ends, {{es}} deletes the search results. You can also delete saved searches using the [delete async SQL search API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-sql-delete-async).

