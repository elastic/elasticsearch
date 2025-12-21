---
navigation_title: "REST API"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/esql-rest.html
applies_to:
  stack: ga
  serverless: ga
products:
  - id: elasticsearch
---

# Use the {{esql}} REST API [esql-rest]

::::{tip}
The [Search and filter with {{esql}}](/reference/query-languages/esql/esql-search-tutorial.md) tutorial provides a hands-on introduction to the {{esql}} `_query` API.
::::

## Overview [esql-rest-overview]

The [`_query` API](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-esql) accepts an {{esql}} query string in the `query` parameter, runs it, and returns the results. For example:

```console
POST /_query?format=txt
{
  "query": "FROM library | KEEP author, name, page_count, release_date | SORT page_count DESC | LIMIT 5"
}
```
% TEST[setup:library]

Which returns:

```text
     author      |        name        |  page_count   | release_date
-----------------+--------------------+---------------+------------------------
Peter F. Hamilton|Pandora's Star      |768            |2004-03-02T00:00:00.000Z
Vernor Vinge     |A Fire Upon the Deep|613            |1992-06-01T00:00:00.000Z
Frank Herbert    |Dune                |604            |1965-06-01T00:00:00.000Z
Alastair Reynolds|Revelation Space    |585            |2000-03-15T00:00:00.000Z
James S.A. Corey |Leviathan Wakes     |561            |2011-06-02T00:00:00.000Z
```
% TESTRESPONSE[s/\|/\\|/ s/\+/\\+/]
% TESTRESPONSE[non_json]

### Run the {{esql}} query API in Console [esql-kibana-console]

We recommend using [Console](docs-content://explore-analyze/query-filter/tools/console.md) to run the {{esql}} query API, because of its rich autocomplete features.

When creating the query, using triple quotes (`"""`) allows you to use special characters like quotes (`"`) without having to escape them. They also make it easier to write multi-line requests.

```console
POST /_query?format=txt
{
  "query": """
    FROM library
    | KEEP author, name, page_count, release_date
    | SORT page_count DESC
    | LIMIT 5
  """
}
```
% TEST[setup:library]

### Response formats [esql-rest-format]

{{esql}} can return the data in the following human readable and binary formats. You can set the format by specifying the `format` parameter in the URL or by setting the `Accept` or `Content-Type` HTTP header.

For example:

```console
POST /_query?format=yaml
{
  "query": """
    FROM library
    | KEEP author, name, page_count, release_date
    | SORT page_count DESC
    | LIMIT 5
  """
}
```
% TEST[setup:library]

::::{note}
The URL parameter takes precedence over the HTTP headers. If neither is specified then the response is returned in the same format as the request.
::::

#### Structured formats

Complete responses with metadata. Useful for automatic parsing.

| `format` | HTTP header | Description |
| --- | --- | --- |
| `json` | `application/json` | [JSON](https://www.json.org/) (JavaScript Object Notation) human-readable format |
| `yaml` | `application/yaml` | [YAML](https://en.wikipedia.org/wiki/YAML) (YAML Ain’t Markup Language) human-readable format |

#### Tabular formats

Query results only, without metadata. Useful for quick and manual data previews.

| `format` | HTTP header | Description |
| --- | --- | --- |
| `csv` | `text/csv` | [Comma-separated values](https://en.wikipedia.org/wiki/Comma-separated_values) |
| `tsv` | `text/tab-separated-values` | [Tab-separated values](https://en.wikipedia.org/wiki/Tab-separated_values) |
| `txt` | `text/plain` | CLI-like representation |

::::{tip}
The `csv` format accepts a formatting URL query attribute, `delimiter`, which indicates which character should be used to separate the CSV values. It defaults to comma (`,`) and cannot take any of the following values: double quote (`"`), carriage-return (`\r`) and new-line (`\n`). The tab (`\t`) can also not be used. Use the `tsv` format instead.
::::

#### Binary formats

Compact binary encoding. To be used by applications.

| `format` | HTTP header | Description |
| --- | --- | --- |
| `cbor` | `application/cbor` | [Concise Binary Object Representation](https://cbor.io/) |
| `smile` | `application/smile` | [Smile](https://en.wikipedia.org/wiki/Smile_(data_interchange_format)) binary data format similarto CBOR |
| `arrow` | `application/vnd.apache.arrow.stream` | **Experimental.** [Apache Arrow](https://arrow.apache.org/) dataframes, [IPC streaming format](https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format) |


### Filtering using {{es}} Query DSL [esql-rest-filtering]

Specify a Query DSL query in the `filter` parameter to filter the set of documents that an {{esql}} query runs on.

```console
POST /_query?format=txt
{
  "query": """
    FROM library
    | KEEP author, name, page_count, release_date
    | SORT page_count DESC
    | LIMIT 5
  """,
  "filter": {
    "range": {
      "page_count": {
        "gte": 100,
        "lte": 200
      }
    }
  }
}
```
% TEST[setup:library]

Which returns:

```text
    author     |                name                |  page_count   | release_date
---------------+------------------------------------+---------------+------------------------
Douglas Adams  |The Hitchhiker's Guide to the Galaxy|180            |1979-10-12T00:00:00.000Z
```
% TESTRESPONSE[s/\|/\\|/ s/\+/\\+/]
% TESTRESPONSE[non_json]

#### Filter vs WHERE clause behavior

The `filter` parameter can eliminate columns from the result set when it skips entire indices.
This is useful for resolving type conflicts between attributes of different indices.

For example, if several days of data in a data stream were indexed with an incorrect type, you can use a filter to exclude the incorrect range.
This allows {{esql}} to use the correct type for the remaining data without changing the source pattern.

##### Example

Consider querying `index-1` with an `f1` attribute and `index-2` with an `f2` attribute.

Using a filter the following query returns only the `f1` column:

```console
POST /_query?format=txt
{
  "query": "FROM index-*",
  "filter": {
    "term": {
      "f1": "*"
    }
  }
}
```
% TEST[skip:no index]

Using a WHERE clause returns both the `f1` and `f2` columns:

```console
POST /_query?format=txt
{
  "query": "FROM index-* WHERE f1 is not null"
}
```
% TEST[skip:no index]

### Columnar results [esql-rest-columnar]

By default, {{esql}} returns results as rows. For example, `FROM` returns each individual document as one row. For the `json`, `yaml`, `cbor` and `smile` [formats](#esql-rest-format), {{esql}} can return the results in a columnar fashion where one row represents all the values of a certain column in the results.

```console
POST /_query?format=json
{
  "query": """
    FROM library
    | KEEP author, name, page_count, release_date
    | SORT page_count DESC
    | LIMIT 5
  """,
  "columnar": true
}
```
% TEST[setup:library]

Which returns:

```console-result
{
  "took": 28,
  "is_partial": false,
  "columns": [
    {"name": "author", "type": "text"},
    {"name": "name", "type": "text"},
    {"name": "page_count", "type": "integer"},
    {"name": "release_date", "type": "date"}
  ],
  "values": [
    ["Peter F. Hamilton", "Vernor Vinge", "Frank Herbert", "Alastair Reynolds", "James S.A. Corey"],
    ["Pandora's Star", "A Fire Upon the Deep", "Dune", "Revelation Space", "Leviathan Wakes"],
    [768, 613, 604, 585, 561],
    ["2004-03-02T00:00:00.000Z", "1992-06-01T00:00:00.000Z", "1965-06-01T00:00:00.000Z", "2000-03-15T00:00:00.000Z", "2011-06-02T00:00:00.000Z"]
  ]
}
```
% TESTRESPONSE[s/"took": 28/"took": "$body.took"/]

### Returning localized results [esql-locale-param]

Use the `locale` parameter in the request body to return results (especially dates) formatted per the conventions of the locale. If `locale` is not specified, defaults to `en-US` (English). Refer to [JDK Supported Locales](https://www.oracle.com/java/technologies/javase/jdk17-suported-locales.html).

Syntax: the `locale` parameter accepts language tags in the (case-insensitive) format `xy` and `xy-XY`.

For example, to return a month name in French:

```console
POST /_query
{
  "locale": "fr-FR",
  "query": """
          ROW birth_date_string = "2023-01-15T00:00:00.000Z"
          | EVAL birth_date = date_parse(birth_date_string)
          | EVAL month_of_birth = DATE_FORMAT("MMMM",birth_date)
          | LIMIT 5
   """
}
```
% TEST[setup:library]
% TEST[skip:This can output a warning, and asciidoc doesn't support allowed_warnings]

### Passing parameters to a query [esql-rest-params]

Values, for example for a condition, can be passed to a query "inline", by integrating the value in the query string itself:

```console
POST /_query
{
  "query": """
    FROM library
    | EVAL year = DATE_EXTRACT("year", release_date)
    | WHERE page_count > 300 AND author == "Frank Herbert"
    | STATS count = COUNT(*) by year
    | WHERE count > 0
    | LIMIT 5
  """
}
```
% TEST[setup:library]

To avoid any attempts of hacking or code injection, extract the values in a separate list of parameters. Use question mark placeholders (`?`) in the query string for each of the parameters:

```console
POST /_query
{
  "query": """
    FROM library
    | EVAL year = DATE_EXTRACT("year", release_date)
    | WHERE page_count > ? AND author == ?
    | STATS count = COUNT(*) by year
    | WHERE count > ?
    | LIMIT 5
  """,
  "params": [300, "Frank Herbert", 0]
}
```
% TEST[setup:library]

The parameters can be named parameters or positional parameters.

Named parameters use question mark placeholders (`?`) followed by a string.

```console
POST /_query
{
  "query": """
    FROM library
    | EVAL year = DATE_EXTRACT("year", release_date)
    | WHERE page_count > ?page_count AND author == ?author
    | STATS count = COUNT(*) by year
    | WHERE count > ?count
    | LIMIT 5
  """,
  "params": [{"page_count" : 300}, {"author" : "Frank Herbert"}, {"count" : 0}]
}
```
% TEST[setup:library]

Positional parameters use question mark placeholders (`?`) followed by an integer.

```console
POST /_query
{
  "query": """
    FROM library
    | EVAL year = DATE_EXTRACT("year", release_date)
    | WHERE page_count > ?1 AND author == ?2
    | STATS count = COUNT(*) by year
    | WHERE count > ?3
    | LIMIT 5
  """,
  "params": [300, "Frank Herbert", 0]
}
```
% TEST[setup:library]

### Running an async {{esql}} query [esql-rest-async-query]

The [{{esql}} async query API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-esql-async-query) lets you asynchronously execute a query request, monitor its progress, and retrieve results when they become available.

Executing an {{esql}} query is commonly quite fast, however queries across large data sets or frozen data can take some time. To avoid long waits, run an async {{esql}} query.

Queries initiated by the async query API may return results or not. The `wait_for_completion_timeout` property determines how long to wait for the results. If the results are not available by this time, a [query id](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-esql-async-query#esql-async-query-api-response-body-query-id) is returned which can be later used to retrieve the results. For example:

```console
POST /_query/async
{
  "query": """
    FROM library
    | EVAL year = DATE_TRUNC(1 YEARS, release_date)
    | STATS MAX(page_count) BY year
    | SORT year
    | LIMIT 5
  """,
  "wait_for_completion_timeout": "2s"
}
```
% TEST[setup:library]
% TEST[skip:awaitsfix https://github.com/elastic/elasticsearch/issues/104013]

If the results are not available within the given timeout period, 2 seconds in this case, no results are returned but rather a response that includes:

* A query ID
* An `is_running` value of *true*, indicating the query is ongoing

The query continues to run in the background without blocking other requests.

```console-result
{
  "id": "FmNJRUZ1YWZCU3dHY1BIOUhaenVSRkEaaXFlZ3h4c1RTWFNocDdnY2FSaERnUTozNDE=",
  "is_running": true
}
```
% TEST[skip: no access to query ID - may return response values]

To check the progress of an async query, use the [{{esql}} async query get API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-esql-async-query-get) with the query ID. Specify how long you’d like to wait for complete results in the `wait_for_completion_timeout` parameter.

```console
GET /_query/async/FmNJRUZ1YWZCU3dHY1BIOUhaenVSRkEaaXFlZ3h4c1RTWFNocDdnY2FSaERnUTozNDE=?wait_for_completion_timeout=30s
```
% TEST[skip: no access to query ID - may return response values]

If the response’s `is_running` value is `false`, the query has finished and the results are returned, along with the `took` time for the query.

```console-result
{
  "is_running": false,
  "took": 48,
  "columns": ...
}
```
% TEST[skip: no access to query ID - may return response values]

To stop a running async query and return the results computed so far, use the [async stop API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-esql-async-query-stop) with the query ID.

```console
POST /_query/async/FmNJRUZ1YWZCU3dHY1BIOUhaenVSRkEaaXFlZ3h4c1RTWFNocDdnY2FSaERnUTozNDE=/stop
```
% TEST[skip: no access to query ID - may return response values]

The query will be stopped and the response will contain the results computed so far. The response format is the same as the `get` API.

```console-result
{
  "is_running": false,
  "took": 48,
  "is_partial": true,
  "columns": ...
}
```
% TEST[skip: no access to query ID - may return response values]

This API can be used to retrieve results even if the query has already completed, as long as it's within the `keep_alive` window.
The `is_partial` field indicates result completeness. A value of `true` means the results are potentially incomplete.

Use the [{{esql}} async query delete API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-esql-async-query-delete) to delete an async query before the `keep_alive` period ends. If the query is still running, {{es}} cancels it.

```console
DELETE /_query/async/FmdMX2pIang3UWhLRU5QS0lqdlppYncaMUpYQ05oSkpTc3kwZ21EdC1tbFJXQToxOTI=
```
% TEST[skip: no access to query ID]

::::{note}
You will also receive the async ID and running status in the `X-Elasticsearch-Async-Id` and `X-Elasticsearch-Async-Is-Running` HTTP headers of the response, respectively.
Useful if you use a tabular text format like `txt`, `csv` or `tsv`, as you won't receive those fields in the body there.
::::
