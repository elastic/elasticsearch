---
applies_to:
  stack:
  serverless:
navigation_title: Get started
---

# Get started with {{esql}} queries [esql-getting-started]

This hands-on guide covers the basics of using {{esql}} to query and aggregate your data.

::::{tip}
This getting started is also available as an [interactive Python notebook](https://github.com/elastic/elasticsearch-labs/blob/main/notebooks/esql/esql-getting-started.ipynb) in the `elasticsearch-labs` GitHub repository.
::::

## Prerequisites [esql-getting-started-prerequisites]

To follow along with the queries in this guide, you can either set up your own deployment, or use Elastic’s public {{esql}} demo environment.

:::::::{tab-set}

::::::{tab-item} Own deployment
First ingest some sample data. In {{kib}}, open the main menu and select **Dev Tools**. Run the following two requests:

```console
PUT sample_data
{
  "mappings": {
    "properties": {
      "client_ip": {
        "type": "ip"
      },
      "message": {
        "type": "keyword"
      }
    }
  }
}

PUT sample_data/_bulk
{"index": {}}
{"@timestamp": "2023-10-23T12:15:03.360Z", "client_ip": "172.21.2.162", "message": "Connected to 10.1.0.3", "event_duration": 3450233}
{"index": {}}
{"@timestamp": "2023-10-23T12:27:28.948Z", "client_ip": "172.21.2.113", "message": "Connected to 10.1.0.2", "event_duration": 2764889}
{"index": {}}
{"@timestamp": "2023-10-23T13:33:34.937Z", "client_ip": "172.21.0.5", "message": "Disconnected", "event_duration": 1232382}
{"index": {}}
{"@timestamp": "2023-10-23T13:51:54.732Z", "client_ip": "172.21.3.15", "message": "Connection error", "event_duration": 725448}
{"index": {}}
{"@timestamp": "2023-10-23T13:52:55.015Z", "client_ip": "172.21.3.15", "message": "Connection error", "event_duration": 8268153}
{"index": {}}
{"@timestamp": "2023-10-23T13:53:55.832Z", "client_ip": "172.21.3.15", "message": "Connection error", "event_duration": 5033755}
{"index": {}}
{"@timestamp": "2023-10-23T13:55:01.543Z", "client_ip": "172.21.3.15", "message": "Connected to 10.1.0.1", "event_duration": 1756467}
```
::::::

::::::{tab-item} Demo environment
The data set used in this guide has been preloaded into the Elastic {{esql}} public demo environment. Visit [ela.st/ql](https://ela.st/ql) to start using it.
::::::

:::::::

## Run an {{esql}} query [esql-getting-started-running-queries]

In {{kib}}, you can use Console or Discover to run {{esql}} queries:

:::::::{tab-set}

::::::{tab-item} Console
To get started with {{esql}} in Console, open the main menu and select **Dev Tools**.

The general structure of an [{{esql}} query API](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-esql) request is:

```txt
POST /_query?format=txt
{
  "query": """

  """
}
```

Enter the actual {{esql}} query between the two sets of triple quotes. For example:

```txt
POST /_query?format=txt
{
  "query": """
FROM kibana_sample_data_logs
  """
}
```

::::::

::::::{tab-item} Discover
To get started with {{esql}} in Discover, open the main menu and select **Discover**. Next, select **Try ES|QL** from the application menu bar.

Adjust the time filter so it includes the timestamps in the sample data (October 23rd, 2023).

After switching to {{esql}} mode, the query bar shows a sample query. You can replace this query with the queries in this getting started guide.

You can adjust the editor’s height by dragging its bottom border to your liking.
::::::

:::::::

## Your first {{esql}} query [esql-getting-started-first-query]

Each {{esql}} query starts with a [source command](commands/source-commands.md). A source command produces a table, typically with data from {{es}}.

:::{image} ../images/elasticsearch-reference-source-command.svg
:alt: A source command producing a table from {{es}}
:::

The [`FROM`](commands/from.md) source command returns a table with documents from a data stream, index, or alias. Each row in the resulting table represents a document. This query returns up to 1000 documents from the `sample_data` index:

```esql
FROM sample_data
```

Each column corresponds to a field, and can be accessed by the name of that field.

::::{tip}
{{esql}} keywords are case-insensitive. The following query is identical to the previous one:

```esql
from sample_data
```

::::



## Processing commands [esql-getting-started-limit]

A source command can be followed by one or more [processing commands](commands/processing-commands.md), separated by a pipe character: `|`. Processing commands change an input table by adding, removing, or changing rows and columns. Processing commands can perform filtering, projection, aggregation, and more.

:::{image} ../images/elasticsearch-reference-esql-limit.png
:alt: A processing command changing an input table
:width: 500px
:::

For example, you can use the [`LIMIT`](commands/limit.md) command to limit the number of rows that are returned, up to a maximum of 10,000 rows:

```esql
FROM sample_data
| LIMIT 3
```

::::{tip}
For readability, you can put each command on a separate line. However, you don’t have to. The following query is identical to the previous one:

```esql
FROM sample_data | LIMIT 3
```

::::



### Sort a table [esql-getting-started-sort]

:::{image} ../images/elasticsearch-reference-esql-sort.png
:alt: A processing command sorting an input table
:width: 500px
:::

Another processing command is the [`SORT`](commands/sort.md) command. By default, the rows returned by `FROM` don’t have a defined sort order. Use the `SORT` command to sort rows on one or more columns:

```esql
FROM sample_data
| SORT @timestamp DESC
```


### Query the data [esql-getting-started-where]

Use the [`WHERE`](commands/where.md) command to query the data. For example, to find all events with a duration longer than 5ms:

```esql
FROM sample_data
| WHERE event_duration > 5000000
```

`WHERE` supports several [operators](functions-operators/operators.md). For example, you can use [`LIKE`](functions-operators/operators.md#esql-like) to run a wildcard query against the `message` column:

```esql
FROM sample_data
| WHERE message LIKE "Connected*"
```


### More processing commands [esql-getting-started-more-commands]

There are many other processing commands, like [`KEEP`](commands/keep.md) and [`DROP`](commands/drop.md) to keep or drop columns, [`ENRICH`](commands/enrich.md) to enrich a table with data from indices in {{es}}, and [`DISSECT`](commands/dissect.md) and [`GROK`](commands/grok.md) to process data. Refer to [Processing commands](commands/processing-commands.md) for an overview of all processing commands.


## Chain processing commands [esql-getting-started-chaining]

You can chain processing commands, separated by a pipe character: `|`. Each processing command works on the output table of the previous command. The result of a query is the table produced by the final processing command.

:::{image} ../images/elasticsearch-reference-esql-sort-limit.png
:alt: Processing commands can be chained
:::

The following example first sorts the table on `@timestamp`, and next limits the result set to 3 rows:

```esql
FROM sample_data
| SORT @timestamp DESC
| LIMIT 3
```

::::{note}
The order of processing commands is important. First limiting the result set to 3 rows before sorting those 3 rows would most likely return a result that is different than this example, where the sorting comes before the limit.
::::



## Compute values [esql-getting-started-eval]

Use the [`EVAL`](commands/eval.md) command to append columns to a table, with calculated values. For example, the following query appends a `duration_ms` column. The values in the column are computed by dividing `event_duration` by 1,000,000. In other words: `event_duration` converted from nanoseconds to milliseconds.

```esql
FROM sample_data
| EVAL duration_ms = event_duration/1000000.0
```

`EVAL` supports several [functions](commands/eval.md). For example, to round a number to the closest number with the specified number of digits, use the [`ROUND`](functions-operators/math-functions.md#esql-round) function:

```esql
FROM sample_data
| EVAL duration_ms = ROUND(event_duration/1000000.0, 1)
```


## Calculate statistics [esql-getting-started-stats]

{{esql}} can not only be used to query your data, you can also use it to aggregate your data. Use the [`STATS`](commands/stats-by.md) command to calculate statistics. For example, the median duration:

```esql
FROM sample_data
| STATS median_duration = MEDIAN(event_duration)
```

You can calculate multiple stats with one command:

```esql
FROM sample_data
| STATS median_duration = MEDIAN(event_duration), max_duration = MAX(event_duration)
```

Use `BY` to group calculated stats by one or more columns. For example, to calculate the median duration per client IP:

```esql
FROM sample_data
| STATS median_duration = MEDIAN(event_duration) BY client_ip
```


## Access columns [esql-getting-started-access-columns]

You can access columns by their name. If a name contains special characters, [it needs to be quoted](esql-syntax.md#esql-identifiers) with backticks (```).

Assigning an explicit name to a column created by `EVAL` or `STATS` is optional. If you don’t provide a name, the new column name is equal to the function expression. For example:

```esql
FROM sample_data
| EVAL event_duration/1000000.0
```

In this query, `EVAL` adds a new column named `event_duration/1000000.0`. Because its name contains special characters, to access this column, quote it with backticks:

```esql
FROM sample_data
| EVAL event_duration/1000000.0
| STATS MEDIAN(`event_duration/1000000.0`)
```


## Create a histogram [esql-getting-started-histogram]

To track statistics over time, {{esql}} enables you to create histograms using the [`BUCKET`](functions-operators/grouping-functions.md#esql-bucket) function. `BUCKET` creates human-friendly bucket sizes and returns a value for each row that corresponds to the resulting bucket the row falls into.

Combine `BUCKET` with [`STATS`](commands/stats-by.md) to create a histogram. For example, to count the number of events per hour:

```esql
FROM sample_data
| STATS c = COUNT(*) BY bucket = BUCKET(@timestamp, 24, "2023-10-23T00:00:00Z", "2023-10-23T23:59:59Z")
```

Or the median duration per hour:

```esql
FROM sample_data
| KEEP @timestamp, event_duration
| STATS median_duration = MEDIAN(event_duration) BY bucket = BUCKET(@timestamp, 24, "2023-10-23T00:00:00Z", "2023-10-23T23:59:59Z")
```


## Enrich data [esql-getting-started-enrich]

{{esql}} enables you to [enrich](esql-enrich-data.md) a table with data from indices in {{es}}, using the [`ENRICH`](commands/enrich.md) command.

:::{image} ../images/elasticsearch-reference-esql-enrich.png
:alt: esql enrich
:::

Before you can use `ENRICH`, you first need to [create](esql-enrich-data.md#esql-create-enrich-policy) and [execute](esql-enrich-data.md#esql-execute-enrich-policy) an [enrich policy](esql-enrich-data.md#esql-enrich-policy).

:::::::{tab-set}

::::::{tab-item} Own deployment
The following requests create and execute a policy called `clientip_policy`. The policy links an IP address to an environment ("Development", "QA", or "Production"):

```console
PUT clientips
{
  "mappings": {
    "properties": {
      "client_ip": {
        "type": "keyword"
      },
      "env": {
        "type": "keyword"
      }
    }
  }
}

PUT clientips/_bulk
{ "index" : {}}
{ "client_ip": "172.21.0.5", "env": "Development" }
{ "index" : {}}
{ "client_ip": "172.21.2.113", "env": "QA" }
{ "index" : {}}
{ "client_ip": "172.21.2.162", "env": "QA" }
{ "index" : {}}
{ "client_ip": "172.21.3.15", "env": "Production" }
{ "index" : {}}
{ "client_ip": "172.21.3.16", "env": "Production" }

PUT /_enrich/policy/clientip_policy
{
  "match": {
    "indices": "clientips",
    "match_field": "client_ip",
    "enrich_fields": ["env"]
  }
}

PUT /_enrich/policy/clientip_policy/_execute?wait_for_completion=false
```
::::::

::::::{tab-item} Demo environment
On the demo environment at [ela.st/ql](https://ela.st/ql/), an enrich policy called `clientip_policy` has already been created an executed. The policy links an IP address to an environment ("Development", "QA", or "Production").
::::::

:::::::
After creating and executing a policy, you can use it with the `ENRICH` command:

```esql
FROM sample_data
| KEEP @timestamp, client_ip, event_duration
| EVAL client_ip = TO_STRING(client_ip)
| ENRICH clientip_policy ON client_ip WITH env
```

You can use the new `env` column that’s added by the `ENRICH` command in subsequent commands. For example, to calculate the median duration per environment:

```esql
FROM sample_data
| KEEP @timestamp, client_ip, event_duration
| EVAL client_ip = TO_STRING(client_ip)
| ENRICH clientip_policy ON client_ip WITH env
| STATS median_duration = MEDIAN(event_duration) BY env
```

For more about data enrichment with {{esql}}, refer to [Data enrichment](esql-enrich-data.md).


## Process data [esql-getting-started-process-data]

Your data may contain unstructured strings that you want to [structure](esql-process-data-with-dissect-grok.md) to make it easier to analyze the data. For example, the sample data contains log messages like:

```txt
"Connected to 10.1.0.3"
```

By extracting the IP address from these messages, you can determine which IP has accepted the most client connections.

To structure unstructured strings at query time, you can use the {{esql}} [`DISSECT`](commands/dissect.md) and [`GROK`](commands/grok.md) commands. `DISSECT` works by breaking up a string using a delimiter-based pattern. `GROK` works similarly, but uses regular expressions. This makes `GROK` more powerful, but generally also slower.

In this case, no regular expressions are needed, as the `message` is straightforward: "Connected to ", followed by the server IP. To match this string, you can use the following `DISSECT` command:

```esql
FROM sample_data
| DISSECT message "Connected to %{server_ip}"
```

This adds a `server_ip` column to those rows that have a `message` that matches this pattern. For other rows, the value of `server_ip` is `null`.

You can use the new `server_ip` column that’s added by the `DISSECT` command in subsequent commands. For example, to determine how many connections each server has accepted:

```esql
FROM sample_data
| WHERE STARTS_WITH(message, "Connected to")
| DISSECT message "Connected to %{server_ip}"
| STATS COUNT(*) BY server_ip
```

For more about data processing with {{esql}}, refer to [Data processing with DISSECT and GROK](esql-process-data-with-dissect-grok.md).


## Learn more [esql-getting-learn-more]

- Explore the zero-setup, live [{{esql}} demo environment](http://esql.demo.elastic.co/).
- 
- Follow along with our hands-on tutorials:
  - [Search and filter with {{esql}}](/reference/query-languages/esql/esql-search-tutorial.md): A hands-on tutorial that shows you how to use {{esql}} to search and filter data.
  - [Threat hunting with {{esql}}](docs-content://solutions/security/esql-for-security/esql-threat-hunting-tutorial.md): A hands-on tutorial that shows you how to use {{esql}} for advanced threat hunting techniques and security analysis.