---
applies_to:
   stack: preview 9.0, ga 9.1
   serverless: ga
navigation_title: "Join data with LOOKUP JOIN"
mapped_pages:
 - https://www.elastic.co/guide/en/elasticsearch/reference/8.18/_lookup_join.html
---

# Join data from multiple indices with `LOOKUP JOIN` [esql-lookup-join-reference]

The {{esql}} [`LOOKUP JOIN`](/reference/query-languages/esql/commands/lookup-join.md) processing command combines data from your {{esql}} query results table with matching records from a specified lookup index. It adds fields from the lookup index as new columns to your results table based on matching values in the join field.

Teams often have data scattered across multiple indices – like logs, IPs, user IDs, hosts, employees etc. Without a direct way to enrich or correlate each event with reference data, root-cause analysis, security checks, and operational insights become time-consuming.

For example, you can use `LOOKUP JOIN` to:

* Retrieve environment or ownership details for each host to correlate your metrics data.
* Quickly see if any source IPs match known malicious addresses.
* Tag logs with the owning team or escalation info for faster triage and incident response.

## Compare with `ENRICH`

[`LOOKUP JOIN`](/reference/query-languages/esql/commands/lookup-join.md) is similar to [`ENRICH`](/reference/query-languages/esql/commands/enrich.md) in the fact that they both help you join data together. You should use `LOOKUP JOIN` when:

* Your enrichment data changes frequently
* You want to avoid index-time processing
* You want SQL-like behavior, so that multiple matches result in multiple rows
* You need to match on any field in a lookup index
* You use document or field level security
* You want to restrict users to use only specific lookup indices
* You do not need to match using ranges or spatial relations

## How the command works [esql-how-lookup-join-works]

The `LOOKUP JOIN` command adds fields from the lookup index as new columns to your results table based on matching values in the join field.

The command requires two parameters:
* The name of the lookup index (which must have the `lookup` [`index.mode setting`](/reference/elasticsearch/index-settings/index-modules.md#index-mode-setting))
* The join condition. Can be one of the following:
   * A single field name
   * A comma-separated list of field names {applies_to}`stack: ga 9.2`
   * An expression with one or more join conditions linked by `AND`. Each condition compares a field from the left index with a field from the lookup index using [binary operators](/reference/query-languages/esql/functions-operators/operators.md#esql-binary-operators) (`==`, `>=`, `<=`, `>`, `<`, `!=`). Each field name in the join condition must exist in only one of the indexes. Use RENAME to resolve naming conflicts. {applies_to}`stack: preview 9.2` {applies_to}`serverless: preview`

```esql
LOOKUP JOIN <lookup_index> ON <field_name>  # Join on a single field
LOOKUP JOIN <lookup_index> ON <field_name1>, <field_name2>, <field_name3>  # Join on multiple fields
LOOKUP JOIN <lookup_index> ON <left_field1> >= <lookup_field1> AND <left_field2> == <lookup_field2>  # Join on expression
```

:::{image} ../images/esql-lookup-join.png
:alt: Illustration of the `LOOKUP JOIN` command, where the input table is joined with a lookup index to create an enriched output table.
:::

If you're familiar with SQL, `LOOKUP JOIN` has left-join behavior. This means that if no rows match in the lookup index, the incoming row is retained and `null`s are added. If many rows in the lookup index match, `LOOKUP JOIN` adds one row per match.

### Cross-cluster support

{applies_to}`stack: ga 9.2.0` Remote lookup joins are supported in [cross-cluster queries](/reference/query-languages/esql/esql-cross-clusters.md). The lookup index must exist on _all_ remote clusters being queried, because each cluster uses its local lookup index data. This follows the same pattern as [remote mode Enrich](/reference/query-languages/esql/esql-cross-clusters.md#esql-enrich-remote).

```esql
FROM log-cluster-*:logs-* | LOOKUP JOIN hosts ON source.ip
```

## Example

You can run this example for yourself if you'd like to see how it works, by setting up the indices and adding sample data.

### Sample data
:::{dropdown} Expand for setup instructions

**Set up indices**

First let's create two indices with mappings: `threat_list` and `firewall_logs`.

```console
PUT threat_list
{
  "settings": {
    "index.mode": "lookup" <1>
  },
  "mappings": {
    "properties": {
      "source.ip": { "type": "ip" },
      "threat_level": { "type": "keyword" },
      "threat_type": { "type": "keyword" },
      "last_updated": { "type": "date" }
    }
  }
}
```
1. The lookup index must use this mode
  
```console
PUT firewall_logs
{
  "mappings": {
    "properties": {
      "timestamp": { "type": "date" },
      "source.ip": { "type": "ip" },
      "destination.ip": { "type": "ip" },
      "action": { "type": "keyword" },
      "bytes_transferred": { "type": "long" }
    }
  }
}
```

**Add sample data**

Next, let's add some sample data to both indices. The `threat_list` index contains known malicious IPs, while the `firewall_logs` index contains logs of network traffic.

```console
POST threat_list/_bulk
{"index":{}}
{"source.ip":"203.0.113.5","threat_level":"high","threat_type":"C2_SERVER","last_updated":"2025-04-22"}
{"index":{}}
{"source.ip":"198.51.100.2","threat_level":"medium","threat_type":"SCANNER","last_updated":"2025-04-23"}
```

```console
POST firewall_logs/_bulk
{"index":{}}
{"timestamp":"2025-04-23T10:00:01Z","source.ip":"192.0.2.1","destination.ip":"10.0.0.100","action":"allow","bytes_transferred":1024}
{"index":{}}
{"timestamp":"2025-04-23T10:00:05Z","source.ip":"203.0.113.5","destination.ip":"10.0.0.55","action":"allow","bytes_transferred":2048}
{"index":{}}
{"timestamp":"2025-04-23T10:00:08Z","source.ip":"198.51.100.2","destination.ip":"10.0.0.200","action":"block","bytes_transferred":0}
{"index":{}}
{"timestamp":"2025-04-23T10:00:15Z","source.ip":"203.0.113.5","destination.ip":"10.0.0.44","action":"allow","bytes_transferred":4096}
{"index":{}}
{"timestamp":"2025-04-23T10:00:30Z","source.ip":"192.0.2.1","destination.ip":"10.0.0.100","action":"allow","bytes_transferred":512}
```
:::

### Query the data

```esql
FROM firewall_logs # The source index
| LOOKUP JOIN threat_list ON source.ip # The lookup index and join field
| WHERE threat_level IS NOT NULL # Filter for rows non-null threat levels
| SORT timestamp # LOOKUP JOIN does not guarantee output order, so you must explicitly sort the results if needed
| KEEP source.ip, action, threat_type, threat_level # Keep only relevant fields
| LIMIT 10 # Limit the output to 10 rows
```

### Response

A successful query will output a table. In this example, you can see that the `source.ip` field from the `firewall_logs` index is matched with the `source.ip` field in the `threat_list` index, and the corresponding `threat_level` and `threat_type` fields are added to the output.

|source.ip|action|threat_type|threat_level|
|---|---|---|---|
|203.0.113.5|allow|C2_SERVER|high|
|198.51.100.2|block|SCANNER|medium|
|203.0.113.5|allow|C2_SERVER|high|

### Additional examples

Refer to the examples section of the [`LOOKUP JOIN`](/reference/query-languages/esql/commands/lookup-join.md) command reference for more examples.

## Prerequisites [esql-lookup-join-prereqs]

### Index configuration

Indices used for lookups must be configured with the [`lookup` index mode](/reference/elasticsearch/index-settings/index-modules.md#index-mode-setting).

### Data type compatibility

Join keys must have compatible data types between the source and lookup indices. Types within the same compatibility group can be joined together:

| Compatibility group    | Types                                                                               | Notes                                                                            |
|------------------------|-------------------------------------------------------------------------------------|----------------------------------------------------------------------------------|
| **Numeric family**     | `byte`, `short`, `integer`, `long`, `half_float`, `float`, `scaled_float`, `double` | All compatible                    |
| **Keyword family**     | `keyword`, `text.keyword`                                                           | Text fields only as join key on left-hand side and must have `.keyword` subfield |
| **Date (Exact)**       | `date`                                                                              | Must match exactly                                                               |
| **Date Nanos (Exact)** | `date_nanos`                                                                        | Must match exactly                                                               |
| **Boolean**            | `boolean`                                                                           | Must match exactly                                                               |

```{tip}
To obtain a join key with a compatible type, use a [conversion function](/reference/query-languages/esql/functions-operators/type-conversion-functions.md) if needed.
```

### Unsupported Types

In addition to the [{{esql}} unsupported field types](/reference/query-languages/esql/limitations.md#_unsupported_types), `LOOKUP JOIN` does not support:

* `VERSION`
* `UNSIGNED_LONG`
* Spatial types like `GEO_POINT`, `GEO_SHAPE`
* Temporal intervals like `DURATION`, `PERIOD`

```{note}
For a complete list of all types supported in `LOOKUP JOIN`, refer to the [`LOOKUP JOIN` supported types table](/reference/query-languages/esql/commands/lookup-join.md).
```

## Usage notes

This section covers important details about `LOOKUP JOIN` that impact query behavior and results. Review these details to ensure your queries work as expected and to troubleshoot unexpected results.

### Handling name collisions

When fields from the lookup index match existing column names, the new columns override the existing ones.
Before the `LOOKUP JOIN` command, preserve columns by either:

* Using `RENAME` to assign non-conflicting names
* Using `EVAL` to create new columns with different names

### Sorting behavior

The output rows produced by `LOOKUP JOIN` can be in any order and may not
respect preceding `SORT`s. To guarantee a certain ordering, place a `SORT` after
any `LOOKUP JOIN`s.

## Limitations

The following are the current limitations with `LOOKUP JOIN`:

* Indices in [`lookup` mode](/reference/elasticsearch/index-settings/index-modules.md#index-mode-setting) are always single-sharded.
* Cross cluster search is unsupported in versions prior to `9.2.0`. Both source and lookup indices must be local for these versions.
* Currently, only matching on equality is supported.
* In Stack versions `9.0-9.1`,`LOOKUP JOIN` can only use a single match field and a single index. Wildcards are not supported.
  * Aliases, datemath, and datastreams are supported, as long as the index pattern matches a single concrete index {applies_to}`stack: ga 9.1.0`.
* The name of the match field in `LOOKUP JOIN lu_idx ON match_field` must match an existing field in the query. This may require `RENAME`s or `EVAL`s to achieve.
* The query will circuit break if there are too many matching documents in the lookup index, or if the documents are too large. More precisely, `LOOKUP JOIN` works in batches of, normally, about 10,000 rows; a large amount of heap space is needed if the matching documents from the lookup index for a batch are multiple megabytes or larger. This is roughly the same as for `ENRICH`.
* Cross-cluster `LOOKUP JOIN` can not be used after aggregations (`STATS`), `SORT` and `LIMIT` commands, and coordinator-side `ENRICH` commands.
