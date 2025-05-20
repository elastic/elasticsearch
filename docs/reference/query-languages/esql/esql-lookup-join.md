---
navigation_title: "Join data with LOOKUP JOIN"
mapped_pages:
 - https://www.elastic.co/guide/en/elasticsearch/reference/8.18/_lookup_join.html
---

# Join data from multiple indices with `LOOKUP JOIN` [esql-lookup-join-reference]

The {{esql}} [`LOOKUP JOIN`](/reference/query-languages/esql/commands/processing-commands.md#esql-lookup-join) processing command combines data from your {{esql}} query results table with matching records from a specified lookup index. It adds fields from the lookup index as new columns to your results table based on matching values in the join field.

Teams often have data scattered across multiple indices – like logs, IPs, user IDs, hosts, employees etc. Without a direct way to enrich or correlate each event with reference data, root-cause analysis, security checks, and operational insights become time-consuming.

For example, you can use `LOOKUP JOIN` to:

* Retrieve environment or ownership details for each host to correlate your metrics data.
* Quickly see if any source IPs match known malicious addresses.
* Tag logs with the owning team or escalation info for faster triage and incident response.

## Compare with `ENRICH`

[`LOOKUP JOIN`](/reference/query-languages/esql/commands/processing-commands.md#esql-lookup-join) is similar to [`ENRICH`](/reference/query-languages/esql/commands/processing-commands.md#esql-enrich) in the fact that they both help you join data together. You should use `LOOKUP JOIN` when:

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
- The name of the lookup index (which must have the `lookup` [`index.mode setting`](/reference/elasticsearch/index-settings/index-modules.md#index-mode-setting))
- The name of the field to join on

```esql
LOOKUP JOIN <lookup_index> ON <field_name>
```

:::{image} ../images/esql-lookup-join.png
:alt: Illustration of the `LOOKUP JOIN` command, where the input table is joined with a lookup index to create an enriched output table.
:::

If you're familiar with SQL, `LOOKUP JOIN` has left-join behavior. This means that if no rows match in the lookup index, the incoming row is retained and `null`s are added. If many rows in the lookup index match, `LOOKUP JOIN` adds one row per match.

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
    "index.mode": "lookup" # The lookup index must use this mode
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

Refer to the examples section of the [`LOOKUP JOIN`](/reference/query-languages/esql/commands/processing-commands.md#esql-lookup-join) command reference for more examples.

## Prerequisites [esql-lookup-join-prereqs]

To use `LOOKUP JOIN`, the following requirements must be met:

* Indices used for lookups must be configured with the [`lookup` index mode](/reference/elasticsearch/index-settings/index-modules.md#index-mode-setting)
* **Compatible data types**: The join key and join field in the lookup index must have compatible data types. This means:
  * The data types must either be identical or be internally represented as the same type in {{esql}}
  * Numeric types follow these compatibility rules:
    * `short` and `byte` are compatible with `integer` (all represented as `int`)
    * `float`, `half_float`, and `scaled_float` are compatible with `double` (all represented as `double`)
  * For text fields: You can only use text fields as the join key on the left-hand side of the join and only if they have a `.keyword` subfield

To obtain a join key with a compatible type, use a [conversion function](/reference/query-languages/esql/functions-operators/type-conversion-functions.md) if needed.

For a complete list of supported data types and their internal representations, see the [Supported Field Types documentation](/reference/query-languages/esql/limitations.md#_supported_types).

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
* Cross cluster search is unsupported initially. Both source and lookup indices must be local.
* Currently, only matching on equality is supported.
* `LOOKUP JOIN` can only use a single match field and a single index. Wildcards, aliases, datemath, and datastreams are not supported.
* The name of the match field in `LOOKUP JOIN lu_idx ON match_field` must match an existing field in the query. This may require `RENAME`s or `EVAL`s to achieve.
* The query will circuit break if there are too many matching documents in the lookup index, or if the documents are too large. More precisely, `LOOKUP JOIN` works in batches of, normally, about 10,000 rows; a large amount of heap space is needed if the matching documents from the lookup index for a batch are multiple megabytes or larger. This is roughly the same as for `ENRICH`.
