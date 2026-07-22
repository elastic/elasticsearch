```yaml {applies_to}
stack: preview
serverless: preview
```

::::{warning}
The `EQL` command is in technical preview and only available in snapshot builds. It may change or be removed in a future release.
::::

The `EQL` source command runs an [EQL (Event Query Language)](docs-content://explore-analyze/query-filter/languages/eql.md)
query and returns its results as a table, so you can continue processing them with {{esql}}. Rather than
re-implementing EQL, the command delegates execution to the EQL engine and exposes the results under a
fixed schema that {{esql}} knows at planning time.

## Syntax

```esql
EQL "<eql_query>" WITH { "indices": "<index_pattern>" [, "<option>": <value>]* }
```

## Parameters

`<eql_query>`
:   The EQL query to run, as a string. Supports event queries, `sequence` queries and `sample`
    queries. See [EQL syntax reference](/reference/query-languages/eql/eql-syntax.md).

`WITH { ... }`
:   A map of options controlling how the EQL query is executed:

    `indices`
    :   (Required) A comma-separated index pattern identifying the indices to query.

    `size`
    :   The maximum number of events (for event queries) or sequences/samples to return. Defaults to `10`.

    `fetch_size`
    :   The number of events to search at a time when paging through sequence and sample matches. Defaults to `1000`.

    `timestamp_field`
    :   The field used to sort events by time. Defaults to `@timestamp`.

    `event_category_field`
    :   The field that classifies events into categories (the value matched by an EQL query's event category,
        such as `process` in `process where ...`). Defaults to `event.category`.

    `tiebreaker_field`
    :   The field used to break ties between events with the same timestamp.

    `result_position`
    :   Whether to return results from the beginning (`head`) or the end (`tail`) of the timeline. Defaults to `tail`.

## Description

EQL results are document-shaped, so the `EQL` command projects them onto a fixed set of columns that
depend only on the kind of EQL query. The event payload is returned as an opaque `_source` column (the
same type as [`METADATA _source`](/reference/query-languages/esql/esql-metadata-fields.md)); use downstream
{{esql}} commands to reduce, count or reshape the results.

For **event queries**, the command returns one row per matching event:

| Column | Type | Description |
| --- | --- | --- |
| `_index` | `keyword` | The index the event came from. |
| `_id` | `keyword` | The event document `_id`. |
| `_source` | `_source` | The event document source. |

For **sequence** and **sample** queries, the matches are *unnested* to one row per event, so the schema
is the same regardless of how many stages the query has:

| Column | Type | Description |
| --- | --- | --- |
| `_seq` | `long` | Which match this event belongs to (`0`, `1`, ...). |
| `_position` | `integer` | The stage index of this event within the match (`0`-based). |
| `join_keys` | `keyword` | The join-key values shared by the match (multivalued). |
| `_index` | `keyword` | The index the event came from. |
| `_id` | `keyword` | The event document `_id`. |
| `_source` | `_source` | The event document source. |

Use `STATS ... BY _seq` to reconstruct or aggregate whole matches.

::::{note}
Because the target indices are given by the `indices` option rather than a `FROM` pattern, the `EQL`
command is not supported in cross-cluster search.
::::

## Examples

Count the process events matching an EQL event query:

```esql
EQL "process where process.name == \"cmd.exe\"" WITH { "indices": "logs-*" }
| STATS count = COUNT(*)
```

Return the raw matching events and keep just the identifiers:

```esql
EQL "network where destination.port == 443" WITH { "indices": "logs-*", "size": 100 }
| KEEP _index, _id
```

Run a sequence query and reconstruct each match with {{esql}} aggregation:

```esql
EQL "sequence by process.pid [process where true] [network where true]" WITH { "indices": "logs-*" }
| STATS events = COUNT(*), matches = COUNT_DISTINCT(_seq)
```

Inspect the events of each matched sequence, ordered by stage:

```esql
EQL "sequence by process.pid [process where true] [network where true]" WITH { "indices": "logs-*" }
| KEEP _seq, _position, join_keys, _id
| SORT _seq, _position
```
