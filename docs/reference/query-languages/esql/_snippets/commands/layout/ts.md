## `TS` [esql-ts]

The `TS` command is similar to the `FROM` source command,
but with two key differences: it targets only [time-series indices](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md)
and enables the use of time-series aggregation functions
with the [STATS](/reference/query-languages/esql/commands/processing-commands.md#esql-stats-by) command.

**Syntax**

```esql
TS index_pattern [METADATA fields]
```

**Parameters**

`index_pattern`
:   A list of indices, data streams or aliases. Supports wildcards and date math.

`fields`
:   A comma-separated list of [metadata fields](/reference/query-languages/esql/esql-metadata-fields.md) to retrieve.

**Examples**

```esql
TS metrics
| STATS sum(last_over_time(memory_usage))
```

