```yaml {applies_to}
stack: preview
serverless: preview
```

::::{warning}
The `EQL` command is in technical preview and only available in snapshot builds. It may change or be removed in a future release.
::::

The `EQL` source command runs an [EQL (Event Query Language)](docs-content://explore-analyze/query-filter/languages/eql.md)
query and returns its matches as a table, so you can continue processing them with {{esql}}. Rather than
re-implementing EQL, the command delegates execution to the EQL engine and exposes the matches as typed
columns that the rest of the query can filter, aggregate and reshape.

## Syntax

```esql
EQL index_pattern [, index_pattern]* "<eql_query>" [METADATA fields] [WITH { "<option>": <value> [, ...] }]
```

## Parameters

`index_pattern`
:   The indices, data streams or aliases to query, given as one or more comma-separated patterns
    directly after the command name — the same leading position `FROM` uses. Supports wildcards, date
    math and remote-cluster patterns (`<remote_cluster>:<target>`).

`<eql_query>`
:   The EQL query to run, as a string. Supports event queries, `sequence` queries and `sample` queries.
    See the [EQL syntax reference](/reference/query-languages/eql/eql-syntax.md).

`fields`
:   A comma-separated list of [metadata fields](/reference/query-languages/esql/esql-metadata-fields.md)
    to retrieve. Only `_index`, `_id` and `_source` are supported (see [Metadata](#metadata)).

`WITH { ... }`
:   An optional map of [options](#with-options) controlling how the EQL query is executed.

## Description

The `EQL` command runs its query on the coordinating node and turns the matches into an {{esql}} table
whose columns are resolved from the target index mapping the same way `FROM` resolves its columns: each
mapped event field becomes a typed column, and a field whose type {{esql}} cannot read surfaces as an
`unsupported` column, exactly as it would under `FROM`. A field that an event does not contain is `null`
in that row.

For **event queries**, the command returns one row per matching event.

For **`sequence`** and **`sample`** queries, each match is *unnested* to one row per event, so the shape
is the same regardless of how many stages the query has. Three synthetic columns are prepended to the
mapped fields to identify the match each row belongs to:

| Column | Type | Description |
| --- | --- | --- |
| `_sequence` | `long` | Which match this event belongs to (`0`, `1`, …). |
| `_sequence_stage` | `integer` | The stage index of this event within the match (`0`-based). |
| `join_keys` | `keyword` | The join-key values shared by the match (multivalued). |

Use `STATS ... BY _sequence` to aggregate or reconstruct whole matches. A mapped field literally named
`_sequence`, `_sequence_stage` or `join_keys` is rejected for a `sequence`/`sample` query, because it would
collide with the synthetic column of the same name and make the output schema ambiguous.

### Coordinator-only execution

Unlike `FROM`, an `EQL` source is not distributed to data nodes. The command issues an EQL search from
the coordinating node and materializes the whole response into a single table there, and the {{esql}}
pipeline attached to it runs on the coordinator too, without data-node parallelism. Coordinator memory and
CPU therefore bound the query, sized by the EQL result set. Keep that result set small with `LIMIT` or
`WITH { "size": … }` (see below).

### Limiting the number of results

`LIMIT` and the `size` option bound different things:

* A `LIMIT n` placed directly after the command is folded into the EQL request in **every mode**, so only
  about `n` matches are fetched. `LIMIT` bounds **rows**: for a `sequence` or `sample` query, whose rows are
  unnested per event, it can return a partial match (some events of a match without the rest).
* `WITH { "size": n }` bounds whole **matches** — events, sequences or samples — and takes precedence over
  `LIMIT`. Use it when you want complete matches rather than a row count.
* If neither a pushed `LIMIT` nor `WITH { "size": … }` sets the size, the request falls back to the
  {{esql}} result-truncation limit. In that case the command warns that the results may be incomplete.

### Partial results

An event query honors the enclosing {{esql}} query's
[`allow_partial_results`](/reference/query-languages/esql/esql-rest.md) setting. When partial results are
allowed and a shard fails, the query returns the events it could and adds a warning that the results may be
incomplete, rather than reporting a partial result as complete. A `sequence` query is fail-safe by default:
a sequence that lost a stage on a failed shard is dropped rather than returned as a shorter, corrupt match.
Set `WITH { "allow_partial_sequence_results": true }` to prefer resilience over completeness.

### Unmapped fields

The command honors [`SET unmapped_fields`](/reference/query-languages/esql/esql-unmapped-fields.md) for
fields a downstream {{esql}} command references but the mapping does not contain, the same as `FROM`: `nullify`
adds a `null` column, and `load` adds a `keyword` column read from `_source`. Field references inside the
EQL query string itself are resolved by the EQL engine, not by this setting.

## Metadata [metadata]

Add a `METADATA` clause — after the query string, before `WITH` — to append provenance columns, populated
from the EQL response. Only the fields the response carries per event are supported:

| Column | Type | Description |
| --- | --- | --- |
| `_index` | `keyword` | The index the event came from. |
| `_id` | `keyword` | The event document `_id`. |
| `_source` | `_source` | The event document source, as an opaque [`_source`](/reference/query-languages/esql/esql-metadata-fields.md) value. |

Any other metadata field (for example `_score` or `_version`), an unknown name, or a wildcard is rejected.

## `WITH` options [with-options]

The `WITH` map tunes how the EQL query runs. `indices` is **not** an option — the target goes in the
leading index pattern, and passing it here is rejected. Any unknown option, or an option given a
wrong-typed value, is rejected when the query is parsed.

`size`
:   The maximum number of events (event queries) or whole matches (`sequence`/`sample` queries) to return.
    See [Limiting the number of results](#limiting-the-number-of-results) for how it interacts with `LIMIT`.

`fetch_size`
:   The number of events to search at a time when paging through `sequence` and `sample` matches. Defaults to `1000`.

`timestamp_field`
:   The field used to sort events by time. Defaults to `@timestamp`.

`tiebreaker_field`
:   The field used to break ties between events with the same timestamp.

`event_category_field`
:   The field that classifies events into categories (the value an EQL query matches on, such as `process`
    in `process where …`). Defaults to `event.category`.

`result_position`
:   Whether to return results from the beginning (`head`) or the end (`tail`) of the timeline. Defaults to `tail`.

`allow_partial_sequence_results`
:   Whether a `sequence` that spanned a failed shard may still be returned. Defaults to `false`
    (see [Partial results](#partial-results)).

`max_samples_per_key`
:   For `sample` queries, the maximum number of samples returned per set of join-key values.

## Composition

Because the `EQL` command is a first-class source, it can be used wherever `FROM` can:

* As a [subquery](/reference/query-languages/esql/esql-subquery.md) source — `FROM (EQL … | …)` — including
  alongside `FROM` subqueries in the same clause.
* On the right-hand side of `IN` — `WHERE x IN (EQL … | KEEP col)`.
* As the upstream of [`FORK`](/reference/query-languages/esql/commands/fork.md) — `EQL … | FORK (…) (…)`.
* As the stored body of a [view](/reference/query-languages/esql/esql-views.md), read with `FROM <view>`.

Under `FORK`, the EQL source is run once per branch, so `_sequence` numbers matches *within* a branch.
Combine it with `_fork` — `… BY _fork, _sequence` — to identify a match across branches.

## Examples

Count the process events matching an event query:

```esql
EQL logs-endpoint "process where process.name == \"cmd.exe\""
| STATS count = COUNT(*)
```

Return matching events as typed columns and keep just a few:

```esql
EQL logs-endpoint "network where destination.port == 443"
| KEEP @timestamp, process.name, destination.port
| SORT @timestamp
| LIMIT 100
```

Run a `sequence` query and count the events and the distinct matches:

```esql
EQL logs-endpoint "sequence by process.pid [process where true] [network where true]"
| STATS events = COUNT(*), matches = COUNT_DISTINCT(_sequence)
```

Inspect the events of each matched sequence, ordered by stage:

```esql
EQL logs-endpoint "sequence by process.pid [process where true] [network where true]"
| KEEP _sequence, _sequence_stage, join_keys, process.name
| SORT _sequence, _sequence_stage
```

Add the source index and document id with `METADATA`:

```esql
EQL logs-endpoint "process where process.name == \"regsvr32.exe\"" METADATA _index, _id
| KEEP _index, _id, process.name
```

Use an `EQL` source inside a subquery and feed a value set to `IN`:

```esql
FROM logs-endpoint
| WHERE process.pid IN (EQL logs-endpoint "process where process.name == \"cmd.exe\"" | KEEP process.pid)
| STATS count = COUNT(*)
```

Split an event query into two branches with `FORK`:

```esql
EQL logs-endpoint "process where true"
| FORK ( WHERE process.pid == 100 ) ( WHERE process.pid == 200 )
| KEEP process.name, process.pid, _fork
| SORT process.pid
```

Query a remote cluster by prefixing the leading pattern with the cluster name:

```esql
EQL my_remote:logs-endpoint "process where process.name == \"regsvr32.exe\""
| STATS count = COUNT(*)
```

## Limitations

* **Coordinator-bound compute.** The EQL source and everything downstream of it run on the coordinating
  node (see [Coordinator-only execution](#coordinator-only-execution)); there is no data-node parallelism.
  Bound the result set with `LIMIT` or `WITH { "size": … }`.
* **No runtime fields in the EQL predicate.** The EQL query can reference only fields present in the index
  mapping. Compute derived columns *after* the command with [`EVAL`](/reference/query-languages/esql/commands/eval.md)
  rather than inside the EQL predicate.
* **The request `filter` is rejected.** A query that combines an enclosing {{esql}} request `filter` with an
  EQL source is rejected rather than silently ignoring the filter, because it is not yet bridged into the EQL
  source. Narrow the events with the EQL predicate itself, or filter the rows with a downstream `WHERE`.
* **A view is not a valid EQL target.** The leading pattern must be indices, data streams or aliases. A
  view whose *body* is an EQL command is supported and read with `FROM <view>`, but a view name in the
  leading pattern is not expanded.
