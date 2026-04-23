```yaml {applies_to}
serverless: preview
stack: preview 9.4.0
```

## Syntax

```esql
WITHOUT([dimension1[, dimension2[, ...]]])
```

## Parameters

`dimension`
:   (Optional) One or more [time series dimension](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md#time-series-dimension)
    fields to exclude from the time series grouping. Must be dimension fields of the index
    (not metrics, not regular fields). When called with no arguments, groups by all dimensions.

## Description

`WITHOUT` is a grouping function used with the `BY` clause of
[`STATS`](/reference/query-languages/esql/commands/stats-by.md) inside a
[`TS`](/reference/query-languages/esql/commands/ts.md) source command. It groups by all
time series dimensions **except** the dimensions listed.

The output of a `STATS ... BY WITHOUT(...)` aggregation includes a `_timeseries`
`keyword` column containing a JSON-encoded object with the dimension key/value pairs that
identify each surviving group. When fields are excluded via `WITHOUT(dim1, dim2, ...)`,
those dimensions are omitted from the `_timeseries` object.

`WITHOUT()` (with no arguments) is equivalent to grouping by all dimensions. This is
the explicit form of the implicit "group by all" behavior that `TS` uses when a bare
[time series aggregation function](/reference/query-languages/esql/functions-operators/time-series-aggregation-functions.md)
is used without a `BY` clause. Refer to
[Grouping time series](/reference/query-languages/esql/commands/ts.md#grouping-time-series) for details.

### Limitations

- `WITHOUT` is only supported inside time series queries that start with a
  [`TS`](/reference/query-languages/esql/commands/ts.md) source command. Using it in a
  regular `FROM | STATS ... BY WITHOUT(...)` pipeline fails with:
  `WITHOUT is only supported in time-series queries (i.e. TS | ...) at the moment`.
- All arguments must be dimension fields. Non-dimension fields or non-field expressions
  produce an error.
- `WITHOUT` can only appear in the first `STATS` command of a `TS` pipeline. A subsequent
  `STATS` is a regular aggregation and does not accept `WITHOUT`.

## Supported types

| dimension | result |
| --- | --- |
| keyword | keyword |

## Examples

### Exclude a single dimension

Aggregate by every dimension except `pod`. Results are grouped per unique
`(cluster, region)` combination:

```esql
TS k8s
| STATS total_cost = sum(network.cost) BY WITHOUT(pod)
| SORT total_cost
```

| total_cost:double | _timeseries:keyword |
| --- | --- |
| 15.875 | `{"cluster":"staging","region":"us"}` |
| 18.625 | `{"cluster":"prod","region":["eu","us"]}` |
| 26.5 | `{"cluster":"qa"}` |

### Exclude multiple dimensions

Exclude both `pod` and `region`; grouping collapses to `cluster`:

```esql
TS k8s
| STATS total_cost = sum(network.cost) BY WITHOUT(pod, region)
| SORT total_cost
```

| total_cost:double | _timeseries:keyword |
| --- | --- |
| 15.875 | `{"cluster":"staging"}` |
| 18.625 | `{"cluster":"prod"}` |
| 26.5 | `{"cluster":"qa"}` |

### Group by all dimensions (empty WITHOUT)

`WITHOUT()` explicitly groups by every dimension — equivalent to the implicit "group by
all" behavior when no `BY` clause is specified:

```esql
TS k8s
| STATS total_cost = sum(network.cost) BY WITHOUT()
| SORT total_cost
```

| total_cost:double | _timeseries:keyword |
| --- | --- |
| 0.0 | `{"cluster":"prod","pod":"three","region":["eu","us"]}` |
| 1.75 | `{"cluster":"staging","pod":"two","region":"us"}` |
| 4.625 | `{"cluster":"staging","pod":"one","region":"us"}` |
| 6.25 | `{"cluster":"qa","pod":"one"}` |
| 7.875 | `{"cluster":"prod","pod":"one","region":["eu","us"]}` |
| 9.375 | `{"cluster":"qa","pod":"two"}` |
| 9.5 | `{"cluster":"staging","pod":"three","region":"us"}` |
| 10.75 | `{"cluster":"prod","pod":"two","region":["eu","us"]}` |
| 10.875 | `{"cluster":"qa","pod":"three"}` |
