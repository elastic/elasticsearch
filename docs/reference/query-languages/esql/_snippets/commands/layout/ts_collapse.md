```yaml {applies_to}
stack: preview 9.5
serverless: preview
```

`TS_COLLAPSE` is a processing command that collapses the expanded, per-step output of a `PROMQL`
command into one row per time series. The metric column and the `step` column become multi-valued
fields; dimension columns remain single-valued.

## Syntax

```esql
... | TS_COLLAPSE
```

## Parameters

None.

## Description

A `PROMQL` command produces one row per `(series, step)` pair, with dimension columns (for
example `host`, `cluster`) repeated on every row. `TS_COLLAPSE` merges all rows that belong to
the same series into a single row:

- The metric column (the named result column, for example `rate`) and the `step` column become
  multi-valued (MV) fields whose elements are ordered chronologically.
- Dimension columns keep a single value per row because they are identical for all steps of a
  given series.
- Missing or `null` samples are omitted from both MV fields, so the result is sparse rather than
  null-filled.

The MV fields are **positionally aligned**: `step[i]` corresponds to the metric value at index
`i`. Applying MV functions independently to one of the two columns—for example
[`MV_SORT`](/reference/query-languages/esql/functions-operators/mv-functions/mv_sort.md)—will
break this alignment.

`TS_COLLAPSE` can only appear immediately after a `PROMQL` command. Other processing commands
can follow `TS_COLLAPSE`.

## Examples

### Collapse a multi-step result

A `PROMQL` query without `TS_COLLAPSE` returns one row per `(series, step)` pair. Adding
`TS_COLLAPSE` produces one row per series, with the metric and `step` columns as positionally
aligned MV arrays:

:::{include} ../examples/k8s-timeseries-promql-collapsed.csv-spec/ts_collapse.md
:::

### Collapse a per-dimension result

When the `PROMQL` query groups results by a dimension (for example `cluster`), `TS_COLLAPSE`
returns one row per unique dimension value, with metric values and steps collapsed into MV arrays:

:::{include} ../examples/k8s-timeseries-promql-collapsed.csv-spec/ts_collapse_by_cluster.md
:::

### Post-process with EVAL

Use [`EVAL`](/reference/query-languages/esql/commands/eval.md) with
[MV functions](/reference/query-languages/esql/functions-operators/mv-functions.md) to compute
per-series aggregates over the collapsed arrays, then sort or filter on the results:

:::{include} ../examples/k8s-timeseries-promql-collapsed.csv-spec/ts_collapse_eval.md
:::
