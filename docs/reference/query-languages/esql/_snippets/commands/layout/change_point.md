```yaml {applies_to}
serverless: ga
stack: preview =9.1, ga 9.2+
```

:::{note}
The `CHANGE_POINT` command requires a [platinum license](https://www.elastic.co/subscriptions).
:::

`CHANGE_POINT` detects spikes, dips, and change points in a metric.

## Syntax

::::{applies-switch}

:::{applies-item} { stack: ga 9.2+, "serverless": "ga"}
```esql
CHANGE_POINT value [ON key] [AS type_name, pvalue_name]
```
:::

:::{applies-item} { "stack": "ga 9.5", "serverless": "ga" }
```esql
CHANGE_POINT value [ON key] [AS type_name, pvalue_name] [BY grouping_expression1[, ..., grouping_expressionN]]
```
:::

::::


## Parameters

`value`
:   The column with the metric in which you want to detect a change point.

`key`
:   The column with the key to order the values by. If not specified, `@timestamp` is used.

`group` {applies_to}`stack: ga 9.5` {applies_to}`serverless: ga`
:   The column to group values by. When specified, change point detection is performed independently for each group.

`type_name`
:   The name of the output column with the change point type. If not specified, `type` is used.

`pvalue_name`
:   The name of the output column with the p-value that indicates how extreme the change point is. If not specified, `pvalue` is used.

## Description

`CHANGE_POINT` detects spikes, dips, and change points in a metric. The command adds columns to
the table with the change point type and p-value, that indicates how extreme the change point is
(lower values indicate greater changes).

The possible change point types are:
* `dip`: a significant dip occurs at this change point
* `distribution_change`: the overall distribution of the values has changed significantly
* `spike`: a significant spike occurs at this point
* `step_change`: the change indicates a statistically significant step up or down in value distribution
* `trend_change`: there is an overall trend change occurring at this point

::::{note}
There must be at least 22 values for change point detection. Any values beyond the first 1,000 are ignored.

When `ON key` matches a `STATS` grouping produced by `BUCKET` on a `datetime` or `date_nanos` field (any field name), and a time range is available from foldable `from` and `to` arguments on `BUCKET` or from an `@timestamp` range filter on the request, missing bucket keys between real buckets are always zero-filled so the value sequence is contiguous. If fewer than 22 real buckets are present and interior gap fill alone does not reach 22, additional buckets are added alternately to the left and right of the real data until 22 are reached or the timerange ends; when one side reaches the range edge, padding continues on the other side only. Real buckets are never removed. Detection is still sequence-based, not time-aware.

Gap-filled buckets always use a value of `0`. That is appropriate for count-like metrics (for example `COUNT()` and `SUM()` of events) where a missing bucket means no activity. It is not correct for other aggregations such as `AVG()`, `MIN()`, and `MAX()`, where a missing bucket does not imply zero. For those metrics, gap filling is best-effort so sparse time-bucketed series can reach the 22-value minimum; interpret results with care. A future ES|QL enhancement may allow choosing the fill value, but the syntax does not expose that today.

When a `BY` clause is provided, these rules apply per group. {applies_to}`stack: ga 9.5` {applies_to}`serverless: ga`
::::

## Examples

The following example detects a step change in a metric:

:::{include} ../../generated/x-pack-esql/commands/examples/change_point.csv-spec/changePointForDocs.md
:::

The following example detects a step change independently for each group: {applies_to}`stack: ga 9.5` {applies_to}`serverless: ga`

:::{include} ../../generated/x-pack-esql/commands/examples/change_point.csv-spec/changePointForDocsByGroup.md
:::
