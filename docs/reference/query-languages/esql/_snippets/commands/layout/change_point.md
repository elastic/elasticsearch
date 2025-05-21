## `CHANGE_POINT` [esql-change_point]

:::{note}
The `CHANGE_POINT` command requires a [platinum license](https://www.elastic.co/subscriptions).
:::

::::{warning}
This functionality is in technical preview and may be
changed or removed in a future release. Elastic will work to fix any
issues, but features in technical preview are not subject to the support
SLA of official GA features.
::::

`CHANGE_POINT` detects spikes, dips, and change points in a metric.

**Syntax**

```esql
CHANGE_POINT value [ON key] [AS type_name, pvalue_name]
```

**Parameters**

`value`
:   The column with the metric in which you want to detect a change point.

`key`
:   The column with the key to order the values by. If not specified, `@timestamp` is used.

`type_name`
:   The name of the output column with the change point type. If not specified, `type` is used.

`pvalue_name`
:   The name of the output column with the p-value that indicates how extreme the change point is. If not specified, `pvalue` is used.

**Description**

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
There must be at least 22 values for change point detection. Fewer than 1,000 is preferred.
::::

**Examples**

The following example shows the detection of a step change:

:::{include} ../examples/change_point.csv-spec/changePointForDocs.md
:::
