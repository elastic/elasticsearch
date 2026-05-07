```yaml {applies_to}
serverless: ga
stack: preview =9.1, ga 9.2+
```

:::{note}
The `CHANGE_POINT` command requires a [platinum license](https://www.elastic.co/subscriptions).
:::

`CHANGE_POINT` detects spikes, dips, and change points in a metric.

## Syntax

```esql
CHANGE_POINT value [ON key] [AS type_name, pvalue_name]
```

% Hidden while CHANGE_POINT BY is behind Build.current().isSnapshot();
% replace the plain code block above with the applies-switch below once the snapshot gate is lifted.
% ::::{applies-switch}
%
% :::{applies-item} { stack: ga 9.2+, "serverless": "ga"}
% ```esql
% CHANGE_POINT value [ON key] [AS type_name, pvalue_name]
% ```
% :::
%
% :::{applies-item} { "stack": "preview 9.5", "serverless": "preview" }
% ```esql
% CHANGE_POINT value [ON key] [AS type_name, pvalue_name] [BY grouping_expression1[, ..., grouping_expressionN]]
% ```
% :::
%
% ::::


## Parameters

`value`
:   The column with the metric in which you want to detect a change point.

`key`
:   The column with the key to order the values by. If not specified, `@timestamp` is used.

% Hidden while CHANGE_POINT BY is behind Build.current().isSnapshot(); restore once the snapshot gate is lifted.
% `group` {applies_to}`stack: preview 9.5` {applies_to}`serverless: preview`
% :   The column to group values by. When specified, change point detection is performed independently for each group.

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
% Hidden while CHANGE_POINT BY is behind Build.current().isSnapshot(); restore once the snapshot gate is lifted.
%
% When a `BY` clause is provided, these rules apply per group. {applies_to}`stack: preview 9.5` {applies_to}`serverless: preview`
::::

## Examples

The following example detects a step change in a metric:

:::{include} ../examples/change_point.csv-spec/changePointForDocs.md
:::

% Hidden while CHANGE_POINT BY is behind Build.current().isSnapshot(); restore once the snapshot gate is lifted.
% The following example detects a step change independently for each group: {applies_to}`stack: preview 9.5` {applies_to}`serverless: preview`
%
% :::{include} ../examples/change_point.csv-spec/changePointForDocsByGroup.md
% :::
