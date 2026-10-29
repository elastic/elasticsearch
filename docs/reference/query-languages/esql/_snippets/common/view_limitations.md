#### Document- and field-level security

A role entry that grants `read` on a view name must not carry DLS or FLS restrictions. If it does, the query fails with a `403`. DLS/FLS on the underlying indices is applied normally. Refer to [view privileges](/reference/query-languages/esql/esql-views.md#esql-views-privileges).

#### Branching inside views

Views may be nested up to depth 10.

::::{applies-switch}
:::{applies-item} { "stack": "ga 9.6+", "serverless": "ga" }
View definitions can contain branched query plans created by `FROM` subqueries,
nested views, and [`FORK`](/reference/query-languages/esql/commands/fork.md).
These branches can be nested, subject to the branch and command limits described
in [How views execute](/reference/query-languages/esql/esql-views.md#how-views-execute).
:::
:::{applies-item} stack: preview 9.4-9.5
Branching and nesting are allowed in combination only when there is no more than
one branch point. A view can contain subqueries only when the resulting plan can
be flattened through
[query compaction](/reference/query-languages/esql/esql-views.md#query-compaction).
:::
::::

#### Cross-cluster and serverless [esql-views-cross-cluster-serverless]

Views can reference remote indices in [cross-cluster search](/reference/query-languages/esql/esql-cross-clusters.md), but you can't query a view that is defined on a remote cluster.

::::{applies-switch}
:::{applies-item} stack: ga 9.6+
Remote views are invisible to source resolution. An exact reference such as `FROM cluster:view` returns an `Unknown index` error.
:::
:::{applies-item} stack: preview 9.4-9.5
A query fails if its source expression matches a remote view.
:::
::::

Views are available in serverless and [cross-project search](/reference/query-languages/esql/esql-cross-serverless-projects.md), but with some limitations:
:::{include} ../common/cps_view_limitations.md
:::

#### Query DSL filters

::::{applies-switch}
:::{applies-item} { "stack": "ga 9.6+", "serverless": "ga" }
A Query DSL filter on the request applies to the output of a view, after the commands in the view definition. If Elasticsearch can't translate a filter clause to ES|QL, it omits that clause for the view and returns a warning. Check the response warnings because omitting a clause can produce more results than the complete filter would.

For example, create a view that counts sales by region:

```console
PUT /_query/view/sales_by_region
{
  "query": "FROM sales | STATS total = COUNT(*) BY region"
}
```

The following request requires both `should` clauses to match:

```console
POST /_query
{
  "query": "FROM sales_by_region | KEEP region, total | LIMIT 1000",
  "filter": {
    "bool": {
      "should": [
        { "term": { "region": "eu" } },
        { "term": { "total": 3 } }
      ],
      "minimum_should_match": 2
    }
  }
}
```

ES|QL can't translate a `minimum_should_match` value greater than `1`. It skips
the `should` group for the view, which can return more rows than the complete
filter would, and returns this warning:

```text
The request filter could not be fully applied to view(s); the following Query DSL constructs are not supported and were skipped: [bool[minimum_should_match=2]] on view [sales_by_region]. Use a WHERE clause to filter rows from views instead
```
:::
:::{applies-item} stack: preview 9.4-9.5
A Query DSL filter on the request applies to the source indices in the view definition, not to the view output.
:::
::::

#### Aliases, data streams, and transform results

A view definition can read from index aliases and data streams. An alias cannot
point to a view.

An [{{es}} transform](docs-content://explore-analyze/transforms.md) is not an
{{esql}} source. To query transform results from a view, use the transform's
destination index as the source in the view definition.

#### Data Federation datasets
```yaml {applies_to}
stack: preview 9.6+, experimental =9.5
serverless: unavailable
```

A view definition can read from a [dataset](/reference/query-languages/esql/esql-data-federation-datasets.md), which [{{esql}} Data Federation](/reference/query-languages/esql/esql-data-federation.md) uses to query external data.

{applies_to}`stack: experimental 9.6+` A wildcard in the view definition
matches datasets only when the view definition sets `wildcards_match_datasets`
to `true`.

#### Query settings

::::{applies-switch}
:::{applies-item} { "stack": "ga 9.6+", "serverless": "ga" }

Only the following settings are supported in a view definition:

| Setting | Supported values |
|---|---|
| [`unmapped_fields`](/reference/query-languages/esql/directives/set.md#esql-unmapped_fields) | `DEFAULT`, `NULLIFY`, and `LOAD` |
| [`wildcards_match_views`](/reference/query-languages/esql/directives/set.md#esql-wildcards_match_views) | `true` and `false` |
| [`wildcards_match_datasets`](/reference/query-languages/esql/directives/set.md#esql-wildcards_match_datasets) {applies_to}`stack: experimental 9.6+` | `true` and `false` |

{applies_to}`{"stack": "preview 9.6+", "serverless": "preview"}` `LOAD_ALL`
is not supported in a view definition.

All other settings, including `time_zone`, are rejected in a view definition.
:::
:::{applies-item} stack: preview 9.4-9.5
A view definition can't contain a `SET` directive.
:::
::::

##### Settings across view boundaries

::::{applies-switch}
:::{applies-item} { "stack": "ga 9.6+", "serverless": "ga" }
The three settings supported in view definitions are scoped at every view
boundary. Values from the calling query, request body, or an enclosing view
don't enter the view. The cluster setting is used to set the default throughout
the query, both inside and outside views. A value in a view definition overrides
that default for its body, but doesn't enter a nested view. If neither is
configured, the built-in default applies.

Other query settings are rejected in a view definition, but aren't scoped to
individual views. Their effective value for the calling query can apply while
evaluating a view. For example, `time_zone` can apply inside a view.

At a view boundary:

* `unmapped_fields`: Fields loaded by an outer query are `null` for rows from a
  view unless the view produces those fields itself. If an outer setting would
  have loaded fields that the view doesn't produce, the response warning
  identifies the view and the fields that are `null`.
* For each supported wildcard setting, exact names work regardless of the
  setting. A wildcard matches the corresponding target type only when the view
  definition sets the setting to `true`. If a `true` value from outside the view
  stops at the boundary and the wildcard would otherwise have matched targets,
  the response returns a warning that identifies the view and the missed
  matches.
:::
:::{applies-item} stack: preview 9.4-9.5
The effective query settings apply throughout the query, including inside view
bodies.
:::
::::

#### Query parameters

Query parameters are not allowed in a stored view definition. A calling query
can still use parameters in commands after `FROM view_name`, but those parameters
are not substituted into the view body.

#### Time series data in views
```{applies_to}
stack: ga 9.6+
serverless: ga
```

A view that starts with [`TS`](/reference/query-languages/esql/commands/ts.md)
must also contain a `STATS` command after it. A view without `STATS` is
rejected when you create or update it. This ensures the view returns plain
tabular results. A `PROMQL` view must also use an aggregation function.

Query a time series view with `FROM view_name`. A view name cannot be used as a
source in `TS` or `PROMQL`. An exact view name in one of those source commands is
rejected, and a wildcard source expression skips matching views.

#### Validation and dependencies

When you create or update a view, Elasticsearch validates its name, size, query
syntax, and the settings that are allowed in a view definition.

{applies_to}`{"stack": "ga 9.6+", "serverless": "ga"}` For a time series
view, Elasticsearch also validates that `TS` is followed by `STATS`.

Elasticsearch doesn't resolve or execute the query, and it doesn't check
whether referenced indices, fields, datasets, or views exist, or whether nested
views form a cycle. Creation doesn't return warnings for those problems.
Elasticsearch reports them when you query the view.

Deleting a resource doesn't check whether a view depends on it. Deleting an index, data stream, alias, dataset, or nested view can therefore cause existing views to fail when queried.
