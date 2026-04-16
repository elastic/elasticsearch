---
navigation_title: "Define virtual indices using ES|QL views"
applies_to:
  serverless: preview
  stack: preview 9.4.0
products:
  - id: elasticsearch
---

# {{esql}} Views [esql-views]

A view is a virtual index with fields that are produced from the output of an ES|QL query.
Each view has a name and a definition. The definition is a complete ES|QL query, with source commands and processing commands.
The view name can be used within an index pattern in the [`FROM`](/reference/query-languages/esql/commands/from.md) command of any normal ES|QL query, as well as within another view definition.
When the main query is executed, all referenced views are also executed independently.
This ensures up-to-date results from all source indexes referenced by both the main query and the view definitions themselves.
The final output combines all these results into a single list, including any duplicate rows.

## Defining views

Define a view using the REST API:
* [Create or update an ES|QL View](/api/doc/elasticsearch/operation/operation-esql-put-view)
* [Delete an ES|QL View](/api/doc/elasticsearch/operation/operation-esql-delete-view)
* [Get or list ES|QL Views](/api/doc/elasticsearch/operation/operation-esql-get-view)

For example, consider the following ES|QL query:

:::{include} _snippets/commands/examples/views.csv-spec/views_plain_addresses.md
:::

We can define a view called `country_addresses` using that query:

```console
PUT /_query/view/country_addresses
{
    "query": """
        FROM addresses
        | RENAME city.country.name AS country
        | EVAL country = CASE(country == "United States of America", "United States", country)
        | STATS count=COUNT() BY country
        """
}
```

Now a query like `FROM country_addresses` will produce the same output:

:::{include} _snippets/commands/examples/views.csv-spec/views_country_addresses.md
:::

## Using views

Use views as if they were ordinary indices:

```esql
FROM index_pattern
```

Where `index_pattern` is a comma-separate list of index or view names, including
wildcards and date-math.

Much like with [subqueries](/reference/query-languages/esql/esql-subquery.md),
views enable you to combine results from multiple independently processed
data sources within a single query. Each view runs its own pipeline of
processing commands (such as `WHERE`, `EVAL`, `STATS`, or `SORT`) and the
results are combined together with results from other index patterns, views or subqueries
in the `FROM` clause.

Fields that exist in one source but not another are filled with `null` values.

### Nesting and Branching

If a view definition contains a reference to another view, that is called a nested view.
ES|QL allows nesting to a depth of 10.
When multiple views are referenced within the same index-pattern, this leads
to a branched query plan where each view will be executed independently, in parallel
if possible, similar to subqueries and [`FORK`](/reference/query-languages/esql/commands/fork.md).
There is a maximum allowed branching factor of 8, for the combination of views, subqueries and `FORK`.
So, for example, a single index pattern could reference four views and four subqueries,
but adding just one more view or subquery would exceed the allowed limit and the query will fail.

Branching and nested are allowed in combination as long as there is never more than one branch point.
This means that nested branching is not allowed:
* A view can contain subqueries, but then that view cannot be used together with other views, and all subqueries can only reference nested views that contain no further branching.
* A subquery can contain views, but then those views must themselves contain no branches (no subqueries or `FORK`)

### Limitations and known issues

Views have certain limitations:
* Commands that also generate branched query plans are usually not allowed within views, unless the branch points can be merged:
  * `FORK`
  * [subqueries](/reference/query-languages/esql/esql-subquery.md]
* Cross-cluster search:
  * Remote views in CCS are not allowed (ie. `FROM cluster:view` will only match remote indexes with the name `view`. If a remote view is found, the query will fail).
  * If a remote index matches a local view name, the query will fail.
* Serverless and Cross-project search
  * Remote views in CPS are not allowed (ie. `FROM view` will only match origin project views, and if linked project views with that name are found, the query will fail).
  * If a linked project contains a view with the same name as a local view or index, the query will fail.
* Query parameters are not allowed in the view definition, and therefor query parameters in the main query will never impact the view results.

Views are in tech-preview and there are a number of known issues, or behavior that is likely to change in the future:
* Query DSL filtering on the main query will currently affect the source indices in the view definition, and this will change in later releases.
  * The future design will have the query filtering impact the output of the view, not the source indices
* METADATA directives inside and outside a view definition behave the same as they do for
  [`METADATA` in subqueries](/reference/query-languages/esql/esql-subquery.md#subqueries-with-metadata).

## Examples

The following examples show how to use views within the `FROM` command.

### Combine data from multiple indices

Assume we've defined three views in a similar way to the example above, each counting the number of documents that reference a particular country, but from three different source indices:
* `country_airports` - reports counts of documents per country from our `airports` index
* `country_addresses` - reports counts of documents per country from our `addresses` index
* `country_languages` - reports counts of documents per country from our `languages` index

Now we can query these together with a query like:

:::{include} _snippets/commands/examples/views.csv-spec/views_country_filtered.md
:::

Note how documents from multiple views are not combined. Since we know these views all have a `count` column, we could `SUM` those.

### Using wildcards

:::{include} _snippets/commands/examples/views.csv-spec/views_country_wildcard_sum.md
:::

Note how we used `SUM` to combine the counts of the three previously aggregated `count` columns.

### Use LOOKUP JOIN inside a view

We can define views with complex queries, including commands like `LOOKUP JOIN`:

```console
PUT /_query/view/airports_mp_filtered
{
    "query": """
        FROM airports
        | RENAME abbrev AS code
        | LOOKUP JOIN airports_mp ON abbrev == code
        | WHERE abbrev IS NOT NULL
        | DROP code
       """
}
```

This creates a view called `airports_mp_filtered` that contains all rows from the `airports` index that also have a matching `abbrev` inside the `airports_mp` index.
This is effectively a subset of the `airports` index.

We could, for example, see how many airports are defined only in `airports` versus how many are defined in the view, by combining both a view and an index in the same `FROM` command:

:::{include} _snippets/commands/examples/views.csv-spec/airports_mp_filtered_combined.md
:::

### Views with METADATA

The [`METADATA` directive](/reference/query-languages/esql/esql-metadata-fields.md) is supported both inside and outside a view, and
follows the same rules as observed for [`METADATA` in Subqueries](/reference/query-languages/esql/esql-subquery.md#subqueries-with-metadata).
Inside the view it generates columns, just like other fields, and these can be used for filtering and as output columns.

Outside the view it generates `null` values.
Note that this is a known limitation of the current tech-preview, and is anticipated to be addressed in a future update,
at which point `METADATA _index` will contain the name of the view.

## Comparing views to subqueries

There are many similarities and differences between views and subqueries.

### Similarities

Both views and subqueries will process the entire set of query definitions at query time, resulting in an up-to-date response when source indexes are changed and the query is re-run.
Columns from the results of views and subqueries are merged into the main query, expanding the table of results, and inserting `null` values if any view or subquery has different columns than the others.
Complex processing commands can be used inside both views and subqueries, as detailed in the [description of subqueries](/reference/query-languages/esql/esql-subquery.md#description).
In theory, neither is capable of supporting nested branching (ie. subqueries within subqueries), however views do support this to some extent, as detailed below.

### Differences

Views have names, and these names are unique within the index namespace. This means a view cannot have the same name as an index, and vice versa.
Views can be nested within one another, as long as neither of the following two rules are broken:
* Cyclic references are not allowed. For example, if `viewA` references `viewB` and `viewB` references `viewC` it is not allowed to have `viewC` reference `viewA`.
  * Detection of cyclic references is done at main query execution time
* Multiple branching points do not exist

This last point highlights a difference between views and subqueries.
While subqueries simply disallow the use of further subqueries or `FORK` within a subquery, views will allow this under limited conditions.

## Query compaction

Consider the case where we defined a view that contains subqueries:

```console
PUT /_query/view/view_x
{
    "query": """
        FROM (
            FROM app-events-* | KEEP msg, level
        ), (
            FROM auth-events-* | KEEP msg, level
        )
       """
}
```

And another view that contains further sub-queries

```console
PUT /_query/view/view_y
{
    "query": """
        FROM (
            FROM nginx-events-* | KEEP msg, level
        ), (
            FROM apache-events-* | KEEP msg, level
        )
       """
}
```

If we attempt to use these together:

```esql
FROM other-events, view_x, view_y
| STATS count(msg) BY level
```

This will be resolved initially to a query plan that has two levels of branching:
* Top of plan does the stats
  * Next level has three branches
    * Read data from `other-events`
    * Read data from `view_x`, which means running the query inside `view_x` which itself contains two branches
      * Read data from `app-events-*` and keep two columns
      * Read data from `auth-events-*` and keep two columns
    * Read data from `view_y`, which means running the query inside `view_x` which itself contains two branches
      * Read data from `nginx-events-*` and keep two columns
      * Read data from `apache-events-*` and keep two columns

So we have three branches, two of which each have a further two branches. This is not allowed. The subquery version of this will fail:

```esql
FROM
    other-events,
    (
        FROM (
            FROM app-events-* | KEEP msg, level
        ), (
            FROM auth-events-* | KEEP msg, level
        )
    ),
    (
        FROM (
            FROM nginx-events-* | KEEP msg, level
        ), (
            FROM apache-events-* | KEEP msg, level
        )
    )
| STATS count(msg) BY level
```

But views have an optimization that allows for compaction of nested branches to the same level.
This means the query will be transformed to instead be a single set of five branches:

* Top of plan does the stats
    * Next level has five branches
        * Read data from `other-events`
        * Read data from `app-events-*` and keep two columns
        * Read data from `auth-events-*` and keep two columns
        * Read data from `nginx-events-*` and keep two columns
        * Read data from `apache-events-*` and keep two columns

This would be as if the user had used the subquery like this:

```esql
FROM
    other-events,
    FROM (
        FROM app-events-* | KEEP msg, level
    ), (
        FROM auth-events-* | KEEP msg, level
    )
    FROM (
        FROM nginx-events-* | KEEP msg, level
    ), (
        FROM apache-events-* | KEEP msg, level
    )
| STATS count(msg) BY level
```

This query will not fail.

However, it is important to notice that this compaction will not happen if the view definition contains any commands after the subqueries.
In that case the branches cannot be combined and the query will fail.
