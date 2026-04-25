---
applies_to:
  stack: unavailable
  serverless: preview
products:
  - id: elasticsearch
description: Learn how to use the ES|QL language in Elasticsearch to query across multiple Serverless projects. Learn about index resolution, project routing, and accessing project metadata.
navigation_title: "CPS with ES|QL"
---

# Query across {{serverless-short}} projects with {{esql}}

[Cross-project search](docs-content://explore-analyze/cross-project-search.md) (CPS) enables you to run queries across multiple [linked {{serverless-short}} projects](docs-content://explore-analyze/cross-project-search.md#project-linking) from a single request.

There are several ways to control which projects a query runs against:

- **[Query all projects](#query-all-projects-default)**: If you just want to query across all linked projects, no special syntax is required. Queries automatically run against the origin and all linked projects by default.
- **[Use project routing](#use-project-routing)**: Use project routing to limit the scope of your search to specific projects before query execution. Excluded projects are not queried.
- **[Use index expressions](#use-index-expressions)**: Use index expressions for fine-grained control over which projects and indices are queried, by qualifying index names with a project alias. Search expressions can be used independently or combined with project routing.

## Before you begin

This page covers {{esql}}-specific CPS behavior. Before continuing, make sure you are familiar with the following:

* [Cross-project search](docs-content://explore-analyze/cross-project-search.md)
* [Linked projects](docs-content://explore-analyze/cross-project-search/cross-project-search-link-projects.md)
* [How search works in CPS](docs-content://explore-analyze/cross-project-search/cross-project-search-search.md)
* [Project routing in CPS](docs-content://explore-analyze/cross-project-search/cross-project-search-project-routing.md)
* [Tags in CPS](docs-content://explore-analyze/cross-project-search/cross-project-search-tags.md)

## Query all projects (default)

The default behavior is to query across the origin project and all linked projects automatically.
The following example queries the `data` index and includes the `_index` metadata field to identify which project each result came from:

```console
GET /_query
{
  "query": "FROM data METADATA _index", <1>
  "include_execution_metadata": true     <2>
}
```

1. `METADATA _index` returns the fully-qualified index name for each document. Documents from linked projects include the project alias prefix, for example `linked-project-1:data`.
2. Required to include the `_clusters` object in the response. Defaults to `false`.

The response includes:
- a `_clusters` object showing the status of each participating project
- a `values` array where each row includes the qualified index name identifying which project the document came from

:::{dropdown} Example response
```json
{
  "took": 329,
  "is_partial": false,
  "columns": [
    { "name": "_index", "type": "keyword" }
  ],
  "values": [
    ["data"],                      <1>
    ["linked-project-1:data"]      <2>
  ],
  "_clusters": {
    "total": 2,
    "successful": 2,
    "running": 0,
    "skipped": 0,
    "partial": 0,
    "failed": 0,
    "details": {
      "_origin": {                 <3>
        "status": "successful",
        "indices": "data",
        "took": 328,
        "_shards": { "total": 1, "successful": 1, "skipped": 0, "failed": 0 }
      },
      "linked-project-1": {        <4>
        "status": "successful",
        "indices": "data",
        "took": 256,
        "_shards": { "total": 1, "successful": 1, "skipped": 0, "failed": 0 }
      }
    }
  }
}
```

1. Documents from the origin project show an unqualified index name.
2. Documents from linked projects show a qualified index name: the project alias, a colon, then the index name.
3. `_origin` is the reserved identifier for the origin project.
4. Each linked project is identified by its project alias.
:::

## Use project routing

[Project routing](docs-content://explore-analyze/cross-project-search/cross-project-search-project-routing.md) limits the scope of a query to specific projects, based on tag values.
Project routing happens before query execution, so excluded projects are never queried. This can help reduce cost and latency.

:::{note}
Project routing expressions use Lucene query syntax. The `:` operator matches a tag value, equivalent to `=` in other query languages. For example, `_alias:my-project` matches projects whose alias is `my-project`.
:::

You can specify project routing in two ways:

- [Embed project routing in the query with `SET`](#option-1-use-the-set-source-command): This approach works wherever you can write an {{esql}} query.
- [Pass project routing in the `_query` API request body](#option-2-pass-project_routing-in-the-api-request-body): You can pass a `project_routing` field to keep project routing logic separate from the query string.

:::{important}
If both options are combined, `SET project_routing` takes precedence.
:::

### Option 1: Use the `SET` source command

`SET project_routing` embeds project routing directly within the {{esql}} query. You can use this approach wherever you write {{esql}}. [`SET`](/reference/query-languages/esql/commands/set.md) must appear before other {{esql}} commands. The semicolon after the last parameter separates it from the rest of the query. The order of parameters within `SET` does not matter.

```esql
SET project_routing="_alias:my-project";    <1>
FROM data
| STATS COUNT(*)
```

1. Routes the query to the project with alias `my-project`.

### Option 2: Pass `project_routing` in the API request body

If you are constructing the full `_query` request, you can pass the `project_routing` field in the request body. This keeps project routing logic separate from the query string:

```console
GET /_query
{
  "query": "FROM data | STATS COUNT(*)",
  "project_routing": "_alias:my-project"    <1>
}
```

1. Routes the query to projects whose alias matches `my-project`.

### Reference a named project routing expression

Both options support referencing a named project routing expression using the `@` prefix.
Before you can reference a named expression, you must create it using the `_project_routing` API.
For instructions, refer to [Using named project routing expressions](docs-content://explore-analyze/cross-project-search/cross-project-search-project-routing.md#creating-and-managing-named-project-routing-expressions).

::::{tab-set}

:::{tab-item} Request body
```console
GET /_query
{
  "query": "FROM logs | STATS COUNT(*)",
  "project_routing": "@custom-expression"
}
```
:::

:::{tab-item} SET directive
```esql
SET project_routing="@custom-expression";
FROM logs
| STATS COUNT(*)
```
:::

::::

## Use index expressions

{{esql}} supports two types of [index expressions](docs-content://explore-analyze/cross-project-search/cross-project-search-search.md#search-expressions):

- **Unqualified expressions** have no project prefix and search across all projects. Example: `logs*`.
- **Qualified expressions** include a project alias prefix to target a specific project or set of projects. Example: `project1:logs*`.

### Restrict to the origin project

Use `_origin:` to target only the project from which the query is run:

```esql
FROM _origin:data    <1>
| STATS COUNT(*)
```

1. `_origin` always refers to the origin project, regardless of its alias.

### Restrict to a specific linked project

Prefix the index name with the linked project's alias:

```esql
FROM linked-project-1:data    <1>
| STATS COUNT(*)
```

1. Replace `linked-project-1` with the actual project alias.

### Exclude specific projects

Prefix an index expression with `-` to exclude it from the resolved set.
The following example uses `-_origin:*` to exclude all indices from the origin project:

```esql
FROM data,-_origin:*    <1>
| STATS COUNT(*)
```

1. `data` is resolved across all projects except the origin project.

::::{note}
`*:` in CPS does not behave like `*:` in [cross-cluster search (CCS)](/reference/query-languages/esql/esql-cross-clusters.md) (which is used to query across clusters in non-serverless deployments):

- In CCS, `*:` targets all remote clusters and excludes the local cluster.
- In CPS, `*:` resolves against all projects including the origin, the same as an unqualified expression.
::::

### Combine qualified and unqualified expressions

You can mix unqualified and qualified expressions in the same query:

```esql
FROM data, _origin:logs    <1>
| LIMIT 100
```

1. `data` is resolved across all projects. `_origin:logs` is resolved only in the origin project.

::::{tip}
Error handling differs between expression types. Unqualified expressions fail only if the index exists in none of the searched projects. Qualified expressions fail if the index is missing from the targeted project, regardless of whether it exists elsewhere.
For a detailed explanation, refer to [Unqualified expression behavior](docs-content://explore-analyze/cross-project-search/cross-project-search-search.md#behavior-unqualified).
::::

## Include project metadata in results

Use the `METADATA` keyword in a `FROM` command to include project-level information alongside query results.
Project metadata fields use the `_project.` prefix to distinguish them from document fields.

You can use project metadata fields in two ways:

* As columns in returned result rows, to identify which project each document came from.
* In downstream commands such as `WHERE`, `STATS`, and `KEEP`, to filter, aggregate, or sort results by project. Note: `WHERE` [filters results after all projects are queried](#filter-results-by-project-tag) and does not limit query scope.

Available fields include all predefined tags and any custom tags you have defined.
You can also use wildcard patterns such as `_project.my-prefix*` or `_project.*`.

For a full list of predefined tags, refer to [Tags in CPS](docs-content://explore-analyze/cross-project-search/cross-project-search-tags.md).

::::{important}
You must declare a project metadata field in the `METADATA` clause to use it anywhere in the query, including in `WHERE`, `STATS`, `KEEP`, and other downstream commands.
::::

### Return project alias alongside results

Include `_project._alias` in `METADATA` to add the project alias as a column on each result row:

```esql
FROM logs* METADATA _project._alias    <1>
| KEEP @timestamp, message, _project._alias
```

1. Declaring `_project._alias` in `METADATA` makes it available in `KEEP` and other downstream commands.

:::{dropdown} Example response
```json
{
  "took": 47,
  "is_partial": false,
  "columns": [
    { "name": "@timestamp", "type": "date" },
    { "name": "message", "type": "keyword" },
    { "name": "_project._alias", "type": "keyword" }
  ],
  "values": [
    ["2025-01-15T10:23:00.000Z", "connection established", "origin-project"],    <1>
    ["2025-01-15T10:24:00.000Z", "request timeout", "linked-project-1"],         <2>
    ["2025-01-15T10:25:00.000Z", "disk full", "linked-project-1"]
  ]
}
```

1. Documents from the origin project show its project alias.
2. Documents from linked projects show the linked project's alias.
:::

### Aggregate results by project

Include `_project._alias` in `METADATA` to group and count results by project:

```esql
FROM logs* METADATA _project._alias    <1>
| STATS doc_count = COUNT(*) BY _project._alias
```

1. `_project._alias` must be in `METADATA` to use it in `STATS ... BY`.

### Filter results by project tag

A project tag in a `WHERE` clause filters the result set after the query runs across all projects. It does not limit which projects are queried.

The following examples show the difference between filtering with `WHERE` and restricting the query scope with project routing.

#### Filter with `WHERE` (post-query)

```esql
FROM logs* METADATA _project._csp    <1>
| WHERE _project._csp == "aws"       <2>
```

1. Declare the tag in `METADATA` to use it in downstream commands.
2. All linked projects are queried. Only results from AWS projects are returned.

::::{important}
Filtering with `WHERE` on a project tag happens after all projects are queried. To optimize a query, use [project routing](#use-project-routing) to select projects before execution.
::::

#### Restrict with project routing (pre-query)

```esql
SET project_routing="_alias:aws-project";    <1>
FROM logs*
| STATS COUNT(*)
```

1. Only `aws-project` is queried. No data is fetched from other projects. For supported project routing tags, refer to [Limitations](#limitations).

### Use project routing and METADATA together

Project routing and project metadata serve different purposes and are independent of each other.
Project routing determines which projects are queried, before execution.
METADATA makes tag values available in query results and downstream commands, at query time.

Using a tag in `METADATA` does not route the query. Using project routing does not populate `METADATA` fields.
To both restrict queried projects and include tag values in results, specify both:

```esql
SET project_routing="_alias:*linked*";    <1>
FROM logs METADATA _project._alias        <2>
| STATS COUNT(*) BY _project._alias
```

1. Routes the query to projects whose alias matches `*linked*`. Only those projects are queried.
2. Declares `_project._alias` so it can be used in `STATS`. Results show a count per matched project.

## Limitations

### Project routing supports alias only

Initially, project routing only supports the `_alias` tag.
Other predefined tags (`_csp`, `_region`, and so on) and custom tags are not yet supported as project routing criteria.

### LOOKUP JOIN across projects

{{esql}} `LOOKUP JOIN` follows the same constraints as [{{esql}} cross-cluster `LOOKUP JOIN`](/reference/query-languages/esql/esql-lookup-join.md#cross-cluster-support).
The lookup index must exist on every project being queried, because each project uses its own local copy of the lookup index data.

## Related pages

* [ES|QL cross-cluster search](/reference/query-languages/esql/esql-cross-clusters.md): the equivalent feature for non-serverless deployments.
* [`FROM` command](/reference/query-languages/esql/commands/from.md): full reference for index expressions and `METADATA` syntax.
* [`SET` directive](/reference/query-languages/esql/commands/set.md): full reference for the `SET` directive in {{esql}}.
* [ES|QL metadata fields](/reference/query-languages/esql/esql-metadata-fields.md): full reference for metadata fields available in ES|QL queries.
* [ES|QL `LOOKUP JOIN`](/reference/query-languages/esql/esql-lookup-join.md): details on `LOOKUP JOIN` constraints, including cross-cluster and cross-project support.
