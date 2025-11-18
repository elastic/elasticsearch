```yaml {applies_to}
stack: preview 9.0.0, ga 9.1.0
serverless: ga
```

`LOOKUP JOIN` enables you to add data from another index, AKA a 'lookup'
index, to your {{esql}} query results, simplifying data enrichment
and analysis workflows.

Refer to [the high-level landing page](../../../../esql/esql-lookup-join.md) for an overview of the `LOOKUP JOIN` command, including use cases, prerequisites, and current limitations.

**Syntax**

```esql
FROM <source_index>
| LOOKUP JOIN <lookup_index> ON <field_name>
```

```esql
FROM <source_index>
| LOOKUP JOIN <lookup_index> ON <field_name1>, <field_name2>, <field_name3>
```
```esql
FROM <source_index>
| LOOKUP JOIN <lookup_index> ON <left_field1> >= <lookup_field1> AND <left_field2> == <lookup_field2>
```

**Parameters**

`<lookup_index>`
:   The name of the lookup index. This must be a specific index name - wildcards, aliases, and remote cluster references are not supported. Indices used for lookups must be configured with the [`lookup` index mode](/reference/elasticsearch/index-settings/index-modules.md#index-mode-setting).

`<field_name>` or `<field_name1>, <field_name2>, <field_name3>` or `<left_field1> >= <lookup_field1> AND <left_field2> == <lookup_field2>`
:   The join condition. Can be one of the following:
   * A single field name
   * A comma-separated list of field names {applies_to}`stack: ga 9.2`
   * An expression with one or more join conditions linked by `AND`. Each condition compares a field from the left index with a field from the lookup index using [binary operators](/reference/query-languages/esql/functions-operators/operators.md#esql-binary-operators) (`==`, `>=`, `<=`, `>`, `<`, `!=`). Each field name in the join condition must exist in only one of the indexes. Use RENAME to resolve naming conflicts. {applies_to}`stack: preview 9.2` {applies_to}`serverless: preview`
:   If using join on a single field or a field list, the fields used must exist in both your current query results and in the lookup index. If the fields contains multi-valued entries, those entries will not match anything (the added fields will contain `null` for those rows).


**Description**

The `LOOKUP JOIN` command adds new columns to your {{esql}} query
results table by finding documents in a lookup index that share the same
join field value as your result rows.

For each row in your results table that matches a document in the lookup
index based on the join fields, all fields from the matching document are
added as new columns to that row.

If multiple documents in the lookup index match a single row in your
results, the output will contain one row for each matching combination.

::::{tip}
For important information about using `LOOKUP JOIN`, refer to [Usage notes](../../../../esql/esql-lookup-join.md#usage-notes).
::::

:::{include} ../types/lookup-join.md
:::

**Examples**

**IP Threat correlation**: This query would allow you to see if any source
IPs match known malicious addresses.

:::{include} ../examples/docs-lookup-join.csv-spec/lookupJoinSourceIp.md
:::

To filter only for those rows that have a matching `threat_list` entry, use `WHERE ... IS NOT NULL` with a field from the lookup index:

:::{include} ../examples/docs-lookup-join.csv-spec/lookupJoinSourceIpWhere.md
:::

**Host metadata correlation**: This query pulls in environment or
ownership details for each host to correlate with your metrics data.

:::{include} ../examples/docs-lookup-join.csv-spec/lookupJoinHostNameTwice.md
:::

**Service ownership mapping**: This query would show logs with the owning
team or escalation information for faster triage and incident response.

:::{include} ../examples/docs-lookup-join.csv-spec/lookupJoinServiceId.md
:::

`LOOKUP JOIN` is generally faster when there are fewer rows to join
with. {{esql}} will try and perform any `WHERE` clause before the
`LOOKUP JOIN` where possible.

The following two examples will have the same results. One has the
`WHERE` clause before and the other after the `LOOKUP JOIN`. It does not
matter how you write your query, our optimizer will move the filter
before the lookup when possible.

:::{include} ../examples/lookup-join.csv-spec/filterOnLeftSide.md
:::

:::{include} ../examples/lookup-join.csv-spec/filterOnRightSide.md
:::
