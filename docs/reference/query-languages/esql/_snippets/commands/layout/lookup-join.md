## `LOOKUP JOIN` [esql-lookup-join]

::::{warning}
This functionality is in technical preview and may be
changed or removed in a future release. Elastic will work to fix any
issues, but features in technical preview are not subject to the support
SLA of official GA features.
::::

`LOOKUP JOIN` enables you to add data from another index, AKA a 'lookup'
index, to your {{esql}} query results, simplifying data enrichment
and analysis workflows.

Refer to [the high-level landing page](../../../../esql/esql-lookup-join.md) for an overview of the `LOOKUP JOIN` command, including use cases, prerequisites, and current limitations.

**Syntax**

```esql
FROM <source_index>
| LOOKUP JOIN <lookup_index> ON <field_name>
```

**Parameters**

`<lookup_index>`
:   The name of the lookup index. This must be a specific index name - wildcards, aliases, and remote cluster references are not supported. Indices used for lookups must be configured with the [`lookup` index mode](/reference/elasticsearch/index-settings/index-modules.md#index-mode-setting).

`<field_name>`
:   The field to join on. This field must exist in both your current query results and in the lookup index. If the field contains multi-valued entries, those entries will not match anything (the added fields will contain `null` for those rows).

**Description**

The `LOOKUP JOIN` command adds new columns to your {{esql}} query
results table by finding documents in a lookup index that share the same
join field value as your result rows.

For each row in your results table that matches a document in the lookup
index based on the join field, all fields from the matching document are
added as new columns to that row.

If multiple documents in the lookup index match a single row in your
results, the output will contain one row for each matching combination.

::::{tip}
For important information about using `LOOKUP JOIN`, refer to [Usage notes](../../../../esql/esql-lookup-join.md#usage-notes).
::::

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
