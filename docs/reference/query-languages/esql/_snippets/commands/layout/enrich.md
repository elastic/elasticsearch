## `ENRICH` [esql-enrich]

`ENRICH` enables you to add data from existing indices as new columns using an
enrich policy.

**Syntax**

```esql
ENRICH policy [ON match_field] [WITH [new_name1 = ]field1, [new_name2 = ]field2, ...]
```

**Parameters**

`policy`
:   The name of the enrich policy.
    You need to [create](/reference/query-languages/esql/esql-enrich-data.md#esql-set-up-enrich-policy)
    and [execute](/reference/query-languages/esql/esql-enrich-data.md#esql-execute-enrich-policy)
    the enrich policy first.

`mode`
:   The mode of the enrich command in cross cluster {{esql}}.
    See [enrich across clusters](docs-content://explore-analyze/query-filter/languages/esql-cross-clusters.md#ccq-enrich).

`match_field`
:   The match field. `ENRICH` uses its value to look for records in the enrich
    index. If not specified, the match will be performed on the column with the same
    name as the `match_field` defined in the [enrich policy](/reference/query-languages/esql/esql-enrich-data.md#esql-enrich-policy).

`fieldX`
:   The enrich fields from the enrich index that are added to the result as new
    columns. If a column with the same name as the enrich field already exists, the
    existing column will be replaced by the new column. If not specified, each of
    the enrich fields defined in the policy is added.
    A column with the same name as the enrich field will be dropped unless the
    enrich field is renamed.

`new_nameX`
:   Enables you to change the name of the column that’s added for each of the enrich
    fields. Defaults to the enrich field name.
    If a column has the same name as the new name, it will be discarded.
    If a name (new or original) occurs more than once, only the rightmost duplicate
    creates a new column.

**Description**

`ENRICH` enables you to add data from existing indices as new columns using an
enrich policy. Refer to [Data enrichment](/reference/query-languages/esql/esql-enrich-data.md)
for information about setting up a policy.

:::{image} /reference/query-languages/images/esql-enrich.png
:alt: esql enrich
:::

::::{tip}
Before you can use `ENRICH`, you need to [create and execute an enrich policy](/reference/query-languages/esql/esql-enrich-data.md#esql-set-up-enrich-policy).
::::


**Examples**

The following example uses the `languages_policy` enrich policy to add a new
column for each enrich field defined in the policy. The match is performed using
the `match_field` defined in the [enrich policy](/reference/query-languages/esql/esql-enrich-data.md#esql-enrich-policy) and
requires that the input table has a column with the same name (`language_code`
in this example). `ENRICH` will look for records in th
[enrich index](/reference/query-languages/esql/esql-enrich-data.md#esql-enrich-index)
based on the match field value.

:::{include} ../examples/enrich.csv-spec/enrich.md
:::

To use a column with a different name than the `match_field` defined in the
policy as the match field, use `ON <column-name>`:

:::{include} ../examples/enrich.csv-spec/enrich_on.md
:::

By default, each of the enrich fields defined in the policy is added as a
column. To explicitly select the enrich fields that are added, use
`WITH <field1>, <field2>, ...`:

:::{include} ../examples/enrich.csv-spec/enrich_with.md
:::

You can rename the columns that are added using `WITH new_name=<field1>`:

:::{include} ../examples/enrich.csv-spec/enrich_rename.md
:::

In case of name collisions, the newly created columns will override existing
columns.
