## `KEEP` [esql-keep]

The `KEEP` processing command enables you to specify what columns are returned
and the order in which they are returned.

**Syntax**

```esql
KEEP columns
```

**Parameters**

`columns`
:   A comma-separated list of columns to keep. Supports wildcards.
    See below for the behavior in case an existing column matches multiple
    given wildcards or column names.

**Description**

The `KEEP` processing command enables you to specify what columns are returned
and the order in which they are returned.

Precedence rules are applied when a field name matches multiple expressions.
Fields are added in the order they appear. If one field matches multiple expressions, the following precedence rules apply (from highest to lowest priority):

1. Complete field name (no wildcards)
2. Partial wildcard expressions (for example: `fieldNam*`)
3. Wildcard only (`*`)

If a field matches two expressions with the same precedence, the rightmost expression wins.

Refer to the examples for illustrations of these precedence rules.

**Examples**

The columns are returned in the specified order:

:::{include} ../examples/docs.csv-spec/keep.md
:::

Rather than specify each column by name, you can use wildcards to return all
columns with a name that matches a pattern:

:::{include} ../examples/docs.csv-spec/keepWildcard.md
:::

The asterisk wildcard (`*`) by itself translates to all columns that do not
match the other arguments.

This query will first return all columns with a name
that starts with `h`, followed by all other columns:

:::{include} ../examples/docs.csv-spec/keepDoubleWildcard.md
:::

The following examples show how precedence rules work when a field name matches multiple expressions.

Complete field name has precedence over wildcard expressions:

:::{include} ../examples/docs.csv-spec/keepCompleteName.md
:::

Wildcard expressions have the same priority, but last one wins (despite being less specific):

:::{include} ../examples/docs.csv-spec/keepWildcardPrecedence.md
:::

A simple wildcard expression `*` has the lowest precedence.
Output order is determined by the other arguments:

:::{include} ../examples/docs.csv-spec/keepWildcardLowest.md
:::
