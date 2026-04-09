```yaml {applies_to}
serverless: ga
stack: ga
```

The `KEEP` processing command enables you to specify what columns are returned
and the order in which they are returned.

## Syntax

```esql
KEEP columns
```

## Parameters

`columns`
:   A comma-separated list of columns to keep. Supports wildcards.
    See below for the behavior in case an existing column matches multiple
    given wildcards or column names.

## Description

The `KEEP` processing command enables you to specify what columns are returned
and the order in which they are returned.

Precedence rules are applied when a field name matches multiple expressions.
Fields are added in the order they appear. If one field matches multiple expressions, the following precedence rules apply (from highest to lowest priority):

1. Complete field name (no wildcards)
2. Partial wildcard expressions (for example: `fieldNam*`)
3. Wildcard only (`*`)

If a field matches two expressions with the same precedence, the rightmost expression wins.

## Examples

The following examples show how to select columns and illustrate the wildcard precedence rules.

### Specify column order

Columns are returned in the order they are listed:

:::{include} ../examples/docs.csv-spec/keep.md
:::

### Use wildcards to select columns

Rather than specify each column by name, you can use wildcards to return all
columns with a name that matches a pattern:

:::{include} ../examples/docs.csv-spec/keepWildcard.md
:::

### Control ordering with wildcard and specific columns

The asterisk wildcard (`*`) by itself translates to all columns that do not
match the other arguments. This query returns all columns with a name
that starts with `h`, followed by all other columns:

:::{include} ../examples/docs.csv-spec/keepDoubleWildcard.md
:::

### Exact name takes precedence over wildcards

Complete field name has precedence over wildcard expressions:

:::{include} ../examples/docs.csv-spec/keepCompleteName.md
:::

### Rightmost wildcard wins when priority is equal

Wildcard expressions have the same priority, but the rightmost one wins (despite being less specific):

:::{include} ../examples/docs.csv-spec/keepWildcardPrecedence.md
:::

### Bare wildcard has lowest priority

A simple wildcard expression `*` has the lowest precedence.
Output order is determined by the other arguments:

:::{include} ../examples/docs.csv-spec/keepWildcardLowest.md
:::
