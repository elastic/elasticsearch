```yaml {applies_to}
serverless: ga
stack: ga 9.7+
```

The `LET` directive binds a [subquery](/reference/query-languages/esql/esql-subquery.md) to a name.
The rest of the query can then reference that name instead of repeating the subquery.

## Syntax

```esql
LET name = ( <query> ); [LET name = ( <query> ); ...] <query>
```

Each `LET` directive declares one binding and ends with a semicolon. A query can
declare several bindings, one `LET` directive each. When a query also uses
[`SET`](/reference/query-languages/esql/directives/set.md), all `SET` directives
come before the `LET` directives.

## Parameters

`name`
:   The binding name. It must be a lowercase, unquoted identifier, and each name can
    be declared only once in a query.

`<query>`
:   The binding body: a complete {{esql}} query in parentheses, starting with a
    `FROM`, `TS`, or `ROW` source command.

## Description

A binding name can be referenced anywhere a subquery can read a source:

* In a `FROM` command, on its own or alongside index patterns and subqueries, for
  example `FROM top_langs` or `FROM languages, top_langs`. Each binding referenced
  this way behaves like a [subquery in `FROM`](/reference/query-languages/esql/esql-from-subquery.md).
* In an [`IN` or `NOT IN` subquery](/reference/query-languages/esql/esql-in-subquery.md),
  as `IN (FROM name)`. A bare `IN name` is not supported, and `IN (name)` is a
  one-element value list, not a reference to the binding.
* In the body of a later `LET` binding.

Bindings are resolved in the order they are declared: a binding body can reference
only bindings declared before it. A reference to a binding declared later, or to the
binding itself, is an error.

A binding name takes precedence over an index, alias, or view with the same name.
Only exact names are matched: an index pattern such as `name*` refers to indices,
not to the binding.

Bindings are local to the query that declares them. A
[view](/reference/query-languages/esql/esql-views.md) referenced by the query does
not see them; a view body can declare its own `LET` bindings.

## Limitations

* A binding cannot be the source of a `TS` command.
* A binding cannot be the right-hand side of a `LOOKUP JOIN`.

## Examples

### Name a subquery and read from it

:::{include} ../../generated/x-pack-esql/commands/examples/named_subquery.csv-spec/let_single_binding.md
:::

### Build a binding on an earlier binding

:::{include} ../../generated/x-pack-esql/commands/examples/named_subquery.csv-spec/let_chained_bindings.md
:::

### Filter with a binding in an `IN` subquery

:::{include} ../../generated/x-pack-esql/commands/examples/named_subquery.csv-spec/let_in_subquery.md
:::

### Identify rows that come from a binding

With [`METADATA _class, _name`](/reference/query-languages/esql/esql-metadata-fields.md),
rows read from a binding have `_class` set to `subquery` and `_name` set to the binding name:

:::{include} ../../generated/x-pack-esql/commands/examples/named_subquery.csv-spec/let_class_and_name.md
:::
