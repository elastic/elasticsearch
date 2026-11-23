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

Each `LET` directive declares one named subquery and ends with a semicolon. A query can
declare several named subqueries, one `LET` directive each. When a query also uses
[`SET`](/reference/query-languages/esql/directives/set.md), all `SET` directives
come before the `LET` directives.

## Parameters

`name`
:   The subquery name. It must be a lowercase, unquoted identifier, and each name can
    be declared only once in a query. Because a named subquery is read through `FROM`, its
    name follows the same rules as an index name: it can't start with `_`, can't be
    `metadata` or `with`, and can't be longer than 255 bytes.

`<query>`
:   The subquery body: a complete {{esql}} query in parentheses, starting with a
    `FROM`, `TS`, or `ROW` source command.

## Description

A named subquery can be referenced anywhere a subquery can read a source:

* In a `FROM` command, on its own or alongside index patterns and subqueries, for
  example `FROM top_langs` or `FROM languages, top_langs`. Each named subquery referenced
  this way behaves like a [subquery in `FROM`](/reference/query-languages/esql/esql-from-subquery.md).
* In an [`IN` or `NOT IN` subquery](/reference/query-languages/esql/esql-in-subquery.md),
  as `IN (FROM name)`. A bare `IN name` is not supported, and `IN (name)` is a
  one-element value list, not a reference to the named subquery.
* In the body of a later `LET` named subquery.

Each reference runs the named subquery's query again, and the results aren't shared between
references. A named subquery whose rows can change from one run to the next, for example one
that uses `SAMPLE`, or `LIMIT` without `SORT`, can return different rows at each
reference.

Named subqueries are resolved in the order they are declared: a subquery body can reference
only named subqueries declared before it. A reference to a named subquery declared later, or to the
named subquery itself, is an error.

Every named subquery must be referenced, either by the main query or by another named
subquery that is itself referenced. Declaring a named subquery that is never referenced is
an error.

A named subquery takes precedence over an index, alias, or view with the same name.
Only exact names are matched: an index pattern such as `name*` refers to indices,
not to the named subquery.

Named subqueries are local to the query that declares them. A
[view](/reference/query-languages/esql/esql-views.md) referenced by the query does
not see them; a view body can declare its own `LET` named subqueries.

## Limitations

* A named subquery can't be the source of a `TS` command.
* A named subquery can't be the right-hand side of a `LOOKUP JOIN`.

## Examples

### Name a subquery and read from it

:::{include} ../../generated/x-pack-esql/commands/examples/named_subquery.csv-spec/let_single_binding.md
:::

### Build a named subquery on an earlier named subquery

:::{include} ../../generated/x-pack-esql/commands/examples/named_subquery.csv-spec/let_chained_bindings.md
:::

### Filter with a named subquery in an `IN` subquery

:::{include} ../../generated/x-pack-esql/commands/examples/named_subquery.csv-spec/let_in_subquery.md
:::

### Identify rows that come from a named subquery

With [`METADATA _class, _name`](/reference/query-languages/esql/esql-metadata-fields.md),
rows read from a named subquery have `_class` set to `subquery` and `_name` set to the subquery name:

:::{include} ../../generated/x-pack-esql/commands/examples/named_subquery.csv-spec/let_class_and_name.md
:::
