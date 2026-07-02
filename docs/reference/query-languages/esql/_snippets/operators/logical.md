## Logical operators [esql-logical-operators]

{{esql}} supports the `AND`, `OR`, and `NOT` logical operators. They use
three-valued logic ([3VL](https://en.wikipedia.org/wiki/Three-valued_logic)):
the result of a logical expression can be `true`, `false`, or `null`. A `null`
operand represents an unknown value.

For `AND`, if one operand is `false`, the result is `false`. Otherwise, if any
operand is `null`, the result is `null`.

| `AND` | `true` | `false` | `null` |
| --- | --- | --- | --- |
| `true` | `true` | `false` | `null` |
| `false` | `false` | `false` | `false` |
| `null` | `null` | `false` | `null` |

For `OR`, if one operand is `true`, the result is `true`. Otherwise, if any
operand is `null`, the result is `null`.

| `OR` | `true` | `false` | `null` |
| --- | --- | --- | --- |
| `true` | `true` | `true` | `true` |
| `false` | `true` | `false` | `null` |
| `null` | `true` | `null` | `null` |

For `NOT`, a `null` operand returns `null`.

| Expression | Result |
| --- | --- |
| `NOT true` | `false` |
| `NOT false` | `true` |
| `NOT null` | `null` |

The [`WHERE`](/reference/query-languages/esql/commands/where.md) command only
keeps rows where the condition evaluates to `true`. If the condition evaluates
to `null`, the row is filtered out.

For example, the following expression evaluates to `null` when `process.name`
is `null`:

```esql
WHERE process.name IS NULL AND NOT process.name == "svchost.exe"
```

The left side is `true`, but `process.name == "svchost.exe"` evaluates to
`null`. `NOT null` is still `null`, and `true AND null` is `null`, so `WHERE`
filters out the row.

To keep rows where `process.name` is either `null` or is not equal to
`"svchost.exe"`, use:

```esql
WHERE process.name IS NULL OR process.name != "svchost.exe"
```
