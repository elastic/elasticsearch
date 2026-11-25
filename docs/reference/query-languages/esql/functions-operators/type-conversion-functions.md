---
applies_to:
  stack: ga
  serverless: ga
navigation_title: "Type conversion functions"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/esql-functions-operators.html#esql-type-conversion-functions
---

# {{esql}} type conversion functions [esql-type-conversion-functions]


::::{tip}
{{esql}} supports implicit casting from string literals to certain data types. Refer to [implicit casting](/reference/query-languages/esql/esql-implicit-casting.md) for details.

::::


{{esql}} supports these type conversion functions:

:::{include} ../_snippets/lists/type-conversion-functions.md
:::

## Inputs that can't be converted [esql-type-conversion-unconvertible-inputs]

```{applies_to}
stack: ga 9.7+
serverless: ga
```

A type conversion function, or a cast with `::`, whose input has a type it can't convert returns `null` for every row, and adds a warning to the response, instead of failing the query. For example, `salary::ip` is `null` when `salary` is an `integer`:

```esql
FROM employees
| EVAL ip = salary::ip
```

The other arguments of the function are still checked. For example, `TO_IP(salary, {"unknown": "x"})` fails because `unknown` is not a valid option.

These functions don't have a `null` result of the right type for an input they can't convert, so they fail the query instead:

* [`TO_GAUGE`](type-conversion-functions/to_gauge.md) and [`TO_COUNTER`](type-conversion-functions/to_counter.md): the type of their result is the type of their input.
* [`TO_DATEPERIOD`](type-conversion-functions/to_dateperiod.md) and [`TO_TIMEDURATION`](type-conversion-functions/to_timeduration.md): they need a constant.
* [`TO_LONG`](type-conversion-functions/to_long.md) and [`TO_INTEGER`](type-conversion-functions/to_integer.md) with a `base`: they only parse strings.

Fields of an unsupported type are not converted to `null` either: they fail the query, as they always did. For fields mapped to more than one type, see [Types that can't be converted](../esql-multi-index.md#esql-multi-index-union-unconvertible).
