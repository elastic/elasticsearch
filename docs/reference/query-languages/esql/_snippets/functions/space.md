## `SPACE` [esql-space]

**Syntax**

:::{image} ../../../../../images/space.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`number`
:   Number of spaces in result.

**Description**

Returns a string made of `number` spaces.

**Supported types**

| number | result |
| --- | --- |
| integer | keyword |

**Example**

```esql
ROW message = CONCAT("Hello", SPACE(1), "World!");
```

| message:keyword |
| --- |
| Hello World! |


