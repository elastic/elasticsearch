## `REPEAT` [esql-repeat]

**Syntax**

:::{image} ../../../../../images/repeat.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`string`
:   String expression.

`number`
:   Number times to repeat.

**Description**

Returns a string constructed by concatenating `string` with itself the specified `number` of times.

**Supported types**

| string | number | result |
| --- | --- | --- |
| keyword | integer | keyword |
| text | integer | keyword |

**Example**

```esql
ROW a = "Hello!"
| EVAL triple_a = REPEAT(a, 3)
```

| a:keyword | triple_a:keyword |
| --- | --- |
| Hello! | Hello!Hello!Hello! |


