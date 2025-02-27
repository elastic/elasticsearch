## `REVERSE` [esql-reverse]

**Syntax**

:::{image} ../../../../../images/reverse.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`str`
:   String expression. If `null`, the function returns `null`.

**Description**

Returns a new string representing the input string in reverse order.

**Supported types**

| str | result |
| --- | --- |
| keyword | keyword |
| text | keyword |

**Examples**

```esql
ROW message = "Some Text" | EVAL message_reversed = REVERSE(message);
```

| message:keyword | message_reversed:keyword |
| --- | --- |
| Some Text | txeT emoS |

`REVERSE` works with unicode, too! It keeps unicode grapheme clusters together during reversal.

```esql
ROW bending_arts = "💧🪨🔥💨" | EVAL bending_arts_reversed = REVERSE(bending_arts);
```

| bending_arts:keyword | bending_arts_reversed:keyword |
| --- | --- |
| 💧🪨🔥💨 | 💨🔥🪨💧 |


