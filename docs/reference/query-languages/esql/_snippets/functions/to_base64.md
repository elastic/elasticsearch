## `TO_BASE64` [esql-to_base64]

**Syntax**

:::{image} ../../../../../images/to_base64.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`string`
:   A string.

**Description**

Encode a string to a base64 string.

**Supported types**

| string | result |
| --- | --- |
| keyword | keyword |
| text | keyword |

**Example**

```esql
row a = "elastic"
| eval e = to_base64(a)
```

| a:keyword | e:keyword |
| --- | --- |
| elastic | ZWxhc3RpYw== |


