## `FROM_BASE64` [esql-from_base64]

**Syntax**

:::{image} ../../../../../images/from_base64.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`string`
:   A base64 string.

**Description**

Decode a base64 string.

**Supported types**

| string | result |
| --- | --- |
| keyword | keyword |
| text | keyword |

**Example**

```esql
row a = "ZWxhc3RpYw=="
| eval d = from_base64(a)
```

| a:keyword | d:keyword |
| --- | --- |
| ZWxhc3RpYw== | elastic |


