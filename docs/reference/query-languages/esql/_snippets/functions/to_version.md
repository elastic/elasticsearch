## `TO_VERSION` [esql-to_version]

**Syntax**

:::{image} ../../../../../images/to_version.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`field`
:   Input value. The input can be a single- or multi-valued column or an expression.

**Description**

Converts an input string to a version value.

**Supported types**

| field | result |
| --- | --- |
| keyword | version |
| text | version |
| version | version |

**Example**

```esql
ROW v = TO_VERSION("1.2.3")
```

| v:version |
| --- |
| 1.2.3 |
