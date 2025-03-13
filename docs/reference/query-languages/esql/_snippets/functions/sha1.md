## `SHA1` [esql-sha1]

**Syntax**

:::{image} ../../../../../images/sha1.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`input`
:   Input to hash.

**Description**

Computes the SHA1 hash of the input.

**Supported types**

| input | result |
| --- | --- |
| keyword | keyword |
| text | keyword |

**Example**

```esql
FROM sample_data
| WHERE message != "Connection error"
| EVAL sha1 = sha1(message)
| KEEP message, sha1;
```

| message:keyword | sha1:keyword |
| --- | --- |
| Connected to 10.1.0.1 | 42b85531a79088036a17759db7d2de292b92f57f |
| Connected to 10.1.0.2 | d30db445da2e9237c9718d0c7e4fb7cbbe9c2cb4 |
| Connected to 10.1.0.3 | 2733848d943809f0b10cad3e980763e88afb9853 |
| Disconnected | 771e05f27b99fd59f638f41a7a4e977b1d4691fe |


