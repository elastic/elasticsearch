## `MD5` [esql-md5]

**Syntax**

:::{image} ../../../../../images/md5.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`input`
:   Input to hash.

**Description**

Computes the MD5 hash of the input.

**Supported types**

| input | result |
| --- | --- |
| keyword | keyword |
| text | keyword |

**Example**

```esql
FROM sample_data
| WHERE message != "Connection error"
| EVAL md5 = md5(message)
| KEEP message, md5;
```

| message:keyword | md5:keyword |
| --- | --- |
| Connected to 10.1.0.1 | abd7d1ce2bb636842a29246b3512dcae |
| Connected to 10.1.0.2 | 8f8f1cb60832d153f5b9ec6dc828b93f |
| Connected to 10.1.0.3 | 912b6dc13503165a15de43304bb77c78 |
| Disconnected | ef70e46fd3bbc21e3e1f0b6815e750c0 |


