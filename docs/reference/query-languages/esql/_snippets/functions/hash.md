## `HASH` [esql-hash]

**Syntax**

:::{image} ../../../../../images/hash.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`algorithm`
:   Hash algorithm to use.

`input`
:   Input to hash.

**Description**

Computes the hash of the input using various algorithms such as MD5, SHA, SHA-224, SHA-256, SHA-384, SHA-512.

**Supported types**

| algorithm | input | result |
| --- | --- | --- |
| keyword | keyword | keyword |
| keyword | text | keyword |
| text | keyword | keyword |
| text | text | keyword |

**Example**

```esql
FROM sample_data
| WHERE message != "Connection error"
| EVAL md5 = hash("md5", message), sha256 = hash("sha256", message)
| KEEP message, md5, sha256;
```

| message:keyword | md5:keyword | sha256:keyword |
| --- | --- | --- |
| Connected to 10.1.0.1 | abd7d1ce2bb636842a29246b3512dcae | 6d8372129ad78770f7185554dd39864749a62690216460752d6c075fa38ad85c |
| Connected to 10.1.0.2 | 8f8f1cb60832d153f5b9ec6dc828b93f | b0db24720f15857091b3c99f4c4833586d0ea3229911b8777efb8d917cf27e9a |
| Connected to 10.1.0.3 | 912b6dc13503165a15de43304bb77c78 | 75b0480188db8acc4d5cc666a51227eb2bc5b989cd8ca912609f33e0846eff57 |
| Disconnected | ef70e46fd3bbc21e3e1f0b6815e750c0 | 04dfac3671b494ad53fcd152f7a14511bfb35747278aad8ce254a0d6e4ba4718 |


