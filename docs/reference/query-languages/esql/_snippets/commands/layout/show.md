## `SHOW` [esql-show]

The `SHOW` source command returns information about the deployment and
its capabilities.

**Syntax**

```esql
SHOW item
```

**Parameters**

`item`
:   Can only be `INFO`.

**Examples**

Use `SHOW INFO` to return the deployment’s version, build date and hash.

```esql
SHOW INFO
```

| version | date | hash |
| --- | --- | --- |
| 8.13.0 | 2024-02-23T10:04:18.123117961Z | 04ba8c8db2507501c88f215e475de7b0798cb3b3 |
