## `CIDR_MATCH` [esql-cidr_match]

**Syntax**

:::{image} ../../../../../images/cidr_match.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`ip`
:   IP address of type `ip` (both IPv4 and IPv6 are supported).

`blockX`
:   CIDR block to test the IP against.

**Description**

Returns true if the provided IP is contained in one of the provided CIDR blocks.

**Supported types**

| ip | blockX | result |
| --- | --- | --- |
| ip | keyword | boolean |
| ip | text | boolean |

**Example**

```esql
FROM hosts
| WHERE CIDR_MATCH(ip1, "127.0.0.2/32", "127.0.0.3/32")
| KEEP card, host, ip0, ip1
```

| card:keyword | host:keyword | ip0:ip | ip1:ip |
| --- | --- | --- | --- |
| eth1 | beta | 127.0.0.1 | 127.0.0.2 |
| eth0 | gamma | fe80::cae2:65ff:fece:feb9 | 127.0.0.3 |


