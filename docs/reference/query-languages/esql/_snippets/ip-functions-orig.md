## {{esql}} IP functions [esql-ip-functions]


{{esql}} supports these IP functions:

:::{include} lists/ip-functions.md
:::


## `CIDR_MATCH` [esql-cidr_match]

**Syntax**

:::{image} ../../../../images/cidr_match.svg
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


## `IP_PREFIX` [esql-ip_prefix]

**Syntax**

:::{image} ../../../../images/ip_prefix.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

`ip`
:   IP address of type `ip` (both IPv4 and IPv6 are supported).

`prefixLengthV4`
:   Prefix length for IPv4 addresses.

`prefixLengthV6`
:   Prefix length for IPv6 addresses.

**Description**

Truncates an IP to a given prefix length.

**Supported types**

| ip | prefixLengthV4 | prefixLengthV6 | result |
| --- | --- | --- | --- |
| ip | integer | integer | ip |

**Example**

```esql
row ip4 = to_ip("1.2.3.4"), ip6 = to_ip("fe80::cae2:65ff:fece:feb9")
| eval ip4_prefix = ip_prefix(ip4, 24, 0), ip6_prefix = ip_prefix(ip6, 0, 112);
```

| ip4:ip | ip6:ip | ip4_prefix:ip | ip6_prefix:ip |
| --- | --- | --- | --- |
| 1.2.3.4 | fe80::cae2:65ff:fece:feb9 | 1.2.3.0 | fe80::cae2:65ff:fece:0000 |
