## `IP_PREFIX` [esql-ip_prefix]

**Syntax**

:::{image} ../../../../../images/ip_prefix.svg
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
