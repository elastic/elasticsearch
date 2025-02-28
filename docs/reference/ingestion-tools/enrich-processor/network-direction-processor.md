---
navigation_title: "Network direction"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/network-direction-processor.html
---

# Network direction processor [network-direction-processor]


Calculates the network direction given a source IP address, destination IP address, and a list of internal networks.

The network direction processor reads IP addresses from [Elastic Common Schema (ECS)][Elastic Common Schema (ECS)](ecs://reference/index.md)) fields by default. If you use the ECS, only the `internal_networks` option must be specified.

$$$network-direction-options$$$

| Name | Required | Default | Description |
| --- | --- | --- | --- |
| `source_ip` | no | `source.ip` | Field containing the source IP address. |
| `destination_ip` | no | `destination.ip` | Field containing the destination IP address. |
| `target_field` | no | `network.direction` | Output field for the network direction. |
| `internal_networks` | yes * |  | List of internal networks. Supports IPv4 andIPv6 addresses and ranges in CIDR notation. Also supports the named ranges listed below. These may be constructed with [template snippets](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#template-snippets). * Must specify only one of `internal_networks` or `internal_networks_field`. |
| `internal_networks_field` | no |  | A field on the given document to read the `internal_networks` configuration from. |
| `ignore_missing` | no | `true` | If `true` and any required fields are missing,the processor quietly exits without modifying the document. |
| `description` | no | - | Description of the processor. Useful for describing the purpose of the processor or its configuration. |
| `if` | no | - | Conditionally execute the processor. See [Conditionally run a processor](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#conditionally-run-processor). |
| `ignore_failure` | no | `false` | Ignore failures for the processor. See [Handling pipeline failures](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#handling-pipeline-failures). |
| `on_failure` | no | - | Handle failures for the processor. See [Handling pipeline failures](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#handling-pipeline-failures). |
| `tag` | no | - | Identifier for the processor. Useful for debugging and metrics. |

One of either `internal_networks` or `internal_networks_field` must be specified. If `internal_networks_field` is specified, it follows the behavior specified by `ignore_missing`.


## Supported named network ranges [supported-named-network-ranges]

The named ranges supported for the `internal_networks` option are:

* `loopback` - Matches loopback addresses in the range of `127.0.0.0/8` or `::1/128`.
* `unicast` or `global_unicast` - Matches global unicast addresses defined in RFC 1122, RFC 4632, and RFC 4291 with the exception of the IPv4 broadcast address (`255.255.255.255`). This includes private address ranges.
* `multicast` - Matches multicast addresses.
* `interface_local_multicast` - Matches IPv6 interface-local multicast addresses.
* `link_local_unicast` - Matches link-local unicast addresses.
* `link_local_multicast` - Matches link-local multicast addresses.
* `private` - Matches private address ranges defined in RFC 1918 (IPv4) and RFC 4193 (IPv6).
* `public` - Matches addresses that are not loopback, unspecified, IPv4 broadcast, link local unicast, link local multicast, interface local multicast, or private.
* `unspecified` - Matches unspecified addresses (either the IPv4 address "0.0.0.0" or the IPv6 address "::").


## Examples [network-direction-processor-ex]

The following example illustrates the use of the network direction processor:

```console
POST _ingest/pipeline/_simulate
{
  "pipeline": {
    "processors": [
      {
        "network_direction": {
          "internal_networks": ["private"]
        }
      }
    ]
  },
  "docs": [
    {
      "_source": {
        "source": {
          "ip": "128.232.110.120"
        },
        "destination": {
          "ip": "192.168.1.1"
        }
      }
    }
  ]
}
```

Which produces the following result:

```console-result
{
  "docs": [
    {
      "doc": {
        ...
        "_source": {
          "destination": {
            "ip": "192.168.1.1"
          },
          "source": {
            "ip": "128.232.110.120"
          },
          "network": {
            "direction": "inbound"
          }
        }
      }
    }
  ]
}
```

