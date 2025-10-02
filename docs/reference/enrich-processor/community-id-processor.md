---
navigation_title: "Community ID"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/community-id-processor.html
---

# Community ID processor [community-id-processor]


Computes the Community ID for network flow data as defined in the [Community ID Specification](https://github.com/corelight/community-id-spec). You can use a community ID to correlate network events related to a single flow.

The community ID processor reads network flow data from related [Elastic Common Schema (ECS)](ecs://reference/index.md) fields by default. If you use the ECS, no configuration is required.

$$$community-id-options$$$

| Name | Required | Default | Description |
| --- | --- | --- | --- |
| `source_ip` | no | `source.ip` | Field containing the source IP address. |
| `source_port` | no | `source.port` | Field containing the source port. |
| `destination_ip` | no | `destination.ip` | Field containing the destination IP address. |
| `destination_port` | no | `destination.port` | Field containing the destination port. |
| `iana_number` | no | `network.iana_number` | Field containing the IANA number. |
| `icmp_type` | no | `icmp.type` | Field containing the ICMP type. |
| `icmp_code` | no | `icmp.code` | Field containing the ICMP code. |
| `transport` | no | `network.transport` | Field containing the transport protocol name or number.Used only when the `iana_number` field is not present. The following protocol names are currently supported:`ICMP`, `IGMP`, `TCP`, `UDP`, `GRE`, `ICMP IPv6`, `EIGRP`, `OSPF`, `PIM`, and `SCTP`. |
| `target_field` | no | `network.community_id` | Output field for the community ID. |
| `seed` | no | `0` | Seed for the community ID hash. Must be between0 and 65535 (inclusive). The seed can prevent hash collisions between network domains, such asa staging and production network that use the same addressing scheme. |
| `ignore_missing` | no | `true` | If `true` and any required fields are missing,the processor quietly exits without modifying the document. |
| `description` | no | - | Description of the processor. Useful for describing the purpose of the processor or its configuration. |
| `if` | no | - | Conditionally execute the processor. See [Conditionally run a processor](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#conditionally-run-processor). |
| `ignore_failure` | no | `false` | Ignore failures for the processor. See [Handling pipeline failures](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#handling-pipeline-failures). |
| `on_failure` | no | - | Handle failures for the processor. See [Handling pipeline failures](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#handling-pipeline-failures). |
| `tag` | no | - | Identifier for the processor. Useful for debugging and metrics. |

Here is an example definition of the community ID processor:

```js
{
  "description" : "...",
  "processors" : [
    {
      "community_id": {
      }
    }
  ]
}
```

When the above processor executes on the following document:

```js
{
  "_source": {
    "source": {
      "ip": "123.124.125.126",
      "port": 12345
    },
    "destination": {
      "ip": "55.56.57.58",
      "port": 80
    },
    "network": {
      "transport": "TCP"
    }
  }
}
```

It produces this result:

```js
"_source" : {
  "destination" : {
    "port" : 80,
    "ip" : "55.56.57.58"
  },
  "source" : {
    "port" : 12345,
    "ip" : "123.124.125.126"
  },
  "network" : {
    "community_id" : "1:9qr9Z1LViXcNwtLVOHZ3CL8MlyM=",
    "transport" : "TCP"
  }
}
```

