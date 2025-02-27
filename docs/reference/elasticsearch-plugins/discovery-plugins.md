---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/discovery.html
---

# Discovery plugins [discovery]

Discovery plugins extend Elasticsearch by adding new seed hosts providers that can be used to extend the [cluster formation module](docs-content://deploy-manage/distributed-architecture/discovery-cluster-formation.md).


## Core discovery plugins [_core_discovery_plugins]

The core discovery plugins are:

[EC2 discovery](/reference/elasticsearch-plugins/discovery-ec2.md)
:   The EC2 discovery plugin uses the [AWS API](https://github.com/aws/aws-sdk-java) to identify the addresses of seed hosts.

[Azure Classic discovery](/reference/elasticsearch-plugins/discovery-azure-classic.md)
:   The Azure Classic discovery plugin uses the Azure Classic API to identify the addresses of seed hosts.

[GCE discovery](/reference/elasticsearch-plugins/discovery-gce.md)
:   The Google Compute Engine discovery plugin uses the GCE API to identify the addresses of seed hosts.




