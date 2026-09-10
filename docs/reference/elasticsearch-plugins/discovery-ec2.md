---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/discovery-ec2.html
sub:
  plugin-name: discovery-ec2
---

# EC2 discovery plugin [discovery-ec2]

The EC2 discovery plugin provides a list of seed addresses to the [discovery process](docs-content://deploy-manage/distributed-architecture/discovery-cluster-formation/discovery-hosts-providers.md) by querying the [AWS API](https://github.com/aws/aws-sdk-java) for a list of EC2 instances matching certain criteria determined by the [plugin settings](/reference/elasticsearch-plugins/discovery-ec2-usage.md).

**If you are looking for a hosted solution of {{es}} on AWS, please visit [https://www.elastic.co/cloud](https://www.elastic.co/cloud).**


## Installation [discovery-ec2-install]

:::{include} _snippets/plugin-install.md
:::


## Removal [discovery-ec2-remove]

:::{include} _snippets/plugin-remove.md
:::



