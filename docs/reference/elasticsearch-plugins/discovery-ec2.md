---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/discovery-ec2.html
---

# EC2 Discovery plugin [discovery-ec2]

The EC2 discovery plugin provides a list of seed addresses to the [discovery process](docs-content://deploy-manage/distributed-architecture/discovery-cluster-formation/discovery-hosts-providers.md) by querying the [AWS API](https://github.com/aws/aws-sdk-java) for a list of EC2 instances matching certain criteria determined by the [plugin settings](/reference/elasticsearch-plugins/discovery-ec2-usage.md).

**If you are looking for a hosted solution of {{es}} on AWS, please visit [https://www.elastic.co/cloud](https://www.elastic.co/cloud).**


## Installation [discovery-ec2-install]

This plugin can be installed using the plugin manager:

```sh
sudo bin/elasticsearch-plugin install discovery-ec2
```

The plugin must be installed on every node in the cluster, and each node must be restarted after installation.

You can download this plugin for [offline install](/reference/elasticsearch-plugins/plugin-management-custom-url.md) from [https://artifacts.elastic.co/downloads/elasticsearch-plugins/discovery-ec2/discovery-ec2-{{version}}.zip](https://artifacts.elastic.co/downloads/elasticsearch-plugins/discovery-ec2/discovery-ec2-{{version}}.zip). To verify the `.zip` file, use the [SHA hash](https://artifacts.elastic.co/downloads/elasticsearch-plugins/discovery-ec2/discovery-ec2-{{version}}.zip.sha512) or [ASC key](https://artifacts.elastic.co/downloads/elasticsearch-plugins/discovery-ec2/discovery-ec2-{{version}}.zip.asc).


## Removal [discovery-ec2-remove]

The plugin can be removed with the following command:

```sh
sudo bin/elasticsearch-plugin remove discovery-ec2
```

The node must be stopped before removing the plugin.



