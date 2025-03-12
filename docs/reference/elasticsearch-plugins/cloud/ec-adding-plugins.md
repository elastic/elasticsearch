---
mapped_pages:
  - https://www.elastic.co/guide/en/cloud/current/ec-adding-plugins.html
---

# Add plugins and extensions [ec-adding-plugins]

Plugins extend the core functionality of {{es}}. There are many suitable plugins, including:

* Discovery plugins, such as the cloud AWS plugin that allows discovering nodes on EC2 instances.
* Analysis plugins, to provide analyzers targeted at languages other than English.
* Scripting plugins, to provide additional scripting languages.

Plugins can come from different sources: the official ones created or at least maintained by Elastic, community-sourced plugins from other users, and plugins that you provide. Some of the official plugins are always provided with our service, and can be [enabled per deployment](/reference/elasticsearch-plugins/cloud/ec-adding-elastic-plugins.md\).

There are two ways to add plugins to a deployment in Elasticsearch Service:

* [Enable one of the official plugins already available in Elasticsearch Service](/reference/elasticsearch-plugins/cloud/ec-adding-elastic-plugins.md\).
* [Upload a custom plugin and then enable it per deployment](/reference/elasticsearch-plugins/cloud/ec-custom-bundles.md\).

Custom plugins can include the official {{es}} plugins not provided with Elasticsearch Service, any of the community-sourced plugins, or [plugins that you write yourself](/extend/index.md). Uploading custom plugins is available only to Gold, Platinum, and Enterprise subscriptions. For more information, check [Upload custom plugins and bundles](/reference/elasticsearch-plugins/cloud/ec-custom-bundles.md\).

To learn more about the official and community-sourced plugins, refer to [{{es}} Plugins and Integrations](/reference/elasticsearch-plugins/index.md).

For a detailed guide with examples of using the Elasticsearch Service API to create, get information about, update, and delete extensions and plugins, check [Managing plugins and extensions through the API](/reference/elasticsearch-plugins/cloud/ec-plugins-guide.md\).

Plugins are not supported for {{kib}}. To learn more, check [Restrictions for {{es}} and {{kib}} plugins](cloud://release-notes/cloud-hosted/known-issues.md#ec-restrictions-plugins).




