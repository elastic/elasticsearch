---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/index.html
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/intro.html
---

# {{es}} plugins [intro]

This section contains reference information for {{es}} plugins.

Refer to [Add plugins and extensions](docs-content://deploy-manage/deploy/elastic-cloud/add-plugins-extensions.md) for an overview, setup instructions, and conceptual details.

Plugins are a way to enhance the core {{es}} functionality in a custom manner. They range from adding custom mapping types, custom analyzers, native scripts, custom discovery and more.

Plugins contain JAR files, but may also contain scripts and config files, and must be installed on every node in the cluster. After installation, each node must be restarted before the plugin becomes visible.

There are two categories of plugins:

Core Plugins
:   This category identifies plugins that are part of {{es}} project. Delivered at the same time as Elasticsearch, their version number always matches the version number of Elasticsearch itself. These plugins are maintained by the Elastic team with the appreciated help of amazing community members (for open source plugins). Issues and bug reports can be reported on the [Github project page](https://github.com/elastic/elasticsearch).

Community contributed
:   This category identifies plugins that are external to the {{es}} project. They are provided by individual developers or private companies and have their own licenses as well as their own versioning system. Issues and bug reports can usually be reported on the community plugin’s web site.

If you want to write your own plugin, refer to [Creating an {{es}} plugin](/extend/index.md).

:::{note}
A full cluster restart is required for installing plugins that have custom cluster state metadata. It is still possible to upgrade such plugins with a rolling restart.
:::