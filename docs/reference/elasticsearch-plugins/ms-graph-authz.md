---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/ms-graph-authz.html
applies_to:
  stack: ga 9.1
---

# Microsoft Graph Authz [ms-graph-authz]

The Microsoft Graph Authz plugin uses [Microsoft Graph](https://learn.microsoft.com/en-us/graph/api/user-list-memberof)
to look up group membership information from Microsoft Entra ID.

This is primarily intended to work around the Microsoft Entra ID maximum group
size limit (see [Group overages](https://learn.microsoft.com/en-us/security/zero-trust/develop/configure-tokens-group-claims-app-roles#group-overages)).

## Installation [ms-graph-authz-install]

If you're using a [self-managed Elasticsearch cluster](docs-content:///deploy-manage/deploy/self-managed.md), then this plugin can be installed using the plugin manager:

```sh
sudo bin/elasticsearch-plugin install microsoft-graph-authz
```

The plugin must be installed on every node in the cluster, and each node must be
restarted after installation.

You can download this plugin
for [offline install](/reference/elasticsearch-plugins/plugin-management-custom-url.md)
from [https://artifacts.elastic.co/downloads/elasticsearch-plugins/microsoft-graph-authz/microsoft-graph-authz-{{version.stack}}.zip](https://artifacts.elastic.co/downloads/elasticsearch-plugins/microsoft-graph-authz/microsoft-graph-authz-{{version.stack}}.zip).
To verify the `.zip` file, use
the [SHA hash](https://artifacts.elastic.co/downloads/elasticsearch-plugins/microsoft-graph-authz/microsoft-graph-authz-{{version.stack}}.zip.sha512)
or [ASC key](https://artifacts.elastic.co/downloads/elasticsearch-plugins/microsoft-graph-authz/microsoft-graph-authz-{{version.stack}}.zip.asc).

For all other deployment types, refer to [plugin management](/reference/elasticsearch-plugins/plugin-management.md).

## Removal [ms-graph-authz-remove]

The plugin can be removed with the following command:

```sh
sudo bin/elasticsearch-plugin remove microsoft-graph-authz
```

The node must be stopped before removing the plugin.

## Configuration

To learn how to configure the Microsoft Graph Authz plugin, refer to [configuration properties](/reference/elasticsearch-plugins/ms-graph-authz-configure-elasticsearch.md).
