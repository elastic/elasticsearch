---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/listing-removing-updating.html
applies_to:
  deployment:
    self: ga
---

# Listing, removing and updating installed plugins [listing-removing-updating]


## Listing plugins [_listing_plugins]

A list of the currently loaded plugins can be retrieved with the `list` option:

```shell
sudo bin/elasticsearch-plugin list
```

Alternatively, use the [node-info API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-nodes-info) to find out which plugins are installed on each node in the cluster


## Removing plugins [_removing_plugins]

Plugins can be removed manually, by deleting the appropriate directory under `plugins/`, or using the public script:

```shell
sudo bin/elasticsearch-plugin remove [pluginname]
```

After a Java plugin has been removed, you will need to restart the node to complete the removal process.

By default, plugin configuration files (if any) are preserved on disk; this is so that configuration is not lost while upgrading a plugin. If you wish to purge the configuration files while removing a plugin, use `-p` or `--purge`. This can option can be used after a plugin is removed to remove any lingering configuration files.


## Removing multiple plugins [removing-multiple-plugins]

Multiple plugins can be removed in one invocation as follows:

```shell
sudo bin/elasticsearch-plugin remove [pluginname] [pluginname] ... [pluginname]
```


## Updating plugins [_updating_plugins]

Except for text analysis plugins that are created using the [stable plugin API](/extend/creating-stable-plugins.md), plugins are built for a specific version of {{es}}, and must be reinstalled each time {{es}} is updated.

```shell
sudo bin/elasticsearch-plugin remove [pluginname]
sudo bin/elasticsearch-plugin install [pluginname]
```

