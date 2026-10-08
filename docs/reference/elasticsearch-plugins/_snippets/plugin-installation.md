This plugin can be installed using the [plugin manager](/reference/elasticsearch/command-line-tools/elasticsearch-plugin.md):

```sh subs=true
sudo bin/elasticsearch-plugin install {{plugin-id}}
```

The plugin must be installed on every node in the cluster, and each node must be restarted after installation.

You can download this plugin for [offline install](/reference/elasticsearch/command-line-tools/elasticsearch-plugin.md#elasticsearch-plugin-ids) from [https://artifacts.elastic.co/downloads/elasticsearch-plugins/{{plugin-id}}/{{plugin-id}}-{{version.stack}}.zip](https://artifacts.elastic.co/downloads/elasticsearch-plugins/{{plugin-id}}/{{plugin-id}}-{{version.stack}}.zip). To verify the `.zip` file, use the [SHA hash](https://artifacts.elastic.co/downloads/elasticsearch-plugins/{{plugin-id}}/{{plugin-id}}-{{version.stack}}.zip.sha512) or [ASC key](https://artifacts.elastic.co/downloads/elasticsearch-plugins/{{plugin-id}}/{{plugin-id}}-{{version.stack}}.zip.asc).
