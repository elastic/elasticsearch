This plugin can be installed using the [plugin manager](/reference/elasticsearch/command-line-tools/elasticsearch-plugin.md):

```sh subs=true
sudo bin/elasticsearch-plugin install {{plugin-id}}
```

The plugin must be installed on every node in the cluster, and each node must be restarted after installation.

You can also download this plugin for [offline install](/reference/elasticsearch/command-line-tools/elasticsearch-plugin.md#elasticsearch-plugin-ids). Download the build that matches the {{es}} version you are running.

::::{tab-set}

:::{tab-item} Latest
Download the build for {{version.stack}}:

[https://artifacts.elastic.co/downloads/elasticsearch-plugins/{{plugin-id}}/{{plugin-id}}-{{version.stack}}.zip](https://artifacts.elastic.co/downloads/elasticsearch-plugins/{{plugin-id}}/{{plugin-id}}-{{version.stack}}.zip)

To verify the `.zip` file, use the [SHA hash](https://artifacts.elastic.co/downloads/elasticsearch-plugins/{{plugin-id}}/{{plugin-id}}-{{version.stack}}.zip.sha512) or [ASC key](https://artifacts.elastic.co/downloads/elasticsearch-plugins/{{plugin-id}}/{{plugin-id}}-{{version.stack}}.zip.asc).
:::

:::{tab-item} Specific version
Replace `<SPECIFIC.VERSION.NUMBER>` with the {{es}} version you are running. For example, you can replace `<SPECIFIC.VERSION.NUMBER>` with `9.0.0`.

* To download the build for the plugin, use this link:

    ```text subs=true
    https://artifacts.elastic.co/downloads/elasticsearch-plugins/{{plugin-id}}/{{plugin-id}}-<SPECIFIC.VERSION.NUMBER>.zip
    ```

* To download the corresponding SHA hash to verify the `.zip` file, use this link:

    ```text subs=true
    https://artifacts.elastic.co/downloads/elasticsearch-plugins/{{plugin-id}}/{{plugin-id}}-<SPECIFIC.VERSION.NUMBER>.zip.sha512
    ```

* To download the corresponding PGP signature (`.asc`) to verify the `.zip` file, use this link:

    ```text subs=true
    https://artifacts.elastic.co/downloads/elasticsearch-plugins/{{plugin-id}}/{{plugin-id}}-<SPECIFIC.VERSION.NUMBER>.zip.asc
    ```
:::

::::
