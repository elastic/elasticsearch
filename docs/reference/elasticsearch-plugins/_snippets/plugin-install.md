<!--
Shared installation steps, included by the plugin reference pages in this folder.

{{plugin-name}} is not a global substitution: it resolves per page from the `sub: plugin-name:`
key in that page's frontmatter, so each page renders its own plugin id. Any page including this
snippet must define that key, and the code block needs `subs=true` for it to resolve there.
-->
This plugin can be installed using the plugin manager:

```sh subs=true
sudo bin/elasticsearch-plugin install {{plugin-name}}
```

The plugin must be installed on every node in the cluster, and each node must be restarted after installation.

You can download this plugin for [offline install](docs-content://deploy-manage/plugins-and-bundles/self-managed/custom-url.md) from [https://artifacts.elastic.co/downloads/elasticsearch-plugins/{{plugin-name}}/{{plugin-name}}-{{version.stack}}.zip](https://artifacts.elastic.co/downloads/elasticsearch-plugins/{{plugin-name}}/{{plugin-name}}-{{version.stack}}.zip). To verify the `.zip` file, use the [SHA hash](https://artifacts.elastic.co/downloads/elasticsearch-plugins/{{plugin-name}}/{{plugin-name}}-{{version.stack}}.zip.sha512) or [ASC key](https://artifacts.elastic.co/downloads/elasticsearch-plugins/{{plugin-name}}/{{plugin-name}}-{{version.stack}}.zip.asc).
