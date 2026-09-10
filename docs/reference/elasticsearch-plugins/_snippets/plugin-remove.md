<!--
Shared removal steps, included by the plugin reference pages in this folder.

{{plugin-name}} is not a global substitution: it resolves per page from the `sub: plugin-name:`
key in that page's frontmatter, so each page renders its own plugin id. Any page including this
snippet must define that key, and the code block needs `subs=true` for it to resolve there.
-->
The plugin can be removed with the following command:

```sh subs=true
sudo bin/elasticsearch-plugin remove {{plugin-name}}
```

The node must be stopped before removing the plugin.
