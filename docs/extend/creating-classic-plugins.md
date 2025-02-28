---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/creating-classic-plugins.html
---

# Creating classic plugins [creating-classic-plugins]

Classic plugins provide {{es}} with mechanisms for custom authentication, authorization, scoring, and more.

::::{admonition} Plugin release lifecycle
:class: important

Classic plugins require you to build a new version for each new {{es}} release. This version is checked when the plugin is installed and when it is loaded. {{es}} will refuse to start in the presence of plugins with the incorrect `elasticsearch.version`.

::::



## Classic plugin file structure [_classic_plugin_file_structure]

Classic plugins are ZIP files composed of JAR files and [a metadata file called `plugin-descriptor.properties`](/extend/plugin-descriptor-file-classic.md), a Java properties file that describes the plugin.

Note that only JAR files at the root of the plugin are added to the classpath for the plugin. If you need other resources, package them into a resources JAR.


## Example plugins [_example_plugins]

The {{es}} repository contains [examples of plugins](https://github.com/elastic/elasticsearch/tree/main/plugins/examples). Some of these include:

* a plugin with [custom settings](https://github.com/elastic/elasticsearch/tree/main/plugins/examples/custom-settings)
* a plugin with a [custom ingest processor](https://github.com/elastic/elasticsearch/tree/main/plugins/examples/custom-processor)
* adding [custom rest endpoints](https://github.com/elastic/elasticsearch/tree/main/plugins/examples/rest-handler)
* adding a [custom rescorer](https://github.com/elastic/elasticsearch/tree/main/plugins/examples/rescore)
* a script [implemented in Java](https://github.com/elastic/elasticsearch/tree/main/plugins/examples/script-expert-scoring)

These examples provide the bare bones needed to get started. For more information about how to write a plugin, we recommend looking at the [source code of existing plugins](https://github.com/elastic/elasticsearch/tree/main/plugins/) for inspiration.


## Testing your plugin [_testing_your_plugin]

Use `bin/elasticsearch-plugin install file:///path/to/your/plugin` to install your plugin for testing. The Java plugin is auto-loaded only if it’s in the `plugins/` directory.


## Java Security permissions [plugin-authors-jsm]

Some plugins may need additional security permissions. A plugin can include the optional `plugin-security.policy` file containing `grant` statements for additional permissions. Any additional permissions will be displayed to the user with a large warning, and they will have to confirm them when installing the plugin interactively. So if possible, it is best to avoid requesting any spurious permissions!

If you are using the {{es}} Gradle build system, place this file in `src/main/plugin-metadata` and it will be applied during unit tests as well.

The Java security model is stack-based, and additional permissions are granted to the jars in your plugin, so you have to write proper security code around operations requiring elevated privileges. You might add a check to prevent unprivileged code (such as scripts) from gaining escalated permissions. For example:

```java
// ES permission you should check before doPrivileged() blocks
import org.elasticsearch.SpecialPermission;

SecurityManager sm = System.getSecurityManager();
if (sm != null) {
  // unprivileged code such as scripts do not have SpecialPermission
  sm.checkPermission(new SpecialPermission());
}
AccessController.doPrivileged(
  // sensitive operation
);
```

Check [Secure Coding Guidelines for Java SE](https://www.oracle.com/technetwork/java/seccodeguide-139067.md) for more information.


