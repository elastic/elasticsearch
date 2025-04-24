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


## Entitlements policy [_entitlements_policy]

Some plugins may need additional _entitlements_.

{{es}} limits the ability to perform certain security-sensitive actions as part of its _Entitlement_ security mechanism (e.g. to limit the potential fallout from remote code execution (RCE) vulnerabilities).

The Entitlement model is based on Java modules.
An _entitlement_ granted to a Java module allows the module's code to perform the security-sensitive action associated with that entitlement. For example, the ability to create threads is limited to modules that have the `manage_threads` entitlement; likewise, the ability to read a file from the filesystem is limited to modules that have the `files` entitlement for that particular file.

In practice, an entitlement allows plugin code to call a well-defined set of corresponding JDK methods; without the entitlement calls to those JDK methods are denied and throw a `NotEntitledException`. Plugin can include the optional `entitlement-policy.yaml` file to define modules and required entitlements. Any additional entitlement requested by the plugin will be displayed to the user with a large warning, and users will have to confirm them when installing the plugin interactively. Therefore, it is best to avoid requesting any spurious entitlement!

If you are using the {{es}} Gradle build system, place this file in `src/main/plugin-metadata` and it will be applied during unit tests as well.

An entitlement policy applies to all of your plugin jars (your own code and third party dependencies). You have to write your policy file accordingly. For example, if a plugin uses the Example API client to perform network operations, it will need a policy that may look like this:

```YAML
org.elasticsearch.example-plugin:
  - manage_threads
com.example.api.client:
  - set_https_connection_properties
  - outbound_network
```

Note how the network related entitlements are granted to the `com.example.api.client` module, as the code performing the sensitive network operations is in the `example-api-client` dependency.

If your plugin is not modular, all entitlements must be specified under the catch-all `ALL-UNNAMED` module name:

```YAML
ALL-UNNAMED:
  - manage_threads
  - set_https_connection_properties
  - outbound_network
```
### Entitlements

The entitlements currently implemented and enforced in {{es}} that are available to plugins are the following ones:

#### `manage_threads`

Allows code to call methods that create or modify properties on Java Threads, for example `Thread#start` or `ThreadGroup#setMaxPriority`.

:::{note}
This entitlement is rarely necessary. Your plugin should use {{es}} thread pools and executors (see `Plugin#getExecutorBuilders`) instead of creating and managing its own threads. Plugins should avoid modifying thread name, priority, daemon state, and context class loader when executing on ES threadpools.

However, many 3rd party libraries that support async operations, such as the Apache HTTP client, need to create and manage their own threads. In such cases, it makes sense to request this entitlement.
:::

Example:
```yaml
org.example.module: # or 'ALL-UNNAMED' if the plugin is non-modular
  - manage_threads
```

#### `outbound_network`

Allows code to call methods to make a network connection. {{es}} does not grant any network access by default; each plugin that needs to directly connect to an external resource (e.g. to upload or download data) must request this entitlement.

Example:
```yaml
org.example.module: # or 'ALL-UNNAMED' if the plugin is non-modular
  - outbound_network
```

#### `set_https_connection_properties`
Allows code to call methods to change properties on an established HTTPS connection. While this is generally innocuous (e.g. the google API client uses it to modify the HTTPS connections they just created), these methods can allow code to change arbitrary connections.

Example:
```yaml
org.example.module: # or 'ALL-UNNAMED' if the plugin is non-modular
  - set_https_connection_properties
```

#### `inbound_network` (deprecated)
Allows code to call methods to listen for incoming connections, so external resources can connect directly to your plugin. This entitlement should only be used when absolutely necessary (e.g. if a library you depend on requires it for authentication). Granting it makes the {{es}} node more vulnerable to attacks. This entitlement is deprecated, and can be removed in a future version of {{es}}.

Example:
```yaml
org.example.module: # or 'ALL-UNNAMED' if the plugin is non-modular
  - inbound_network
```

#### `load_native_libraries`
Allows code to load native libraries and call [restricted methods](https://docs.oracle.com/en/java/javase/24/core/restricted-methods.html). This entitlement also enables native access for modules it is granted to. Native code may alter the JVM or circumvent access checks such as file or network restrictions.

Example:
```yaml
org.example.module: # or 'ALL-UNNAMED' if the plugin is non-modular
  - load_native_libraries
```

#### `files`

Allows code to access the filesystem, to read or write paths as specified by the entitlement's fields. The filesystem of the OS hosting {{es}} may contain sensitive files, for example credentials. Some files are meant to be always accessible to {{es}}, but plugins can not access them directly: {{es}} enforces that certain files can only be read by its core code, while some other files can not be read or written at all. A plugin is always granted `read` access to the {{es}} config directory and `read_write` access to the temp directory; if the plugin requires to read, write or access additional files or directories, it must specify them via this entitlement.

It is possible to specify 3 different types of file entitlement:
  - `path` to specify an absolute path
  - `relative_path` to specify a relative path. The path will be resolved via the `relative_to` field, which is used to qualify the relative path. It can be a specific {{es}} directory (`config` or `data`), or to the user home directory (`home`) (the home of the user running {{es}})
  - `relative_path` to specify a path resolved via the `relative_to` field, which can have the following values:
    - `config`: the {{es}} [config directory](https://www.elastic.co/guide/en/elasticsearch/reference/current/settings.html#config-files-location)
    - `data`: the {{es}} [data directory](https://www.elastic.co/guide/en/elasticsearch/reference/current/path-settings-overview.html)
    - `home`: the home directory of the user running {{es}}
  - `path_setting` to specify a path defined via an {{es}} setting. The path can be absolute or relative; in the latter case, the path will be resolved using the `basedir_if_relative` path (which can assume the same values as `relative_to`)

Each of the 3 types has some additional fields:
- `mode` (required): can be either `read` or `read_write`
- `platform` (optional): indicates this item applies only to one platform, which can be one of `linux`, `macos` or `windows`. On other platforms, the item is ignored. If this field is not specified, the item applies to all platforms.
- `exclusive`: access to this path is exclusive for this plugin; this means that other plugins will not be able to access to it, not even if they have an entitlement that would normally grant access to that path.

Example:
```yaml
org.example.module: # or 'ALL-UNNAMED' if the plugin is non-modular
  - files:
    - path: "/absolute/path"
      mode: read
    - relative_path: "relative/file.txt"
      relative_to: data
      mode: read_write
    - path_setting: setting.name
      basedir_if_relative: data
      mode: read
```


#### `write_system_properties`
Allows code to set one or more system properties (e.g. by calling `System#setProperty`). The code to which this entitlement is granted can change the properties listed in the `properties` field. In general, it's best to avoid changing a system property dynamically as this can have effects on code which later reads the property. The global nature of system properties means one plugin could then affect another, depending on load order.

Example:
```yaml
org.example.module: # or 'ALL-UNNAMED' if the plugin is non-modular
  - write_system_properties:
      properties:
        - property.one
        - property.two
```
Check the Entitlements [README in the elasticsearch repository](https://github.com/elastic/elasticsearch/blob/main/libs/entitlement/README.md) for more information.
