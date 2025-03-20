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

The Entitlement model is _scope_-based: the subset of code to which we grant the ability to perform a security-sensitive action is called a _scope_.
Currently, scope granularity is at java-module level; in other words, an _entitlement scope_ corresponds to a java module.
An _entitlement_ granted to a scope gives it back the ability to perform the security-sensitive action associated with that entitlement. For example, the ability to read a file from the filesystem is limited to scopes that have the `files` entitlement.

In practice, an entitlement allows code in a plugin to call a well-defined set of JDK methods related to the sensitive action; without the entitlement, calls to those JDK methods are not allowed and throw a `NotEntitledException`. A plugin can include the optional `entitlement-policy.yaml` file to define scopes and entitlements it requires. Any additional entitlement requested for the plugin will be displayed to the user with a large warning, and they will have to confirm them when installing the plugin interactively. So if possible, it is best to avoid requesting any spurious entitlement!

If you are using the {{es}} Gradle build system, place this file in `src/main/plugin-metadata` and it will be applied during unit tests as well.

 An entitlement policy applies to all of your plugin jars (your own code and third party dependencies), and you have to write your policy file accordingly. For example, a plugin using the google API client to perform network operations will need a policy that looks like this:

```YAML
org.elasticsearch.example-plugin:
  - manage_threads
com.google.api.client:
  - set_https_connection_properties
  - outbound_network
```

Note how here we grant the network related entitlements to the `com.google.api.client` module, as the code that will need to perform the sensitive network operations is in the `com.google.api-client` dependency.

If your plugin is not modular, all entitlements must be specified under the catch-all `ALL-UNNAMED` scope:

```YAML
ALL-UNNAMED:
  - manage_threads
  - set_https_connection_properties
  - outbound_network
```
### Entitlements

The entitlements currently implemented and enforced in {{es}}, and available to plugins, are the following ones:

#### `manage_threads`

Allows code to call methods that create or modify properties on Java Threads, for example `Thread#start` or `ThreadGroup#setMaxPriority`. In general, setting the name, priority, daemon state and context class loader are things no plugins should do when executing on
{{es}} threadpools; however, many 3rd party libraries that support async operations (e.g. Apache HTTP client) need to manage their own threads. In this case it is justifiable to need and add this entitlement.
Format:
```yaml
org.example.module: # or 'ALL-UNNAMED' if the plugin is non-modular
  - manage_threads
```

#### `outbound_network`

Allows code to call methods to connect to an endpoint. {{es}} does not grant any network access by default; each plugin that needs to directly connect to an external resource (e.g. to upload or download data) must specify this entitlement.
Format:
```yaml
org.example.module: # or 'ALL-UNNAMED' if the plugin is non-modular
  - outbound_network
```

#### `set_https_connection_properties`
Allows code to call methods to change properties on an established HTTPS connection. While this is generally innocuous (e.g. the google API client uses it to modify the HTTPS connections they just created), these methods can allow code to change arbitrary connections.
  Format:
```yaml
org.example.module: # or 'ALL-UNNAMED' if the plugin is non-modular
  - set_https_connection_properties
```

#### `inbound_network` (deprecated)
Allows code to call methods to listen for incoming connections, so external resources can connect directly to your plugin. This entitlement should be used sparingly, only when absolutely necessary (e.g. if a library you depend on requires it for authentication), as granting it makes the {{es}} node more vulnerable to attacks. This entitlement is deprecated, and can be removed in a future version of {{es}}.
  Format:
```yaml
org.example.module: # or 'ALL-UNNAMED' if the plugin is non-modular
  - inbound_network
```

#### `load_native_libraries`
Allows code to load native libraries and call restricted methods. This entitlement also enable native access for the modules to which it is granted (see [Restricted Methods](https://docs.oracle.com/en/java/javase/24/core/restricted-methods.html) in the Java documentation). Native code may alter the JVM or circumvent access checks such as files or network restrictions.
  Format:
```yaml
org.example.module: # or 'ALL-UNNAMED' if the plugin is non-modular
  - load_native_libraries
```

#### `files`

Allows code to access the filesystem, to read or write paths as specified by the entitlement's fields. The filesystem of the OS hosting {{es}} may contain sensitive files, for example containing credentials. Some files are meant to be always accessible to {{es}}, but we do not allow plugins to access them directly: {{es}} enforces that certain files can only be read by its core code, and some files are not be read or written at all. A plugin is always granted `read` access to the {{es}} config directory and `read_write` access to the temp directory; if it wants to read or access additional files or directories, the plugin must specify them via this entitlement.

It is possible to specify 3 different types of file entitlement:
  - `path` to specify an absolute path
  - `relative_path` to specify a relative path. The path will be resolved via the `relative_to` field, which is used to qualify the relative path. It can be a specific {{es}} directory (`config` or `data`), or to the user home directory (`home`) (the home of the user running {{es}})
  - `path_setting` to specify a path defined via an {{es}} setting. The path can be absolute or relative; in the latter case, the path will be resolved using the `basedir_if_relative` path (which can assume the same values as `relative_to`)

Each of the 3 types has some additional fields:
- `mode` (required): can be either `read` or `read_write`
- `platform` (optional): if a path is specific to a platform. Can be `linux`, `macos` or `windows`. If not specified, the path is assumed to be valid on all platforms.
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
Allows code to set one or more system properties (e.g. by calling `System#setProperty`). The code to which this entitlement is granted can change the properties listed in the `properties` field. In general, it's best to avoid changing a system property dynamically as this can have effects on code which later reads the property. The global nature of system properties means one plugin could then affect another, depending on load order. Example:
```yaml
org.example.module: # or 'ALL-UNNAMED' if the plugin is non-modular
  - write_system_properties:
      properties:
        - property.one
        - property.two
```
