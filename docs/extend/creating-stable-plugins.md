---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/creating-stable-plugins.html
---

# Creating text analysis plugins with the stable plugin API [creating-stable-plugins]

Text analysis plugins provide {{es}} with custom [Lucene analyzers, token filters, character filters, and tokenizers](docs-content://manage-data/data-store/text-analysis.md).


## The stable plugin API [_the_stable_plugin_api]

Text analysis plugins can be developed against the stable plugin API. This API consists of the following dependencies:

* `plugin-api` - an API used by plugin developers to implement custom {{es}} plugins.
* `plugin-analysis-api` - an API used by plugin developers to implement analysis plugins and integrate them into {{es}}.
* `lucene-analysis-common` - a dependency of `plugin-analysis-api` that contains core Lucene analysis interfaces like `Tokenizer`, `Analyzer`, and `TokenStream`.

For new versions of {{es}} within the same major version, plugins built against this API does not need to be recompiled. Future versions of the API will be backwards compatible and plugins are binary compatible with future versions of {{es}}. In other words, once you have a working artifact, you can re-use it when you upgrade {{es}} to a new bugfix or minor version.

A text analysis plugin can implement four factory classes that are provided by the analysis plugin API.

* `AnalyzerFactory` to create a Lucene analyzer
* `CharFilterFactory` to create a character character filter
* `TokenFilterFactory` to create a Lucene token filter
* `TokenizerFactory` to create a Lucene tokenizer

The key to implementing a stable plugin is the `@NamedComponent` annotation. Many of {{es}}'s components have names that are used in configurations. For example, the keyword analyzer is referenced in configuration with the name `"keyword"`. Once your custom plugin is installed in your cluster, your named components may be referenced by name in these configurations as well.

You can also create text analysis plugins as a [classic plugin](/extend/creating-classic-plugins.md). However, classic plugins are pinned to a specific version of {{es}}. You need to recompile them when upgrading {{es}}. Because classic plugins are built against internal APIs that can change, upgrading to a new version may require code changes.


## Stable plugin file structure [_stable_plugin_file_structure]

Stable plugins are ZIP files composed of JAR files and two metadata files:

* `stable-plugin-descriptor.properties` - a Java properties file that describes the plugin. Refer to [The plugin descriptor file for stable plugins](/extend/plugin-descriptor-file-stable.md).
* `named_components.json` - a JSON file mapping interfaces to key-value pairs of component names and implementation classes.

Note that only JAR files at the root of the plugin are added to the classpath for the plugin. If you need other resources, package them into a resources JAR.


## Development process [_development_process]

Elastic provides a Gradle plugin, `elasticsearch.stable-esplugin`, that makes it easier to develop and package stable plugins. The steps in this section assume you use this plugin. However, you don’t need Gradle to create plugins.

The {{es}} Github repository contains [an example analysis plugin](https://github.com/elastic/elasticsearch/tree/main/plugins/examples/stable-analysis). The example `build.gradle` build script provides a good starting point for developing your own plugin.


### Prerequisites [_prerequisites]

Plugins are written in Java, so you need to install a Java Development Kit (JDK). Install Gradle if you want to use Gradle.


### Step by step [_step_by_step]

1. Create a directory for your project.
2. Copy the example `build.gradle` build script to your project directory.  Note that this build script uses the `elasticsearch.stable-esplugin` gradle plugin to build your plugin.
3. Edit the `build.gradle` build script:

    * Add a definition for the `pluginApiVersion` and matching `luceneVersion` variables to the top of the file. You can find these versions in the `build-tools-internal/version.properties` file in the [Elasticsearch Github repository](https://github.com/elastic/elasticsearch/).
    * Edit the `name` and `description` in the `esplugin` section of the build script. This will create the plugin descriptor file. If you’re not using the `elasticsearch.stable-esplugin` gradle plugin, refer to [The plugin descriptor file for stable plugins](/extend/plugin-descriptor-file-stable.md) to create the file manually.
    * Add module information.
    * Ensure you have declared the following compile-time dependencies. These dependencies are compile-time only because {{es}} will provide these libraries at runtime.

        * `org.elasticsearch.plugin:elasticsearch-plugin-api`
        * `org.elasticsearch.plugin:elasticsearch-plugin-analysis-api`
        * `org.apache.lucene:lucene-analysis-common`

    * For unit testing, ensure these dependencies have also been added to the `build.gradle` script as `testImplementation` dependencies.

4. Implement an interface from the analysis plugin API, annotating it with `NamedComponent`. Refer to [Example text analysis plugin](/extend/example-text-analysis-plugin.md) for an example.
5. You should now be able to assemble a plugin ZIP file by running:

    ```sh
    gradle bundlePlugin
    ```

    The resulting plugin ZIP file is written to the  `build/distributions` directory.



### YAML REST tests [_yaml_rest_tests]

The Gradle `elasticsearch.yaml-rest-test` plugin enables testing of your plugin using the [{{es}} yamlRestTest framework](https://github.com/elastic/elasticsearch/blob/main/rest-api-spec/src/yamlRestTest/resources/rest-api-spec/test/README.asciidoc). These tests use a YAML-formatted domain language to issue REST requests against an internal {{es}} cluster that has your plugin installed, and to check the results of those requests. The structure of a YAML REST test directory is as follows:

* A test suite class, defined under `src/yamlRestTest/java`. This class should extend `ESClientYamlSuiteTestCase`.
* The YAML tests themselves should be defined under `src/yamlRestTest/resources/test/`.



