NOTE: this plugin is not related to APM Metrics used in ES codebase. The APM Metrics are in :modules:apm

## APM Ingest plugin

The APM Ingest plugin installs index templates, component templates, and ingest pipelines for Elastic APM Server.

All resources are defined as YAML under [src/main/resources](src/main/resources).

The APM index templates rely on mappings from `x-pack-core`.
See [x-pack/plugin/core/src/main/resources](../core/src/main/resources).

This plugin is intended to work with data produced by https://github.com/elastic/apm-data.


## Adding/Removing/Updating a resource

All resources are defined as YAML under [src/main/resources](src/main/resources).

For a resource to be known to the plugin it must be added to
[src/main/resources/resources.yaml](src/main/resources/resources.yaml) in the
appropriate section.

Any update to resources included by this package also requires a bump to the
`version` property included in the resources file.

## Testing

## Unit testing

Java unit tests cover basic, low-level details of the plugin, such as the parsing and loading of resources.
These can be run with:

```
./gradlew x-pack:plugin:apm-data:test
```

## Integration testing

The index templates and ingest pipeline functionality is tested using YAML REST tests.
These can be run with:

```
./gradlew x-pack:plugin:apm-data:yamlRestTest
```

Refer to the [rest-api-spec documentation](../../../rest-api-spec/src/yamlRestTest/resources/rest-api-spec/test/README.asciidoc)
for information about writing YAML REST tests.
