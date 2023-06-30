## APM plugin

The APM plugin installs index templates, component templates, and ingest pipelines for Elastic APM.

All resources are defined as YAML under [src/main/resources](src/main/resources).

The APM index templates rely on mappings from `x-pack-core`.
See [x-pack/plugin/core/src/main/resources](../core/src/main/resources).

## Testing

## Unit testing

Java unit tests cover basic, low-level details of the plugin, such as the parsing and loading of resources.
These can be run with:

```
./gradlew x-pack:plugin:apm:test
```

## Integration testing

The index templates and ingest pipeline functionality is tested using YAML REST tests.
These can be run with:

```
./gradlew x-pack:plugin:apm:yamlRestTest
```

Refer to the [rest-api-spec documentation](../../../rest-api-spec/src/yamlRestTest/resources/rest-api-spec/test/README.asciidoc)
for information about writing YAML REST tests.
