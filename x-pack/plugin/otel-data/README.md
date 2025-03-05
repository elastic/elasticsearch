## OpenTelemetry Ingest plugin

The OpenTelemetry Ingest plugin installs index templates and component templates for OpenTelemetry data.

All resources are defined as YAML under [src/main/resources](src/main/resources).

The OpenTelemetry index templates rely on mappings from `x-pack-core`.
See [x-pack/plugin/core/src/main/resources](../core/src/main/resources).

## Adding/Removing/Updating a resource

All resources are defined as YAML under [src/main/resources](src/main/resources).

For a resource to be known to the plugin it must be added to
[src/main/resources/resources.yaml](src/main/resources/resources.yaml) in the
appropriate section.

Any update to resources included by this package also requires a bump to the
`version` property included in the resources file.

## Testing

## Integration testing

The index templates and ingest pipeline functionality is tested using YAML REST tests.
These can be run with:

```
./gradlew :x-pack:plugin:otel-data:yamlRestTest
```

Refer to the [rest-api-spec documentation](../../../rest-api-spec/src/yamlRestTest/resources/rest-api-spec/test/README.asciidoc)
for information about writing YAML REST tests.
