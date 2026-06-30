---
navigation_title: Columnar
applies_to:
  stack: preview
  serverless: preview
---

# Columnar index mode [columnar-index-mode]

With columnar mode enabled, Elasticsearch becomes a full **analytical and search columnar store**. Setting `index.mode` to `columnar` or `logsdb_columnar` activates a set of changes that collectively align Elasticsearch's storage model with dedicated columnar stores:

- Fields are stored **once, as doc values only**. All non-text fields are not indexed by default, eliminating the storage cost of maintaining redundant index structures.
- For non-indexed fields, [doc values skippers](/reference/elasticsearch/mapping-reference/doc-values.md#doc-values-skippers) are enabled by default. Doc values skippers are compact skip lists with metadata (e.g. min and max values) to avoid scanning large blocks of data when executing a query.
- Mappings are always flat, and object like fields in mappings are always auto-flattened.
- The original source is no longer stored. If source is requested at query time, then a flattened or columnar representation of the source is generated on the fly and returned. Optionally, the columnar source can also be generated at index time and stored to disk as doc values.
- New multi-value semantics: the original ordering of multiple values per field per document (e.g., in arrays) is preserved by default. Optionally, fields in mappings can be configured to only allow one value per document.
- Fields in mappings can be configured to reject documents that have no value for them.
- Metadata fields like `_routing` and `_id` are stored using doc values as well.
- An optimized doc values format is used by default, further reducing storage footprint. especially when combined with index sorting.

Together with index sorting, columnar mode brings Elasticsearch's storage footprint and columnar access in line with dedicated columnar stores while retaining its full search and aggregation capabilities.

## Columnar index modes [columnar-modes]

Two columnar index modes are available:

`columnar`
:   A general-purpose columnar store with no use-case-specific defaults. Use this mode for bare indices and data streams that do not fit the logging paradigm.

`logsdb_columnar`
:   A columnar store with logging-oriented defaults. It inherits all behavior of the `columnar` mode and additionally:
    - Applies a default mapping that includes a `@timestamp` field (and optionally `host.name`).
    - Enables sort-field-based routing by default (`index.logsdb.route_on_sort_fields: true`), which routes documents based on the configured index sort fields rather than a routing path.

    Use this mode for log data.

Both modes are strictly columnar: they reject mapping-level runtime fields and prevent disabling `_source`.

## Enabling columnar mode [enabling-columnar-mode]

Set `mode` index setting at index creation time. The setting cannot be changed after the index is created.

```console
PUT my-index
{
  "settings": {
    "mode": "columnar"
  }
}
```

For log data, use a component template to enable the `logsdb_columnar` mode for all `logs-*-*` data streams:

```console
PUT _component_template/logs@custom
{
  "template": {
    "settings": {
      "mode": "logsdb_columnar"
    }
  }
}
```

By default, all data streams matching with `logs-*-*` use `logsdb`, but with the custom component template in place, all data streams matching with `logs-*-*` will use `logsdb_columnar`.

Or set it directly at index creation:

```console
PUT my-logs-index
{
  "settings": {
    "mode": "logsdb_columnar"
  }
}
```

### Index sorting [index-sorting]

An important configuration step when setting up columnar index mode is determining the index sort fields.
Good index sorting fields allow storing data more efficiently and improving query response times.
Good index sorting fields are dependent on the use case and the data.

The `logsdb_columnar` index mode, just like `logsdb` index mode, uses by default the `host.name` field in ascending order and `@timestamp` field in descending order as the index sort fields.
Hosts typically emit similar logs and therefor it makes sense logs from the host are adjacent on disk. This ensures that logs are compressed efficiently on disk.
The `@timestamp` field is also used as the index sort field because then within a host logs are stored from newest to oldest, which aligns with most queries.

The `columnar` index mode doesn't enable index sorting by default. Let's say you collect logs from different agents, then sorting by agent id is a good choice:

```console
PUT my-index
{
  "settings": {
    "mode": "columnar",
    "sort.field": [ "agent.id", "@timestamp" ],
    "sort.order": [ "asc", "desc" ]
  }
}
```

If you query latency is more important than storage efficiency, then only sorting by `@timestamp` is a good choice:

```console
PUT my-logs-index
{
  "settings": {
    "mode": "logsdb_columnar",
    "sort.field": [ "@timestamp" ],
    "sort.order": [ "desc" ]
  }
}
```

## Dynamic mappings [dynamic-mappings]

With the columnar modes, fields that are unmapped will continue to be mapped via dynamic mappings:
* Whole numbers are mapped as a long field type.
* Decimal numbers are mapped as a double type.
* Strings are mapped as a keyword field type.
* Objects/arrays are mapped as one or more leaf fields (depending on the number of unmapped field paths). Mappings are flattened in columnar mode and that means that unmapped object will be flattened too. Each field path in the unmapped object will be mapped as a separate leaf field. No object fields will not be mapped.

By default, for all unmapped fields doc values will be enabled. However unmapped fields will not indexed by default. This follows the design of storing each field once as doc values.

The default unmapped field experience is provided by dynamic mappings. Dynamic mappings are controlled via dynamic mapping configuration and can be set to:
- **true** (default): Enables the dynamic mapping behaviour as is described above.
- **false**: Doesn't automatically map unmapped fields. Meaning that unmapped fields will not be stored at all. Data in unmapped fields will be lost.
- **strict**: Doesn't automatically map unmapped fields and fails write requests with documents that contain unmapped fields.
- **runtime**: This is not supported in columnar mode.

## Auto flattening [auto-flattening]

Mappings are always flattened. When defining mappings, any object field mapper is removed and leaf field mappings are created for each field path.
The same applied with dynamic mapping updates during indexing.

If `object` field mapping are flattened, the `enabled` and `dynamic` settings are preserved.
For `passthrough` field mappings, the `priority` setting is preserved.

For example, given a mapping with an `attributes` object (`dynamic: false`) and a `labels` passthrough field (`priority: 10`):

```console
PUT my-index
{
  "settings": {
    "mode": "columnar"
  },
  "mappings": {
    "properties": {
      "attributes": {
        "type": "object",
        "dynamic": false,
        "properties": {
          "host": { "type": "keyword" },
          "ip":   { "type": "ip" }
        }
      },
      "labels": {
        "type": "passthrough",
        "priority": 10,
        "properties": {
          "env": { "type": "keyword" }
        }
      }
    }
  }
}
```

The processed mapping (`GET my-index/_mapping`) shows the object mappers removed and their settings captured under `prefix_properties`:

```json
{
  "my-index": {
    "mappings": {
      "prefix_properties": {
        "attributes": {
          "dynamic": "false"
        },
        "labels": {
          "passthrough": 10
        }
      },
      "properties": {
        "attributes.host": { "type": "keyword" },
        "attributes.ip":   { "type": "ip" },
        "labels.env":      { "type": "keyword" }
      }
    }
  }
}
```

The `attributes` and `labels` object mappers are gone from `properties`; only the flat dotted-path leaf fields remain. The `dynamic: false` from `attributes` is preserved under `prefix_properties.attributes.dynamic`, preventing new fields under `attributes.*` from being auto-mapped at index time. The `priority: 10` from `labels` is preserved under `prefix_properties.labels.passthrough`.

## Limitations [columnar-limitations]

The following features are not supported in columnar index modes:

- **nested field type**: The nested field type is supported in a limited fashion in the columnar index modes. Nesting of nested field types is not supported.
- **Mapping-level runtime fields**: Defining runtime fields in the index mappings is rejected. Runtime fields can still be defined on individual search requests.
- **Disabling doc values**: Mapped fields cannot disable doc values. Setting `doc_values` to `false` in mappings will be ignored.
- **Field type without doc values support**: Field types that don't support doc values are not supported in columnar mode (e.g. `search_as_you_type`).
- **Disabling `_source`**: Setting `"_source": {"enabled": false}` is not allowed.
- **Stored source mode**: The traditional `stored` source mode is not supported; only `synthetic` and `columnar_stored` are available.
- **Default query fields**: The `index.query.default_field` index setting in columnar mode will by default only include fields that are indexed (by default text based fields are indexed).
