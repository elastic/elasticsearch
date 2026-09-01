---
navigation_title: Columnar
applies_to:
  stack: preview 9.5
  serverless: preview
---

# Columnar index mode [columnar-index-mode]

When you turn on columnar mode, {{es}} becomes a full **analytical and search columnar store**.
For a conceptual introduction to when and why to use it, go to [Columnar index mode](docs-content://manage-data/data-store/columnar.md). This page describes how to turn it on and configure index sorting and provides more details about`_source` modes and limitations.

You activate a set of changes that collectively align the {{es}} storage model with dedicated columnar stores:

- Fields are stored **once, as doc values only**. Non-text fields are not indexed by default, eliminating the storage cost of maintaining redundant index structures. Text fields remain indexed by default to support full-text search.
- For non-indexed fields, [doc values skippers](/reference/elasticsearch/mapping-reference/doc-values.md#doc-values-skippers) are enabled by default. Doc values skippers are compact skip lists with metadata (for example, minimum and maximum values) to avoid scanning large blocks of data when executing a query.
- Mappings are always flat, and object and passthrough fields in mappings are always auto-flattened. Nested fields are not auto flattened.
- Depending on your license, you can choose between two [`_source` modes](#columnar-source). If using synthetic source, a flattened or columnar representation of the source is generated automatically when it's requested at query time. Alternatively, this columnar source can be generated at index time and stored to the disk as doc values.
- New multi-value semantics: the original ordering of multiple values per field per document (for example, in arrays) is preserved by default. Optionally, fields in mappings can be configured to only allow one value per document.
- Fields in mappings can be configured to reject documents that have no value for them.
- Metadata fields like `_routing` and `_id` are stored using doc values as well.
- An optimized doc values format is used by default, further reducing storage footprint, especially when combined with index sorting.

Together with index sorting, columnar mode brings Elasticsearch's storage footprint and columnar access in line with dedicated columnar stores while retaining its full search and aggregation capabilities.

## Columnar index modes [columnar-modes]

Two columnar index modes are available:

`columnar`
:   A general-purpose columnar store with no use-case-specific defaults. Use this mode for bare indices and data streams that do not fit the logging paradigm.

`logsdb_columnar`
:   A columnar store with logging-oriented defaults. It inherits all behavior of the `columnar` mode and additionally:
    - Applies a default mapping that includes a `@timestamp` field (and optionally `host.name`).
    - Enables index sorting if `@timestamp` and `host.name` mappings exist.

    Use this mode for log data.

Both modes are strictly columnar: they reject mapping-level runtime fields and prevent turning off `_source`.

## Turning on columnar mode [enabling-columnar-mode]

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

When you set up columnar index mode, you must determine the index sort fields.
Good index sorting fields facilitate more efficient data storage and improved query response times.
Good index sorting fields are dependent on the use case and the data.

The `logsdb_columnar` index mode, like `logsdb` index mode, uses by default the `host.name` field in ascending order and `@timestamp` field in descending order as the index sort fields.
Hosts typically emit similar logs, so storing log entries from the same host sequentially on disk improves the efficiency of compression techniques such as run-length and delta encoding.
The `@timestamp` field is also used as the index sort field with recent log entries appearing first, improving query performance for queries over recent data.

The `columnar` index mode doesn't enable index sorting by default. If you collect logs from different agents, sorting by agent ID and timestamp might be a good choice:

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

If query latency is more important than storage efficiency, sorting by just `@timestamp` might improve query response times:

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

With the columnar modes, fields that are not referenced in static mappings get dynamically mapped as follows:
* Whole numbers are mapped as a long field type.
* Decimal numbers are mapped as a double type.
* Strings are mapped as a keyword field type.
* Objects and arrays are mapped as one or more leaf fields (depending on the number of unmapped field paths). Mappings are flattened in columnar mode and that applies to unmapped objects too. Each leaf field under the unmapped object will be mapped as a separate leaf field. No object fields are added to the mappings.

Dynamically mapped fields are configured with doc values turned on and indexes turned off by default, in line with the columnar premise of storing each field once using doc values.

Dynamic mapping behavior is controlled by the [`dynamic`](/reference/elasticsearch/mapping-reference/dynamic.md) configuration parameter, which can be set to:
- `true` (default): Turns on the dynamic mapping behavior as described in the preceding paragraphs.
- `false`: Unmapped fields are not mapped or stored. Data in unmapped fields are lost.
- `strict`: Documents containing unmapped fields don't get indexed, raising indexing errors instead.

Note that the `runtime` option is not supported in columnar mode.

:::{warning}

If you configure `"dynamic": false`, then only fields that are explicitly mapped in the mappings are stored.
Fields that are not explicitly mapped in the mappings are not stored and therefore lost.
:::

## Auto flattening [auto-flattening]

If you use columnar mode, mappings are always flattened. When you define mappings, object and passthrough field mappers are removed and leaf field mappings are created for each field path.
The same applies to dynamic mapping updates during indexing.

During `object` flattening, the `enabled` and `dynamic` settings are preserved and separately tracked.
Same applies to `passthrough` fields, along with their `priority` setting.

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

The processed mapping shows the object mappers removed and their settings captured under `prefix_properties`. For example, `GET my-index/_mapping` returns:

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

## Columnar `_source` [columnar-source]

Columnar index modes don't store the original JSON `_source` on disk. Two `_source` modes are supported:

**Synthetic columnar `_source`**
:   Reconstructs a flattened representation of `_source` from doc values at query time. Requires an [appropriate license](https://www.elastic.co/subscriptions) for synthetic columnar `_source`. For more information, refer to [Synthetic `_source`](/reference/elasticsearch/mapping-reference/mapping-source-field.md#synthetic-source).

**Columnar stored `_source`**
:   Materializes and stores the columnar `_source` representation on disk at index time as doc values. Used automatically when synthetic columnar `_source` is not licensed, and can also be configured explicitly to speed up `_source` retrieval. For more information, see [Columnar source](/reference/elasticsearch/mapping-reference/mapping-source-field.md#columnar-stored).

## Limitations [columnar-limitations]

The following features are not supported in columnar index modes:

- **Nested field type**: The [nested field type](/reference/elasticsearch/mapping-reference/nested.md) is supported in a limited fashion in the columnar index modes. Nesting of nested field types is not supported.
- **Mapping-level runtime fields**: Defining runtime fields in the index mappings is rejected. Runtime fields can still be defined on individual search requests.
- **Turning off doc values**: Mapped fields cannot turn off doc values. Setting `doc_values` to `false` leads to mapping errors. The only exception is multi-fields, where it's often desirable to store doc values for just one and use different index configurations for the rest.
- **Incompatible field types**: Field types that don't support doc values are not supported in columnar mode (for example, `search_as_you_type`).
- **Turning off `_source`**: Setting `"_source": {"enabled": false}` is not allowed.
- **Stored source mode**: The traditional `stored` source mode is not supported; only synthetic columnar and columnar stored modes are available. See [Columnar `_source`](#columnar-source).
- **`dynamic: false` and `enabled: false`** are lossy: Setting `dynamic: false` on an object prevents unmapped sub-fields from being stored; their data is permanently lost. Setting `enabled: false` ignores the entire object subtree; its data is permanently lost.
- **Default query fields**: The `index.query.default_field` index setting in columnar mode will by default only include fields that are indexed (by default text based fields are indexed).
