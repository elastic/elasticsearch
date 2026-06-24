---
navigation_title: Columnar
applies_to:
  stack: preview
  serverless: preview
---

# Columnar index mode [columnar-index-mode]

With columnar mode enabled, Elasticsearch becomes a full **analytical and search columnar store**. Setting `index.mode` to `columnar` or `logsdb_columnar` activates a set of changes that collectively align Elasticsearch's storage model with dedicated columnar stores:

- Fields are stored **once, as doc values only**. All non-text fields are not indexed by default, eliminating the storage cost of maintaining redundant index structures.
- Mappings are always flat, and object like fields in mappings are always auto-flattened.
- The original source is no longer stored. If source is requested at query time, then a flattened or columnar representation of the source is generated on the fly and returned. Optionally, the columnar source can also be generated at index time and stored to disk as doc values.
- New multi-value semantics: the original ordering of multiple values per field per document (e.g., in arrays) is preserved by default. Optionally, fields in mappings can be configured to only allow one value per document.
- Fields in mappings can be configured to reject documents that have no value for them.
- Metadata fields like `_routing` and `_id` are stored using doc values as well.
- An optimized doc values format is used by default, further reducing storage footprint. especially when combined with index sorting.

These changes bring Elasticsearch's storage footprint and columnar access in line with dedicated columnar stores while retaining its full search and aggregation capabilities.

## Columnar index modes [columnar-modes]

Two columnar index modes are available:

`columnar`
:   A general-purpose columnar store with no use-case-specific defaults. Use this mode for bare indices and data streams that does not fit the logging paradigm.

`logsdb_columnar`
:   A columnar store with logging-oriented defaults. It inherits all behavior of the `columnar` mode and additionally:
    - Applies a default mapping that includes a `@timestamp` field (and optionally `host.name`).
    - Enables sort-field-based routing by default (`index.logsdb.route_on_sort_fields: true`), which routes documents based on the configured index sort fields rather than a routing path.

    Use this mode for log data.

Both modes are strictly columnar: they reject mapping-level runtime fields and prevent disabling `_source`.

## Enabling columnar mode [enabling-columnar-mode]

Set `index.mode` at index creation time. The setting cannot be changed after the index is created.

```console
PUT my-index
{
  "settings": {
    "index": {
      "mode": "columnar"
    }
  }
}
```

For log data, use a component template to enable the `logsdb_columnar` mode for all `logs-*-*` data streams:

```console
PUT _component_template/logs@custom
{
  "template": {
    "settings": {
      "index": {
        "mode": "logsdb_columnar"
      }
    }
  }
}
```

Or set it directly at index creation:

```console
PUT my-logs-index
{
  "settings": {
    "index": {
      "mode": "logsdb_columnar"
    }
  }
}
```

## Limitations [columnar-limitations]

The following features are not supported in columnar index modes:

- **Mapping-level runtime fields**: Defining runtime fields in the index mappings is rejected. Runtime fields can still be defined on individual search requests.
- **Disabling doc values**: Mapped fields cannot disable doc values.
- **Disabling `_source`**: Setting `"_source": {"enabled": false}` is not allowed.
- **Stored source mode**: The traditional `stored` source mode is not supported; only `synthetic` and `columnar_stored` are available.

## Settings reference [columnar-settings-reference]

$$$columnar-use-columnar-id-by-default$$$ `index.mapping.use_columnar_id_mode_by_default`
:   (Static, boolean) Controls whether the columnar `_id` storage mode is used by default. Defaults to `true` for columnar index modes. When `true`, `_id` is stored as sorted doc values with an inverted index and no stored field is written. Can be overridden per-index via the `mode` attribute on the `_id` field mapper.
