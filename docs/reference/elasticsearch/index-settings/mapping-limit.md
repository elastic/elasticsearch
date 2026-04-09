---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-settings-limit.html
navigation_title: Mapping limit
applies_to:
  stack: all
  serverless: all
---

# Mapping limit settings [mapping-settings-limit]

:::{include} _snippets/serverless-availability.md
:::

Use the following settings to limit the number of field mappings (created manually or dynamically) and prevent documents from causing a mapping explosion:

$$$total-fields-limit$$$
`index.mapping.total_fields.limit` {applies_to}`serverless: all`
:   The maximum number of fields in an index. Field and object mappings, as well as field aliases count towards this limit. Mapped runtime fields count towards this limit as well. The default value is `1000`.

    ::::{important}
    The limit is in place to prevent mappings and searches from becoming too large. Higher values can lead to performance degradations and memory issues, especially in clusters with a high load or few resources.

    If you increase this setting, we recommend you also increase the [`indices.query.bool.max_clause_count`](/reference/elasticsearch/configuration-reference/search-settings.md) setting, which limits the maximum number of clauses in a query.

    ::::


    ::::{tip}
    If your field mappings contain a large, arbitrary set of keys, consider using the [flattened](/reference/elasticsearch/mapping-reference/flattened.md) data type, or setting the index setting `index.mapping.total_fields.ignore_dynamic_beyond_limit` to `true`.

    ::::

### How fields are counted [mapping-fields-counting]

The `index.mapping.total_fields.limit` setting counts **all mappers**, not just leaf fields. This includes:

- **Object mappers**: Each level in a dotted field path counts separately
- **Field mappers**: Leaf fields like `keyword`, `text`, `long`, etc.
- **Multi-fields**: Each sub-field (e.g., `.keyword`, `.raw`) counts individually
- **Field aliases**: Each alias counts as one mapper
- **Runtime fields**: Each runtime field counts towards the limit

Metadata fields (`_id`, `_source`, `_routing`, etc.) do **not** count towards this limit.

#### Example: Counting a nested field path

For a field path like `host.os.name`, Elasticsearch creates three mappers:

- `host` (object mapper)
- `host.os` (object mapper)
- `host.os.name` (field mapper)

A common mistake is counting only leaf fields (fields with a `type` property). This undercounts because object mappers have `properties` instead of `type`.

#### Example: Full mapping count

```json
{
  "mappings": {
    "properties": {
      "host": {
        "properties": {
          "name": { "type": "keyword" }
        }
      },
      "message": {
        "type": "text",
        "fields": {
          "keyword": { "type": "keyword" }
        }
      }
    }
  }
}
```

This mapping counts as **4 mappers**:

- `host` (object mapper)
- `host.name` (field mapper)
- `message` (field mapper)
- `message.keyword` (multi-field)

#### Example: Multi-fields

Each multi-field counts separately. A text field with multiple sub-fields can quickly add up:

```json
{
  "mappings": {
    "properties": {
      "title": {
        "type": "text",
        "fields": {
          "keyword": { "type": "keyword" },
          "raw": { "type": "keyword", "index": false }
        }
      }
    }
  }
}
```

This mapping counts as **3 mappers**:

- `title` (field mapper)
- `title.keyword` (multi-field)
- `title.raw` (multi-field)

#### Example: Runtime fields

Runtime fields also count towards the limit:

```json
{
  "mappings": {
    "properties": {
      "timestamp": { "type": "date" }
    },
    "runtime": {
      "day_of_week": {
        "type": "keyword",
        "script": {
          "source": "emit(doc['timestamp'].value.dayOfWeekEnum.getDisplayName(TextStyle.FULL, Locale.ROOT))"
        }
      }
    }
  }
}
```

This mapping counts as **2 mappers**:

- `timestamp` (field mapper)
- `day_of_week` (runtime field)

#### Example: Field aliases

Field aliases count towards the limit:

```json
{
  "mappings": {
    "properties": {
      "user_id": { "type": "keyword" },
      "user": {
        "type": "alias",
        "path": "user_id"
      }
    }
  }
}
```

This mapping counts as **2 mappers**:

- `user_id` (field mapper)
- `user` (field alias)

### Reducing field count with `subobjects: false` [reducing-field-count]

```{applies_to}
stack: ga 8.3
```

The [`subobjects`](/reference/elasticsearch/mapping-reference/subobjects.md) setting prevents the creation of intermediate object mappers. With `subobjects: false`, dotted field names are stored as literal strings rather than creating nested objects.

```json
{
  "mappings": {
    "subobjects": false,
    "properties": {
      "host.os.name": { "type": "keyword" }
    }
  }
}
```

This creates only **1 mapper** instead of 3, because `host.os.name` is treated as a literal field name rather than a nested path.

For indices with deeply nested fields (such as ECS-style mappings), using `subobjects: false` can significantly reduce the mapper count.

$$$ignore-dynamic-beyond-limit$$$
`index.mapping.total_fields.ignore_dynamic_beyond_limit` {applies_to}`serverless: all`
:   This setting determines what happens when a dynamically mapped field would exceed the total fields limit. When set to `false` (the default), the index request of the document that tries to add a dynamic field to the mapping will fail with the message `Limit of total fields [X] has been exceeded`. When set to `true`, the index request will not fail. Instead, fields that would exceed the limit are not added to the mapping, similar to [`dynamic: false`](/reference/elasticsearch/mapping-reference/dynamic.md). The fields that were not added to the mapping will be added to the [`_ignored` field](/reference/elasticsearch/mapping-reference/mapping-ignored-field.md). The default value is `false`.

`index.mapping.depth.limit`
:   The maximum depth for a field, which is measured as the number of inner objects. For instance, if all fields are defined at the root object level, then the depth is `1`. If there is one object mapping, then the depth is `2`, etc. Default is `20`.

`index.mapping.nested_fields.limit`
:   The maximum number of distinct `nested` mappings in an index. The `nested` type should only be used in special cases, when arrays of objects need to be queried independently of each other. To safeguard against poorly designed mappings, this setting limits the number of unique `nested` types per index. Default is `100`.

`index.mapping.nested_parents.limit`
:   The maximum number of nested fields that act as parents of other nested fields. Each nested parent requires its own in-memory parent bitset. Root-level nested fields share a parent bitset, but nested fields under other nested fields require additional bitsets. This setting limits the number of unique nested parents to prevent excessive memory usage. Default is `50`.

`index.mapping.nested_objects.limit`
:   The maximum number of nested JSON objects that a single document can contain across all `nested` types. This limit helps to prevent out of memory errors when a document contains too many nested objects. Default is `10000`.

`index.mapping.field_name_length.limit`
:   Setting for the maximum length of a field name. This setting isn’t really something that addresses mappings explosion but might still be useful if you want to limit the field length. It usually shouldn’t be necessary to set this setting. The default is okay unless a user starts to add a huge number of fields with really long names. Default is `Long.MAX_VALUE` (no limit).

`index.mapping.field_name_length.ignore_dynamic_beyond_limit` {applies_to}`stack: ga 9.3`
:   This setting determines what happens when a the name of a dynamically mapped field would exceed the configured maximum length. When set to `false` (the default), the index request of the document that tries to add a dynamic field to the mapping will fail with the message `Field name [x] is longer than the limit of [y] characters`. When set to `true`, the index request will not fail. Instead, fields that would exceed the limit are not added to the mapping, similar to [`dynamic: false`](/reference/elasticsearch/mapping-reference/dynamic.md). The fields that were not added to the mapping will be added to the [`_ignored` field](/reference/elasticsearch/mapping-reference/mapping-ignored-field.md). The default value is `false`.

`index.mapping.dimension_fields.limit`
:   (Dynamic, integer) Maximum number of [time series dimensions](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md#time-series-dimension) for the index. Defaults to `32768`.

