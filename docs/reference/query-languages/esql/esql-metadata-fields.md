---
navigation_title: "Metadata fields"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/esql-metadata-fields.html
---

# {{esql}} metadata fields [esql-metadata-fields]

{{esql}} can access [metadata fields](/reference/elasticsearch/mapping-reference/document-metadata-fields.md).

To access these fields, use the `METADATA` directive with the [`FROM`](/reference/query-languages/esql/commands/source-commands.md#esql-from) source command. For example:

```esql
FROM index METADATA _index, _id
```

## Available metadata fields

The following metadata fields are available in {{esql}}:

| Metadata field | Type | Description |
|---------------|------|-------------|
| [`_id`](/reference/elasticsearch/mapping-reference/mapping-id-field.md) | [keyword](/reference/elasticsearch/mapping-reference/keyword.md) | Unique document ID. |
| [`_ignored`](/reference/elasticsearch/mapping-reference/mapping-ignored-field.md) | [keyword](/reference/elasticsearch/mapping-reference/keyword.md) | Names every field in a document that was ignored when the document was indexed. |
| [`_index`](/reference/elasticsearch/mapping-reference/mapping-index-field.md) | [keyword](/reference/elasticsearch/mapping-reference/keyword.md) | Index name. |
| `_index_mode` | [keyword](/reference/elasticsearch/mapping-reference/keyword.md) | [Index mode](/reference/elasticsearch/index-settings/index-modules.md#index-mode-setting). For example: `standard`, `lookup`, or `logsdb`. |
| `_score` | [`float`](/reference/elasticsearch/mapping-reference/number.md) | Query relevance score (when enabled). Scores are updated when using [full text search functions](/reference/query-languages/esql/functions-operators/search-functions.md). |
| [`_source`](/reference/elasticsearch/mapping-reference/mapping-source-field.md) | Special `_source` type | Original JSON document body passed at index time (or a reconstructed version if [synthetic `_source`](/reference/elasticsearch/mapping-reference/mapping-source-field.md#synthetic-source) is enabled). This field is not supported by functions. |
| `_version` | [`long`](/reference/elasticsearch/mapping-reference/number.md) | Document version number |

## Usage and limitations

- Metadata fields are only available when the data source is an index
- The `_source` field is not supported by functions
- Only the `FROM` command supports the `METADATA` directive
- Once enabled, metadata fields work like regular index fields
- Fields are dropped after aggregations unless used for grouping ([example](#group-results-by-metadata-fields))

## Examples

### Basic metadata usage

Once enabled, metadata fields are available to subsequent processing commands, just like other index fields:

```esql
FROM ul_logs, apps METADATA _index, _version
| WHERE id IN (13, 14) AND _version == 1
| EVAL key = CONCAT(_index, "_", TO_STR(id))
| SORT id, _index
| KEEP id, _index, _version, key
```

| id:long | _index:keyword | _version:long | key:keyword |
| --- | --- | --- | --- |
| 13 | apps | 1 | apps_13 |
| 13 | ul_logs | 1 | ul_logs_13 |
| 14 | apps | 1 | apps_14 |
| 14 | ul_logs | 1 | ul_logs_14 |

### Metadata fields and aggregations

Similar to index fields, once an aggregation is performed, a metadata field will no longer be accessible to subsequent commands, unless used as a grouping field:

```esql
FROM employees METADATA _index, _id
| STATS max = MAX(emp_no) BY _index
```

| max:integer | _index:keyword |
| --- | --- |
| 10100 | employees |

### Sort results by search score

```esql
FROM products METADATA _score
| WHERE MATCH(description, "wireless headphones")
| SORT _score DESC
| KEEP name, description, _score
```

:::{tip}
Refer to [{{esql}} for search](docs-content://solutions/search/esql-for-search.md#esql-for-search-scoring) for more information on relevance scoring and how to use `_score` in your queries.
:::