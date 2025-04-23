---
navigation_title: "Metadata fields"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/esql-metadata-fields.html
---

# {{esql}} metadata fields [esql-metadata-fields]


{{esql}} can access [metadata fields](/reference/elasticsearch/mapping-reference/document-metadata-fields.md). The currently supported ones are:

* [`_index`](/reference/elasticsearch/mapping-reference/mapping-index-field.md): the index to which the document belongs. The field is of the type [keyword](/reference/elasticsearch/mapping-reference/keyword.md).
* [`_id`](/reference/elasticsearch/mapping-reference/mapping-id-field.md): the source document’s ID. The field is of the type [keyword](/reference/elasticsearch/mapping-reference/keyword.md).
* `_version`: the source document’s version. The field is of the type [long](/reference/elasticsearch/mapping-reference/number.md).
* [`_ignored`](/reference/elasticsearch/mapping-reference/mapping-ignored-field.md): the ignored source document fields. The field is of the type [keyword](/reference/elasticsearch/mapping-reference/keyword.md).
* `_score`: when enabled, the final score assigned to each row matching an ES|QL query. Scoring will be updated when using [full text search functions](/reference/query-languages/esql/functions-operators/search-functions.md).

To enable the access to these fields, the [`FROM`](/reference/query-languages/esql/commands/source-commands.md#esql-from) source command needs to be provided with a dedicated directive:

```esql
FROM index METADATA _index, _id
```

Metadata fields are only available if the source of the data is an index. Consequently, `FROM` is the only source commands that supports the `METADATA` directive.

Once enabled, these fields will be available to subsequent processing commands, just like other index fields:

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

Similar to index fields, once an aggregation is performed, a metadata field will no longer be accessible to subsequent commands, unless used as a grouping field:

```esql
FROM employees METADATA _index, _id
| STATS max = MAX(emp_no) BY _index
```

| max:integer | _index:keyword |
| --- | --- |
| 10100 | employees |

