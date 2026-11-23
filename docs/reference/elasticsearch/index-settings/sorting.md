---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/index-modules-index-sorting.html
navigation_title: Sorting
applies_to:
  stack: all
  serverless: all
---

# Index sorting settings [index-modules-index-sorting]

When creating a new index in Elasticsearch it is possible to configure how the segments inside each shard will be sorted. By default Lucene does not apply any sort. The `index.sort.*` settings define which fields should be used to sort the documents inside each Segment.

::::{warning}
It is allowed to apply index sorting to mappings with nested objects, so long as the `index.sort.*` setting contains no nested fields.
::::


For instance the following example shows how to define a sort on a single field:

```console
PUT my-index-000001
{
  "settings": {
    "index": {
      "sort.field": "date", <1>
      "sort.order": "desc"  <2>
    }
  },
  "mappings": {
    "properties": {
      "date": {
        "type": "date"
      }
    }
  }
}
```

1. This index is sorted by the `date` field
2. In descending order.


It is also possible to sort the index by more than one field:

```console
PUT my-index-000001
{
  "settings": {
    "index": {
      "sort.field": [ "username", "date" ], <1>
      "sort.order": [ "asc", "desc" ]       <2>
    }
  },
  "mappings": {
    "properties": {
      "username": {
        "type": "keyword",
        "doc_values": true
      },
      "date": {
        "type": "date"
      }
    }
  }
}
```

1. This index is sorted by `username` first then by `date`
2. In ascending order for the `username` field and in descending order for the `date` field.


{applies_to}`stack: ga 9.7` Instead of the separate `index.sort.*` settings, you can configure the whole sort with the single `index.sort` expression:

```console
PUT my-index-000001
{
  "settings": {
    "index": {
      "sort": "username, MV_MAX(date) DESC NULLS LAST" <1>
    }
  },
  "mappings": {
    "properties": {
      "username": {
        "type": "keyword",
        "doc_values": true
      },
      "date": {
        "type": "date"
      }
    }
  }
}
```

1. Sorts by `username` ascending, then by the highest `date` value descending, with documents that have no `date` last.


Index sorting supports the following settings:

$$$index-sort-expression$$$
`index.sort` {applies_to}`stack: ga 9.7`
:   A comma-separated list of sort keys that configures the whole index sort, using the [ES|QL `SORT` syntax](/reference/query-languages/esql/commands/sort.md). Each entry is a key, optionally followed by a sort order and the placement of documents without a value:

    * `field`, `MV_MIN(field)` or `MV_MAX(field)`: The field to sort by. For a multi-valued field, `MV_MIN` and `MV_MAX` sort by its lowest or highest value. Defaults to `MV_MIN` when ascending and `MV_MAX` when descending.
    * Any other ES|QL expression, for example `DATE_TRUNC(1 hour, @timestamp)`: A key computed from the doc values of the fields it reads each time a segment is sorted, and never stored. It can use arithmetic, comparison and logical operators, and functions whose results are the same on every node and version, such as `CASE`, `COALESCE`, `DATE_TRUNC`, `DATE_EXTRACT`, `ABS`, `FLOOR`, `MV_MAX` and `LENGTH`. Functions that may compute different values on another JVM for some of their inputs, such as `SIN`, `ROUND`, `TO_LOWER` and the type conversions, are rejected when the index is created. It can read numeric, `date`, `date_nanos`, `boolean`, `keyword`, `ip` and `version` fields with doc values, and must return a `long`, `integer`, `double`, `boolean`, `date` or `date_nanos` value. Documents for which it returns no value or several values sort like documents without a value.
    * `ASC` or `DESC`: The sort order. Defaults to `ASC`.
    * `NULLS FIRST` or `NULLS LAST`: Where documents without a value are sorted. Like in ES|QL, a missing value counts as larger than any value, so this defaults to `NULLS LAST` for `ASC` and `NULLS FIRST` for `DESC`.

    Keywords and function names are case-insensitive. Unquoted field names follow the ES|QL identifier rules: dot-separated parts that start with a letter, `_` or `@` and contain only letters, digits and `_`, and that aren't a reserved word such as `null` or `asc`. Any other field name, for example one with a hyphen, whitespace or a comma, must be quoted with backticks, doubling any backtick in the name.

    The `NULLS` default for `DESC` differs from `index.sort.missing`, which defaults to `_last` for either order. It matches what an ES|QL `SORT field DESC` asks for, so the index sort and the query agree. To keep documents without a value last, write `NULLS LAST`.

    `index.sort` is parsed by ES|QL, so it needs the ES|QL plugin, which the default distribution includes. The `index.sort.*` settings don't.

    The expression is stored as written, and the index is sorted from it directly. The `index.sort.*` settings stay unset and default to empty, not to the index mode defaults or the values from the expression. `index.sort` can't be combined with any of the `index.sort.*` settings.

$$$index-sort-field$$$
`index.sort.field`
:   The list of fields used to sort the index. Only `boolean`, `numeric`, `date` and `keyword` fields with `doc_values` are allowed here.

$$$index-sort-order$$$
`index.sort.order`
:   The sort order to use for each field. The order option can have the following values:

    * `asc`:  For ascending order.
    * `desc`: For descending order.

    When `index.sort.field` is set but `index.sort.order` is not explicitly provided, each field defaults to `asc`.

$$$index-sort-mode$$$
`index.sort.mode`
:   Elasticsearch supports sorting by multi-valued fields. The mode option controls what value is picked to sort the document. The mode option can have the following values:

    * `min`: 	Pick the lowest value.
    * `max`: 	Pick the highest value.

    When `index.sort.order` is set to `asc`, `index.sort.mode` defaults to `min`. If you set the order to `desc`, the mode defaults to `max`.


$$$index-sort-missing$$$
`index.sort.missing`
:   The missing parameter specifies how docs which are missing the field should be treated. The missing value can have the following values:

    * `_last`: Documents without value for the field are sorted last.
    * `_first`: Documents without value for the field are sorted first.

    Documents missing the sort field are always sorted last by default, regardless of sort order.


::::{warning}
Index sorting can be defined only once at index creation. It is not allowed to add or update a sort on an existing index. Index sorting also has a cost in terms of indexing throughput since documents must be sorted at flush and merge time. You should test the impact on your application before activating this feature.
::::



## Early termination of search request [early-terminate]

By default in Elasticsearch a search request must visit every document that matches a query to retrieve the top documents sorted by a specified sort. Though when the index sort and the search sort are the same it is possible to limit the number of documents that should be visited per segment to retrieve the N top ranked documents globally. For example, let’s say we have an index that contains events sorted by a timestamp field:

```console
PUT events
{
  "settings": {
    "index": {
      "sort.field": "timestamp",
      "sort.order": "desc" <1>
    }
  },
  "mappings": {
    "properties": {
      "timestamp": {
        "type": "date"
      }
    }
  }
}
```

1. This index is sorted by timestamp in descending order (most recent first)


You can search for the last 10 events with:

```console
GET /events/_search
{
  "size": 10,
  "sort": [
    { "timestamp": "desc" }
  ]
}
```
% TEST[continued]

Elasticsearch will detect that the top docs of each segment are already sorted in the index and will only compare the first N documents per segment. The rest of the documents matching the query are collected to count the total number of results and to build aggregations.

If you’re only looking for the last 10 events and have no interest in the total number of documents that match the query you can set `track_total_hits` to false:

```console
GET /events/_search
{
  "size": 10,
  "sort": [ <1>
      { "timestamp": "desc" }
  ],
  "track_total_hits": false
}
```
% TEST[continued]

1. The index sort will be used to rank the top documents and each segment will early terminate the collection after the first 10 matches.


This time, Elasticsearch will not try to count the number of documents and will be able to terminate the query as soon as N documents have been collected per segment.

```console-result
{
  "_shards": ...
   "hits" : {  <1>
      "max_score" : null,
      "hits" : []
  },
  "took": 20,
  "timed_out": false
}
```
% TESTRESPONSE[s/"_shards": \.\.\./"_shards": "$body._shards",/]
% TESTRESPONSE[s/"took": 20,/"took": "$body.took",/]

1. The total number of hits matching the query is unknown because of early termination.


::::{note}
Aggregations will collect all documents that match the query regardless of the value of `track_total_hits`
::::



