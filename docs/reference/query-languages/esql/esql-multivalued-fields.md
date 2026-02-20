---
applies_to:
  stack:
  serverless:
navigation_title: "Multivalued fields"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/esql-multivalued-fields.html
---

# {{esql}} multivalued fields [esql-multivalued-fields]

{{esql}} can read from multivalued fields:

$$$esql-multivalued-fields-reorders$$$

```console
POST /mv/_bulk?refresh
{ "index" : {} }
{ "a": 1, "b": [2, 1] }
{ "index" : {} }
{ "a": 2, "b": 3 }

POST /_query
{
  "query": "FROM mv | LIMIT 2"
}
```

Multivalued fields come back as a JSON array:

```console-result
{
  "took": 28,
  "is_partial": false,
  "columns": [
    { "name": "a", "type": "long"},
    { "name": "b", "type": "long"}
  ],
  "values": [
    [1, [1, 2]],
    [2,      3]
  ]
}
```
% TESTRESPONSE[s/"took": 28/"took": "$body.took"/]

The relative order of values in a multivalued field is undefined. They’ll frequently be in ascending order but don’t rely on that.


## Duplicate values [esql-multivalued-fields-dups]

Some field types, like [`keyword`](/reference/elasticsearch/mapping-reference/keyword.md#keyword-field-type) remove duplicate values on write:

$$$esql-multivalued-fields-kwdups$$$

```console
PUT /mv
{
  "mappings": {
    "properties": {
      "b": {"type": "keyword"}
    }
  }
}

POST /mv/_bulk?refresh
{ "index" : {} }
{ "a": 1, "b": ["foo", "foo", "bar"] }
{ "index" : {} }
{ "a": 2, "b": ["bar", "bar"] }

POST /_query
{
  "query": "FROM mv | LIMIT 2"
}
```

And {{esql}} sees that removal:

```console-result
{
  "took": 28,
  "is_partial": false,
  "columns": [
    { "name": "a", "type": "long"},
    { "name": "b", "type": "keyword"}
  ],
  "values": [
    [1, ["bar", "foo"]],
    [2,          "bar"]
  ]
}
```
% TESTRESPONSE[s/"took": 28/"took": "$body.took"/]

But other types, like `long` don’t remove duplicates.

$$$esql-multivalued-fields-longdups$$$

```console
PUT /mv
{
  "mappings": {
    "properties": {
      "b": {"type": "long"}
    }
  }
}

POST /mv/_bulk?refresh
{ "index" : {} }
{ "a": 1, "b": [2, 2, 1] }
{ "index" : {} }
{ "a": 2, "b": [1, 1] }

POST /_query
{
  "query": "FROM mv | LIMIT 2"
}
```

And {{esql}} also sees that:

```console-result
{
  "took": 28,
  "is_partial": false,
  "columns": [
    { "name": "a", "type": "long"},
    { "name": "b", "type": "long"}
  ],
  "values": [
    [1, [1, 2, 2]],
    [2,    [1, 1]]
  ]
}
```
% TESTRESPONSE[s/"took": 28/"took": "$body.took"/]

This is all at the storage layer. If you store duplicate `long`s and then convert them to strings the duplicates will stay:

$$$esql-multivalued-fields-longdups-tostring$$$

```console
PUT /mv
{
  "mappings": {
    "properties": {
      "b": {"type": "long"}
    }
  }
}

POST /mv/_bulk?refresh
{ "index" : {} }
{ "a": 1, "b": [2, 2, 1] }
{ "index" : {} }
{ "a": 2, "b": [1, 1] }

POST /_query
{
  "query": "FROM mv | EVAL b=TO_STRING(b) | LIMIT 2"
}
```

```console-result
{
  "took": 28,
  "is_partial": false,
  "columns": [
    { "name": "a", "type": "long"},
    { "name": "b", "type": "keyword"}
  ],
  "values": [
    [1, ["1", "2", "2"]],
    [2,      ["1", "1"]]
  ]
}
```
% TESTRESPONSE[s/"took": 28/"took": "$body.took"/]


## `null` in a list [esql-multivalued-nulls]

`null` values in a list are not preserved at the storage layer:

$$$esql-multivalued-fields-multivalued-nulls$$$

```console
POST /mv/_doc?refresh
{ "a": [2, null, 1] }

POST /_query
{
  "query": "FROM mv | LIMIT 1"
}
```

```console-result
{
  "took": 28,
  "is_partial": false,
  "columns": [
    { "name": "a", "type": "long"}
  ],
  "values": [
    [[1, 2]]
  ]
}
```
% TESTRESPONSE[s/"took": 28/"took": "$body.took"/]

## Functions [esql-multivalued-fields-functions]

Unless otherwise documented functions will return `null` when applied to a multivalued field.

$$$esql-multivalued-fields-mv-into-null$$$

```console
POST /mv/_bulk?refresh
{ "index" : {} }
{ "a": 1, "b": [2, 1] }
{ "index" : {} }
{ "a": 2, "b": 3 }
```

```console
POST /_query
{
  "query": "FROM mv | EVAL b + 2, a + b | LIMIT 4"
}
```
% TEST[continued]
% TEST[warning:Line 1:16: evaluation of [b + 2] failed, treating result as null. Only first 20 failures recorded.]
% TEST[warning:Line 1:16: java.lang.IllegalArgumentException: single-value function encountered multi-value]
% TEST[warning:Line 1:23: evaluation of [a + b] failed, treating result as null. Only first 20 failures recorded.]
% TEST[warning:Line 1:23: java.lang.IllegalArgumentException: single-value function encountered multi-value]

```console-result
{
  "took": 28,
  "is_partial": false,
  "columns": [
    { "name": "a",   "type": "long"},
    { "name": "b",   "type": "long"},
    { "name": "b + 2", "type": "long"},
    { "name": "a + b", "type": "long"}
  ],
  "values": [
    [1, [1, 2], null, null],
    [2,      3,    5,    5]
  ]
}
```
% TESTRESPONSE[s/"took": 28/"took": "$body.took"/]

Work around this limitation by converting the field to single value with one of:

* [`MV_AVG`](/reference/query-languages/esql/functions-operators/mv-functions.md#esql-mv_avg)
* [`MV_CONCAT`](/reference/query-languages/esql/functions-operators/mv-functions.md#esql-mv_concat)
* [`MV_COUNT`](/reference/query-languages/esql/functions-operators/mv-functions.md#esql-mv_count)
* [`MV_MAX`](/reference/query-languages/esql/functions-operators/mv-functions.md#esql-mv_max)
* [`MV_MEDIAN`](/reference/query-languages/esql/functions-operators/mv-functions.md#esql-mv_median)
* [`MV_MIN`](/reference/query-languages/esql/functions-operators/mv-functions.md#esql-mv_min)
* [`MV_SUM`](/reference/query-languages/esql/functions-operators/mv-functions.md#esql-mv_sum)

To filter on individual values in a multivalued field (for example, to keep only rows where the field contains a given value), use the [`MV_EXPAND`](/reference/query-languages/esql/commands/mv_expand.md) command to expand the field into one row per value, then apply `WHERE` to the expanded column.

```console
POST /_query
{
  "query": "FROM mv | EVAL b=MV_MIN(b) | EVAL b + 2, a + b | LIMIT 4"
}
```
% TEST[continued]

```console-result
{
  "took": 28,
  "is_partial": false,
  "columns": [
    { "name": "a",   "type": "long"},
    { "name": "b",   "type": "long"},
    { "name": "b + 2", "type": "long"},
    { "name": "a + b", "type": "long"}
  ],
  "values": [
    [1, 1, 3, 2],
    [2, 3, 5, 5]
  ]
}
```
% TESTRESPONSE[s/"took": 28/"took": "$body.took"/]

## Filter pushdown may miss warnings [esql-multivalued-fields-filter-pushdown]

Touching a multivalued field with a function that only supports single valued
fields will normally produce a warning that looks like
`evaluation of [b > 1] failed, treating result as null`. You'll get it
consistently for queries like this:

```console
POST /_query
{
  "query": "FROM mv | EVAL gt_1 = b > 1 | LIMIT 4"
}
```
% TEST[continued]
% TEST[warning:Line 1:23: evaluation of [b > 1] failed, treating result as null. Only first 20 failures recorded.]
% TEST[warning:Line 1:23: java.lang.IllegalArgumentException: single-value function encountered multi-value]

When a filter can be evaluated using the search index, {{esql}} might miss the
warning for documents that contain a multivalued field where **none** of the values
match the filter. For example:

```console
POST /mv/_bulk?refresh
{ "index" : {} }
{ "a": 1, "b": [2, 1] }
{ "index" : {} }
{ "a": 2, "b": 3 }

POST /_query
{
  "query": "FROM mv | WHERE b > 2 | LIMIT 4"
}
```
