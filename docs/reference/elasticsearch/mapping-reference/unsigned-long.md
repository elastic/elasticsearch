---
navigation_title: "Unsigned long"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/unsigned-long.html
---

# Unsigned long field type [unsigned-long]


Unsigned long is a numeric field type that represents an unsigned 64-bit integer with a minimum value of 0 and a maximum value of `2^64 - 1` (from 0 to 18446744073709551615 inclusive).

```console
PUT my_index
{
  "mappings": {
    "properties": {
      "my_counter": {
        "type": "unsigned_long"
      }
    }
  }
}
```

Unsigned long can be indexed in a numeric or string form, representing integer values in the range [0, 18446744073709551615]. They can’t have a decimal part.

```console
POST /my_index/_bulk?refresh
{"index":{"_id":1}}
{"my_counter": 0}
{"index":{"_id":2}}
{"my_counter": 9223372036854775808}
{"index":{"_id":3}}
{"my_counter": 18446744073709551614}
{"index":{"_id":4}}
{"my_counter": 18446744073709551615}
```
% TEST[continued]

Term queries accept any numbers in a numeric or string form.

```console
GET /my_index/_search
{
    "query": {
        "term" : {
            "my_counter" : 18446744073709551615
        }
    }
}
```
% TEST[continued]

Range query terms can contain values with decimal parts. In this case {{es}} converts them to integer values: `gte` and `gt` terms are converted to the nearest integer up inclusive, and `lt` and `lte` ranges are converted to the nearest integer down inclusive.

It is recommended to pass ranges as strings to ensure they are parsed without any loss of precision.

```console
GET /my_index/_search
{
    "query": {
        "range" : {
            "my_counter" : {
                "gte" : "9223372036854775808",
                "lte" : "18446744073709551615"
            }
        }
    }
}
```
% TEST[continued]

## Sort values [_sort_values_2]

For queries with sort on an `unsigned_long` field, for a particular document {{es}} returns a sort value of the type `long` if the value of this document is within the range of long values, or of the type `BigInteger` if the value exceeds this range.

::::{note}
REST clients need to be able to handle big integer values in JSON to support this field type correctly.
::::


```console
GET /my_index/_search
{
    "query": {
        "match_all" : {}
    },
    "sort" : {"my_counter" : "desc"}
}
```
% TEST[continued]


## Stored fields [_stored_fields]

A stored field of `unsigned_long` is stored and returned as `String`.


## Aggregations [_aggregations]

For `terms` aggregations, similarly to sort values, `Long` or `BigInteger` values are used. For other aggregations, values are converted to the `double` type.


## Script values [_script_values]

By default, script values of an `unsigned_long` field are returned as Java signed `Long`, which means that values that are greater than `Long.MAX_VALUE` are shown as negative values. You can use `Long.compareUnsigned(long, long)`, `Long.divideUnsigned(long, long)` and `Long.remainderUnsigned(long, long)` to correctly work with these values.

For example, the script below returns a value of the counter divided by 10.

```console
GET /my_index/_search
{
    "query": {
        "match_all" : {}
    },
    "script_fields": {
        "count10" : {
          "script": {
            "source": "Long.divideUnsigned(doc['my_counter'].value, 10)"
          }
        }
    }
}
```
% TEST[continued]

Alternatively, you can treat the unsigned long type as `BigInteger` in your scripts by using the field API. For example, this script treats `my_counter` as `BigInteger` with a default value of `BigInteger.ZERO`:

```js
"script": {
    "source": "field('my_counter').asBigInteger(BigInteger.ZERO)"
}
```
% NOTCONSOLE

For scripts that need to return float or double values, you can further convert `BigInteger` values to double or float:

```console
GET /my_index/_search
{
    "query": {
        "script_score": {
          "query": {"match_all": {}},
          "script": {
            "source": "field('my_counter').asBigInteger(BigInteger.ZERO).floatValue()"
          }
        }
    }
}
```
% TEST[continued]


## Queries with mixed numeric types [_queries_with_mixed_numeric_types]

Searches with mixed numeric types one of which is `unsigned_long` are supported, except queries with sort. Thus, a sort query across two indexes where the same field name has an `unsigned_long` type in one index, and `long` type in another, doesn’t produce correct results and must be avoided. If there is a need for such kind of sorting, script based sorting can be used instead.

Aggregations across several numeric types one of which is `unsigned_long` are supported. In this case, values are converted to the `double` type.


