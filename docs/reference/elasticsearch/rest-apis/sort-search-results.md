---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/sort-search-results.html
applies_to:
  stack: all
---

# Sort search results

Allows you to add one or more sorts on specific fields. Each sort can be reversed as well. The sort is defined on a per field level, with special field name for `_score` to sort by score, and `_doc` to sort by index order.

To optimize sorting performance, avoid sorting by [`text`](/reference/elasticsearch/mapping-reference/text.md)fields; instead, use [`keyword`](/reference/elasticsearch/mapping-reference/keyword.md) or [`numerical`](/reference/elasticsearch/mapping-reference/number.md) fields. Additionally, you can improve performance by enabling pre-sorting at index time using [index sorting](/reference/elasticsearch/index-settings/sorting.md). While this can speed up query-time sorting, it may reduce indexing performance and increase memory usage.

Assuming the following index mapping:

```console
PUT /my-index-000001
{
  "mappings": {
    "properties": {
      "post_date": { "type": "date" },
      "user": {
        "type": "keyword"
      },
      "name": {
        "type": "keyword"
      },
      "age": { "type": "integer" }
    }
  }
}
```

```console
GET /my-index-000001/_search
{
  "sort" : [
    { "post_date" : {"order" : "asc", "format": "strict_date_optional_time_nanos"}},
    { "name" : "desc" },
    { "age" : "desc" },
    "user",
    "_score"
  ],
  "query" : {
    "term" : { "user" : "kimchy" }
  }
}
```
Order matters when defining multiple sort fields, because {{es}} attempts to sort on the first field in the array. Each subsequent field in the array is only used if the previous fields result in a tie. If documents have identical values across all specified sort fields, {{es}} uses the document ID as the final tie-breaker.

Here's how the example query attempts to sort results:

- First by `post_date` 
- If `post_date` values are identical, sorts by `name` 
- If both `post_date` and `name` are identical, sorts by `age`
- If the first three fields are identical, sorts by `user` 
- If all previous fields are identical, sorts by `_score`

By default, Elasticsearch sorts `numeric` fields in descending order and `string` fields in ascending order unless you explicitly specify otherwise. All three formats shown in the example above are valid. Some make the sort order explicit, while others rely on Elasticsearch’s default behavior.

::::{note}
`_doc` has no real use-case besides being the most efficient sort order. So if you don’t care about the order in which documents are returned, then you should sort by `_doc`. This especially helps when [scrolling](/reference/elasticsearch/rest-apis/paginate-search-results.md#scroll-search-results).
::::



## Sort values [_sort_values]

The search response includes `sort` values for each document. Use the `format` parameter to specify a [date format](/reference/elasticsearch/mapping-reference/mapping-date-format.md#built-in-date-formats) for the `sort` values of [`date`](/reference/elasticsearch/mapping-reference/date.md) and [`date_nanos`](/reference/elasticsearch/mapping-reference/date_nanos.md) fields. The following search returns `sort` values for the `post_date` field in the `strict_date_optional_time_nanos` format.

```console
GET /my-index-000001/_search
{
  "sort" : [
    { "post_date" : {"format": "strict_date_optional_time_nanos"}}
  ],
  "query" : {
    "term" : { "user" : "kimchy" }
  }
}
```


## Sort order [_sort_order]

The `order` option can have the following values:

`asc`
:   Sort in ascending order

`desc`
:   Sort in descending order

The order defaults to `desc` when sorting on the `_score`, and defaults to `asc` when sorting on anything else.


## Sort mode option [_sort_mode_option]

Elasticsearch supports sorting by array or multi-valued fields. The `mode` option controls what array value is picked for sorting the document it belongs to. The `mode` option can have the following values:

`min`
:   Pick the lowest value.

`max`
:   Pick the highest value.

`sum`
:   Use the sum of all values as sort value. Only applicable for number based array fields.

`avg`
:   Use the average of all values as sort value. Only applicable for number based array fields.

`median`
:   Use the median of all values as sort value. Only applicable for number based array fields.

The default sort mode in the ascending sort order is `min` — the lowest value is picked. The default sort mode in the descending order is `max` — the highest value is picked.


### Sort mode example usage [_sort_mode_example_usage]

In the example below the field price has multiple prices per document. In this case the result hits will be sorted by price ascending based on the average price per document.

```console
PUT /my-index-000001/_doc/1?refresh
{
   "product": "chocolate",
   "price": [20, 4]
}

POST /_search
{
   "query" : {
      "term" : { "product" : "chocolate" }
   },
   "sort" : [
      {"price" : {"order" : "asc", "mode" : "avg"}}
   ]
}
```


## Sorting numeric fields [_sorting_numeric_fields]

For numeric fields it is also possible to cast the values from one type to another using the `numeric_type` option. This option accepts the following values: [`"double", "long", "date", "date_nanos"`] and can be useful for searches across multiple data streams or indices where the sort field is mapped differently.

Consider for instance these two indices:

```console
PUT /index_double
{
  "mappings": {
    "properties": {
      "field": { "type": "double" }
    }
  }
}
```

```console
PUT /index_long
{
  "mappings": {
    "properties": {
      "field": { "type": "long" }
    }
  }
}
```

Since `field` is mapped as a `double` in the first index and as a `long` in the second index, it is not possible to use this field to sort requests that query both indices by default. However you can force the type to one or the other with the `numeric_type` option in order to force a specific type for all indices:

```console
POST /index_long,index_double/_search
{
   "sort" : [
      {
        "field" : {
            "numeric_type" : "double"
        }
      }
   ]
}
```

In the example above, values for the `index_long` index are casted to a double in order to be compatible with the values produced by the `index_double` index. It is also possible to transform a floating point field into a `long` but note that in this case floating points are replaced by the largest value that is less than or equal (greater than or equal if the value is negative) to the argument and is equal to a mathematical integer.

This option can also be used to convert a `date` field that uses millisecond resolution to a `date_nanos` field with nanosecond resolution. Consider for instance these two indices:

```console
PUT /index_double
{
  "mappings": {
    "properties": {
      "field": { "type": "date" }
    }
  }
}
```

```console
PUT /index_long
{
  "mappings": {
    "properties": {
      "field": { "type": "date_nanos" }
    }
  }
}
```

Values in these indices are stored with different resolutions so sorting on these fields will always sort the `date` before the `date_nanos` (ascending order). With the `numeric_type` type option it is possible to set a single resolution for the sort, setting to `date` will convert the `date_nanos` to the millisecond resolution while `date_nanos` will convert the values in the `date` field to the nanoseconds resolution:

```console
POST /index_long,index_double/_search
{
   "sort" : [
      {
        "field" : {
            "numeric_type" : "date_nanos"
        }
      }
   ]
}
```

::::{warning}
To avoid overflow, the conversion to `date_nanos` cannot be applied on dates before 1970 and after 2262 as nanoseconds are represented as longs.
::::



## Sorting within nested objects. [nested-sorting]

Elasticsearch also supports sorting by fields that are inside one or more nested objects. The sorting by nested field support has a `nested` sort option with the following properties:

`path`
:   Defines on which nested object to sort. The actual sort field must be a direct field inside this nested object. When sorting by nested field, this field is mandatory.

`filter`
:   A filter that the inner objects inside the nested path should match with in order for its field values to be taken into account by sorting. Common case is to repeat the query / filter inside the nested filter or query. By default no `filter` is active.

`max_children`
:   The maximum number of children to consider per root document when picking the sort value. Defaults to unlimited.

`nested`
:   Same as top-level `nested` but applies to another nested path within the current nested object.

::::{note}
Elasticsearch will throw an error if a nested field is defined in a sort without a `nested` context.
::::



### Nested sorting examples [_nested_sorting_examples]

In the below example `offer` is a field of type `nested`. The nested `path` needs to be specified; otherwise, Elasticsearch doesn’t know on what nested level sort values need to be captured.

```console
POST /_search
{
   "query" : {
      "term" : { "product" : "chocolate" }
   },
   "sort" : [
       {
          "offer.price" : {
             "mode" :  "avg",
             "order" : "asc",
             "nested": {
                "path": "offer",
                "filter": {
                   "term" : { "offer.color" : "blue" }
                }
             }
          }
       }
    ]
}
```

In the below example `parent` and `child` fields are of type `nested`. The `nested.path` needs to be specified at each level; otherwise, Elasticsearch doesn’t know on what nested level sort values need to be captured.

```console
POST /_search
{
   "query": {
      "nested": {
         "path": "parent",
         "query": {
            "bool": {
                "must": {"range": {"parent.age": {"gte": 21}}},
                "filter": {
                    "nested": {
                        "path": "parent.child",
                        "query": {"match": {"parent.child.name": "matt"}}
                    }
                }
            }
         }
      }
   },
   "sort" : [
      {
         "parent.child.age" : {
            "mode" :  "min",
            "order" : "asc",
            "nested": {
               "path": "parent",
               "filter": {
                  "range": {"parent.age": {"gte": 21}}
               },
               "nested": {
                  "path": "parent.child",
                  "filter": {
                     "match": {"parent.child.name": "matt"}
                  }
               }
            }
         }
      }
   ]
}
```

Nested sorting is also supported when sorting by scripts and sorting by geo distance.


## Missing values [_missing_values]

The `missing` parameter specifies how docs which are missing the sort field should be treated: The `missing` value can be set to `_last`, `_first`, or a custom value (that will be used for missing docs as the sort value). The default is `_last`.

For example:

```console
GET /_search
{
  "sort" : [
    { "price" : {"missing" : "_last"} }
  ],
  "query" : {
    "term" : { "product" : "chocolate" }
  }
}
```

::::{note}
If a nested inner object doesn’t match with the `nested.filter` then a missing value is used.
::::



## Ignoring unmapped fields [_ignoring_unmapped_fields]

By default, the search request will fail if there is no mapping associated with a field. The `unmapped_type` option allows you to ignore fields that have no mapping and not sort by them. The value of this parameter is used to determine what sort values to emit. Here is an example of how it can be used:

```console
GET /_search
{
  "sort" : [
    { "price" : {"unmapped_type" : "long"} }
  ],
  "query" : {
    "term" : { "product" : "chocolate" }
  }
}
```

If any of the indices that are queried doesn’t have a mapping for `price` then Elasticsearch will handle it as if there was a mapping of type `long`, with all documents in this index having no value for this field.


## Geo distance sorting [geo-sorting]

Allow to sort by `_geo_distance`. Here is an example, assuming `pin.location` is a field of type `geo_point`:

```console
GET /_search
{
  "sort" : [
    {
      "_geo_distance" : {
          "pin.location" : [-70, 40],
          "order" : "asc",
          "unit" : "km",
          "mode" : "min",
          "distance_type" : "arc",
          "ignore_unmapped": true
      }
    }
  ],
  "query" : {
    "term" : { "user" : "kimchy" }
  }
}
```

`distance_type`
:   How to compute the distance. Can either be `arc` (default), or `plane` (faster, but inaccurate on long distances and close to the poles).

`mode`
:   What to do in case a field has several geo points. By default, the shortest distance is taken into account when sorting in ascending order and the longest distance when sorting in descending order. Supported values are `min`, `max`, `median` and `avg`.

`unit`
:   The unit to use when computing sort values. The default is `m` (meters).

`ignore_unmapped`
:   Indicates if the unmapped field should be treated as a missing value. Setting it to `true` is equivalent to specifying an `unmapped_type` in the field sort. The default is `false` (unmapped field cause the search to fail).

::::{note}
geo distance sorting does not support configurable missing values: the distance will always be considered equal to `Infinity` when a document does not have values for the field that is used for distance computation.
::::


The following formats are supported in providing the coordinates:


### Lat lon as properties [_lat_lon_as_properties]

```console
GET /_search
{
  "sort" : [
    {
      "_geo_distance" : {
        "pin.location" : {
          "lat" : 40,
          "lon" : -70
        },
        "order" : "asc",
        "unit" : "km"
      }
    }
  ],
  "query" : {
    "term" : { "user" : "kimchy" }
  }
}
```


### Lat lon as WKT string [_lat_lon_as_wkt_string]

Format in [Well-Known Text](https://docs.opengeospatial.org/is/12-063r5/12-063r5.html).

```console
GET /_search
{
  "sort": [
    {
      "_geo_distance": {
        "pin.location": "POINT (-70 40)",
        "order": "asc",
        "unit": "km"
      }
    }
  ],
  "query": {
    "term": { "user": "kimchy" }
  }
}
```


### Geohash [_geohash]

```console
GET /_search
{
  "sort": [
    {
      "_geo_distance": {
        "pin.location": "drm3btev3e86",
        "order": "asc",
        "unit": "km"
      }
    }
  ],
  "query": {
    "term": { "user": "kimchy" }
  }
}
```


### Lat lon as array [_lat_lon_as_array]

Format in `[lon, lat]`, note, the order of lon/lat here in order to conform with [GeoJSON](http://geojson.org/).

```console
GET /_search
{
  "sort": [
    {
      "_geo_distance": {
        "pin.location": [ -70, 40 ],
        "order": "asc",
        "unit": "km"
      }
    }
  ],
  "query": {
    "term": { "user": "kimchy" }
  }
}
```


## Multiple reference points [_multiple_reference_points]

Multiple geo points can be passed as an array containing any `geo_point` format, for example

```console
GET /_search
{
  "sort": [
    {
      "_geo_distance": {
        "pin.location": [ [ -70, 40 ], [ -71, 42 ] ],
        "order": "asc",
        "unit": "km"
      }
    }
  ],
  "query": {
    "term": { "user": "kimchy" }
  }
}
```

and so forth.

The final distance for a document will then be `min`/`max`/`avg` (defined via `mode`) distance of all points contained in the document to all points given in the sort request.


## Script based sorting [script-based-sorting]

Allow to sort based on custom scripts, here is an example:

```console
GET /_search
{
  "query": {
    "term": { "user": "kimchy" }
  },
  "sort": {
    "_script": {
      "type": "number",
      "script": {
        "lang": "painless",
        "source": "doc['field_name'].value * params.factor",
        "params": {
          "factor": 1.1
        }
      },
      "order": "asc"
    }
  }
}
```


## Track scores [_track_scores]

When sorting on a field, scores are not computed. By setting `track_scores` to true, scores will still be computed and tracked.

```console
GET /_search
{
  "track_scores": true,
  "sort" : [
    { "post_date" : {"order" : "desc"} },
    { "name" : "desc" },
    { "age" : "desc" }
  ],
  "query" : {
    "term" : { "user" : "kimchy" }
  }
}
```


## Memory considerations [_memory_considerations]

When sorting, the relevant sorted field values are loaded into memory. This means that per shard, there should be enough memory to contain them. For string based types, the field sorted on should not be analyzed / tokenized. For numeric types, if possible, it is recommended to explicitly set the type to narrower types (like `short`, `integer` and `float`).

