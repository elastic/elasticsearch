---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-execute-api.html
products:
  - id: painless
---

# Painless API examples [painless-execute-api]

::::{warning}
This functionality is in technical preview and may be changed or removed in a future release. Elastic will work to fix any issues, but features in technical preview are not subject to the support SLA of official GA features.
::::


The Painless execute API runs a script and returns a result.

## {{api-request-title}} [painless-execute-api-request]

`POST /_scripts/painless/_execute`


## {{api-description-title}} [painless-execute-api-desc]

Use this API to build and test scripts, such as when defining a script for a [runtime field](docs-content://manage-data/data-store/mapping/runtime-fields.md). This API requires very few dependencies, and is especially useful if you don’t have permissions to write documents on a cluster.

The API uses several *contexts*, which control how scripts are executed, what variables are available at runtime, and what the return type is.

Each context requires a script, but additional parameters depend on the context you’re using for that script.


## {{api-request-body-title}} [painless-execute-api-request-body]

`script`
:   (Required, object) The Painless script to execute.

### Properties of `script`

  `emit`
  :   (Required) Accepts the values from the script valuation. Scripts can call the `emit` method multiple times to emit multiple values.

  The `emit` method applies only to scripts used in a [runtime fields context](#painless-execute-runtime-context).

:::{important}
The `emit` method cannot accept `null` values. Do not call this method if the referenced fields do not have any values.
:::

  ::::{dropdown} Signatures of emit
  The signature for `emit` depends on the `type` of the field.

  `boolean`
  :   `emit(boolean)`

  `date`
  :   `emit(long)`

  `double`
  :   `emit(double)`

  `geo_point`
  :   `emit(double lat, double lon)`

  `ip`
  :   `emit(String)`

  `long`
  :   `emit(long)`

  `keyword`
  :   `emit(String)`
  ::::

$$$_contexts$$$

`context`
:   (Optional, string) The context that the script should run in. Defaults to `painless_test` if no context is specified.

::::::{dropdown} Properties of context
`painless_test`
:   The default context if no other context is specified. See [test context](#painless-execute-test).

`filter`
:   Treats scripts as if they were run inside a `script` query. See [filter context](#painless-execute-filter-context).

`score`
:   Treats scripts as if they were run inside a `script_score` function in a `function_score` query. See [score context](#painless-execute-core-context).

  $$$painless-execute-runtime-context$$$

:::::{dropdown} Field contexts
The following options are specific to the field contexts.

::::{note}
Result ordering in the field contexts is not guaranteed.
::::


  ::::{admonition}
  `boolean_field`
  :   The context for [`boolean` fields](/reference/elasticsearch/mapping-reference/boolean.md). The script returns a `true` or `false` response. See [boolean_field context](#painless-runtime-boolean).

  `date_field`
  :   The context for [`date` fields](/reference/elasticsearch/mapping-reference/date.md). `emit` takes a `long` value and the script returns a sorted list of dates. See [date_time context](#painless-runtime-datetime).

  `double_field`
  :   The context for `double` [numeric fields](/reference/elasticsearch/mapping-reference/number.md). The script returns a sorted list of `double` values. See [double_field context](#painless-runtime-double).

  `geo_point_field`
  :   The context for [`geo-point` fields](/reference/elasticsearch/mapping-reference/geo-point.md). `emit` takes two double parameters, the latitude and longitude values, and the script returns an object in GeoJSON format containing the coordinates for the geo point. See [geo_point_field context](#painless-runtime-geo).

  `ip_field`
  :   The context for [`ip` fields](/reference/elasticsearch/mapping-reference/ip.md). The script returns a sorted list of IP addresses. See [ip_field context](#painless-runtime-ip).

  `keyword_field`
  :   The context for [`keyword` fields](/reference/elasticsearch/mapping-reference/keyword.md). The script returns a sorted list of `string` values. See [keyword_field context](#painless-runtime-keyword).

  `long_field`
  :   The context for `long` [numeric fields](/reference/elasticsearch/mapping-reference/number.md). The script returns a sorted list of `long` values. See [long_field context](#painless-runtime-long).

  `composite_field`
  :   The context for `composite` [runtime fields](docs-content://manage-data/data-store/mapping/runtime-fields.md). The script returns a map of values. See [composite_field context](#painless-runtime-composite).

  ::::
  :::::
  ::::::


`context_setup`
:   (Required, object) Additional parameters for the `context`.

    ::::{note}
    This parameter is required for all contexts except `painless_test`, which is the default if no value is provided for `context`.
    ::::


  :::::{dropdown} Properties of context_setup
  `document`
  :   (Required, string) Document that’s temporarily indexed in-memory and accessible from the script.

  `index`
  :   (Required, string) Index containing a mapping that’s compatible with the indexed document. You may specify a remote index by prefixing the index with the remote cluster alias. For example, `remote1:my_index` indicates that you want to execute the painless script against the "my_index" index on the "remote1" cluster. This request will be forwarded to the "remote1" cluster if you have [configured a connection](docs-content://deploy-manage/remote-clusters/remote-clusters-self-managed.md) to that remote cluster.

::::{note}
Wildcards are not accepted in the index expression for this endpoint. The expression `*:myindex` will return the error "No such remote cluster" and the expression `logs*` or `remote1:logs*` will return the error "index not found".
::::


  :::::


`params`
:   (`Map`, read-only) Specifies any named parameters that are passed into the script as variables.

`query`
:   (Optional, object)

    ::::{note}
    This parameter only applies when `score` is specified as the script `context`.
    ::::


    Use this parameter to specify a query for computing a score. Besides deciding whether or not the document matches, the [query clause](/reference/query-languages/query-dsl/query-filter-context.md#query-context) also calculates a relevance score in the `_score` metadata field.



## Test context [painless-execute-test]

The `painless_test` context runs scripts without additional parameters. The only variable that is available is `params`, which can be used to access user defined values. The result of the script is always converted to a string.

Because the default context is `painless_test`, you don’t need to specify the `context` or `context_setup`.

### Request [_request]

```console
POST /_scripts/painless/_execute
{
  "script": {
    "source": "params.count / params.total",
    "params": {
      "count": 100.0,
      "total": 1000.0
    }
  }
}
```


### Response [_response]

```console-result
{
  "result": "0.1"
}
```



## Filter context [painless-execute-filter-context]

The `filter` context treats scripts as if they were run inside a `script` query. For testing purposes, a document must be provided so that it will be temporarily indexed in-memory and is accessible from the script. More precisely, the `_source`, stored fields and doc values of such a document are available to the script being tested.

### Request [_request_2]

```console
PUT /my-index-000001
{
  "mappings": {
    "properties": {
      "field": {
        "type": "keyword"
      }
    }
  }
}
```

```console
POST /_scripts/painless/_execute
{
  "script": {
    "source": "doc['field'].value.length() <= params.max_length",
    "params": {
      "max_length": 4
    }
  },
  "context": "filter",
  "context_setup": {
    "index": "my-index-000001",
    "document": {
      "field": "four"
    }
  }
}
```


### Response [_response_2]

```console-result
{
  "result": true
}
```



## Score context [painless-execute-core-context]

The `score` context treats scripts as if they were run inside a `script_score` function in a `function_score` query.

### Request [_request_3]

```console
PUT /my-index-000001
{
  "mappings": {
    "properties": {
      "field": {
        "type": "keyword"
      },
      "rank": {
        "type": "long"
      }
    }
  }
}
```

```console
POST /_scripts/painless/_execute
{
  "script": {
    "source": "doc['rank'].value / params.max_rank",
    "params": {
      "max_rank": 5.0
    }
  },
  "context": "score",
  "context_setup": {
    "index": "my-index-000001",
    "document": {
      "rank": 4
    }
  }
}
```


### Response [_response_3]

```console-result
{
  "result": 0.8
}
```



## Field contexts [painless-execute-runtime-field-context]

The field contexts treat scripts as if they were run inside the [`runtime_mappings` section](docs-content://manage-data/data-store/mapping/define-runtime-fields-in-search-request.md) of a search query. You can use field contexts to test scripts for different field types, and then include those scripts anywhere that they’re supported, such as  [runtime fields](/reference/scripting-languages/painless/use-painless-scripts-in-runtime-fields.md).

Choose a field context based on the data type you want to return.

### `boolean_field` [painless-runtime-boolean]

Use the `boolean_field` field context when you want to return a `true` or `false` value from a script valuation. [Boolean fields](/reference/elasticsearch/mapping-reference/boolean.md) accept `true` and `false` values, but can also accept strings that are interpreted as either true or false.

Let’s say you have data for the top 100 science fiction books of all time. You want to write scripts that return a boolean response such as whether books exceed a certain page count, or if a book was published after a specific year.

Consider that your data is structured like this:

```console
PUT /my-index-000001
{
  "mappings": {
    "properties": {
      "name": {
        "type": "keyword"
      },
      "author": {
        "type": "keyword"
      },
      "release_date": {
        "type": "date"
      },
      "page_count": {
        "type": "double"
      }
    }
  }
}
```

You can then write a script in the `boolean_field` context that indicates whether a book was published before the year 1972:

```console
POST /_scripts/painless/_execute
{
  "script": {
    "source": """
      emit(doc['release_date'].value.year < 1972);
    """
  },
  "context": "boolean_field",
  "context_setup": {
    "index": "my-index-000001",
    "document": {
      "name": "Dune",
      "author": "Frank Herbert",
      "release_date": "1965-06-01",
      "page_count": 604
    }
  }
}
```

Because *Dune* was published in 1965, the result returns as `true`:

```console-result
{
  "result" : [
    true
  ]
}
```

Similarly, you could write a script that determines whether the first name of an author exceeds a certain number of characters. The following script operates on the `author` field to determine whether the author’s first name contains at least one character, but is less than five characters:

```console
POST /_scripts/painless/_execute
{
  "script": {
    "source": """
      int space = doc['author'].value.indexOf(' ');
      emit(space > 0 && space < 5);
    """
  },
  "context": "boolean_field",
  "context_setup": {
    "index": "my-index-000001",
    "document": {
      "name": "Dune",
      "author": "Frank Herbert",
      "release_date": "1965-06-01",
      "page_count": 604
    }
  }
}
```

Because `Frank` is five characters, the response returns `false` for the script valuation:

```console-result
{
  "result" : [
    false
  ]
}
```


### `date_time` [painless-runtime-datetime]

Several options are available for using [using datetime in Painless](/reference/scripting-languages/painless/using-datetime-in-painless.md). In this example, you’ll estimate when a particular author starting writing a book based on its release date and the writing speed of that author. The example makes some assumptions, but shows to write a script that operates on a date while incorporating additional information.

Add the following fields to your index mapping to get started:

```console
PUT /my-index-000001
{
  "mappings": {
    "properties": {
      "name": {
        "type": "keyword"
      },
      "author": {
        "type": "keyword"
      },
      "release_date": {
        "type": "date"
      },
      "page_count": {
        "type": "long"
      }
    }
  }
}
```

The following script makes the incredible assumption that when writing a book, authors just write each page and don’t do research or revisions. Further, the script assumes that the average time it takes to write a page is eight hours.

The script retrieves the `author` and makes another fantastic assumption to either divide or multiply the `pageTime` value based on the author’s perceived writing speed (yet another wild assumption).

The script subtracts the release date value (in milliseconds) from the calculation of `pageTime` times the `page_count` to determine approximately (based on numerous assumptions) when the author began writing the book.

```console
POST /_scripts/painless/_execute
{
  "script": {
    "source": """
      String author = doc['author'].value;
      long pageTime = 28800000;  <1>
      if (author == 'Robert A. Heinlein') {
        pageTime /= 2;           <2>
      } else if (author == 'Alastair Reynolds') {
        pageTime *= 2;           <3>
      }
      emit(doc['release_date'].value.toInstant().toEpochMilli() - pageTime * doc['page_count'].value);
    """
  },
  "context": "date_field",
  "context_setup": {
    "index": "my-index-000001",
    "document": {
      "name": "Revelation Space",
      "author": "Alastair Reynolds",
      "release_date": "2000-03-15",
      "page_count": 585
    }
  }
}
```

1. Eight hours, represented in milliseconds
2. Incredibly fast writing from Robert A. Heinlein
3. Alastair Reynolds writes space operas at a much slower speed


In this case, the author is Alastair Reynolds. Based on a release date of `2000-03-15`, the script calculates that the author started writing `Revelation Space` on 19 February 1999. Writing a 585 page book in just over one year is pretty impressive!

```console-result
{
  "result" : [
    "1999-02-19T00:00:00.000Z"
  ]
}
```


### `double_field` [painless-runtime-double]

Use the `double_field` context for [numeric data](/reference/elasticsearch/mapping-reference/number.md) of type `double`. For example, let’s say you have sensor data that includes a `voltage` field with values like 5.6. After indexing millions of documents, you discover that the sensor with model number `QVKC92Q` is under reporting its voltage by a factor of 1.7. Rather than reindex your data, you can fix it with a runtime field.

You need to multiply this value, but only for sensors that match a specific model number.

Add the following fields to your index mapping. The `voltage` field is a sub-field of the `measures` object.

```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "@timestamp": {
        "type": "date"
      },
      "model_number": {
        "type": "keyword"
      },
      "measures": {
        "properties": {
          "voltage": {
            "type": "double"
          }
        }
      }
    }
  }
}
```

The following script matches on any documents where the `model_number` equals `QVKC92Q`, and then multiplies the `voltage` value by `1.7`. This script is useful when you want to select specific documents and only operate on values that match the specified criteria.

```console
POST /_scripts/painless/_execute
{
  "script": {
    "source": """
      if (doc['model_number'].value.equals('QVKC92Q'))
      {emit(1.7 * params._source['measures']['voltage']);}
      else{emit(params._source['measures']['voltage']);}
    """
  },
  "context": "double_field",
  "context_setup": {
    "index": "my-index-000001",
    "document": {
      "@timestamp": 1516470094000,
      "model_number": "QVKC92Q",
      "measures": {
        "voltage": 5.6
      }
    }
  }
}
```

The result includes the calculated voltage, which was determined by multiplying the original value of `5.6` by `1.7`:

```console-result
{
  "result" : [
    9.52
  ]
}
```


### `geo_point_field` [painless-runtime-geo]

[Geo-point](/reference/elasticsearch/mapping-reference/geo-point.md) fields accept latitude-longitude pairs. You can define a geo-point field in several ways, and include values for latitude and longitude in the document for your script.

If you already have a known geo-point, it’s simpler to clearly state the positions of `lat` and `lon` in your index mappings.

```console
PUT /my-index-000001/
{
  "mappings": {
    "properties": {
      "lat": {
        "type": "double"
      },
      "lon": {
        "type": "double"
      }
    }
  }
}
```

You can then use the `geo_point_field` runtime field context to write a script that retrieves the `lat` and `lon` values.

```console
POST /_scripts/painless/_execute
{
  "script": {
    "source": """
      emit(doc['lat'].value, doc['lon'].value);
    """
  },
  "context": "geo_point_field",
  "context_setup": {
    "index": "my-index-000001",
    "document": {
      "lat": 41.12,
      "lon": -71.34
    }
  }
}
```

Because you’re working with a geo-point field type, the response includes results that are formatted as `coordinates`.

```console-result
{
  "result" : [
    {
      "coordinates" : [
        -71.34,
        41.12
      ],
      "type" : "Point"
    }
  ]
}
```

::::{note}
The emit function for [geo-point](/reference/elasticsearch/mapping-reference/geo-point.md) fields takes two parameters ordered with `lat` before `lon`, but the output GeoJSON format orders the `coordinates` as `[ lon, lat ]`.
::::



### `ip_field` [painless-runtime-ip]

The `ip_field` context is useful for data that includes IP addresses of type [`ip`](/reference/elasticsearch/mapping-reference/ip.md). For example, let’s say you have a `message` field from an Apache log. This field contains an IP address, but also other data that you don’t need.

You can add the `message` field to your index mappings as a `wildcard` to accept pretty much any data you want to put in that field.

```console
PUT /my-index-000001/
{
  "mappings": {
    "properties": {
      "message": {
        "type": "wildcard"
      }
    }
  }
}
```

You can then define a runtime script with a grok pattern that extracts structured fields out of the `message` field.

The script matches on the `%{{COMMONAPACHELOG}}` log pattern, which understands the structure of Apache logs. If the pattern matches, the script emits the value matching the IP address. If the pattern doesn’t match (`clientip != null`), the script just returns the field value without crashing.

```console
POST /_scripts/painless/_execute
{
  "script": {
    "source": """
      String clientip=grok('%{COMMONAPACHELOG}').extract(doc["message"].value)?.clientip;
      if (clientip != null) emit(clientip);
    """
  },
  "context": "ip_field",
  "context_setup": {
    "index": "my-index-000001",
    "document": {
      "message": "40.135.0.0 - - [30/Apr/2020:14:30:17 -0500] \"GET /images/hm_bg.jpg HTTP/1.0\" 200 24736"
    }
  }
}
```

The response includes only the IP address, ignoring all of the other data in the `message` field.

```console-result
{
  "result" : [
    "40.135.0.0"
  ]
}
```


### `keyword_field` [painless-runtime-keyword]

[Keyword fields](/reference/elasticsearch/mapping-reference/keyword.md) are often used in sorting, aggregations, and term-level queries.

Let’s say you have a timestamp. You want to calculate the day of the week based on that value and return it, such as `Thursday`. The following request adds a `@timestamp` field of type `date` to the index mappings:

```console
PUT /my-index-000001
{
  "mappings": {
    "properties": {
      "@timestamp": {
        "type": "date"
      }
    }
  }
}
```

To return the equivalent day of week based on your timestamp, you can create a script in the `keyword_field` runtime field context:

```console
POST /_scripts/painless/_execute
{
  "script": {
    "source": """
      emit(doc['@timestamp'].value.dayOfWeekEnum.getDisplayName(TextStyle.FULL, Locale.ENGLISH));
    """
  },
  "context": "keyword_field",
  "context_setup": {
    "index": "my-index-000001",
    "document": {
      "@timestamp": "2020-04-30T14:31:43-05:00"
    }
  }
}
```

The script operates on the value provided for the `@timestamp` field to calculate and return the day of the week:

```console-result
{
  "result" : [
    "Thursday"
  ]
}
```


### `long_field` [painless-runtime-long]

Let’s say you have sensor data that a `measures` object. This object includes a `start` and `end` field, and you want to calculate the difference between those values.

The following request adds a `measures` object to the mappings with two fields, both of type `long`:

```console
PUT /my-index-000001/
{
  "mappings": {
    "properties": {
      "measures": {
        "properties": {
          "start": {
            "type": "long"
          },
          "end": {
           "type": "long"
          }
        }
      }
    }
  }
}
```

You can then define a script that assigns values to the `start` and `end` fields and operate on them. The following script extracts the value for the `end` field from the `measures` object and subtracts it from the `start` field:

```console
POST /_scripts/painless/_execute
{
  "script": {
    "source": """
      emit(doc['measures.end'].value - doc['measures.start'].value);
    """
  },
  "context": "long_field",
  "context_setup": {
    "index": "my-index-000001",
    "document": {
      "measures": {
        "voltage": "4.0",
        "start": "400",
        "end": "8625309"
      }
    }
  }
}
```

The response includes the calculated value from the script valuation:

```console-result
{
  "result" : [
    8624909
  ]
}
```


### `composite_field` [painless-runtime-composite]

Let’s say you have logging data with a raw `message` field which you want to split in multiple sub-fields that can be accessed separately.

The following request adds a `message` field to the mappings of type `keyword`:

```console
PUT /my-index-000001/
{
  "mappings": {
    "properties": {
      "message": {
        "type" : "keyword"
      }
    }
  }
}
```

You can then define a script that splits such message field into subfields using the grok function:

```console
POST /_scripts/painless/_execute
{
  "script": {
    "source": "emit(grok(\"%{COMMONAPACHELOG}\").extract(doc[\"message\"].value));"
  },
  "context": "composite_field",
  "context_setup": {
    "index": "my-index-000001",
    "document": {
      "timestamp":"2020-04-30T14:31:27-05:00",
      "message":"252.0.0.0 - - [30/Apr/2020:14:31:27 -0500] \"GET /images/hm_bg.jpg HTTP/1.0\" 200 24736"
    }
  }
}
```

The response includes the values that the script emitted:

```console-result
{
  "result" : {
    "composite_field.timestamp" : [
      "30/Apr/2020:14:31:27 -0500"
    ],
    "composite_field.auth" : [
      "-"
    ],
    "composite_field.response" : [
      "200"
    ],
    "composite_field.ident" : [
      "-"
    ],
    "composite_field.httpversion" : [
      "1.0"
    ],
    "composite_field.verb" : [
      "GET"
    ],
    "composite_field.bytes" : [
      "24736"
    ],
    "composite_field.clientip" : [
      "252.0.0.0"
    ],
    "composite_field.request" : [
      "/images/hm_bg.jpg"
    ]
  }
}
```



