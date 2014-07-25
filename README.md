Mapper Attachments Type for Elasticsearch
=========================================

The mapper attachments plugin adds the `attachment` type to Elasticsearch using [Apache Tika](http://lucene.apache.org/tika/).

In order to install the plugin, simply run: `bin/plugin -install elasticsearch/elasticsearch-mapper-attachments/2.0.0`.

* For master elasticsearch versions, look at [master branch](https://github.com/elasticsearch/elasticsearch-mapper-attachments/tree/master).
* For 1.4.x elasticsearch versions, look at [es-1.4 branch](https://github.com/elasticsearch/elasticsearch-mapper-attachments/tree/es-1.4).
* For 1.3.x elasticsearch versions, look at [es-1.3 branch](https://github.com/elasticsearch/elasticsearch-mapper-attachments/tree/es-1.3).
* For 1.2.x elasticsearch versions, look at [es-1.2 branch](https://github.com/elasticsearch/elasticsearch-mapper-attachments/tree/es-1.2).
* For 1.1.x elasticsearch versions, look at [es-1.1 branch](https://github.com/elasticsearch/elasticsearch-mapper-attachments/tree/es-1.1).
* For 1.0.x elasticsearch versions, look at [es-1.0 branch](https://github.com/elasticsearch/elasticsearch-mapper-attachments/tree/es-1.0).
* For 0.90.x elasticsearch versions, look at [es-0.90 branch](https://github.com/elasticsearch/elasticsearch-mapper-attachments/tree/es-0.90).


|   Attachment Mapper Plugin  | elasticsearch         |  Tika  | Release date |
|-----------------------------|-----------------------|--------|:------------:|
| 3.0.0-SNAPSHOT              | master                |  1.5   |  XXXX-XX-XX  |

Please read documentation relative to the version you are using:

* [3.0.0-SNAPSHOT](https://github.com/elasticsearch/elasticsearch-analysis-stempel/blob/master/README.md)

The `attachment` type allows to index different "attachment" type field (encoded as `base64`), for example,
microsoft office formats, open document formats, ePub, HTML, and so on (full list can be found [here](http://tika.apache.org/1.5/formats.html)).

The `attachment` type is provided as a plugin extension. The plugin is a simple zip file that can be downloaded and 
placed under `$ES_HOME/plugins/mapper-attachments` location. When the node will start, it will be automatically detected 
and the `attachment` type will be added.

Using the attachment type is simple, in your mapping JSON, simply set a certain JSON element as attachment, for example:

```javascript
PUT /test/person/_mapping
{
    "person" : {
        "properties" : {
            "my_attachment" : { "type" : "attachment" }
        }
    }
}
```

In this case, the JSON to index can be:

```javascript
PUT /test/person/1
{
    "my_attachment" : "... base64 encoded attachment ..."
}
```

Or it is possible to use more elaborated JSON if content type, resource name or language need to be set explicitly:

```
PUT /test/person/1
{
    "my_attachment" : {
        "_content_type" : "application/pdf",
        "_name" : "resource/name/of/my.pdf",
        "_language" : "en",
        "_content" : "... base64 encoded attachment ..."
    }
}
```

The `attachment` type not only indexes the content of the doc, but also automatically adds meta data on the attachment 
as well (when available).

The metadata supported are:

* `date`
* `title`
* `name` only available if you set `_name` see above
* `author`
* `keywords`
* `content_type`
* `content_length` is the original content_length before text extraction (aka file size)
* `language`

They can be queried using the "dot notation", for example: `my_attachment.author`.

Both the meta data and the actual content are simple core type mappers (string, date, ...), thus, they can be controlled 
in the mappings. For example:

```javascript
PUT /test/person/_mapping
{
    "person" : {
        "properties" : {
            "file" : {
                "type" : "attachment",
                "fields" : {
                    "file" : {"index" : "no"},
                    "title" : {"store" : "yes"},
                    "date" : {"store" : "yes"},
                    "author" : {"analyzer" : "myAnalyzer"},
                    "keywords" : {"store" : "yes"},
                    "content_type" : {"store" : "yes"},
                    "content_length" : {"store" : "yes"},
                    "language" : {"store" : "yes"}
                }
            }
        }
    }
}
```

In the above example, the actual content indexed is mapped under `fields` name `file`, and we decide not to index it, so 
it will only be available in the `_all` field. The other fields map to their respective metadata names, but there is no 
need to specify the `type` (like `string` or `date`) since it is already known.

Querying or accessing metadata
------------------------------

If you need to query on metadata fields, use the attachment field name dot the metadata field. For example:

```
DELETE /test
PUT /test
PUT /test/person/_mapping
{
  "person": {
    "properties": {
      "file": {
        "type": "attachment",
        "path": "full",
        "fields": {
          "content_type": {
            "type": "string",
            "store": true
          }
        }
      }
    }
  }
}
PUT /test/person/1?refresh=true
{
  "file": "IkdvZCBTYXZlIHRoZSBRdWVlbiIgKGFsdGVybmF0aXZlbHkgIkdvZCBTYXZlIHRoZSBLaW5nIg=="
}
GET /test/person/_search
{
  "fields": [ "file.content_type" ], 
  "query": {
    "match": {
      "file.content_type": "text plain"
    }
  }
}
```

Will give you:

```
{
   "took": 2,
   "timed_out": false,
   "_shards": {
      "total": 5,
      "successful": 5,
      "failed": 0
   },
   "hits": {
      "total": 1,
      "max_score": 0.16273327,
      "hits": [
         {
            "_index": "test",
            "_type": "person",
            "_id": "1",
            "_score": 0.16273327,
            "fields": {
               "file.content_type": [
                  "text/plain; charset=ISO-8859-1"
               ]
            }
         }
      ]
   }
}
```

Indexed Characters
------------------

By default, `100000` characters are extracted when indexing the content. This default value can be changed by setting 
the `index.mapping.attachment.indexed_chars` setting. It can also be provided on a per document indexed using the 
`_indexed_chars` parameter. `-1` can be set to extract all text, but note that all the text needs to be allowed to be 
represented in memory:

```
PUT /test/person/1
{
    "my_attachment" : {
        "_indexed_chars" : -1,
        "_content" : "... base64 encoded attachment ..."
    }
}
```

Metadata parsing error handling
-------------------------------

While extracting metadata content, errors could happen for example when parsing dates.
Parsing errors are ignored so your document is indexed.

You can disable this feature by setting the `index.mapping.attachment.ignore_errors` setting to `false`.

Language Detection
------------------

By default, language detection is disabled (`false`) as it could come with a cost.
This default value can be changed by setting the `index.mapping.attachment.detect_language` setting.
It can also be provided on a per document indexed using the `_detect_language` parameter.

Note that you can force language using `_language` field when sending your actual document:

```javascript
{
    "my_attachment" : {
        "_language" : "en",
        "_content" : "... base64 encoded attachment ..."
    }
}
```

Highlighting attachments
------------------------

If you want to highlight your attachment content, you will need to set `"store": true` and `"term_vector":"with_positions_offsets"`
for your attachment field. Here is a full script which does it:

```
DELETE /test
PUT /test
PUT /test/person/_mapping
{
  "person": {
    "properties": {
      "file": {
        "type": "attachment",
        "path": "full",
        "fields": {
          "file": {
            "type": "string",
            "term_vector":"with_positions_offsets",
            "store": true
          }
        }
      }
    }
  }
}
PUT /test/person/1?refresh=true
{
  "file": "IkdvZCBTYXZlIHRoZSBRdWVlbiIgKGFsdGVybmF0aXZlbHkgIkdvZCBTYXZlIHRoZSBLaW5nIg=="
}
GET /test/person/_search
{
  "fields": [], 
  "query": {
    "match": {
      "file": "king queen"
    }
  },
  "highlight": {
    "fields": {
      "file": {
      }
    }
  }
}
```

It gives back:

```js
{
   "took": 9,
   "timed_out": false,
   "_shards": {
      "total": 1,
      "successful": 1,
      "failed": 0
   },
   "hits": {
      "total": 1,
      "max_score": 0.13561106,
      "hits": [
         {
            "_index": "test",
            "_type": "person",
            "_id": "1",
            "_score": 0.13561106,
            "highlight": {
               "file": [
                  "\"God Save the <em>Queen</em>\" (alternatively \"God Save the <em>King</em>\"\n"
               ]
            }
         }
      ]
   }
}
```

License
-------

    This software is licensed under the Apache 2 license, quoted below.

    Copyright 2009-2014 Elasticsearch <http://www.elasticsearch.org>

    Licensed under the Apache License, Version 2.0 (the "License"); you may not
    use this file except in compliance with the License. You may obtain a copy of
    the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
    License for the specific language governing permissions and limitations under
    the License.
