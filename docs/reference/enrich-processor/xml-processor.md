---
navigation_title: "XML"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/xml-processor.html
---

# XML processor [xml-processor]


Parses XML documents and converts them to JSON objects using a streaming XML parser. This processor efficiently handles XML data by avoiding loading the entire document into memory.

$$$xml-options$$$

| Name | Required | Default | Description |
| --- | --- | --- | --- |
| `field` | yes | - | The field containing the XML string to be parsed. |
| `target_field` | no | `field` | The field that the converted structured object will be written into. Any existing content in this field will be overwritten. |
| `ignore_missing` | no | `false` | If `true` and `field` does not exist, the processor quietly exits without modifying the document. |
| `ignore_failure` | no | `false` | Ignore failures for the processor. See [Handling pipeline failures](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#handling-pipeline-failures). |
| `to_lower` | no | `false` | Convert XML element names to lowercase. |
| `ignore_empty_value` | no | `false` | If `true`, the processor will filter out null and empty values from the parsed XML structure, including empty elements, elements with null values, and elements with whitespace-only content. |
| `description` | no | - | Description of the processor. Useful for describing the purpose of the processor or its configuration. |
| `if` | no | - | Conditionally execute the processor. See [Conditionally run a processor](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#conditionally-run-processor). |
| `on_failure` | no | - | Handle failures for the processor. See [Handling pipeline failures](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#handling-pipeline-failures). |
| `tag` | no | - | Identifier for the processor. Useful for debugging and metrics. |

## Configuration

```js
{
  "xml": {
    "field": "xml_field",
    "target_field": "parsed_xml",
    "ignore_empty_value": true
  }
}
```

## Examples

### Basic XML parsing

```console
POST _ingest/pipeline/_simulate
{
  "pipeline": {
    "processors": [
      {
        "xml": {
          "field": "xml_content"
        }
      }
    ]
  },
  "docs": [
    {
      "_source": {
        "xml_content": "<catalog><book><author>William H. Gaddis</author><title>The Recognitions</title><review>One of the great seminal American novels.</review></book></catalog>"
      }
    }
  ]
}
```

Result:

```console-result
{
  "docs": [
    {
      "doc": {
        "_index": "_index",
        "_id": "_id",
        "_version": "-3",
        "_source": {
          "xml_content": "<catalog><book><author>William H. Gaddis</author><title>The Recognitions</title><review>One of the great seminal American novels.</review></book></catalog>",
          "catalog": {
            "book": {
              "author": "William H. Gaddis",
              "title": "The Recognitions",
              "review": "One of the great seminal American novels."
            }
          }
        },
        "_ingest": {
          "timestamp": "2019-03-11T21:54:37.909224Z"
        }
      }
    }
  ]
}
```

### Filtering empty values

When `ignore_empty_value` is set to `true`, the processor will remove empty elements from the parsed XML:

```console
POST _ingest/pipeline/_simulate
{
  "pipeline": {
    "processors": [
      {
        "xml": {
          "field": "xml_content",
          "target_field": "parsed_xml",
          "ignore_empty_value": true
        }
      }
    ]
  },
  "docs": [
    {
      "_source": {
        "xml_content": "<catalog><book><author>William H. Gaddis</author><title></title><review>One of the great seminal American novels.</review><empty/><nested><empty_text>   </empty_text><valid_content>Some content</valid_content></nested></book><empty_book></empty_book></catalog>"
      }
    }
  ]
}
```

Result with empty elements filtered out:

```console-result
{
  "docs": [
    {
      "doc": {
        "_index": "_index",
        "_id": "_id",
        "_version": "-3",
        "_source": {
          "xml_content": "<catalog><book><author>William H. Gaddis</author><title></title><review>One of the great seminal American novels.</review><empty/><nested><empty_text>   </empty_text><valid_content>Some content</valid_content></nested></book><empty_book></empty_book></catalog>",
          "parsed_xml": {
            "catalog": {
              "book": {
                "author": "William H. Gaddis",
                "review": "One of the great seminal American novels.",
                "nested": {
                  "valid_content": "Some content"
                }
              }
            }
          }
        },
        "_ingest": {
          "timestamp": "2019-03-11T21:54:37.909224Z"
        }
      }
    }
  ]
}
```

### Converting element names to lowercase

```console
POST _ingest/pipeline/_simulate
{
  "pipeline": {
    "processors": [
      {
        "xml": {
          "field": "xml_content",
          "to_lower": true
        }
      }
    ]
  },
  "docs": [
    {
      "_source": {
        "xml_content": "<Catalog><Book><Author>William H. Gaddis</Author><Title>The Recognitions</Title></Book></Catalog>"
      }
    }
  ]
}
```

Result:

```console-result
{
  "docs": [
    {
      "doc": {
        "_index": "_index",
        "_id": "_id",
        "_version": "-3",
        "_source": {
          "xml_content": "<Catalog><Book><Author>William H. Gaddis</Author><Title>The Recognitions</Title></Book></Catalog>",
          "catalog": {
            "book": {
              "author": "William H. Gaddis",
              "title": "The Recognitions"
            }
          }
        },
        "_ingest": {
          "timestamp": "2019-03-11T21:54:37.909224Z"
        }
      }
    }
  ]
}
```

### Handling XML attributes

XML attributes are included as properties in the resulting JSON object alongside element content:

```console
POST _ingest/pipeline/_simulate
{
  "pipeline": {
    "processors": [
      {
        "xml": {
          "field": "xml_content"
        }
      }
    ]
  },
  "docs": [
    {
      "_source": {
        "xml_content": "<catalog version=\"1.0\"><book id=\"123\" isbn=\"978-0-684-80335-9\"><title lang=\"en\">The Recognitions</title><author nationality=\"American\">William H. Gaddis</author></book></catalog>"
      }
    }
  ]
}
```

Result:

```console-result
{
  "docs": [
    {
      "doc": {
        "_index": "_index",
        "_id": "_id",
        "_version": "-3",
        "_source": {
          "xml_content": "<catalog version=\"1.0\"><book id=\"123\" isbn=\"978-0-684-80335-9\"><title lang=\"en\">The Recognitions</title><author nationality=\"American\">William H. Gaddis</author></book></catalog>",
          "catalog": {
            "version": "1.0",
            "book": {
              "id": "123",
              "isbn": "978-0-684-80335-9",
              "title": {
                "lang": "en",
                "#text": "The Recognitions"
              },
              "author": {
                "nationality": "American",
                "#text": "William H. Gaddis"
              }
            }
          }
        },
        "_ingest": {
          "timestamp": "2019-03-11T21:54:37.909224Z"
        }
      }
    }
  ]
}
```

## XML features

The XML processor supports:

- **Elements with text content**: Converted to key-value pairs where the element name is the key and text content is the value
- **Nested elements**: Converted to nested JSON objects
- **Empty elements**: Converted to `null` values (can be filtered with `ignore_empty_value`)
- **Repeated elements**: Converted to arrays when multiple elements with the same name exist at the same level
- **XML attributes**: Included as properties in the JSON object alongside element content. When an element has both attributes and text content, the text is stored under a special `#text` key
- **Mixed content**: Elements with both text and child elements include text under a special `#text` key while attributes and child elements become object properties
- **Namespaces**: Local names are used, namespace prefixes are ignored
