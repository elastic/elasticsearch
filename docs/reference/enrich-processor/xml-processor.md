---
navigation_title: "XML"
applies_to:
  stack: ga 9.2
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/xml-processor.html
---

# XML processor [xml-processor]

Parses XML documents and converts them to JSON objects using a streaming SAX parser. This processor efficiently handles XML data with a single-pass architecture that supports both structured output and XPath extraction for optimal performance.

$$$xml-options$$$

| Name | Required | Default | Description |
| --- | --- | --- | --- |
| `field` | yes | - | The field containing the XML string to be parsed. |
| `target_field` | no | `field` | The field that the converted structured object will be written into. Any existing content in this field will be overwritten. |
| `store_xml` | no | `true` | If `true`, stores the parsed XML structure in the target field. If `false`, only XPath extraction results are stored and `target_field` is ignored. |
| `ignore_missing` | no | `false` | If `true` and `field` does not exist, the processor quietly exits without modifying the document. |
| `ignore_failure` | no | `false` | Ignore failures for the processor. See [Handling pipeline failures](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#handling-pipeline-failures). |
| `to_lower` | no | `false` | Convert XML element names and attribute names to lowercase. |
| `remove_empty_values` | no | `false` | If `true`, the processor will filter out null and empty values from the parsed XML structure, including empty elements, elements with null values, and elements with whitespace-only content. |
| `remove_namespaces` | no | `false` | If `true`, removes namespace prefixes from element and attribute names. |
| `force_content` | no | `false` | If `true`, forces text content and attributes to always parse to a hash value with `#text` key for content. |
| `force_array` | no | `false` | If `true`, forces all parsed values to be arrays. Single elements are wrapped in arrays. |
| `xpath` | no | - | Map of XPath expressions to target field names. Extracts values from the XML using XPath and stores them in the specified fields. |
| `namespaces` | no | - | Map of namespace prefixes to URIs for use with XPath expressions. Required when XPath expressions contain namespace prefixes. |
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
    "remove_empty_values": true
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
        ...
        "_source": {
          "xml_content": "<catalog><book><author>William H. Gaddis</author><title>The Recognitions</title><review>One of the great seminal American novels.</review></book></catalog>",
          "catalog": {
            "book": {
              "author": "William H. Gaddis",
              "title": "The Recognitions",
              "review": "One of the great seminal American novels."
            }
          }
        }
      }
    }
  ]
}
```

### Filtering empty values

When `remove_empty_values` is set to `true`, the processor will remove empty elements from the parsed XML:

```console
POST _ingest/pipeline/_simulate
{
  "pipeline": {
    "processors": [
      {
        "xml": {
          "field": "xml_content",
          "target_field": "parsed_xml",
          "remove_empty_values": true
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
        ...
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
        ...
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
        }
      }
    }
  ]
}
```

### XPath extraction

The XML processor can extract specific values using XPath expressions and store them in designated fields:

```console
POST _ingest/pipeline/_simulate
{
  "pipeline": {
    "processors": [
      {
        "xml": {
          "field": "xml_content",
          "store_xml": false,
          "xpath": {
            "//book/title/text()": "book_title",
            "//book/author/text()": "book_author",
            "//book/@id": "book_id"
          }
        }
      }
    ]
  },
  "docs": [
    {
      "_source": {
        "xml_content": "<catalog><book id=\"123\"><title>The Recognitions</title><author>William H. Gaddis</author></book><book id=\"456\"><title>1984</title><author>George Orwell</author></book></catalog>"
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
        ...
        "_source": {
          "xml_content": "<catalog><book id=\"123\"><title>The Recognitions</title><author>William H. Gaddis</author></book><book id=\"456\"><title>1984</title><author>George Orwell</author></book></catalog>",
          "book_title": ["The Recognitions", "1984"],
          "book_author": ["William H. Gaddis", "George Orwell"],
          "book_id": ["123", "456"]
        }
      }
    }
  ]
}
```

### XPath with namespaces

When working with XML that uses namespaces, you need to configure namespace mappings:

```console
POST _ingest/pipeline/_simulate
{
  "pipeline": {
    "processors": [
      {
        "xml": {
          "field": "xml_content",
          "namespaces": {
            "book": "http://example.com/book",
            "author": "http://example.com/author"
          },
          "xpath": {
            "//book:catalog/book:item/book:title/text()": "titles",
            "//author:info/@name": "author_names"
          }
        }
      }
    ]
  },
  "docs": [
    {
      "_source": {
        "xml_content": "<book:catalog xmlns:book=\"http://example.com/book\" xmlns:author=\"http://example.com/author\"><book:item><book:title>The Recognitions</book:title><author:info name=\"William H. Gaddis\"/></book:item></book:catalog>"
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
        ...
        "_source": {
          "xml_content": "<book:catalog xmlns:book=\"http://example.com/book\" xmlns:author=\"http://example.com/author\"><book:item><book:title>The Recognitions</book:title><author:info name=\"William H. Gaddis\"/></book:item></book:catalog>",
          "titles": "The Recognitions",
          "author_names": "William H. Gaddis",
          "book:catalog": {
            "book:item": {
              "book:title": "The Recognitions",
              "author:info": {
                "name": "William H. Gaddis"
              }
            }
          }
        }
      }
    }
  ]
}
```

### Mixed content handling

When XML contains mixed content (text interspersed with elements), text fragments are combined and stored under the special `#text` key:

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
        "xml_content": "<foo>This text is <b>bold</b> and this is <i>italic</i>!</foo>"
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
        ...
        "_source": {
          "xml_content": "<foo>This text is <b>bold</b> and this is <i>italic</i>!</foo>",
          "foo": {
            "b": "bold",
            "i": "italic",
            "#text": "This text is  and this is !"
          }
        }
      }
    }
  ]
}
```

### Force content mode

When `force_content` is `true`, all element text content is stored under the special `#text` key, even for simple elements without attributes. This provides a consistent structure when elements may have varying complexity.

**Without force_content (default behavior):**

```console
POST _ingest/pipeline/_simulate
{
  "pipeline": {
    "processors": [
      {
        "xml": {
          "field": "xml_content",
          "force_content": false
        }
      }
    ]
  },
  "docs": [
    {
      "_source": {
        "xml_content": "<book><title>The Recognitions</title><author nationality=\"American\">William H. Gaddis</author></book>"
      }
    }
  ]
}
```

Result (simple elements as string values, complex elements with #text):

```console-result
{
  "docs": [
    {
      "doc": {
        ...
        "_source": {
          "xml_content": "<book><title>The Recognitions</title><author nationality=\"American\">William H. Gaddis</author></book>",
          "book": {
            "title": "The Recognitions",
            "author": {
              "nationality": "American",
              "#text": "William H. Gaddis"
            }
          }
        }
      }
    }
  ]
}
```

**With force_content enabled:**

```console
POST _ingest/pipeline/_simulate
{
  "pipeline": {
    "processors": [
      {
        "xml": {
          "field": "xml_content",
          "force_content": true
        }
      }
    ]
  },
  "docs": [
    {
      "_source": {
        "xml_content": "<book><title>The Recognitions</title><author nationality=\"American\">William H. Gaddis</author></book>"
      }
    }
  ]
}
```

Result (all text content under #text key):

```console-result
{
  "docs": [
    {
      "doc": {
        ...
        "_source": {
          "xml_content": "<book><title>The Recognitions</title><author nationality=\"American\">William H. Gaddis</author></book>",
          "book": {
            "title": {
              "#text": "The Recognitions"
            },
            "author": {
              "nationality": "American",
              "#text": "William H. Gaddis"
            }
          }
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
- **Empty elements**: Converted to `null` values (can be filtered with `remove_empty_values`)
- **Repeated elements**: Converted to arrays when multiple elements with the same name exist at the same level
- **XML attributes**: Included as properties in the JSON object alongside element content. When an element has both attributes and text content, the text is stored under a special `#text` key
- **Mixed content**: Elements with both text and child elements include text under a special `#text` key while attributes and child elements become object properties
- **Namespaces**: Namespace prefixes are preserved by default and can be used in XPath expressions with the `namespaces` configuration. Use `remove_namespaces: true` to strip namespace prefixes from element names
