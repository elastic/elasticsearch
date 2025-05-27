---
navigation_title: "Attachment"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/attachment.html
---

# Attachment processor [attachment]


The attachment processor lets Elasticsearch extract file attachments in common formats (such as PPT, XLS, and PDF) by using the Apache text extraction library [Tika](https://tika.apache.org/).

The source field must be a base64 encoded binary. If you do not want to incur the overhead of converting back and forth between base64, you can use the CBOR format instead of JSON and specify the field as a bytes array instead of a string representation. The processor will skip the base64 decoding then.

## Using the attachment processor in a pipeline [using-attachment]

$$$attachment-options$$$

| Name | Required | Default | Description |
| --- | --- | --- | --- |
| `field` | yes | - | The field to get the base64 encoded field from |
| `target_field` | no | attachment | The field that will hold the attachment information |
| `indexed_chars` | no | 100000 | The number of chars being used for extraction to prevent huge fields. Use `-1` for no limit. |
| `indexed_chars_field` | no | `null` | Field name from which you can overwrite the number of chars being used for extraction. See `indexed_chars`. |
| `properties` | no | all properties |  Array of properties to select to be stored. Can be `content`, `title`, `name`, `author`, `keywords`, `date`, `content_type`, `content_length`, `language` |
| `ignore_missing` | no | `false` | If `true` and `field` does not exist, the processor quietly exits without modifying the document |
| `remove_binary` | encouraged | `false` | If `true`, the binary `field` will be removed from the document. This option is not required, but setting it explicitly is encouraged, and omitting it will result in a warning. |
| `resource_name` | no |  | Field containing the name of the resource to decode. If specified, the processor passes this resource name to the underlying Tika library to enable [Resource Name Based Detection](https://tika.apache.org/1.24.1/detection.html#Resource_Name_Based_Detection). |


### Example [attachment-json-ex]

If attaching files to JSON documents, you must first encode the file as a base64 string. On Unix-like systems, you can do this using a `base64` command:

```shell
base64 -in myfile.rtf
```

The command returns the base64-encoded string for the file. The following base64 string is for an `.rtf` file containing the text `Lorem ipsum dolor sit amet`: `e1xydGYxXGFuc2kNCkxvcmVtIGlwc3VtIGRvbG9yIHNpdCBhbWV0DQpccGFyIH0=`.

Use an attachment processor to decode the string and extract the file’s properties:

```console
PUT _ingest/pipeline/attachment
{
  "description" : "Extract attachment information",
  "processors" : [
    {
      "attachment" : {
        "field" : "data",
        "remove_binary": true
      }
    }
  ]
}
PUT my-index-000001/_doc/my_id?pipeline=attachment
{
  "data": "e1xydGYxXGFuc2kNCkxvcmVtIGlwc3VtIGRvbG9yIHNpdCBhbWV0DQpccGFyIH0="
}
GET my-index-000001/_doc/my_id
```

The document’s `attachment` object contains extracted properties for the file:

```console-result
{
  "found": true,
  "_index": "my-index-000001",
  "_id": "my_id",
  "_version": 1,
  "_seq_no": 22,
  "_primary_term": 1,
  "_source": {
    "attachment": {
      "content_type": "application/rtf",
      "language": "ro",
      "content": "Lorem ipsum dolor sit amet",
      "content_length": 28
    }
  }
}
```


## Exported fields [attachment-fields]

The fields which might be extracted from a document are:

* `content`,
* `title`,
* `author`,
* `keywords`,
* `date`,
* `content_type`,
* `content_length`,
* `language`,
* `modified`,
* `format`,
* `identifier`,
* `contributor`,
* `coverage`,
* `modifier`,
* `creator_tool`,
* `publisher`,
* `relation`,
* `rights`,
* `source`,
* `type`,
* `description`,
* `print_date`,
* `metadata_date`,
* `latitude`,
* `longitude`,
* `altitude`,
* `rating`,
* `comments`

To extract only certain `attachment` fields, specify the `properties` array:

```console
PUT _ingest/pipeline/attachment
{
  "description" : "Extract attachment information",
  "processors" : [
    {
      "attachment" : {
        "field" : "data",
        "properties": [ "content", "title" ],
        "remove_binary": true
      }
    }
  ]
}
```

::::{note}
Extracting contents from binary data is a resource intensive operation and consumes a lot of resources. It is highly recommended to run pipelines using this processor in a dedicated ingest node.
::::



## Keeping the attachment binary [attachment-keep-binary]

Keeping the binary as a field within the document might consume a lot of resources. It is highly recommended to remove that field from the document, by setting `remove_binary` to `true` to automatically remove the field, as in the other examples shown on this page. If you *do* want to keep the binary field, explicitly set `remove_binary` to `false` to avoid the warning you get from omitting it:

```console
PUT _ingest/pipeline/attachment
{
  "description" : "Extract attachment information including original binary",
  "processors" : [
    {
      "attachment" : {
        "field" : "data",
        "remove_binary": false
      }
    }
  ]
}
PUT my-index-000001/_doc/my_id?pipeline=attachment
{
  "data": "e1xydGYxXGFuc2kNCkxvcmVtIGlwc3VtIGRvbG9yIHNpdCBhbWV0DQpccGFyIH0="
}
GET my-index-000001/_doc/my_id
```

The document’s `_source` object includes the original binary field:

```console-result
{
  "found": true,
  "_index": "my-index-000001",
  "_id": "my_id",
  "_version": 1,
  "_seq_no": 22,
  "_primary_term": 1,
  "_source": {
    "data": "e1xydGYxXGFuc2kNCkxvcmVtIGlwc3VtIGRvbG9yIHNpdCBhbWV0DQpccGFyIH0=",
    "attachment": {
      "content_type": "application/rtf",
      "language": "ro",
      "content": "Lorem ipsum dolor sit amet",
      "content_length": 28
    }
  }
}
```


## Use the attachment processor with CBOR [attachment-cbor]

To avoid encoding and decoding JSON to base64, you can instead pass CBOR data to the attachment processor. For example, the following request creates the `cbor-attachment` pipeline, which uses the attachment processor.

```console
PUT _ingest/pipeline/cbor-attachment
{
  "description" : "Extract attachment information",
  "processors" : [
    {
      "attachment" : {
        "field" : "data",
        "remove_binary": true
      }
    }
  ]
}
```

The following Python script passes CBOR data to an HTTP indexing request that includes the `cbor-attachment` pipeline. The HTTP request headers use a `content-type` of `application/cbor`.

::::{note}
Not all {{es}} clients support custom HTTP request headers.
::::


```python
import cbor2
import requests

file = 'my-file'
headers = {'content-type': 'application/cbor'}

with open(file, 'rb') as f:
  doc = {
    'data': f.read()
  }
  requests.put(
    'http://localhost:9200/my-index-000001/_doc/my_id?pipeline=cbor-attachment',
    data=cbor2.dumps(doc),
    headers=headers
  )
```


## Limit the number of extracted chars [attachment-extracted-chars]

To prevent extracting too many chars and overload the node memory, the number of chars being used for extraction is limited by default to `100000`. You can change this value by setting `indexed_chars`. Use `-1` for no limit but ensure when setting this that your node will have enough HEAP to extract the content of very big documents.

You can also define this limit per document by extracting from a given field the limit to set. If the document has that field, it will overwrite the `indexed_chars` setting. To set this field, define the `indexed_chars_field` setting.

For example:

```console
PUT _ingest/pipeline/attachment
{
  "description" : "Extract attachment information",
  "processors" : [
    {
      "attachment" : {
        "field" : "data",
        "indexed_chars" : 11,
        "indexed_chars_field" : "max_size",
        "remove_binary": true
      }
    }
  ]
}
PUT my-index-000001/_doc/my_id?pipeline=attachment
{
  "data": "e1xydGYxXGFuc2kNCkxvcmVtIGlwc3VtIGRvbG9yIHNpdCBhbWV0DQpccGFyIH0="
}
GET my-index-000001/_doc/my_id
```

Returns this:

```console-result
{
  "found": true,
  "_index": "my-index-000001",
  "_id": "my_id",
  "_version": 1,
  "_seq_no": 35,
  "_primary_term": 1,
  "_source": {
    "attachment": {
      "content_type": "application/rtf",
      "language": "is",
      "content": "Lorem ipsum",
      "content_length": 11
    }
  }
}
```

```console
PUT _ingest/pipeline/attachment
{
  "description" : "Extract attachment information",
  "processors" : [
    {
      "attachment" : {
        "field" : "data",
        "indexed_chars" : 11,
        "indexed_chars_field" : "max_size",
        "remove_binary": true
      }
    }
  ]
}
PUT my-index-000001/_doc/my_id_2?pipeline=attachment
{
  "data": "e1xydGYxXGFuc2kNCkxvcmVtIGlwc3VtIGRvbG9yIHNpdCBhbWV0DQpccGFyIH0=",
  "max_size": 5
}
GET my-index-000001/_doc/my_id_2
```

Returns this:

```console-result
{
  "found": true,
  "_index": "my-index-000001",
  "_id": "my_id_2",
  "_version": 1,
  "_seq_no": 40,
  "_primary_term": 1,
  "_source": {
    "max_size": 5,
    "attachment": {
      "content_type": "application/rtf",
      "language": "sl",
      "content": "Lorem",
      "content_length": 5
    }
  }
}
```


## Using the attachment processor with arrays [attachment-with-arrays]

To use the attachment processor within an array of attachments the [foreach processor](/reference/enrich-processor/foreach-processor.md) is required. This enables the attachment processor to be run on the individual elements of the array.

For example, given the following source:

```js
{
  "attachments" : [
    {
      "filename" : "ipsum.txt",
      "data" : "dGhpcyBpcwpqdXN0IHNvbWUgdGV4dAo="
    },
    {
      "filename" : "test.txt",
      "data" : "VGhpcyBpcyBhIHRlc3QK"
    }
  ]
}
```

In this case, we want to process the data field in each element of the attachments field and insert the properties into the document so the following `foreach` processor is used:

```console
PUT _ingest/pipeline/attachment
{
  "description" : "Extract attachment information from arrays",
  "processors" : [
    {
      "foreach": {
        "field": "attachments",
        "processor": {
          "attachment": {
            "target_field": "_ingest._value.attachment",
            "field": "_ingest._value.data",
            "remove_binary": true
          }
        }
      }
    }
  ]
}
PUT my-index-000001/_doc/my_id?pipeline=attachment
{
  "attachments" : [
    {
      "filename" : "ipsum.txt",
      "data" : "dGhpcyBpcwpqdXN0IHNvbWUgdGV4dAo="
    },
    {
      "filename" : "test.txt",
      "data" : "VGhpcyBpcyBhIHRlc3QK"
    }
  ]
}
GET my-index-000001/_doc/my_id
```

Returns this:

```console-result
{
  "_index" : "my-index-000001",
  "_id" : "my_id",
  "_version" : 1,
  "_seq_no" : 50,
  "_primary_term" : 1,
  "found" : true,
  "_source" : {
    "attachments" : [
      {
        "filename" : "ipsum.txt",
        "attachment" : {
          "content_type" : "text/plain; charset=ISO-8859-1",
          "language" : "en",
          "content" : "this is\njust some text",
          "content_length" : 24
        }
      },
      {
        "filename" : "test.txt",
        "attachment" : {
          "content_type" : "text/plain; charset=ISO-8859-1",
          "language" : "en",
          "content" : "This is a test",
          "content_length" : 16
        }
      }
    ]
  }
}
```

Note that the `target_field` needs to be set, otherwise the default value is used which is a top level field `attachment`. The properties on this top level field will contain the value of the first attachment only. However, by specifying the `target_field` on to a value on `_ingest._value` it will correctly associate the properties with the correct attachment.


