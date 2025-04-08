---
navigation_title: "Foreach"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/foreach-processor.html
---

# Foreach processor [foreach-processor]


Runs an ingest processor on each element of an array or object.

All ingest processors can run on array or object elements. However, if the number of elements is unknown, it can be cumbersome to process each one in the same way.

The `foreach` processor lets you specify a `field` containing array or object values and a `processor` to run on each element in the field.

$$$foreach-options$$$

| Name | Required | Default | Description |
| --- | --- | --- | --- |
| `field` | yes | - | Field containing array or objectvalues. |
| `processor` | yes | - | Ingest processor to run on eachelement. |
| `ignore_missing` | no | false | If `true`, the processor silentlyexits without changing the document if the `field` is `null` or missing. |
| `description` | no | - | Description of the processor. Useful for describing the purpose of the processor or its configuration. |
| `if` | no | - | Conditionally execute the processor. See [Conditionally run a processor](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#conditionally-run-processor). |
| `ignore_failure` | no | `false` | Ignore failures for the processor. See [Handling pipeline failures](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#handling-pipeline-failures). |
| `on_failure` | no | - | Handle failures for the processor. See [Handling pipeline failures](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#handling-pipeline-failures). |
| `tag` | no | - | Identifier for the processor. Useful for debugging and metrics. |


## Access keys and values [foreach-keys-values]

When iterating through an array or object, the `foreach` processor stores the current element’s value in the `_ingest._value` [ingest metadata](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#access-ingest-metadata) field. `_ingest._value` contains the entire element value, including any child fields. You can access child field values using dot notation on the `_ingest._value` field.

When iterating through an object, the `foreach` processor also stores the current element’s key as a string in `_ingest._key`.

You can access and change `_ingest._key` and `_ingest._value` in the `processor`. For an example, see the [object example](#foreach-object-ex).


## Failure handling [foreach-failure-handling]

If the `foreach` processor fails to process an element and no `on_failure` processor is specified, the `foreach` processor silently exits. This leaves the entire array or object value unchanged.


## Examples [foreach-examples]

The following examples show how you can use the `foreach` processor with different data types and options:

* [Array](#foreach-array-ex)
* [Array of objects](#foreach-array-objects-ex)
* [Object](#foreach-object-ex)
* [Failure handling](#failure-handling-ex)


### Array [foreach-array-ex]

Assume the following document:

```js
{
  "values" : ["foo", "bar", "baz"]
}
```

When this `foreach` processor operates on this sample document:

```js
{
  "foreach" : {
    "field" : "values",
    "processor" : {
      "uppercase" : {
        "field" : "_ingest._value"
      }
    }
  }
}
```

Then the document will look like this after processing:

```js
{
  "values" : ["FOO", "BAR", "BAZ"]
}
```


### Array of objects [foreach-array-objects-ex]

Assume the following document:

```js
{
  "persons" : [
    {
      "id" : "1",
      "name" : "John Doe"
    },
    {
      "id" : "2",
      "name" : "Jane Doe"
    }
  ]
}
```

In this case, the `id` field needs to be removed, so the following `foreach` processor is used:

```js
{
  "foreach" : {
    "field" : "persons",
    "processor" : {
      "remove" : {
        "field" : "_ingest._value.id"
      }
    }
  }
}
```

After processing the result is:

```js
{
  "persons" : [
    {
      "name" : "John Doe"
    },
    {
      "name" : "Jane Doe"
    }
  ]
}
```

For another array of objects example, refer to the [attachment processor documentation](/reference/enrich-processor/attachment.md#attachment-with-arrays).


### Object [foreach-object-ex]

You can also use the `foreach` processor on object fields. For example, the following document contains a `products` field with object values.

```js
{
  "products" : {
    "widgets" : {
      "total_sales" : 50,
      "unit_price": 1.99,
      "display_name": ""
    },
    "sprockets" : {
      "total_sales" : 100,
      "unit_price": 9.99,
      "display_name": "Super Sprockets"
    },
    "whizbangs" : {
      "total_sales" : 200,
      "unit_price": 19.99,
      "display_name": "Wonderful Whizbangs"
    }
  }
}
```

The following `foreach` processor changes the value of `products.display_name` to uppercase.

```js
{
  "foreach": {
    "field": "products",
    "processor": {
      "uppercase": {
        "field": "_ingest._value.display_name"
      }
    }
  }
}
```

When run on the document, the `foreach` processor returns:

```js
{
  "products" : {
    "widgets" : {
      "total_sales" : 50,
      "unit_price" : 1.99,
      "display_name" : ""
    },
    "sprockets" : {
      "total_sales" : 100,
      "unit_price" : 9.99,
      "display_name" : "SUPER SPROCKETS"
    },
    "whizbangs" : {
      "total_sales" : 200,
      "unit_price" : 19.99,
      "display_name" : "WONDERFUL WHIZBANGS"
    }
  }
}
```

The following `foreach` processor sets each element’s key to the value of `products.display_name`. If `products.display_name` contains an empty string, the processor deletes the element.

```js
{
  "foreach": {
    "field": "products",
    "processor": {
      "set": {
        "field": "_ingest._key",
        "value": "{{_ingest._value.display_name}}"
      }
    }
  }
}
```

When run on the previous document, the `foreach` processor returns:

```js
{
  "products" : {
    "Wonderful Whizbangs" : {
      "total_sales" : 200,
      "unit_price" : 19.99,
      "display_name" : "Wonderful Whizbangs"
    },
    "Super Sprockets" : {
      "total_sales" : 100,
      "unit_price" : 9.99,
      "display_name" : "Super Sprockets"
    }
  }
}
```


### Failure handling [failure-handling-ex]

The wrapped processor can have a `on_failure` definition. For example, the `id` field may not exist on all person objects. Instead of failing the index request, you can use an `on_failure` block to send the document to the *failure_index* index for later inspection:

```js
{
  "foreach" : {
    "field" : "persons",
    "processor" : {
      "remove" : {
        "field" : "_value.id",
        "on_failure" : [
          {
            "set" : {
              "field": "_index",
              "value": "failure_index"
            }
          }
        ]
      }
    }
  }
}
```

In this example, if the `remove` processor does fail, then the array elements that have been processed thus far will be updated.

