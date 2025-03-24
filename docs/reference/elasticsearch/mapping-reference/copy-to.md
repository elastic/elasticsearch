---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/copy-to.html
---

# copy_to [copy-to]

The `copy_to` parameter allows you to copy the values of multiple fields into a group field, which can then be queried as a single field.

::::{tip}
If you often search multiple fields, you can improve search speeds by using `copy_to` to search fewer fields. See [Search as few fields as possible](docs-content://deploy-manage/production-guidance/optimize-performance/search-speed.md#search-as-few-fields-as-possible).
::::


For example, the `first_name` and `last_name` fields can be copied to the `full_name` field as follows:

```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "first_name": {
        "type": "text",
        "copy_to": "full_name" <1>
      },
      "last_name": {
        "type": "text",
        "copy_to": "full_name" <1>
      },
      "full_name": {
        "type": "text"
      }
    }
  }
}

PUT my-index-000001/_doc/1
{
  "first_name": "John",
  "last_name": "Smith"
}

GET my-index-000001/_search
{
  "query": {
    "match": {
      "full_name": { <2>
        "query": "John Smith",
        "operator": "and"
      }
    }
  }
}
```

1. The values of the `first_name` and `last_name` fields are copied to the `full_name` field.
2. The `first_name` and `last_name` fields can still be queried for the first name and last name respectively, but the `full_name` field can be queried for both first and last names.


Some important points:

* It is the field *value* which is copied, not the terms (which result from the analysis process).
* The original [`_source`](/reference/elasticsearch/mapping-reference/mapping-source-field.md) field will not be modified to show the copied values.
* The same value can be copied to multiple fields, with `"copy_to": [ "field_1", "field_2" ]`
* You cannot copy recursively using intermediary fields. The following configuration will not copy data from `field_1` to `field_3`:

    ```console
    PUT bad_example_index
    {
      "mappings": {
        "properties": {
          "field_1": {
            "type": "text",
            "copy_to": "field_2"
          },
          "field_2": {
            "type": "text",
            "copy_to": "field_3"
          },
          "field_3": {
            "type": "text"
          }
        }
      }
    }
    ```

    Instead, copy to multiple fields from the source field:

    ```console
    PUT good_example_index
    {
      "mappings": {
        "properties": {
          "field_1": {
            "type": "text",
            "copy_to": ["field_2", "field_3"]
          },
          "field_2": {
            "type": "text"
          },
          "field_3": {
            "type": "text"
          }
        }
      }
    }
    ```


::::{note}
`copy_to` is not supported for field types where values take the form of objects, e.g. `date_range`.
::::



## Dynamic mapping [copy-to-dynamic-mapping]

Consider the following points when using `copy_to` with dynamic mappings:

* If the target field does not exist in the index mappings, the usual [dynamic mapping](docs-content://manage-data/data-store/mapping/dynamic-mapping.md) behavior applies. By default, with [`dynamic`](/reference/elasticsearch/mapping-reference/dynamic.md) set to `true`, a non-existent target field will be dynamically added to the index mappings.
* If `dynamic` is set to `false`, the target field will not be added to the index mappings, and the value will not be copied.
* If `dynamic` is set to `strict`, copying to a non-existent field will result in an error.

* If the target field is nested, then `copy_to` fields must specify the full path to the nested field. Omitting the full path will lead to a `strict_dynamic_mapping_exception`. Use `"copy_to": ["parent_field.child_field"]` to correctly target a nested field.

  For example:

  ```console
  PUT /test_index
  {
    "mappings": {
      "dynamic": "strict",
      "properties": {
        "description": {
          "properties": {
            "notes": {
              "type": "text",
              "copy_to": [ "description.notes_raw"], <1>
              "analyzer": "standard",
              "search_analyzer": "standard"
            },
            "notes_raw": {
              "type": "keyword"
            }
          }
        }
      }
    }
  }
  ```

  1. The `notes` field is copied to the `notes_raw` field. Targeting `notes_raw` alone instead of `description.notes_raw` would lead to a `strict_dynamic_mapping_exception`.In this example, `notes_raw` is not defined at the root of the mapping, but under the `description` field. Without the fully qualified path, {{es}} would interpret the `copy_to` target as a root-level field, not as a nested field under `description`.



