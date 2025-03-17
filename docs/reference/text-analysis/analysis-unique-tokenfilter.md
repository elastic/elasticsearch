---
navigation_title: "Unique"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-unique-tokenfilter.html
---

# Unique token filter [analysis-unique-tokenfilter]


Removes duplicate tokens from a stream. For example, you can use the `unique` filter to change `the lazy lazy dog` to `the lazy dog`.

If the `only_on_same_position` parameter is set to `true`, the `unique` filter removes only duplicate tokens *in the same position*.

::::{note}
When `only_on_same_position` is `true`, the `unique` filter works the same as [`remove_duplicates`](/reference/text-analysis/analysis-remove-duplicates-tokenfilter.md) filter.

::::


## Example [analysis-unique-tokenfilter-analyze-ex]

The following [analyze API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-analyze) request uses the `unique` filter to remove duplicate tokens from `the quick fox jumps the lazy fox`:

```console
GET _analyze
{
  "tokenizer" : "whitespace",
  "filter" : ["unique"],
  "text" : "the quick fox jumps the lazy fox"
}
```

The filter removes duplicated tokens for `the` and `fox`, producing the following output:

```text
[ the, quick, fox, jumps, lazy ]
```


## Add to an analyzer [analysis-unique-tokenfilter-analyzer-ex]

The following [create index API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-create) request uses the `unique` filter to configure a new [custom analyzer](docs-content://manage-data/data-store/text-analysis/create-custom-analyzer.md).

```console
PUT custom_unique_example
{
  "settings" : {
    "analysis" : {
      "analyzer" : {
        "standard_truncate" : {
        "tokenizer" : "standard",
        "filter" : ["unique"]
        }
      }
    }
  }
}
```


## Configurable parameters [analysis-unique-tokenfilter-configure-parms]

`only_on_same_position`
:   (Optional, Boolean) If `true`, only remove duplicate tokens in the same position. Defaults to `false`.


## Customize [analysis-unique-tokenfilter-customize]

To customize the `unique` filter, duplicate it to create the basis for a new custom token filter. You can modify the filter using its configurable parameters.

For example, the following request creates a custom `unique` filter with `only_on_same_position` set to `true`.

```console
PUT letter_unique_pos_example
{
  "settings": {
    "analysis": {
      "analyzer": {
        "letter_unique_pos": {
          "tokenizer": "letter",
          "filter": [ "unique_pos" ]
        }
      },
      "filter": {
        "unique_pos": {
          "type": "unique",
          "only_on_same_position": true
        }
      }
    }
  }
}
```


