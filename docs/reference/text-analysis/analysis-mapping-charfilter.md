---
navigation_title: "Mapping"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-mapping-charfilter.html
---

# Mapping character filter [analysis-mapping-charfilter]


The `mapping` character filter accepts a map of keys and values. Whenever it encounters a string of characters that is the same as a key, it replaces them with the value associated with that key.

Matching is greedy; the longest pattern matching at a given point wins. Replacements are allowed to be the empty string.

The `mapping` filter uses Lucene’s [MappingCharFilter](https://lucene.apache.org/core/10_0_0/analysis/common/org/apache/lucene/analysis/charfilter/MappingCharFilter.md).

## Example [analysis-mapping-charfilter-analyze-ex]

The following [analyze API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-analyze) request uses the `mapping` filter to convert Hindu-Arabic numerals (٠‎١٢٣٤٥٦٧٨‎٩‎) into their Arabic-Latin equivalents (0123456789), changing the text `My license plate is ٢٥٠١٥` to `My license plate is 25015`.

```console
GET /_analyze
{
  "tokenizer": "keyword",
  "char_filter": [
    {
      "type": "mapping",
      "mappings": [
        "٠ => 0",
        "١ => 1",
        "٢ => 2",
        "٣ => 3",
        "٤ => 4",
        "٥ => 5",
        "٦ => 6",
        "٧ => 7",
        "٨ => 8",
        "٩ => 9"
      ]
    }
  ],
  "text": "My license plate is ٢٥٠١٥"
}
```

The filter produces the following text:

```text
[ My license plate is 25015 ]
```


## Configurable parameters [analysis-mapping-charfilter-configure-parms]

`mappings`
:   (Required*, array of strings) Array of mappings, with each element having the form `key => value`.

    Either this or the `mappings_path` parameter must be specified.


`mappings_path`
:   (Required*, string) Path to a file containing `key => value` mappings.

    This path must be absolute or relative to the `config` location, and the file must be UTF-8 encoded. Each mapping in the file must be separated by a line break.

    Either this or the `mappings` parameter must be specified.



## Customize and add to an analyzer [analysis-mapping-charfilter-customize]

To customize the `mappings` filter, duplicate it to create the basis for a new custom character filter. You can modify the filter using its configurable parameters.

The following [create index API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-create) request configures a new [custom analyzer](docs-content://manage-data/data-store/text-analysis/create-custom-analyzer.md) using a custom `mappings` filter, `my_mappings_char_filter`.

The `my_mappings_char_filter` filter replaces the `:)` and `:(` emoticons with a text equivalent.

```console
PUT /my-index-000001
{
  "settings": {
    "analysis": {
      "analyzer": {
        "my_analyzer": {
          "tokenizer": "standard",
          "char_filter": [
            "my_mappings_char_filter"
          ]
        }
      },
      "char_filter": {
        "my_mappings_char_filter": {
          "type": "mapping",
          "mappings": [
            ":) => _happy_",
            ":( => _sad_"
          ]
        }
      }
    }
  }
}
```

The following [analyze API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-analyze) request uses the custom `my_mappings_char_filter` to replace `:(` with `_sad_` in the text `I'm delighted about it :(`.

```console
GET /my-index-000001/_analyze
{
  "tokenizer": "keyword",
  "char_filter": [ "my_mappings_char_filter" ],
  "text": "I'm delighted about it :("
}
```

The filter produces the following text:

```text
[ I'm delighted about it _sad_ ]
```


