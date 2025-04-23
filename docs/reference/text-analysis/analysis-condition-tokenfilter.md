---
navigation_title: "Conditional"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-condition-tokenfilter.html
---

# Conditional token filter [analysis-condition-tokenfilter]


Applies a set of token filters to tokens that match conditions in a provided predicate script.

This filter uses Lucene’s [ConditionalTokenFilter](https://lucene.apache.org/core/10_0_0/analysis/common/org/apache/lucene/analysis/miscellaneous/ConditionalTokenFilter.md).

## Example [analysis-condition-analyze-ex]

The following [analyze API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-analyze) request uses the `condition` filter to match tokens with fewer than 5 characters in `THE QUICK BROWN FOX`. It then applies the [`lowercase`](/reference/text-analysis/analysis-lowercase-tokenfilter.md) filter to those matching tokens, converting them to lowercase.

```console
GET /_analyze
{
  "tokenizer": "standard",
  "filter": [
    {
      "type": "condition",
      "filter": [ "lowercase" ],
      "script": {
        "source": "token.getTerm().length() < 5"
      }
    }
  ],
  "text": "THE QUICK BROWN FOX"
}
```

The filter produces the following tokens:

```text
[ the, QUICK, BROWN, fox ]
```


## Configurable parameters [analysis-condition-tokenfilter-configure-parms]

`filter`
:   (Required, array of token filters) Array of token filters. If a token matches the predicate script in the `script` parameter, these filters are applied to the token in the order provided.

These filters can include custom token filters defined in the index mapping.


`script`
:   (Required, [script object](docs-content://explore-analyze/scripting/modules-scripting-using.md)) Predicate script used to apply token filters. If a token matches this script, the filters in the `filter` parameter are applied to the token.

For valid parameters, see [*How to write scripts*](docs-content://explore-analyze/scripting/modules-scripting-using.md). Only inline scripts are supported. Painless scripts are executed in the [analysis predicate context](/reference/scripting-languages/painless/painless-analysis-predicate-context.md) and require a `token` property.



## Customize and add to an analyzer [analysis-condition-tokenfilter-customize]

To customize the `condition` filter, duplicate it to create the basis for a new custom token filter. You can modify the filter using its configurable parameters.

For example, the following [create index API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-create) request uses a custom `condition` filter to configure a new [custom analyzer](docs-content://manage-data/data-store/text-analysis/create-custom-analyzer.md). The custom `condition` filter matches the first token in a stream. It then reverses that matching token using the [`reverse`](/reference/text-analysis/analysis-reverse-tokenfilter.md) filter.

```console
PUT /palindrome_list
{
  "settings": {
    "analysis": {
      "analyzer": {
        "whitespace_reverse_first_token": {
          "tokenizer": "whitespace",
          "filter": [ "reverse_first_token" ]
        }
      },
      "filter": {
        "reverse_first_token": {
          "type": "condition",
          "filter": [ "reverse" ],
          "script": {
            "source": "token.getPosition() === 0"
          }
        }
      }
    }
  }
}
```


