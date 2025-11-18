---
navigation_title: "Elision"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-elision-tokenfilter.html
---

# Elision token filter [analysis-elision-tokenfilter]


Removes specified [elisions](https://en.wikipedia.org/wiki/Elision) from the beginning of tokens. For example, you can use this filter to change `l'avion` to `avion`.

When not customized, the filter removes the following French elisions by default:

`l'`, `m'`, `t'`, `qu'`, `n'`, `s'`, `j'`, `d'`, `c'`, `jusqu'`, `quoiqu'`, `lorsqu'`, `puisqu'`

Customized versions of this filter are included in several of {{es}}'s built-in [language analyzers](/reference/text-analysis/analysis-lang-analyzer.md):

* [Catalan analyzer](/reference/text-analysis/analysis-lang-analyzer.md#catalan-analyzer)
* [French analyzer](/reference/text-analysis/analysis-lang-analyzer.md#french-analyzer)
* [Irish analyzer](/reference/text-analysis/analysis-lang-analyzer.md#irish-analyzer)
* [Italian analyzer](/reference/text-analysis/analysis-lang-analyzer.md#italian-analyzer)

This filter uses Lucene’s [ElisionFilter](https://lucene.apache.org/core/10_0_0/analysis/common/org/apache/lucene/analysis/util/ElisionFilter.md).

## Example [analysis-elision-tokenfilter-analyze-ex]

The following [analyze API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-analyze) request uses the `elision` filter to remove `j'` from `j’examine près du wharf`:

```console
GET _analyze
{
  "tokenizer" : "standard",
  "filter" : ["elision"],
  "text" : "j’examine près du wharf"
}
```

The filter produces the following tokens:

```text
[ examine, près, du, wharf ]
```


## Add to an analyzer [analysis-elision-tokenfilter-analyzer-ex]

The following [create index API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-create) request uses the `elision` filter to configure a new [custom analyzer](docs-content://manage-data/data-store/text-analysis/create-custom-analyzer.md).

```console
PUT /elision_example
{
  "settings": {
    "analysis": {
      "analyzer": {
        "whitespace_elision": {
          "tokenizer": "whitespace",
          "filter": [ "elision" ]
        }
      }
    }
  }
}
```


## Configurable parameters [analysis-elision-tokenfilter-configure-parms]

$$$analysis-elision-tokenfilter-articles$$$

`articles`
:   (Required*, array of string) List of elisions to remove.

To be removed, the elision must be at the beginning of a token and be immediately followed by an apostrophe. Both the elision and apostrophe are removed.

For custom `elision` filters, either this parameter or `articles_path` must be specified.


`articles_path`
:   (Required*, string) Path to a file that contains a list of elisions to remove.

This path must be absolute or relative to the `config` location, and the file must be UTF-8 encoded. Each elision in the file must be separated by a line break.

To be removed, the elision must be at the beginning of a token and be immediately followed by an apostrophe. Both the elision and apostrophe are removed.

For custom `elision` filters, either this parameter or `articles` must be specified.


`articles_case`
:   (Optional, Boolean) If `true`, elision matching is case insensitive. If `false`, elision matching is case sensitive. Defaults to `false`.


## Customize [analysis-elision-tokenfilter-customize]

To customize the `elision` filter, duplicate it to create the basis for a new custom token filter. You can modify the filter using its configurable parameters.

For example, the following request creates a custom case-insensitive `elision` filter that removes the `l'`, `m'`, `t'`, `qu'`, `n'`, `s'`, and `j'` elisions:

```console
PUT /elision_case_insensitive_example
{
  "settings": {
    "analysis": {
      "analyzer": {
        "default": {
          "tokenizer": "whitespace",
          "filter": [ "elision_case_insensitive" ]
        }
      },
      "filter": {
        "elision_case_insensitive": {
          "type": "elision",
          "articles": [ "l", "m", "t", "qu", "n", "s", "j" ],
          "articles_case": true
        }
      }
    }
  }
}
```


