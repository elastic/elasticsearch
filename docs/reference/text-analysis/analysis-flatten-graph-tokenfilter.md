---
navigation_title: "Flatten graph"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-flatten-graph-tokenfilter.html
---

# Flatten graph token filter [analysis-flatten-graph-tokenfilter]


Flattens a [token graph](docs-content://manage-data/data-store/text-analysis/token-graphs.md) produced by a graph token filter, such as [`synonym_graph`](/reference/text-analysis/analysis-synonym-graph-tokenfilter.md) or [`word_delimiter_graph`](/reference/text-analysis/analysis-word-delimiter-graph-tokenfilter.md).

Flattening a token graph containing [multi-position tokens](docs-content://manage-data/data-store/text-analysis/token-graphs.md#token-graphs-multi-position-tokens) makes the graph suitable for [indexing](docs-content://manage-data/data-store/text-analysis/index-search-analysis.md). Otherwise, indexing does not support token graphs containing multi-position tokens.

::::{warning}
Flattening graphs is a lossy process.

If possible, avoid using the `flatten_graph` filter. Instead, use graph token filters in [search analyzers](docs-content://manage-data/data-store/text-analysis/index-search-analysis.md) only. This eliminates the need for the `flatten_graph` filter.

::::


The `flatten_graph` filter uses Lucene’s [FlattenGraphFilter](https://lucene.apache.org/core/10_0_0/analysis/common/org/apache/lucene/analysis/core/FlattenGraphFilter.md).

## Example [analysis-flatten-graph-tokenfilter-analyze-ex]

To see how the `flatten_graph` filter works, you first need to produce a token graph containing multi-position tokens.

The following [analyze API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-analyze) request uses the `synonym_graph` filter to add `internet phonebook` as a multi-position synonym for `domain name system` in the text `domain name system is fragile`:

```console
GET /_analyze
{
  "tokenizer": "standard",
  "filter": [
    {
      "type": "synonym_graph",
      "synonyms": [ "internet phonebook, domain name system" ]
    }
  ],
  "text": "domain name system is fragile"
}
```

The filter produces the following token graph with `internet phonebook` as a multi-position token.

:::{image} images/token-graph-dns-synonym-ex2.svg
:alt: token graph dns synonym example
:::

Indexing does not support token graphs containing multi-position tokens. To make this token graph suitable for indexing, it needs to be flattened.

To flatten the token graph, add the `flatten_graph` filter after the `synonym_graph` filter in the previous analyze API request.

```console
GET /_analyze
{
  "tokenizer": "standard",
  "filter": [
    {
      "type": "synonym_graph",
      "synonyms": [ "internet phonebook, domain name system" ]
    },
    "flatten_graph"
  ],
  "text": "domain name system is fragile"
}
```

The filter produces the following flattened token graph, which is suitable for indexing.

:::{image} images/token-graph-dns-synonym-flattened-ex2.svg
:alt: token graph dns flattened example
:::


## Add to an analyzer [analysis-keyword-marker-tokenfilter-analyzer-ex]

The following [create index API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-create) request uses the `flatten_graph` token filter to configure a new [custom analyzer](docs-content://manage-data/data-store/text-analysis/create-custom-analyzer.md).

In this analyzer, a custom `word_delimiter_graph` filter produces token graphs containing catenated, multi-position tokens. The `flatten_graph` filter flattens these token graphs, making them suitable for indexing.

```console
PUT /my-index-000001
{
  "settings": {
    "analysis": {
      "analyzer": {
        "my_custom_index_analyzer": {
          "type": "custom",
          "tokenizer": "standard",
          "filter": [
            "my_custom_word_delimiter_graph_filter",
            "flatten_graph"
          ]
        }
      },
      "filter": {
        "my_custom_word_delimiter_graph_filter": {
          "type": "word_delimiter_graph",
          "catenate_all": true
        }
      }
    }
  }
}
```


