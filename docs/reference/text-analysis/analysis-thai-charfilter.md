---
navigation_title: "Thai"
---

# Thai character filter [analysis-thai-charfilter]


Normalizes Thai orthography before tokenization. This includes replacing double Sara E (`เเ`) with Sara Ae (`แ`), recomposing decomposed Sara Am, removing zero-width characters, and reordering misplaced tone marks.

Pre-tokenization normalization is required for Thai: typographic anomalies prevent the dictionary-based JDK `BreakIterator` used by the [`thai` tokenizer](/reference/text-analysis/analysis-thai-tokenizer.md) from finding word boundaries. A downstream token filter can only fix characters inside an already-emitted token.

This filter is included in {{es}}'s built-in [Thai language analyzer](/reference/text-analysis/analysis-lang-analyzer.md#thai-analyzer). It uses Lucene’s [ThaiCharFilter](https://lucene.apache.org/core/10_0_0/analysis/common/org/apache/lucene/analysis/th/ThaiCharFilter.html).

The `thai` character filter is not configurable.

## Example [analysis-thai-charfilter-analyze-ex]

The following [analyze API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-analyze) request uses the `thai` character filter so the Thai tokenizer can segment text that contains double Sara E.

```console
GET /_analyze
{
  "tokenizer": "thai",
  "char_filter": ["thai"],
  "text": "ฉันรักเเมวมาก"
}
```

Without the character filter, BreakIterator would emit `[ฉัน, รักเเมวมาก]`. With it, the filter produces:

```text
[ ฉัน, รัก, แมว, มาก ]
```


## Add to an analyzer [analysis-thai-charfilter-analyzer-ex]

The following [create index API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-create) request uses the `thai` character filter to configure a new [custom analyzer](docs-content://manage-data/data-store/text-analysis/create-custom-analyzer.md).

```console
PUT /thai_charfilter_example
{
  "settings": {
    "analysis": {
      "analyzer": {
        "thai_custom": {
          "tokenizer": "thai",
          "char_filter": [ "thai" ]
        }
      }
    }
  }
}
```
