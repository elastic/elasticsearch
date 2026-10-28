---
navigation_title: "Thai"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-thai-tokenizer.html
---

# Thai tokenizer [analysis-thai-tokenizer]


The `thai` tokenizer segments Thai text into words, using the Thai segmentation algorithm included with Java. Text in other languages in general will be treated the same as the [`standard` tokenizer](/reference/text-analysis/analysis-standard-tokenizer.md).

::::{warning}
This tokenizer may not be supported by all JREs. It is known to work with Sun/Oracle and OpenJDK. If your application needs to be fully portable, consider using the [ICU Tokenizer](/reference/elasticsearch-plugins/analysis-icu-tokenizer.md) instead.
::::



## Example output [_example_output_17]

```console
POST _analyze
{
  "tokenizer": "thai",
  "text": "การที่ได้ต้องแสดงว่างานดี"
}
```

The above sentence would produce the following terms:

```text
[ การ, ที่, ได้, ต้อง, แสดง, ว่า, งาน, ดี ]
```


## Configuration [_configuration_20]

The `thai` tokenizer is not configurable.


## Behavior by index version [analysis-thai-tokenizer-index-version]

The tokenizer reads text in 1024-character chunks and only cuts a chunk at a point where it cannot split a word. Since Lucene 10.6 any whitespace counts as such a point; before that only line breaks did, so in long text without line breaks a word that straddled a chunk boundary could be split into fragments.

Indices created before {{es}} upgraded to Lucene 10.6 keep the previous behavior for both indexing and search, including for documents indexed after an upgrade, so that queries keep matching existing documents. Indices created on or after the upgrade, and [analyze API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-analyze) requests that do not specify an index, use the current behavior. To adopt the current behavior for an existing index, reindex it into a new index. The [`thai` analyzer](/reference/text-analysis/analysis-lang-analyzer.md#thai-analyzer-index-version) follows the same rule.
