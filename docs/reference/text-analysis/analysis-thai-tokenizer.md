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

