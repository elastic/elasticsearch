---
navigation_title: "Keyword"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-keyword-tokenizer.html
---

# Keyword tokenizer [analysis-keyword-tokenizer]


The `keyword` tokenizer is a noop tokenizer that accepts whatever text it is given and outputs the exact same text as a single term. It can be combined with token filters to normalise output, e.g. lower-casing email addresses.


## Example output [_example_output_10]

```console
POST _analyze
{
  "tokenizer": "keyword",
  "text": "New York"
}
```

The above sentence would produce the following term:

```text
[ New York ]
```


## Combine with token filters [analysis-keyword-tokenizer-token-filters]

You can combine the `keyword` tokenizer with token filters to normalise structured data, such as product IDs or email addresses.

For example, the following [analyze API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-analyze) request uses the `keyword` tokenizer and [`lowercase`](/reference/text-analysis/analysis-lowercase-tokenfilter.md) filter to convert an email address to lowercase.

```console
POST _analyze
{
  "tokenizer": "keyword",
  "filter": [ "lowercase" ],
  "text": "john.SMITH@example.COM"
}
```

The request produces the following token:

```text
[ john.smith@example.com ]
```


## Configuration [_configuration_11]

The `keyword` tokenizer accepts the following parameters:

`buffer_size`
:   The number of characters read into the term buffer in a single pass. Defaults to `256`. The term buffer will grow by this size until all the text has been consumed. It is advisable not to change this setting.

