---
navigation_title: "Simple pattern split"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-simplepatternsplit-tokenizer.html
---

# Simple pattern split tokenizer [analysis-simplepatternsplit-tokenizer]


The `simple_pattern_split` tokenizer uses a regular expression to split the input into terms at pattern matches. The set of regular expression features it supports is more limited than the [`pattern`](/reference/text-analysis/analysis-pattern-tokenizer.md) tokenizer, but the tokenization is generally faster.

This tokenizer does not produce terms from the matches themselves. To produce terms from matches using patterns in the same restricted regular expression subset, see the [`simple_pattern`](/reference/text-analysis/analysis-simplepattern-tokenizer.md) tokenizer.

This tokenizer uses [Lucene regular expressions](https://lucene.apache.org/core/10_0_0/core/org/apache/lucene/util/automaton/RegExp.html). For an explanation of the supported features and syntax, see [Regular Expression Syntax](/reference/query-languages/query-dsl/regexp-syntax.md).

The default pattern is the empty string, which produces one term containing the full input. This tokenizer should always be configured with a non-default pattern.


## Configuration [_configuration_18]

The `simple_pattern_split` tokenizer accepts the following parameters:

`pattern`
:   A [Lucene regular expression](https://lucene.apache.org/core/10_0_0/core/org/apache/lucene/util/automaton/RegExp.html), defaults to the empty string.


## Example configuration [_example_configuration_12]

This example configures the `simple_pattern_split` tokenizer to split the input text on underscores.

```console
PUT my-index-000001
{
  "settings": {
    "analysis": {
      "analyzer": {
        "my_analyzer": {
          "tokenizer": "my_tokenizer"
        }
      },
      "tokenizer": {
        "my_tokenizer": {
          "type": "simple_pattern_split",
          "pattern": "_"
        }
      }
    }
  }
}

POST my-index-000001/_analyze
{
  "analyzer": "my_analyzer",
  "text": "an_underscored_phrase"
}
```

The above example produces these terms:

```text
[ an, underscored, phrase ]
```

