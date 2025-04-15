---
navigation_title: "Simple pattern"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-simplepattern-tokenizer.html
---

# Simple pattern tokenizer [analysis-simplepattern-tokenizer]


The `simple_pattern` tokenizer uses a regular expression to capture matching text as terms. The set of regular expression features it supports is more limited than the [`pattern`](/reference/text-analysis/analysis-pattern-tokenizer.md) tokenizer, but the tokenization is generally faster.

This tokenizer does not support splitting the input on a pattern match, unlike the [`pattern`](/reference/text-analysis/analysis-pattern-tokenizer.md) tokenizer. To split on pattern matches using the same restricted regular expression subset, see the [`simple_pattern_split`](/reference/text-analysis/analysis-simplepatternsplit-tokenizer.md) tokenizer.

This tokenizer uses [Lucene regular expressions](https://lucene.apache.org/core/10_0_0/core/org/apache/lucene/util/automaton/RegExp.html). For an explanation of the supported features and syntax, see [Regular Expression Syntax](/reference/query-languages/query-dsl/regexp-syntax.md).

The default pattern is the empty string, which produces no terms. This tokenizer should always be configured with a non-default pattern.


## Configuration [_configuration_17]

The `simple_pattern` tokenizer accepts the following parameters:

`pattern`
:   [Lucene regular expression](https://lucene.apache.org/core/10_0_0/core/org/apache/lucene/util/automaton/RegExp.html), defaults to the empty string.


## Example configuration [_example_configuration_11]

This example configures the `simple_pattern` tokenizer to produce terms that are three-digit numbers

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
          "type": "simple_pattern",
          "pattern": "[0123456789]{3}"
        }
      }
    }
  }
}

POST my-index-000001/_analyze
{
  "analyzer": "my_analyzer",
  "text": "fd-786-335-514-x"
}
```

The above example produces these terms:

```text
[ 786, 335, 514 ]
```

