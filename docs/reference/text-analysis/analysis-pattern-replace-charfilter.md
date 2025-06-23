---
navigation_title: "Pattern replace"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-pattern-replace-charfilter.html
---

# Pattern replace character filter [analysis-pattern-replace-charfilter]


The `pattern_replace` character filter uses a regular expression to match characters which should be replaced with the specified replacement string. The replacement string can refer to capture groups in the regular expression.

::::{admonition} Beware of Pathological Regular Expressions
:class: warning

The pattern replace character filter uses [Java Regular Expressions](https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.md).

A badly written regular expression could run very slowly or even throw a StackOverflowError and cause the node it is running on to exit suddenly.

Read more about [pathological regular expressions and how to avoid them](https://www.regular-expressions.info/catastrophic.html).

::::



## Configuration [_configuration_23]

The `pattern_replace` character filter accepts the following parameters:

`pattern`
:   A [Java regular expression](https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.md). Required.

`replacement`
:   The replacement string, which can reference capture groups using the `$1`..`$9` syntax, as explained [here](https://docs.oracle.com/javase/8/docs/api/java/util/regex/Matcher.md#appendReplacement-java.lang.StringBuffer-java.lang.String-).

`flags`
:   Java regular expression [flags](https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.md#field.summary). Flags should be pipe-separated, eg `"CASE_INSENSITIVE|COMMENTS"`.


## Example configuration [_example_configuration_15]

In this example, we configure the `pattern_replace` character filter to replace any embedded dashes in numbers with underscores, i.e `123-456-789` → `123_456_789`:

```console
PUT my-index-000001
{
  "settings": {
    "analysis": {
      "analyzer": {
        "my_analyzer": {
          "tokenizer": "standard",
          "char_filter": [
            "my_char_filter"
          ]
        }
      },
      "char_filter": {
        "my_char_filter": {
          "type": "pattern_replace",
          "pattern": "(\\d+)-(?=\\d)",
          "replacement": "$1_"
        }
      }
    }
  }
}

POST my-index-000001/_analyze
{
  "analyzer": "my_analyzer",
  "text": "My credit card is 123-456-789"
}
```

The above example produces the following terms:

```text
[ My, credit, card, is, 123_456_789 ]
```

::::{warning}
Using a replacement string that changes the length of the original text will work for search purposes, but will result in incorrect highlighting, as can be seen in the following example.
::::


This example inserts a space whenever it encounters a lower-case letter followed by an upper-case letter (i.e. `fooBarBaz` → `foo Bar Baz`), allowing camelCase words to be queried individually:

```console
PUT my-index-000001
{
  "settings": {
    "analysis": {
      "analyzer": {
        "my_analyzer": {
          "tokenizer": "standard",
          "char_filter": [
            "my_char_filter"
          ],
          "filter": [
            "lowercase"
          ]
        }
      },
      "char_filter": {
        "my_char_filter": {
          "type": "pattern_replace",
          "pattern": "(?<=\\p{Lower})(?=\\p{Upper})",
          "replacement": " "
        }
      }
    }
  },
  "mappings": {
    "properties": {
      "text": {
        "type": "text",
        "analyzer": "my_analyzer"
      }
    }
  }
}

POST my-index-000001/_analyze
{
  "analyzer": "my_analyzer",
  "text": "The fooBarBaz method"
}
```

The above returns the following terms:

```text
[ the, foo, bar, baz, method ]
```

Querying for `bar` will find the document correctly, but highlighting on the result will produce incorrect highlights, because our character filter changed the length of the original text:

```console
PUT my-index-000001/_doc/1?refresh
{
  "text": "The fooBarBaz method"
}

GET my-index-000001/_search
{
  "query": {
    "match": {
      "text": "bar"
    }
  },
  "highlight": {
    "fields": {
      "text": {}
    }
  }
}
```

The output from the above is:

```console-result
{
  "timed_out": false,
  "took": $body.took,
  "_shards": {
    "total": 1,
    "successful": 1,
    "skipped" : 0,
    "failed": 0
  },
  "hits": {
    "total" : {
        "value": 1,
        "relation": "eq"
    },
    "max_score": 0.2876821,
    "hits": [
      {
        "_index": "my-index-000001",
        "_id": "1",
        "_score": 0.2876821,
        "_source": {
          "text": "The fooBarBaz method"
        },
        "highlight": {
          "text": [
            "The foo<em>Ba</em>rBaz method" <1>
          ]
        }
      }
    ]
  }
}
```

1. Note the incorrect highlight.


