---
navigation_title: "Pattern"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-pattern-analyzer.html
---

# Pattern analyzer [analysis-pattern-analyzer]


The `pattern` analyzer uses a regular expression to split the text into terms. The regular expression should match the **token separators**  not the tokens themselves. The regular expression defaults to `\W+` (or all non-word characters).

::::{admonition} Beware of Pathological Regular Expressions
:class: warning

The pattern analyzer uses [Java Regular Expressions](https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.md).

A badly written regular expression could run very slowly or even throw a StackOverflowError and cause the node it is running on to exit suddenly.

Read more about [pathological regular expressions and how to avoid them](https://www.regular-expressions.info/catastrophic.html).

::::



## Example output [_example_output_3]

```console
POST _analyze
{
  "analyzer": "pattern",
  "text": "The 2 QUICK Brown-Foxes jumped over the lazy dog's bone."
}
```

The above sentence would produce the following terms:

```text
[ the, 2, quick, brown, foxes, jumped, over, the, lazy, dog, s, bone ]
```


## Configuration [_configuration_4]

The `pattern` analyzer accepts the following parameters:

`pattern`
:   A [Java regular expression](https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.md), defaults to `\W+`.

`flags`
:   Java regular expression [flags](https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.md#field.summary). Flags should be pipe-separated, eg `"CASE_INSENSITIVE|COMMENTS"`.

`lowercase`
:   Should terms be lowercased or not. Defaults to `true`.

`stopwords`
:   A pre-defined stop words list like `_english_` or an array containing a list of stop words. Defaults to `_none_`.

`stopwords_path`
:   The path to a file containing stop words.

See the [Stop Token Filter](/reference/text-analysis/analysis-stop-tokenfilter.md) for more information about stop word configuration.


## Example configuration [_example_configuration_3]

In this example, we configure the `pattern` analyzer to split email addresses on non-word characters or on underscores (`\W|_`), and to lower-case the result:

```console
PUT my-index-000001
{
  "settings": {
    "analysis": {
      "analyzer": {
        "my_email_analyzer": {
          "type":      "pattern",
          "pattern":   "\\W|_", <1>
          "lowercase": true
        }
      }
    }
  }
}

POST my-index-000001/_analyze
{
  "analyzer": "my_email_analyzer",
  "text": "John_Smith@foo-bar.com"
}
```

1. The backslashes in the pattern need to be escaped when specifying the pattern as a JSON string.


The above example produces the following terms:

```text
[ john, smith, foo, bar, com ]
```


### CamelCase tokenizer [_camelcase_tokenizer]

The following more complicated example splits CamelCase text into tokens:

```console
PUT my-index-000001
{
  "settings": {
    "analysis": {
      "analyzer": {
        "camel": {
          "type": "pattern",
          "pattern": "([^\\p{L}\\d]+)|(?<=\\D)(?=\\d)|(?<=\\d)(?=\\D)|(?<=[\\p{L}&&[^\\p{Lu}]])(?=\\p{Lu})|(?<=\\p{Lu})(?=\\p{Lu}[\\p{L}&&[^\\p{Lu}]])"
        }
      }
    }
  }
}

GET my-index-000001/_analyze
{
  "analyzer": "camel",
  "text": "MooseX::FTPClass2_beta"
}
```

The above example produces the following terms:

```text
[ moose, x, ftp, class, 2, beta ]
```

The regex above is easier to understand as:

```text
  ([^\p{L}\d]+)                 # swallow non letters and numbers,
| (?<=\D)(?=\d)                 # or non-number followed by number,
| (?<=\d)(?=\D)                 # or number followed by non-number,
| (?<=[ \p{L} && [^\p{Lu}]])    # or lower case
  (?=\p{Lu})                    #   followed by upper case,
| (?<=\p{Lu})                   # or upper case
  (?=\p{Lu}                     #   followed by upper case
    [\p{L}&&[^\p{Lu}]]          #   then lower case
  )
```


## Definition [_definition_3]

The `pattern` analyzer consists of:

Tokenizer
:   * [Pattern Tokenizer](/reference/text-analysis/analysis-pattern-tokenizer.md)


Token Filters
:   * [Lower Case Token Filter](/reference/text-analysis/analysis-lowercase-tokenfilter.md)
* [Stop Token Filter](/reference/text-analysis/analysis-stop-tokenfilter.md) (disabled by default)


If you need to customize the `pattern` analyzer beyond the configuration parameters then you need to recreate it as a `custom` analyzer and modify it, usually by adding token filters. This would recreate the built-in `pattern` analyzer and you can use it as a starting point for further customization:

```console
PUT /pattern_example
{
  "settings": {
    "analysis": {
      "tokenizer": {
        "split_on_non_word": {
          "type":       "pattern",
          "pattern":    "\\W+" <1>
        }
      },
      "analyzer": {
        "rebuilt_pattern": {
          "tokenizer": "split_on_non_word",
          "filter": [
            "lowercase"       <2>
          ]
        }
      }
    }
  }
}
```

1. The default pattern is `\W+` which splits on non-word characters and this is where you’d change it.
2. You’d add other token filters after `lowercase`.


