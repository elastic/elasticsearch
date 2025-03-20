---
navigation_title: "Pattern capture"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-pattern-capture-tokenfilter.html
---

# Pattern capture token filter [analysis-pattern-capture-tokenfilter]


The `pattern_capture` token filter, unlike the `pattern` tokenizer, emits a token for every capture group in the regular expression. Patterns are not anchored to the beginning and end of the string, so each pattern can match multiple times, and matches are allowed to overlap.

::::{admonition} Beware of Pathological Regular Expressions
:class: warning

The pattern capture token filter uses [Java Regular Expressions](https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.md).

A badly written regular expression could run very slowly or even throw a StackOverflowError and cause the node it is running on to exit suddenly.

Read more about [pathological regular expressions and how to avoid them](https://www.regular-expressions.info/catastrophic.md).

::::


For instance a pattern like :

```text
"(([a-z]+)(\d*))"
```

when matched against:

```text
"abc123def456"
```

would produce the tokens: [ `abc123`, `abc`, `123`, `def456`, `def`, `456` ]

If `preserve_original` is set to `true` (the default) then it would also emit the original token: `abc123def456`.

This is particularly useful for indexing text like camel-case code, eg `stripHTML` where a user may search for `"strip html"` or `"striphtml"`:

```console
PUT test
{
   "settings" : {
      "analysis" : {
         "filter" : {
            "code" : {
               "type" : "pattern_capture",
               "preserve_original" : true,
               "patterns" : [
                  "(\\p{Ll}+|\\p{Lu}\\p{Ll}+|\\p{Lu}+)",
                  "(\\d+)"
               ]
            }
         },
         "analyzer" : {
            "code" : {
               "tokenizer" : "pattern",
               "filter" : [ "code", "lowercase" ]
            }
         }
      }
   }
}
```

When used to analyze the text

```java
import static org.apache.commons.lang.StringEscapeUtils.escapeHtml
```

this emits the tokens: [ `import`, `static`, `org`, `apache`, `commons`, `lang`, `stringescapeutils`, `string`, `escape`, `utils`, `escapehtml`, `escape`, `html` ]

Another example is analyzing email addresses:

```console
PUT test
{
   "settings" : {
      "analysis" : {
         "filter" : {
            "email" : {
               "type" : "pattern_capture",
               "preserve_original" : true,
               "patterns" : [
                  "([^@]+)",
                  "(\\p{L}+)",
                  "(\\d+)",
                  "@(.+)"
               ]
            }
         },
         "analyzer" : {
            "email" : {
               "tokenizer" : "uax_url_email",
               "filter" : [ "email", "lowercase",  "unique" ]
            }
         }
      }
   }
}
```

When the above analyzer is used on an email address like:

```text
john-smith_123@foo-bar.com
```

it would produce the following tokens:

```
john-smith_123@foo-bar.com, john-smith_123,
john, smith, 123, foo-bar.com, foo, bar, com
```
Multiple patterns are required to allow overlapping captures, but also means that patterns are less dense and easier to understand.

**Note:** All tokens are emitted in the same position, and with the same character offsets. This means, for example, that a `match` query for `john-smith_123@foo-bar.com` that uses this analyzer will return documents containing any of these tokens, even when using the `and` operator. Also, when combined with highlighting, the whole original token will be highlighted, not just the matching subset. For instance, querying the above email address for `"smith"` would highlight:

```html
  <em>john-smith_123@foo-bar.com</em>
```

not:

```html
  john-<em>smith</em>_123@foo-bar.com
```

