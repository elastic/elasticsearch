---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/regexp-syntax.html
---

# Regular expression syntax [regexp-syntax]

A [regular expression](https://en.wikipedia.org/wiki/Regular_expression) is a way to match patterns in data using placeholder characters, called operators.

{{es}} supports regular expressions in the following queries:

* [`regexp`](/reference/query-languages/query-dsl/query-dsl-regexp-query.md)
* [`query_string`](/reference/query-languages/query-dsl/query-dsl-query-string-query.md)

{{es}} uses [Apache Lucene](https://lucene.apache.org/core/)'s regular expression engine to parse these queries.


## Reserved characters [regexp-reserved-characters]

Lucene’s regular expression engine supports all Unicode characters. However, the following characters are reserved as operators:

```
. ? + * | { } [ ] ( ) " \
```
Depending on the [optional operators](#regexp-optional-operators) enabled, the following characters may also be reserved:

```
# @ & < >  ~
```
To use one of these characters literally, escape it with a preceding backslash or surround it with double quotes. For example:

```
\@                  # renders as a literal '@'
\\                  # renders as a literal '\'
"john@smith.com"    # renders as 'john@smith.com'
```
::::{note}
The backslash is an escape character in both JSON strings and regular expressions. You need to escape both backslashes in a query, unless you use a language client, which takes care of this. For example, the string `a\b` needs to be indexed as `"a\\b"`:

```console
PUT my-index-000001/_doc/1
{
  "my_field": "a\\b"
}
```

This document matches the following `regexp` query:

```console
GET my-index-000001/_search
{
  "query": {
    "regexp": {
      "my_field.keyword": "a\\\\.*"
    }
  }
}
```
% TEST[continued]

::::



## Standard operators [regexp-standard-operators]

Lucene’s regular expression engine does not use the [Perl Compatible Regular Expressions (PCRE)](https://en.wikipedia.org/wiki/Perl_Compatible_Regular_Expressions) library, but it does support the following standard operators.

`.`
:   Matches any character. For example:

```
ab.     # matches 'aba', 'abb', 'abz', etc.
```

`?`
:   Repeat the preceding character zero or one times. Often used to make the preceding character optional. For example:

```
abc?     # matches 'ab' and 'abc'
```

`+`
:   Repeat the preceding character one or more times. For example:

```
ab+     # matches 'ab', 'abb', 'abbb', etc.
```

`*`
:   Repeat the preceding character zero or more times. For example:

```
ab*     # matches 'a', 'ab', 'abb', 'abbb', etc.
```

`{}`
:   Minimum and maximum number of times the preceding character can repeat. For example:

```
a{{2}}    # matches 'aa'
a{2,4}  # matches 'aa', 'aaa', and 'aaaa'
a{2,}   # matches 'a` repeated two or more times
```

`|`
:   OR operator. The match will succeed if the longest pattern on either the left side OR the right side matches. For example:

```
abc|xyz  # matches 'abc' and 'xyz'
```

`( … )`
:   Forms a group. You can use a group to treat part of the expression as a single character. For example:

```
abc(def)?  # matches 'abc' and 'abcdef' but not 'abcd'
```

`[ … ]`
:   Match one of the characters in the brackets. For example:

```
[abc]   # matches 'a', 'b', 'c'
```
Inside the brackets, `-` indicates a range unless `-` is the first character or escaped. For example:

```
[a-c]   # matches 'a', 'b', or 'c'
[-abc]  # '-' is first character. Matches '-', 'a', 'b', or 'c'
[abc\-] # Escapes '-'. Matches 'a', 'b', 'c', or '-'
```
A `^` before a character in the brackets negates the character or range. For example:

```
[^abc]      # matches any character except 'a', 'b', or 'c'
[^a-c]      # matches any character except 'a', 'b', or 'c'
[^-abc]     # matches any character except '-', 'a', 'b', or 'c'
[^abc\-]    # matches any character except 'a', 'b', 'c', or '-'
```

:::{note}
Character range classes such as `[a-c]` do not behave as expected when using `case_insensitive: true` — they remain case sensitive. For example, `[a-c]+` with `case_insensitive: true` will match strings containing only the characters 'a', 'b', and 'c', but not 'A', 'B', or 'C'. Use `[a-zA-Z]` to match both uppercase and lowercase characters.

This is due to a known limitation in Lucene's regular expression engine.
See [Lucene issue #14378](https://github.com/apache/lucene/issues/14378) for details.
:::


## Optional operators [regexp-optional-operators]

You can use the `flags` parameter to enable more optional operators for Lucene’s regular expression engine.

To enable multiple operators, use a `|` separator. For example, a `flags` value of `COMPLEMENT|INTERVAL` enables the `COMPLEMENT` and `INTERVAL` operators.


### Valid values [_valid_values]

`ALL` (Default)
:   Enables all optional operators.

`""` (empty string)
:   Alias for the `ALL` value.

`COMPLEMENT`
:   Enables the `~` operator. You can use `~` to negate the shortest following pattern. For example:

```
a~bc   # matches 'adc' and 'aec' but not 'abc'
```

`EMPTY`
:   Enables the `#` (empty language) operator. The `#` operator doesn’t match any string, not even an empty string.

If you create regular expressions by programmatically combining values, you can pass `#` to specify "no string." This lets you avoid accidentally matching empty strings or other unwanted strings. For example:

```
#|abc  # matches 'abc' but nothing else, not even an empty string
```

`INTERVAL`
:   Enables the `<>` operators. You can use `<>` to match a numeric range. For example:

```
foo<1-100>      # matches 'foo1', 'foo2' ... 'foo99', 'foo100'
foo<01-100>     # matches 'foo01', 'foo02' ... 'foo99', 'foo100'
```

`INTERSECTION`
:   Enables the `&` operator, which acts as an AND operator. The match will succeed if patterns on both the left side AND the right side matches. For example:

```
aaa.+&.+bbb  # matches 'aaabbb'
```

`ANYSTRING`
:   Enables the `@` operator. You can use `@` to match any entire string.

You can combine the `@` operator with `&` and `~` operators to create an "everything except" logic. For example:

```
@&~(abc.+)  # matches everything except terms beginning with 'abc'
```

`NONE`
:   Disables all optional operators.


## Unsupported operators [regexp-unsupported-operators]

Lucene’s regular expression engine does not support anchor operators, such as `^` (beginning of line) or `$` (end of line). To match a term, the regular expression must match the entire string.

