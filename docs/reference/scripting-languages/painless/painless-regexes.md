---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-regexes.html
---

# Regexes [painless-regexes]

Regular expression constants are directly supported. To ensure fast performance, this is the only mechanism for creating patterns. Regular expressions are always constants and compiled efficiently a single time.

```painless
Pattern p = /[aeiou]/
```

::::{warning}
A poorly written regular expression can significantly slow performance. If possible, avoid using regular expressions, particularly in frequently run scripts.
::::


## Pattern flags [pattern-flags]

You can define flags on patterns in Painless by adding characters after the trailing `/` like `/foo/i` or `/foo \w #comment/iUx`. Painless exposes all of the flags from Java’s [ Pattern class](https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.md) using these characters:

| Character | Java Constant | Example |
| --- | --- | --- |
| `c` | CANON_EQ | `'å' ==~ /å/c` (open in hex editor to see) |
| `i` | CASE_INSENSITIVE | `'A' ==~ /a/i` |
| `l` | LITERAL | `'[a]' ==~ /[a]/l` |
| `m` | MULTILINE | `'a\nb\nc' =~ /^b$/m` |
| `s` | DOTALL (aka single line) | `'a\nb\nc' =~ /.b./s` |
| `U` | UNICODE_CHARACTER_CLASS | `'Ɛ' ==~ /\\w/U` |
| `u` | UNICODE_CASE | `'Ɛ' ==~ /ɛ/iu` |
| `x` | COMMENTS (aka extended) | `'a' ==~ /a #comment/x` |


