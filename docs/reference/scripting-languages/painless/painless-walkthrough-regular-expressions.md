---
applies_to:
  stack: ga
  serverless: ga
products:
  - id: painless
---

# Regular expressions [modules-scripting-painless-regex]

::::{note}
Regexes are enabled by default by the setting `script.painless.regex.enabled` which has a default value of `limited` the default. This enables the use of regular expressions but limits their complexity. Innocuous looking regexes can have sometimes have adverse performance and stack depth behavior, but they still remain a powerful tool. In addition to `limited`, you can set `script.painless.regex.enabled` to `true` in `elasticsearch.yml` to enable regular expressions without limiting them.
::::


Painless’s native support for regular expressions has syntax constructs:

* `/pattern/`: Pattern literals create patterns. This is the only way to create a pattern in painless. The pattern inside the `/’s are just [Java regular expressions](https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.md). See [Pattern flags](/reference/scripting-languages/painless/painless-regexes.md#pattern-flags) for more.
* `=~`: The find operator return a `boolean`, `true` if a subsequence of the text matches, `false` otherwise.
* `==~`: The match operator returns a `boolean`, `true` if the text matches, `false` if it doesn’t.

Using the find operator (`=~`) you can update all hockey players with "b" in their last name:

```console
POST hockey/_update_by_query
{
  "script": {
    "lang": "painless",
    "source": """
      if (ctx._source.last =~ /b/) {
        ctx._source.last += "matched";
      } else {
        ctx.op = "noop";
      }
    """
  }
}
```

Using the match operator (`==~`) you can update all the hockey players whose names start with a consonant and end with a vowel:

```console
POST hockey/_update_by_query
{
  "script": {
    "lang": "painless",
    "source": """
      if (ctx._source.last ==~ /[^aeiou].*[aeiou]/) {
        ctx._source.last += "matched";
      } else {
        ctx.op = "noop";
      }
    """
  }
}
```

You can use the `Pattern.matcher` directly to get a `Matcher` instance and remove all of the vowels in all of their last names:

```console
POST hockey/_update_by_query
{
  "script": {
    "lang": "painless",
    "source": "ctx._source.last = /[aeiou]/.matcher(ctx._source.last).replaceAll('')"
  }
}
```

`Matcher.replaceAll` is just a call to Java’s `Matcher`'s [replaceAll](https://docs.oracle.com/javase/8/docs/api/java/util/regex/Matcher.md#replaceAll-java.lang.String-) method so it supports `$1` and `\1` for replacements:

```console
POST hockey/_update_by_query
{
  "script": {
    "lang": "painless",
    "source": "ctx._source.last = /n([aeiou])/.matcher(ctx._source.last).replaceAll('$1')"
  }
}
```

If you need more control over replacements you can call `replaceAll` on a `CharSequence` with a `Function<Matcher, String>` that builds the replacement. This does not support `$1` or `\1` to access replacements because you already have a reference to the matcher and can get them with `m.group(1)`.

::::{important}
Calling `Matcher.find` inside of the function that builds the replacement is rude and will likely break the replacement process.
::::


This will make all of the vowels in the hockey player’s last names upper case:

```console
POST hockey/_update_by_query
{
  "script": {
    "lang": "painless",
    "source": """
      ctx._source.last = ctx._source.last.replaceAll(/[aeiou]/, m ->
        m.group().toUpperCase(Locale.ROOT))
    """
  }
}
```

Or you can use the `CharSequence.replaceFirst` to make the first vowel in their last names upper case:

```console
POST hockey/_update_by_query
{
  "script": {
    "lang": "painless",
    "source": """
      ctx._source.last = ctx._source.last.replaceFirst(/[aeiou]/, m ->
        m.group().toUpperCase(Locale.ROOT))
    """
  }
}
```

Note: all of the `_update_by_query` examples above could really do with a `query` to limit the data that they pull back. While you could use a [script query](/reference/query-languages/query-dsl/query-dsl-script-query.md) it wouldn’t be as efficient as using any other query because script queries aren’t able to use the inverted index to limit the documents that they have to check.

