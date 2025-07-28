---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-analysis-predicate-context.html
---

# Analysis Predicate Context [painless-analysis-predicate-context]

Use a painless script to determine whether or not the current token in an analysis chain matches a predicate.

**Variables**

`params` (`Map`, read-only)
:   User-defined parameters passed in as part of the query.

`token.term` (`CharSequence`, read-only)
:   The characters of the current token

`token.position` (`int`, read-only)
:   The position of the current token

`token.positionIncrement` (`int`, read-only)
:   The position increment of the current token

`token.positionLength` (`int`, read-only)
:   The position length of the current token

`token.startOffset` (`int`, read-only)
:   The start offset of the current token

`token.endOffset` (`int`, read-only)
:   The end offset of the current token

`token.type` (`String`, read-only)
:   The type of the current token

`token.keyword` (`boolean`, read-only)
:   Whether or not the current token is marked as a keyword

**Return**

`boolean`
:   Whether or not the current token matches the predicate

**API**

The standard [Painless API](https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-api-reference-shared.html) is available.

