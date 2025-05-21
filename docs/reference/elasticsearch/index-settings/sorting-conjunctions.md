---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/index-modules-index-sorting-conjunctions.html
---

# Use index sorting to speed up conjunctions [index-modules-index-sorting-conjunctions]

Index sorting can be useful in order to organize Lucene doc ids (not to be conflated with `_id`) in a way that makes conjunctions (a AND b AND … ) more efficient. In order to be efficient, conjunctions rely on the fact that if any clause does not match, then the entire conjunction does not match. By using index sorting, we can put documents that do not match together, which will help skip efficiently over large ranges of doc IDs that do not match the conjunction.

This trick only works with low-cardinality fields. A rule of thumb is that you should sort first on fields that both have a low cardinality and are frequently used for filtering. The sort order (`asc` or `desc`) does not matter as we only care about putting values that would match the same clauses close to each other.

For instance if you were indexing cars for sale, it might be interesting to sort by fuel type, body type, make, year of registration and finally mileage.
