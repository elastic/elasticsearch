---
applies_to:
  stack: ga 9.6
  serverless: ga
navigation_title: "Roaring bitmap"
description: "Use the `roaring_bitmap` aggregation to return the exact distinct integer or long values matching a search as a base64-encoded portable roaring bitmap."
---

# Roaring bitmap aggregation [search-aggregations-metrics-roaring-bitmap-aggregation]

The `roaring_bitmap` metrics aggregation returns the exact distinct values of an `integer` or `long` field across all matching documents. It serializes those values as a [roaring bitmap](https://roaringbitmap.org) and returns the bytes as a base64-encoded string.

Use this aggregation when you need the set of values itself, rather than only its size. The result uses the same portable format accepted by the [`bitmap_terms` query](/reference/query-languages/query-dsl/query-dsl-bitmap-terms-query.md), so you can pass the result directly into a later search or consume it with a compatible roaring bitmap library.

Values must be non-negative: `0` to `2^31 - 1` for `integer` fields, and `0` to `2^63 - 1` for `long` fields.
If a matching document contains a negative value, the search request fails.

::::{tip}
Mapping the field with [`index_terms: true`](/reference/elasticsearch/mapping-reference/number.md#index-terms-mapping-param) lets the aggregation read the distinct values directly from the inverted index, which is much faster on searches that match every document. See [Speed up collection with `index_terms`](#roaring-bitmap-aggregation-index-terms). The aggregation also works on the default BKD-indexed numeric fields.
::::

## Example request [roaring-bitmap-aggregation-example]

1. Create an index with a `long` product identifier.

    ```console
    PUT product-catalog
    {
      "mappings": {
        "properties": {
          "product_id": {
            "type": "long",
            "index_terms": true
          },
          "featured": {
            "type": "boolean"
          }
        }
      }
    }
    ```

2. Index some products.

    ```console
    POST product-catalog/_bulk?refresh=true
    { "index": {} }
    { "product_id": 1, "featured": true }
    { "index": {} }
    { "product_id": 2, "featured": false }
    { "index": {} }
    { "product_id": 3, "featured": true }
    { "index": {} }
    { "product_id": 4, "featured": false }
    { "index": {} }
    { "product_id": 5, "featured": true }
    ```
    % TEST[continued]

3. Aggregate the product IDs from the documents where `featured` is `true`.

    ```console
    GET product-catalog/_search
    {
      "size": 0,
      "query": {
        "term": {
          "featured": true
        }
      },
      "aggs": {
        "product_ids": {
          "roaring_bitmap": {
            "field": "product_id"
          }
        }
      }
    }
    ```
    % TEST[continued]

The response contains a portable 64-bit roaring bitmap holding the distinct values `1`, `3`, and `5`:

```console-result
{
  ...
  "aggregations": {
    "product_ids": {
      "value": "AQAAAAAAAAAAAAAAOjAAAAEAAAAAAAIAEAAAAAEAAwAFAA=="
    }
  }
}
```
% TESTRESPONSE[s/\.\.\./"took": $body.took,"timed_out": false,"_shards": $body._shards,"hits": $body.hits,/]

## Parameters [roaring-bitmap-aggregation-parameters]

`field`
:   (Required, string) Mapped `integer` or `long` field whose values are added to the bitmap. Scripts and runtime fields are not supported because the mapped field type determines the bitmap width.

`missing`
:   (Optional, integer) Value to use for documents that do not have a value for `field`. The value must be non-negative and fit the field's `integer` or `long` type. By default, documents without a value are ignored.

## Response and bitmap format [roaring-bitmap-aggregation-format]

The field type determines the result format:

* An `integer` field produces a portable 32-bit roaring bitmap.
* A `long` field produces a portable 64-bit roaring bitmap.

In JSON responses, the `value` field is the base64 encoding of the serialized bitmap. Duplicate field values, including duplicates across documents and shards, appear only once in the result.

If the field is mapped but no matching document has a value, `value` contains an empty bitmap. If the field is unmapped, `value` is `null`.

Sampling is not supported because a bitmap produced from a sample cannot be scaled into the complete set of values.

## Placement in the aggregation tree [roaring-bitmap-aggregation-placement]

A bitmap holds a complete value set, so one bitmap per bucket would multiply memory use by the number of buckets. The aggregation therefore rejects placement inside an aggregation that produces more than one bucket, such as [`terms`](/reference/aggregations/search-aggregations-bucket-terms-aggregation.md) or [`histogram`](/reference/aggregations/search-aggregations-bucket-histogram-aggregation.md), and returns an error instead.

Single-bucket parents are allowed. Wrapping the aggregation in a [`filter`](/reference/aggregations/search-aggregations-bucket-filter-aggregation.md) aggregation is the usual way to restrict a bitmap to a subset of documents, and several sibling filters give one bitmap per subset in a single request:

```json
{
  "size": 0,
  "aggs": {
    "featured": {
      "filter": { "term": { "featured": true } },
      "aggs": { "ids": { "roaring_bitmap": { "field": "product_id" } } }
    },
    "not_featured": {
      "filter": { "term": { "featured": false } },
      "aggs": { "ids": { "roaring_bitmap": { "field": "product_id" } } }
    }
  }
}
```

A search-level `query` also restricts the bitmap, as in the [example request](#roaring-bitmap-aggregation-example) above.

### Use the result with `bitmap_terms` [roaring-bitmap-aggregation-use-result]

Supply the returned `value` directly to `bitmap_terms` on a field of the same type:

```json
{
  "query": {
    "bitmap_terms": {
      "field": "product_id",
      "value": "<aggregations.product_ids.value>"
    }
  }
}
```

The field types must match. Use a bitmap produced from an `integer` field with another `integer` field, and a bitmap produced from a `long` field with another `long` field.

### Deserialize the result in Java [roaring-bitmap-aggregation-java]

For a `long` field, decode the base64 value and use `Roaring64NavigableMap#deserializePortable`:

```java
byte[] bytes = Base64.getDecoder().decode(value);
Roaring64NavigableMap bitmap = new Roaring64NavigableMap();
try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
    bitmap.deserializePortable(in);
}
```

::::{warning}
`Roaring64NavigableMap#deserialize` uses a different, class-wide serialization mode and is not guaranteed to read the portable 64-bit format. Use `deserializePortable` for `long` results.
::::

For an `integer` field, deserialize the value as a 32-bit `RoaringBitmap`:

```java
byte[] bytes = Base64.getDecoder().decode(value);
RoaringBitmap bitmap = new RoaringBitmap();
bitmap.deserialize(ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN));
```

See [Bitmap format](/reference/query-languages/query-dsl/query-dsl-bitmap-terms-query.md#bitmap-terms-format) for compatible libraries and serialization details.

## Speed up collection with `index_terms` [roaring-bitmap-aggregation-index-terms]

By default the aggregation reads the [`doc_values`](/reference/elasticsearch/mapping-reference/doc-values.md) of every matching document, so its cost grows with the number of matching documents. When the field is mapped with [`index_terms: true`](/reference/elasticsearch/mapping-reference/number.md#index-terms-mapping-param), {{es}} can read the distinct values straight from the terms dictionary instead, where each value appears exactly once. The cost then grows with the number of distinct values rather than the number of documents, which is a large saving when many documents share a value.

This optimization applies when:

* The field is mapped with `index_terms: true`.
* The search matches every document, either by omitting `query` or by using [`match_all`](/reference/query-languages/query-dsl/query-dsl-match-all-query.md). Any other query reads doc values, because the terms dictionary does not record which documents hold each value.
* The aggregation is at the top level. A single-bucket parent such as `filter` is permitted but reads doc values, because the terms dictionary does not record which documents the parent passed through.
* The aggregation does not use the `missing` parameter, whose substituted value is not present in the index.

## Choosing an aggregation [roaring-bitmap-aggregation-choose]

Use `roaring_bitmap` when you need the exact distinct set of numeric values. Its memory use depends on the number and distribution of values; dense or run-like sets usually compress more efficiently than sparse 64-bit sets.

Memory use and response size depend on the distinct values, not the number of matching documents. Very large exact sets can trip the request circuit breaker and return a `circuit_breaking_exception`. Set the search request's `timeout` when it might scan a very large match set.

If you only need a count:

* Use the [`cardinality` aggregation](/reference/aggregations/search-aggregations-metrics-cardinality-aggregation.md) for an approximate distinct count with bounded memory use.
* Use the [`value_count` aggregation](/reference/aggregations/search-aggregations-metrics-valuecount-aggregation.md) to count every extracted value without removing duplicates.
