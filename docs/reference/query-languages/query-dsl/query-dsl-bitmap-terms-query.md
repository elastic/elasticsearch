---
applies_to:
  stack: ga 9.6
  serverless: ga
navigation_title: "Bitmap terms"
Use the `bitmap_terms` query to match documents whose integer or long field value is contained in a roaring bitmap provided as a base64-encoded string.
---

# Bitmap terms query [query-dsl-bitmap-terms-query]

Returns documents whose `integer` or `long` field value is contained in a [roaring bitmap](https://roaringbitmap.org) that you provide as a base64-encoded string.

Bitmap values must be non-negative: `0` to `2^31 - 1` for `integer` fields, and `0` to `2^63 - 1` for `long` fields. {{es}} rejects a bitmap that contains a negative value. The field itself can still hold negative values; they simply can never be matched by a `bitmap_terms` query.

`bitmap_terms` is the counterpart of the [`terms` query](/reference/query-languages/query-dsl/query-dsl-terms-query.md) for very large sets of numeric values. Rather than listing the values in a JSON array, you build a roaring bitmap on the client, serialize it, and send it as a single compact string.

A common use case is filtering by an externally maintained set of identifiers, such as the product or document IDs a given user is entitled to see.

::::{tip}
For the best performance, map the field with [`index_terms: true`](/reference/elasticsearch/mapping-reference/number.md#index-terms-mapping-param). The query also works on the default BKD-indexed numeric fields.
::::


## Example request [bitmap-terms-query-ex-request]

1. Create an index with a `long` field mapped for set membership filtering.

    ```console
    PUT my-index-000001
    {
      "mappings": {
        "properties": {
          "product_id": {
            "type": "long",
            "index_terms": true
          }
        }
      }
    }
    ```

2. Index a few documents.

    ```console
    POST my-index-000001/_bulk?refresh=true
    { "index": {} }
    { "product_id": 1 }
    { "index": {} }
    { "product_id": 2 }
    { "index": {} }
    { "product_id": 3 }
    { "index": {} }
    { "product_id": 4 }
    { "index": {} }
    { "product_id": 5 }
    ```
    % TEST[continued]

3. Search with a bitmap holding the values `1`, `3`, and `5`. The query returns the three matching documents.

    ```console
    GET my-index-000001/_search
    {
      "query": {
        "bitmap_terms": {
          "field": "product_id",
          "value": "AQAAAAAAAAAAAAAAOjAAAAEAAAAAAAIAEAAAAAEAAwAFAA=="
        }
      }
    }
    ```
    % TEST[continued]


## Top-level parameters for `bitmap_terms` [bitmap-terms-top-level-params]

`field`
:   (Required, string) Field you wish to search. Must be an `integer` or `long` field with [`index`](/reference/elasticsearch/mapping-reference/mapping-index.md) enabled.

`value`
:   (Required, string) Base64-encoded serialized roaring bitmap holding the values to match. The expected bitmap width depends on the type of `field`. See [Bitmap format](#bitmap-terms-format).

`boost`
:   (Optional, float) Floating point number used to decrease or increase the [relevance scores](/reference/query-languages/query-dsl/query-filter-context.md#relevance-scores) of a query. Defaults to `1.0`. Matching documents all receive the same constant score.

`_name`
:   (Optional, string) Name to identify the query for [named queries](/reference/query-languages/query-dsl/query-dsl-bool-query.md).


## Bitmap format [bitmap-terms-format]

The field type determines which roaring bitmap width {{es}} expects:

| Field type | Expected bitmap |
| --- | --- |
| `integer` | 32-bit roaring bitmap |
| `long` | 64-bit roaring bitmap, in the **portable** format |

Elasticsearch rejects a bitmap of the wrong width rather than silently mismatching it: a 32-bit bitmap on a `long` field, or a 64-bit bitmap on an `integer` field, returns an error.

### Generating the bitmap [bitmap-terms-generating-the-bitmap]

Build the bitmap in your client, serialize it, then base64-encode the resulting bytes.

For `long` fields, the portable format is what CRoaring's `roaring64_bitmap_portable_serialize`, Go's `roaring64.Bitmap#WriteTo`, and pyroaring's `BitMap64#serialize` all emit, so clients in those languages need no special handling:

```python
from pyroaring import BitMap64
import base64

bm = BitMap64([1_000_000_000_001, 4_000_000_000_004])
value = base64.b64encode(bm.serialize()).decode()
```

In Java, `Roaring64NavigableMap` defaults to a different layout, so you have to ask for the portable one explicitly:

```java
Roaring64NavigableMap bitmap = new Roaring64NavigableMap();
bitmap.addLong(1_000_000_000_001L);
bitmap.addLong(4_000_000_000_004L);

ByteArrayOutputStream bytes = new ByteArrayOutputStream();
bitmap.serializePortable(new DataOutputStream(bytes));  // not serialize()
String value = Base64.getEncoder().encodeToString(bytes.toByteArray());
```

::::{warning}
For `long` fields, Java's `Roaring64NavigableMap#serialize` writes a non-portable layout that {{es}} cannot read. Use `serializePortable` instead.
::::

For `integer` fields, build a 32-bit bitmap instead. There is only one serialization format at this width, so no special handling is needed in any language:

```python
from pyroaring import BitMap
import base64

bm = BitMap([1, 3, 5])
value = base64.b64encode(bm.serialize()).decode()
```

```java
RoaringBitmap bitmap = RoaringBitmap.bitmapOf(1, 3, 5);
ByteBuffer buffer = ByteBuffer.allocate(bitmap.serializedSizeInBytes()).order(ByteOrder.LITTLE_ENDIAN);
bitmap.serialize(buffer);
String value = Base64.getEncoder().encodeToString(buffer.array());
```


## Advantages over the `terms` query [bitmap-terms-advantages]

Both queries match documents against a set of exact values. On `integer` and `long` fields, `bitmap_terms` is the better choice once that set gets large:

**No cap on the number of values**
:   A `terms` query is limited to [`index.max_terms_count`](/reference/elasticsearch/index-settings/index-modules.md#index-max-terms-count) values, 65,536 by default. A bitmap has no such limit.

**Smaller requests**
:   Roaring bitmaps compress dense integer sets aggressively, so millions of values travel as a compact base64 string instead of a multi-megabyte JSON array.

**Query construction is effectively free**
:   A `terms` query has to parse, sort, and encode every value on every request, and that cost grows with the size of the set, at 100,000 values that overhead alone can account for several milliseconds per request. A bitmap is deserialized once and the query wraps it directly, so construction cost barely moves as the set grows.

**Faster search**
:   The bitmap is intersected with the index in a single ordered pass, instead of looking up each value separately.

**Lower heap usage**
:   The values stay in the bitmap's compressed form rather than being expanded into a list of boxed numbers, and matches are streamed lazily out of the bitmap during execution, so the full set is never materialized into an intermediate structure.

For a set of 100,000 values, `bitmap_terms` can be several times faster than an equivalent `terms` query, and its advantage grows with the number of values.


## Optimization on a sorted index [bitmap-terms-sorted-index]

`bitmap_terms` is especially fast when the index is [sorted](/reference/elasticsearch/index-settings/sorting.md) in ascending order on the field being queried. This applies to both index structures: fields on the default BKD mapping and fields mapped with `index_terms: true`.

Under such a sort, value order is document order, so matches come out already ordered as the query walks the bitmap. {{es}} can then stream them and stop as soon as it has collected enough hits, instead of building the complete set of matching documents up front.

The optimization applies when:

* The index is sorted in ascending order on the queried field.
* The field is single-valued. Documents that have no value at all are fine.

If either condition does not hold, the query falls back to collecting all matches first, but results are the same either way.

```console
PUT my-index-000002
{
  "settings": {
    "index": {
      "sort.field": "product_id",
      "sort.order": "asc"
    }
  },
  "mappings": {
    "properties": {
      "product_id": {
        "type": "integer",
        "index_terms": true
      }
    }
  }
}
```

### Lower `track_total_hits` to get the full benefit [bitmap-terms-track-total-hits]

Early termination only helps if the search does not need a full hit count. By default {{es}} counts matches up to 10,000, so the scan has to produce that many documents before it can stop, which means the full early-termination benefit is not realized.

Set `track_total_hits` to `false` so the query can terminate as soon as `size` hits have been collected:

```console
GET my-index-000002/_search
{
  "size": 10,
  "track_total_hits": false,
  "query": {
    "bitmap_terms": {
      "field": "product_id",
      "value": "OjAAAAEAAAAAAAIAEAAAAAEAAwAFAA=="
    }
  }
}
```
% TEST[continued]

A small number, such as `"track_total_hits": 10`, also works if you want a lower bound on the count.

On an unsorted index this setting makes no difference to `bitmap_terms`, because the full set of matches is built before any hits are collected.
