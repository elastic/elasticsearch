# Root Cause Analysis: Shard Failure on `search-auctions-v5` After Upgrade to 9.3.1

## Summary

Following an upgrade from 8.19.12 to 9.3.1, the index `search-auctions-v5` experienced repeated shard failures on indexing operations. The failures were caused by an incompatibility between the index's original creation version (8.10.2) and the `bbq_disk` vector index type, which does not support the `VectorSimilarityFunction.COSINE` encoding used by indices created before Elasticsearch 8.12.

---

## Timeline

| Event | Details |
|---|---|
| Index originally created | Elasticsearch 8.10.2 (`index.version.created = 8100299`) |
| Index carried forward via snapshot restore | Preserved original creation version through successive cluster upgrades |
| `bbq_disk` index type applied to mapping | At some point on the 8.19.12 or 9.3.1 cluster |
| Cluster upgraded to 9.3.1 | `bbq_disk` now routes to `IVFVectorsWriter` |
| Shard failures begin | Every document write to a vector field triggers the failure |

---

## Technical Detail

Elasticsearch 8.12 introduced a change (`NORMALIZED_VECTOR_COSINE`, index version `8_500_005`) that altered how `similarity: cosine` is stored for floating-point dense vector fields. On indices created at or after 8.12, Elasticsearch normalizes vectors at index time and stores the similarity function as `DOT_PRODUCT` in Lucene's segment metadata (`FieldInfo`). This allows cosine similarity to be computed efficiently as a dot product over pre-normalized vectors.

For indices created **before** 8.12 (creation version < `8_500_005`), the original Lucene `COSINE` similarity function is stored directly in the segment metadata. This value is part of the immutable index state and is not updated when the cluster is upgraded.

In Elasticsearch 9.x, the `bbq_disk` index type was introduced, backed by an IVF (Inverted File Index) vector codec (`IVFVectorsWriter`). This codec was written to assume all cosine-similarity fields have already been normalized to `DOT_PRODUCT` — consistent with the post-8.12 behavior. It explicitly rejects `VectorSimilarityFunction.COSINE` with an `IllegalArgumentException`.

When a document was indexed into `search-auctions-v5`, Elasticsearch used `index.version.created = 8100299` to determine the similarity function, correctly resolved it to `COSINE` (per the pre-8.12 contract), and passed that `FieldInfo` to `IVFVectorsWriter.addField`. The writer threw immediately, causing the shard to be marked failed.

**Why it was not reproducible on a fresh 9.3.1 cluster:** Any index created on 9.3.1 has `index.version.created` well above `8_500_005`, so cosine always resolves to `DOT_PRODUCT` and the rejection path in `IVFVectorsWriter` is never reached.

**Why snapshot restore is the root entry point:** `index.version.created` is preserved verbatim when a snapshot is restored. The index name suffix (`v5`) and the creation version (`8.10.2`) confirm the index originated on an older cluster and was carried forward through at least one snapshot restore, retaining its original creation version across every subsequent cluster upgrade.

---

## Impact

- All indexing operations to vector fields on `search-auctions-v5` failed after the upgrade to 9.3.1.
- Search continued to work against existing segments (the failure is write-path only).
- The shard was marked failed, preventing further indexing until remediated.
