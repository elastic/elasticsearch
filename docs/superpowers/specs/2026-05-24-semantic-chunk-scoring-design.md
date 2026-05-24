# Per-Chunk Scoring for semantic_text Queries

**Date**: 2026-05-24
**Status**: Draft

## Problem

`semantic_query` on a `semantic_text` field returns whole documents ranked by the highest-scoring chunk (`ScoreMode.Max`). There's no way to see which chunks matched, how well each scored, or filter to only relevant chunks. Users who need per-chunk relevance (e.g., RAG pipelines) must abandon `semantic_text` and manually manage `dense_vector` fields with nested structures.

## Goals

- Return per-chunk scores and text from `semantic_query` results
- Allow filtering chunks by minimum score threshold
- Allow limiting the number of chunks returned per document
- Maintain full backward compatibility when new parameters are not used

## Non-Goals

- Chunk-level ranking across documents (returning results ranked by chunk, not document)
- Changing document-level scoring or ranking behavior
- Supporting per-chunk results for sparse vector queries (future work)

## Design

### 1. Query API

`semantic_query` gains two new optional parameters:

```json
{
  "query": {
    "semantic": {
      "field": "content",
      "query": "machine learning",
      "min_score": 0.7,
      "chunks_per_doc": 5
    }
  }
}
```

**`min_score`** (float, optional): Minimum chunk score threshold. Only chunks scoring at or above this value are included in the `_chunks` response. When set without `chunks_per_doc`, all qualifying chunks are returned.

**`chunks_per_doc`** (int, optional, default 3 when used): Maximum number of chunks to return per document. Returns the top N chunks by score. When set without `min_score`, returns top N chunks regardless of score.

**Trigger behavior**: Chunk details are included in the response when **either** parameter is set. When neither is set, behavior is identical to today — no `_chunks` in the response.

**Combination semantics:**

| `min_score` | `chunks_per_doc` | Behavior |
|-------------|-----------------|----------|
| not set | not set | Today's behavior, no chunk details |
| not set | 3 | Top 3 chunks per doc with scores, no score filter |
| 0.7 | not set | All chunks above 0.7, no per-doc cap |
| 0.7 | 5 | Top 5 chunks per doc, but only if above 0.7 |

### 2. Response Format

When chunk details are requested, each hit includes a `_chunks` section:

```json
{
  "hits": {
    "hits": [{
      "_id": "doc1",
      "_score": 0.92,
      "_source": { "title": "..." },
      "_chunks": {
        "content": [
          { "text": "...chunk text...", "start_offset": 0, "end_offset": 512, "score": 0.92 },
          { "text": "...chunk text...", "start_offset": 512, "end_offset": 1024, "score": 0.78 }
        ]
      }
    }]
  }
}
```

- `_chunks` is keyed by field name (supports multiple semantic_text fields on the same document).
- Chunks are sorted by score descending, with ties broken by `start_offset` ascending.
- `text` is the actual chunk text extracted from the original field value using the stored offsets.
- `start_offset` / `end_offset` are character positions in the original text.
- `score` is the similarity score (cosine, dot product, etc., depending on the configured model).
- If a document has no chunks above `min_score`, the document still appears in hits (it matched the query) but `_chunks` for that field is an empty array.

### 3. Implementation

The implementation leverages existing infrastructure — `SemanticChunkScorer`, `SemanticTextChunkUtils`, and `SemanticFieldValueFetcher` — which already score individual chunks and retrieve chunk text for highlighting and ES|QL.

#### SemanticQueryBuilder changes

Add `minScore` (Float) and `chunksPerDoc` (Integer) fields to `SemanticQueryBuilder`. Parse from XContent, serialize/deserialize for transport and wire format. Validate: `min_score >= 0`, `chunks_per_doc >= 1`.

#### SemanticFieldType.semanticQuery() changes

Accept optional chunk parameters. Store them in a way accessible during the fetch phase. The query structure itself (NestedQueryBuilder wrapping KnnVectorQueryBuilder with ScoreMode.Max) does not change — document retrieval and ranking work the same as today.

#### New fetch sub-phase: SemanticChunkFetchSubPhase

A new `FetchSubPhase` that runs during the fetch phase on retrieved hits. For each hit where chunk details were requested:

1. Uses `SemanticChunkScorer.scoreChunks()` to compute per-chunk scores (exact similarity, not approximate KNN — correct since we're scoring within already-retrieved documents).
2. Filters results by `min_score` if set.
3. Sorts by score descending, breaking ties by `start_offset` ascending.
4. Caps to `chunks_per_doc` if set.
5. Retrieves chunk text from the document source using stored offsets (following the pattern in `SemanticFieldValueFetcher` with the "chunks" format).
6. Attaches results to the `SearchHit` as `_chunks`.

#### SearchHit changes

Add a `_chunks` field (`Map<String, List<ChunkResult>>`) to `SearchHit`. Serialize in `toXContent` alongside existing fields like `_source`, `highlight`, `inner_hits`. A new `ChunkResult` record holds `text` (String), `startOffset` (int), `endOffset` (int), and `score` (float).

### 4. What Does NOT Change

- Document-level scoring and ranking — `ScoreMode.Max` continues to determine document scores
- Query rewriting — the NestedQueryBuilder/KnnVectorQueryBuilder structure is unchanged
- Existing highlighting — continues to work alongside `_chunks`
- The `_explain` API — no per-chunk explain (future work)

### 5. Test Cases

#### Parameter combinations
- `chunks_per_doc` only — returns top N chunks with scores, no score filter
- `min_score` only — returns all chunks above threshold, no cap
- Both `chunks_per_doc` and `min_score` — both filters applied
- Neither set — today's behavior, no `_chunks` in response

#### Chunk filtering
- All chunks above `min_score` — verify only qualifying chunks returned
- No chunks above `min_score` — verify empty array, document still in hits
- More qualifying chunks than `chunks_per_doc` — verify capped to N
- Fewer qualifying chunks than `chunks_per_doc` — verify all returned (no padding)
- `chunks_per_doc: 1` — verify only the best chunk returned

#### Score ordering
- Chunks sorted by score descending
- Tied scores break by start_offset ascending (deterministic)

#### Multi-document
- Different documents have different numbers of qualifying chunks
- Document with 0 qualifying chunks vs document with many

#### Edge cases
- Single chunk covering entire text
- `min_score: 0.0` — effectively returns all chunks with scores
- `min_score: 1.0` — likely returns nothing
- Document with many chunks (20+) — verify reasonable performance
- Multiple semantic_text fields on same document — verify `_chunks` keyed correctly

#### Backward compatibility
- Query without chunk parameters returns identical response to today
- Existing highlighting still works alongside `_chunks`

#### Integration test
- Index documents, search with chunk parameters, verify end-to-end response format
