# Semantic Text Per-Chunk Scoring — Analysis and Options

**Date**: 2026-05-24
**Status**: Analysis

## Problem

Today, `semantic_query` on a `semantic_text` field returns **whole documents** ranked by the highest-scoring chunk (`ScoreMode.Max`). There's no way to:

1. See which chunks matched or how well each scored
2. Get only the relevant chunks back (not the entire document)
3. Get per-chunk cosine similarity scores

In the Dow Jones POC, we had to abandon `semantic_text` entirely and use raw `dense_vector` fields with manual nested structures to get per-chunk relevance. This analysis explores options to close that gap.

## Current Architecture (What Already Exists)

### The query path

`semantic_query` rewrites to a `NestedQueryBuilder` wrapping a `KnnVectorQueryBuilder` (or `SparseVectorQueryBuilder`):

```
SemanticQueryBuilder
  → SemanticFieldType.semanticQuery()
    → NestedQueryBuilder(
        path = "my_field.inference.chunks",
        query = KnnVectorQueryBuilder("my_field.inference.chunks.embeddings", queryVector),
        scoreMode = ScoreMode.Max    // <-- takes best chunk score only
      )
```

**File**: `SemanticFieldMapper.java:1004`

The parent document gets one score — the max across all chunks. Per-chunk scores are computed by Lucene internally but discarded at the parent level.

### What's already built but not surfaced

| Component | What it does | Where |
|-----------|-------------|-------|
| **SemanticChunkScorer** | Scores individual chunks for a given document, returns `List<ScoredChunk>` with offsets + scores | `SemanticChunkScorer.java` |
| **SemanticTextHighlighter** | Uses chunk scoring to return top-N chunks sorted by score for highlighting | `SemanticTextHighlighter.java` |
| **SemanticTextChunkUtils.extractOffsetAndScores()** | Runs chunk-level queries against nested docs, returns `OffsetAndScore` records | `SemanticTextChunkUtils.java` |
| **NestedQueryBuilder.innerHit()** | Fully supports inner_hits on nested queries — returns per-nested-doc results | `NestedQueryBuilder.java` |
| **KnnSearchBuilder inner_hits** | API-level support for inner_hits on KNN search, tested in `VectorNestedIT` | `KnnSearchBuilder.java` |
| **"chunks" format fetcher** | `fields: {"my_field": {"format": "chunks"}}` fetches chunk text/metadata | `SemanticFieldValueFetcher.java` |

The infrastructure for per-chunk results is **almost entirely built**. It's just not wired into the `semantic_query` response.

## Options

### Option A: Add `inner_hits` to `semantic_query` (Smallest Change)

Add an optional `inner_hits` parameter to `semantic_query` that exposes Elasticsearch's existing nested inner_hits mechanism.

**User experience:**
```json
POST /my-index/_search
{
  "query": {
    "semantic": {
      "field": "content",
      "query": "machine learning",
      "inner_hits": {
        "size": 3,
        "_source": false,
        "fields": ["content.inference.chunks.offset"]
      }
    }
  }
}
```

**Response:**
```json
{
  "hits": {
    "hits": [{
      "_id": "doc1",
      "_score": 0.92,
      "inner_hits": {
        "content.inference.chunks": {
          "hits": {
            "hits": [
              { "_nested": { "offset": 0 }, "_score": 0.92, ... },
              { "_nested": { "offset": 1 }, "_score": 0.78, ... },
              { "_nested": { "offset": 2 }, "_score": 0.65, ... }
            ]
          }
        }
      }
    }]
  }
}
```

**Implementation:**
- Modify `SemanticFieldMapper.semanticQuery()` (line 1004): accept an optional `InnerHitBuilder` and attach it to the `NestedQueryBuilder`
- Modify `SemanticQueryBuilder`: add `inner_hits` field, parse from XContent, pass through to `semanticQuery()`
- ~50-100 lines of code changes

**Trade-offs:**
- (+) Trivial implementation — leverages existing nested inner_hits infrastructure
- (+) Returns per-chunk Lucene scores (cosine similarity for dense vectors)
- (+) Users can control `size`, `_source`, `fields`, `sort` on inner_hits
- (-) Inner_hits uses `ExactKnnQueryBuilder` for scoring (exact, not approximate) — but this is fine since we're scoring within an already-retrieved document's chunks
- (-) Returns nested document format (users need to understand `_nested.offset` mapping to chunk positions)
- (-) Chunk text retrieval requires additional `fields` configuration

### Option B: New `chunks` Response Option on `semantic_query` (Better UX)

Add a dedicated `chunks` option to `semantic_query` that returns a curated per-chunk response with scores, text, and offsets.

**User experience:**
```json
POST /my-index/_search
{
  "query": {
    "semantic": {
      "field": "content",
      "query": "machine learning",
      "chunks": {
        "size": 3,
        "min_score": 0.5,
        "include_text": true
      }
    }
  }
}
```

**Response:**
```json
{
  "hits": {
    "hits": [{
      "_id": "doc1",
      "_score": 0.92,
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

**Implementation:**
- Use `SemanticChunkScorer` (already built) during the fetch phase to score chunks per document
- Add a new fetch sub-phase (`SemanticChunkFetchSubPhase`) that runs after the search phase
- Add `chunks` configuration to `SemanticQueryBuilder`
- Retrieve chunk text using the existing "chunks" format fetcher from `SemanticFieldValueFetcher`
- ~200-400 lines of new code

**Trade-offs:**
- (+) Clean, purpose-built response format — users don't need to understand nested internals
- (+) Includes chunk text directly (no extra `fields` config needed)
- (+) `min_score` filtering removes low-relevance chunks
- (+) Builds on existing `SemanticChunkScorer` infrastructure
- (-) More implementation work than Option A
- (-) New response format to document and maintain
- (-) Chunk scoring happens in the fetch phase (post-retrieval), not during search — adds latency proportional to `size * num_chunks`

### Option C: Both A and B (Progressive Disclosure)

Implement both, positioning them for different use cases:

- **`inner_hits`** (Option A) — for power users who want raw Lucene-level control, integration with existing tools that understand inner_hits format, and compatibility with nested query composition
- **`chunks`** (Option B) — for application developers who want a clean API with chunk text and scores without understanding nested document internals

**Implementation**: Option A first (small, quick), then Option B built on top.

### Option D: Chunk-Level Results API (Separate Endpoint)

Add a new endpoint `POST /{index}/_semantic_chunks` that returns results ranked by chunk, not by document. Each result is a chunk, not a document.

**User experience:**
```json
POST /my-index/_semantic_chunks
{
  "field": "content",
  "query": "machine learning",
  "size": 10,
  "min_score": 0.5
}
```

**Response:**
```json
{
  "chunks": [
    { "_id": "doc3", "text": "...", "start_offset": 512, "end_offset": 1024, "score": 0.95 },
    { "_id": "doc1", "text": "...", "start_offset": 0, "end_offset": 512, "score": 0.92 },
    { "_id": "doc3", "text": "...", "start_offset": 0, "end_offset": 512, "score": 0.88 },
    { "_id": "doc2", "text": "...", "start_offset": 1024, "end_offset": 1536, "score": 0.85 }
  ]
}
```

Note: the same document can appear multiple times (different chunks).

**Implementation:**
- New REST endpoint and transport action
- Internally runs a KNN query on the nested embeddings field
- Collects results at the nested document (chunk) level rather than parent level
- ~500-800 lines of new code

**Trade-offs:**
- (+) True chunk-level ranking across documents — solves the RAG use case directly
- (+) Clean separation from document-level search
- (+) Multiple chunks from the same document can appear in results
- (-) Most implementation work
- (-) New API surface to maintain
- (-) Doesn't compose with existing search features (aggregations, highlighting, etc.)
- (-) Requires rethinking pagination (chunk-level vs document-level)

## Recommendation

**Start with Option A** (inner_hits on semantic_query). It's the smallest change (~50-100 lines), leverages existing infrastructure completely, and immediately solves the core problem: per-chunk scores and the ability to retrieve only relevant chunks.

**Then evaluate Option B** based on user feedback. If the inner_hits format is too low-level for most users, a cleaner `chunks` response option can be layered on top.

**Option D** (chunk-level results API) is the long-term vision for RAG use cases but should be a separate initiative — it's a fundamentally different query model.

## What Would Have Solved the Dow Jones POC

Option A alone would have been sufficient. The POC needed:
1. Per-chunk cosine similarity scores — inner_hits provides this
2. Ability to filter to only relevant chunks — `inner_hits.size` + client-side `min_score` filtering
3. Chunk text retrieval — `inner_hits.fields` with the chunks format fetcher

All three are achievable with Option A.
