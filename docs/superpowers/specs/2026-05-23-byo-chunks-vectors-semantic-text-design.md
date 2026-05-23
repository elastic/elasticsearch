# BYO Chunks and Vectors for semantic_text

**Date**: 2026-05-23
**Status**: Draft

## Problem

Today, `semantic_text` fields are tightly coupled to inline embedding generation. When a document is indexed with a string value in a `semantic_text` field, the `ShardBulkInferenceActionFilter` intercepts the request, calls `chunkedInfer()` on the configured inference endpoint, and injects the resulting chunks and embeddings into `_inference_metadata` before the document reaches the shard.

This works well for the common case, but prevents users who generate embeddings externally (different chunking strategy, specialized model, pre-processed pipeline, embeddings from a third-party service) from using `semantic_text` fields. Users who want to bring their own vectors today must use lower-level field types (`dense_vector`, `sparse_vector`) and manage the nested chunk structure themselves, losing the ergonomics of `semantic_text` and `semantic_query`.

## Goals

- Allow users to ingest precomputed chunks and embeddings into `semantic_text` fields without triggering inference.
- Support multi-part upload for large documents where the full set of chunks exceeds practical payload limits.
- Maintain full backward compatibility with existing string-value ingestion.
- Keep query-time behavior unchanged — `semantic_query` continues to work via the configured inference endpoint.

## Non-Goals

- Supporting user-provided query vectors via `semantic_query` (users can use `knn` query directly for this).
- Making `inference_id` optional in the mapping (it remains required for query-time inference and dimensional validation).
- Changing the internal storage format beyond the additive `_staged` section.

## Design

### 1. Field Value Protocol

A `semantic_text` field distinguishes intent by the shape of the indexed value.

#### Plain string (backward compatible, no change)

```json
{ "my_field": "some text to embed" }
```

The inference pipeline runs as it does today. Zero behavior change.

#### Single-shot BYO (complete in one request)

```json
{
  "my_field": {
    "text": "the full original text",
    "chunks": [
      { "start_offset": 0, "end_offset": 200, "embeddings": [0.1, 0.2, ...] },
      { "start_offset": 200, "end_offset": 450, "embeddings": [0.3, 0.4, ...] }
    ]
  }
}
```

No `_action` field is needed. The presence of `text` + `chunks` with embeddings means "this is complete, index it." Validation runs atomically: no gaps, no overlaps, offsets within bounds, dimensional compatibility with the configured inference endpoint.

#### Multi-part: stage_init (start a staged upload)

```json
{
  "my_field": {
    "_action": "stage_init",
    "text": "the full original text",
    "chunks": [...]
  }
}
```

- `text` is required and must be a non-empty string.
- `chunks` is optional (first batch of chunks can be included).
- Creates a `_staged` section in `_inference_metadata` for this field.

#### Multi-part: stage (append chunks)

```json
{
  "my_field": {
    "_action": "stage",
    "chunks": [
      { "start_offset": 200, "end_offset": 450, "embeddings": [0.3, 0.4, ...] }
    ]
  }
}
```

- Appends chunks to the existing `_staged` section.
- Rejects if no `_staged` exists for this field (must `stage_init` first).

#### Multi-part: commit (finalize and promote)

```json
{
  "my_field": {
    "_action": "commit",
    "chunks": [...]
  }
}
```

- `chunks` is optional (final batch can be included).
- Validates full coverage, promotes staged chunks to committed inference metadata, document becomes searchable.

#### Cancel (discard staged data)

```json
{
  "my_field": {
    "_action": "cancel"
  }
}
```

- Clears the `_staged` section for this field.
- Document is otherwise untouched.

### Detection Logic in `ShardBulkInferenceActionFilter`

The filter inspects the field value:

1. **String** -> existing inference path (no change).
2. **Object with `_action`** -> multi-part staging path (route to `stage_init`, `stage`, `commit`, or `cancel` handler).
3. **Object with `text` + `chunks`, no `_action`** -> single-shot BYO (atomic validation and direct indexing, no staging).

### 2. Storage Format

#### Committed data (no change from today)

Committed BYO chunks are stored in the same structure as inference-generated chunks:

```json
{
  "_inference_metadata": {
    "my_field": {
      "inference_id": "my-endpoint",
      "model_settings": { "task_type": "text_embedding", "dimensions": 768, ... },
      "chunking_settings": null,
      "chunks": {
        "my_field": [
          { "start_offset": 0, "end_offset": 200, "embeddings": [0.1, ...] },
          { "start_offset": 200, "end_offset": 450, "embeddings": [0.3, ...] }
        ]
      }
    }
  }
}
```

`model_settings` are resolved from the configured inference endpoint at commit time (or at single-shot validation time). `chunking_settings` is `null` for BYO data since the user performed their own chunking.

#### Staged data (new)

During multi-part upload, a `_staged` section is added at the field level:

```json
{
  "_inference_metadata": {
    "my_field": {
      "_staged": {
        "text": "the full original text",
        "text_length": 450,
        "last_modified": "2026-05-23T14:30:00Z",
        "chunks": [
          { "start_offset": 0, "end_offset": 200, "embeddings": [0.1, ...] }
        ]
      }
    }
  }
}
```

- `_staged` is present only during multi-part upload. Its presence signals "this field has an in-progress upload."
- `text` and `text_length` are set at `stage_init` and are immutable after that.
- `last_modified` is set on `stage_init` and updated on each `stage` request.
- On commit: chunks are promoted into the committed structure, `_staged` is removed.
- `_staged` data is filtered from search — documents with only staged data are not returned by `semantic_query`.

### 3. Validation Rules

#### On `stage_init`

- `text` is required and must be a non-empty string.
- `text_length` is computed and stored.
- If chunks are included, per-chunk and cross-chunk validation (below) is applied.
- Rejected if `_staged` already exists for this field. User must `commit` or `cancel` first.

#### On each `stage` request

- Rejected if no `_staged` exists for this field.
- Per-chunk validation:
  - `start_offset` and `end_offset` are required non-negative integers.
  - `start_offset < end_offset`.
  - `end_offset <= text_length`.
  - `embeddings` is required and non-empty.
- Cross-chunk validation against already-staged chunks:
  - No overlap: the new chunk's `[start_offset, end_offset)` must not intersect any existing staged chunk's range.
  - Duplicate ranges (exact same offsets) are rejected.

#### On `commit`

- Rejected if no `_staged` exists.
- If final chunks are included, per-chunk and cross-chunk validation runs first.
- Full coverage check: sort all staged chunks by `start_offset` and verify contiguous coverage of `[0, text_length)`:
  - First chunk starts at 0.
  - Each chunk's `start_offset` equals the previous chunk's `end_offset`.
  - Last chunk's `end_offset` equals `text_length`.
- Dimensional compatibility: resolve `model_settings` from the inference endpoint and verify all chunk embeddings have the correct dimensionality.
- If validation fails, the commit is rejected and `_staged` is preserved — user can send missing chunks and retry.

#### On single-shot BYO

All of the above runs atomically in one pass before indexing.

### 4. Staged Data Lifecycle & Cleanup

#### Index setting

`index.semantic_text.staged_ttl` — duration, default `"24h"`. Controls how long uncommitted staged data is retained before automatic cleanup. Set to `"0"` or `"-1"` to disable automatic cleanup.

#### Background cleanup task

- Periodic task runs on each node holding shards for the index (similar to ILM or merge scheduling).
- Scans for documents where `now - _staged.last_modified > staged_ttl`.
- Clears the `_staged` section (equivalent to `cancel`).
- Logs at WARN level: `"Cleared stale staged semantic_text data for field [{}] on document [{}], last modified [{}]"`.

#### On-demand cleanup API

```
POST /<index>/_semantic_cleanup
POST /<index>/_semantic_cleanup?field=my_field
POST /<index>/_semantic_cleanup?max_age=1h
```

Response:

```json
{
  "cleared": 42,
  "failed": 0
}
```

- `field`: optional, scope cleanup to a specific semantic_text field.
- `max_age`: optional, override the TTL for this run.

### 5. Interaction with Existing Features

#### Backward compatibility

- String values work exactly as today.
- Mappings are unchanged — `inference_id` remains required, no new mapping parameters.
- The `_inference_metadata` structure is additive (new `_staged` key). Existing documents and queries are unaffected.

#### Query time

- No changes. `semantic_query` calls the configured inference endpoint to embed the query text, then searches against committed chunks.
- Staged chunks are invisible to search.
- Users who want to query with their own vector can use `knn` query against the nested `chunks.embeddings` field directly.

#### `_update` API

- Partial updates to non-semantic fields proceed normally. `_inference_metadata` (including `_staged`) is preserved.
- Partial updates that set the semantic_text field to a plain string trigger normal inference and overwrite committed chunks. If `_staged` exists, it is cleared (the field is being fully replaced).
- Partial updates with a staging action work as described above.

#### Reindex / force merge / snapshot-restore

- `_staged` data is part of `_inference_metadata` and survives these operations.
- A document with `_staged` after restore is still uncommitted and invisible to semantic queries.

#### Mixed mode

A document can have committed BYO chunks and later be reindexed with a plain string (triggers inference, replaces BYO chunks), or vice versa. The field does not track how it was populated.

### 6. Error Handling & Edge Cases

#### Stage request failures

- Validation failures (overlapping offsets, out-of-bounds, wrong format) reject the request. Already-staged chunks are preserved. User retries with corrected data.
- Index operation failures (shard unavailable, version conflict) follow standard bulk error handling.

#### Commit failures

- Validation failures (gaps, dimensional mismatch) reject the commit but preserve `_staged`. User can send missing chunks and retry.
- Error responses clearly indicate the problem: `"Gap detected between offsets [200, 300)"` or `"Expected 768 dimensions, chunk at [0, 200) has 384"`.

#### Concurrent staging conflicts

- Two clients staging chunks for the same field on the same document concurrently: standard optimistic concurrency control applies. The second write gets a version conflict.
- Users should use `if_seq_no` / `if_primary_term` for safe concurrent appends.
- Docs should recommend coordinating multi-part uploads from a single client per document/field.

#### Document deletion during staging

- Staged data is deleted with the document. No special handling.

#### Inference endpoint mismatch at commit

- If the inference endpoint's model settings change between `stage_init` and `commit` (e.g., dimensions changed), commit validates against the current model settings.
- If embeddings no longer match, commit fails with a clear error. User must cancel and re-upload.

#### Re-init without commit

- `stage_init` on a field that already has `_staged` is rejected. User must `commit` or `cancel` first. This prevents accidental loss of partially uploaded data.

#### Index close/reopen

- `_staged` survives index close/reopen (it is part of the stored document).
