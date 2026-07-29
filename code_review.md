# Code Review: `chunk_fetch_coordinator_accounting`

**Branch**: `chunk_fetch_coordinator_accounting`  
**Reviewed**: 2026-07-20  
**Scope**: 9 changed files — new `SearchHitRamUsageEstimator`, `ramBytesUsed()` on `SearchHit`, `estimatedRetainedBytes()` on `FetchPhaseResponseChunk`, updated circuit-breaker accounting in `FetchPhaseResponseStream.writeChunk()` and `TransportFetchPhaseCoordinationAction`, updated tests and benchmarks.

**Summary**: The branch replaces serialized-byte-size accounting in the request circuit breaker with a retained-heap estimate computed via `SearchHitRamUsageEstimator`. The motivation is sound — serialized bytes systematically under-count retained heap — but the implementation introduces two regressions in the ordering of the circuit-breaker check vs. deserialization/enqueue, and the estimator has structural accuracy gaps that contradict its "conservative upper-bound" contract.

---

## Findings

### 1. [CONFIRMED — Correctness] Last-chunk: hits enqueued before circuit-breaker check

**File**: `server/src/main/java/org/elasticsearch/search/fetch/chunk/TransportFetchPhaseCoordinationAction.java` line 241  
**Severity**: High

The last-chunk path deserializes all `hitCount` hits and enqueues each via `responseStream.addHitWithSequence()` inside the for-loop, then calls `circuitBreaker.addEstimateBytesAndMaybeBreak()` after the loop closes. All heap is committed before the guard can fire.

```java
// NEW — hits in queue BEFORE CB check
long estimatedRetainedBytes = 0L;
try (StreamInput in = ...) {
    for (int i = 0; i < hitCount; i++) {
        SearchHit hit = SearchHit.readFrom(in);
        estimatedRetainedBytes += hit.ramBytesUsed();
        responseStream.addHitWithSequence(hit, position);  // ← enqueued
    }
}
circuitBreaker.addEstimateBytesAndMaybeBreak(estimatedRetainedBytes, ...);  // ← AFTER loop
```

```java
// ORIGINAL — CB checked on serialized bytes BEFORE deserialization
int bytesSize = lastChunkBytes.length();
circuitBreaker.addEstimateBytesAndMaybeBreak(bytesSize, ...);  // ← BEFORE deserialization
responseStream.trackBreakerBytes(bytesSize);
// ... then deserialize and enqueue
```

This is a regression: the CB can no longer prevent the allocation it is supposed to guard. On a CB trip the hits are already in the queue; cleanup is correct (they are decRef'd by `closeInternal()`), but the memory-pressure protection window is gone.

A secondary gap: if `SearchHit.readFrom()` throws `IOException` mid-loop, hits `0..k-1` are in the queue with neither a CB charge nor a `trackBreakerBytes` call — those hits live entirely outside CB accounting for their lifetime.

**Action**: Move the CB check before the deserialization loop. One approach: use a preliminary estimate from `lastChunkBytes.length()` as a pre-flight gate (consistent with what the intermediate-chunk path now does with `estimatedRetainedBytes()` for the pre-queue serialized representation), or wrap the last chunk in a `FetchPhaseResponseChunk` and process it through `writeChunk()` to unify both paths.

---

### 2. [CONFIRMED — Correctness] `writeChunk`: full deserialization before circuit-breaker check

**File**: `server/src/main/java/org/elasticsearch/search/fetch/chunk/FetchPhaseResponseStream.java` line 100  
**Severity**: High

`estimatedRetainedBytes()` internally calls `ensureDeserialized()`, which allocates `SearchHit[hitCount]` and calls `SearchHit.readFrom()` for every hit. The circuit-breaker check follows only after. If the CB trips, the deserialized hits are properly cleaned up via `chunk.close()` (no leak), but the memory was already committed before the guard could prevent it.

```java
void writeChunk(FetchPhaseResponseChunk chunk, Releasable releasable) {
    ...
    long estimatedRetainedBytes = chunk.estimatedRetainedBytes(); // deserializes all hits
    circuitBreaker.addEstimateBytesAndMaybeBreak(estimatedRetainedBytes, ...); // AFTER
```

Original code:
```java
long bytesSize = chunk.getBytesLength(); // O(1), no deserialization
circuitBreaker.addEstimateBytesAndMaybeBreak(bytesSize, ...); // BEFORE deserialization
```

The protection is now post-hoc for the intermediate-chunk path too. Under memory pressure, a large chunk can trigger a GC pause or transient OOM during `ensureDeserialized()` before the CB rejects it.

**Action**: Consider a two-gate approach: use `chunk.getBytesLength()` as a cheap lower-bound pre-flight before deserialization, then update the CB with the accurate retained-heap estimate after (similar to the pattern used by some other estimators in the codebase). Alternatively, accept the protection inversion as a deliberate trade-off and document it explicitly.

---

### 3. [CONFIRMED — Correctness] HashMap backing-array size under-estimated in `estimateFields()`

**File**: `server/src/main/java/org/elasticsearch/search/SearchHitRamUsageEstimator.java` line 59  
**Severity**: Medium

`estimateFields()` charges `fields.size() × NUM_BYTES_OBJECT_REF` for the HashMap backing array, but the actual capacity is always the next power-of-2 above `ceil(fields.size() / 0.75)`. For `fields.size()` in 1–11, the actual capacity is 16 slots; the estimator charges 1–11. The same formula is repeated at line 40 for the `innerHits` map.

Concrete magnitude for one field: 15 unaccounted slots × 8 bytes = **120 bytes per map**. Each `SearchHit` has two such maps (`documentFields` + `metaFields`) = **240 bytes per hit**. At 10,000 hits this is a ~2.4 MB silent under-count.

The class Javadoc says "conservative upper-bound estimator," but this calculation produces a systematic under-estimate, violating the contract.

`DocumentFieldRamUsageEstimator.estimateMap()` already contains a correct `hashTableCapacity()` helper that computes `max(16, nextPowerOfTwo(ceil(n / 0.75)))`. The helper is package-private; a shared utility method should be extracted.

**Action**: Use the capacity-aware formula from `DocumentFieldRamUsageEstimator.hashTableCapacity()` (or inline it) in both `estimateFields()` and the innerHits map block.

---

### 4. [CONFIRMED — Correctness] Inner `SearchHits` estimate omits `TotalHits` and other reference-type fields

**File**: `server/src/main/java/org/elasticsearch/search/SearchHitRamUsageEstimator.java` line 45  
**Severity**: Medium

`SEARCH_HITS_SHALLOW_SIZE` covers only the shell of `SearchHits`. The following separately heap-allocated fields are not counted:

- `TotalHits totalHits` — present in all normal requests with `track_total_hits` (the default); holds a `long` and a `Relation` enum ref, ~24–32 bytes per inner-hits entry.
- `SortField[] sortFields` — non-null when sorting is in use.
- `String collapseField` / `Object[] collapseValues` — non-null when field collapsing is in use.
- `RefCounted refCounted` — always present.

The undercount is at least one `TotalHits` object per inner-hits entry, compounding with nesting depth.

**Action**: Add explicit accounting for at least `TotalHits` (e.g. `RamUsageEstimator.shallowSizeOfInstance(TotalHits.class)`) per inner-hits `SearchHits` entry.

---

### 5. [PLAUSIBLE — Efficiency] Two full passes over `deserializedHits[]` per `writeChunk` call

**File**: `server/src/main/java/org/elasticsearch/search/fetch/chunk/FetchPhaseResponseChunk.java` line 158  
**Severity**: Low–Medium

`writeChunk` first calls `estimatedRetainedBytes()` (iterates the array calling `hit.ramBytesUsed()` → `SearchHitRamUsageEstimator.estimate()` per hit), then calls `consumeHits()` → `drainDeserializedHits()` (iterates again to drain into the queue). This is two full passes over `deserializedHits[]`, with the first pass doing O(fields-per-hit) work per hit.

For 50 hits × 200 fields, the estimate pass alone involves ~10,000 method calls on the coordinator transport thread, compared to the original `getBytesLength()` (a single field read). This is bounded in the common case (0–10 fields per hit), but not negligible for field-heavy payloads.

**Action**: Consider combining estimate-and-drain into a single pass that accumulates the estimate while draining hits into the queue. This would require the CB check to happen either before deserialization (a pre-flight with `getBytesLength()`) or after the drain (post-hoc), so there is a design tension with finding #2.

---

### 6. [PLAUSIBLE — Altitude] `trackBreakerBytes()` creates a two-step charge pattern with no contract enforcement

**File**: `server/src/main/java/org/elasticsearch/search/fetch/chunk/FetchPhaseResponseStream.java` line 187  
**Severity**: Low–Medium

The last-chunk path in `TransportFetchPhaseCoordinationAction` must manually coordinate:
1. `circuitBreaker.addEstimateBytesAndMaybeBreak(bytes, ...)` — charges the CB
2. `responseStream.trackBreakerBytes(bytes)` — records the charge for later release

These two calls are on consecutive lines but there is no contract that enforces they are always called together or in the right order. If a future caller calls `trackBreakerBytes` without a matching `addEstimateBytesAndMaybeBreak` (or skips one on an exception path), `closeInternal()` will call `circuitBreaker.addWithoutBreaking(-totalBreakerBytes.get())` for bytes never charged, driving the CB counter negative and corrupting per-node breaker accounting for all subsequent requests.

`writeChunk()` encapsulates both operations atomically inside a single method. The divergent two-path design is the structural root cause.

**Action**: Unify the last-chunk path through `writeChunk()` if possible, or extract a helper that atomically charges the CB and updates `totalBreakerBytes` together.

---

### 7. [PLAUSIBLE — Reuse] Parallel independent hit-level estimator in `TransportMultiSearchAction` not updated

**File**: `server/src/main/java/org/elasticsearch/action/search/TransportMultiSearchAction.java` line 442  
**Severity**: Low

`TransportMultiSearchAction.estimateHitBytes()` is a fully independent hit-level memory estimator using hardcoded constants (`PER_HIT_OBJECT_OVERHEAD=400`, `PER_FIELD_OVERHEAD=200`) with no reference to `SearchHit.ramBytesUsed()` or `SearchHitRamUsageEstimator`. Improvements to either estimator (e.g. fixing the HashMap capacity undercount in finding #3) will silently not propagate to the other, causing the msearch CB and the fetch-chunk CB to diverge in their accounting semantics over time.

**Action**: Consider having `estimateHitBytes()` delegate to `SearchHit.ramBytesUsed()`, or at minimum note the relationship in a comment so both are updated together.

---

### 8. [PLAUSIBLE — Cleanup] `RAM_BYTES_FLOOR = 512L` is an undocumented magic number

**File**: `server/src/main/java/org/elasticsearch/search/SearchHitRamUsageEstimator.java` line 30  
**Severity**: Low

The constant covers many `SearchHit` fields not explicitly estimated: `id` (Text), `nestedIdentity`, `score`, `docId`, `shard` (SearchShardTarget), `explanation`, `matchedQueries`, `sortValues`, `highlightFields`, and more. Without a comment listing which fields it intends to cover, a contributor adding a new `SearchHit` field has no way to know whether it is included in the floor or needs explicit accounting.

`TransportMultiSearchAction.PER_HIT_OBJECT_OVERHEAD` has a Javadoc breakdown; `RAM_BYTES_FLOOR` has none.

**Action**: Add a comment listing the fields this floor is intended to cover, similar to the breakdown in `TransportMultiSearchAction`.

---

### 9. [PLAUSIBLE — Cleanup] Redundant null guard in `estimatedRetainedBytes()` and dead per-slot null check

**File**: `server/src/main/java/org/elasticsearch/search/fetch/chunk/FetchPhaseResponseChunk.java` line 160  
**Severity**: Low

After `ensureDeserialized()`, `deserializedHits` is `null` only when `hitCount == 0` (the `serializedHits == null` case arises only on the data-node path, never on the coordinator where this method is called). The guard `if (deserializedHits == null) return 0L` is correct but less explicit than `if (hitCount == 0) return 0L`.

More importantly, the per-slot `if (hit != null)` check at line 165 is dead code: `ensureDeserialized()` fills every slot from `SearchHit.readFrom()` (always non-null), and slot-nulling only happens in `drainDeserializedHits()`, which is called by `consumeHits()` — which always runs after `estimatedRetainedBytes()` in `writeChunk()`. The null check implies to readers that partial null arrays are a normal case here, which is misleading.

**Action**: Replace the `deserializedHits == null` guard with `if (hitCount == 0) return 0L;` placed before the `ensureDeserialized()` call. Remove the per-slot `if (hit != null)` guard, or add a comment explaining why it cannot be null at this call site.

---

### 10. [PLAUSIBLE — Cleanup] HashMap overhead formula duplicated between `estimate()` and `estimateFields()`

**File**: `server/src/main/java/org/elasticsearch/search/SearchHitRamUsageEstimator.java` line 40  
**Severity**: Low

The expression `HASH_MAP_SHALLOW_SIZE + NUM_BYTES_ARRAY_HEADER + (long) n * (HASH_MAP_NODE_SIZE + NUM_BYTES_OBJECT_REF)` appears verbatim at line 40 (for the `innerHits` map) and inside `estimateFields()` at line 59. Both share the same under-estimation bug (finding #3). A private helper `estimateHashMapShallowBytes(int entryCount)` would consolidate the formula, ensure a single-site fix for the capacity calculation, and reduce cognitive load.

**Action**: Extract a private helper and update both call sites. When fixing finding #3, doing so via this helper will automatically correct both locations.

---

## Summary Table

| # | Severity | Status | File | Finding |
|---|----------|--------|------|---------|
| 1 | High | CONFIRMED | `TransportFetchPhaseCoordinationAction.java:241` | Last-chunk hits enqueued before CB check |
| 2 | High | CONFIRMED | `FetchPhaseResponseStream.java:100` | Full deserialization before CB check in `writeChunk` |
| 3 | Medium | CONFIRMED | `SearchHitRamUsageEstimator.java:59` | HashMap backing-array under-estimated (violates upper-bound contract) |
| 4 | Medium | CONFIRMED | `SearchHitRamUsageEstimator.java:45` | Inner `SearchHits` missing `TotalHits` and other reference fields |
| 5 | Low–Med | PLAUSIBLE | `FetchPhaseResponseChunk.java:158` | Two-pass iteration over `deserializedHits[]` per chunk |
| 6 | Low–Med | PLAUSIBLE | `FetchPhaseResponseStream.java:187` | `trackBreakerBytes()` two-step pattern with no enforcement |
| 7 | Low | PLAUSIBLE | `TransportMultiSearchAction.java:442` | Parallel independent estimator not using `SearchHit.ramBytesUsed()` |
| 8 | Low | PLAUSIBLE | `SearchHitRamUsageEstimator.java:30` | `RAM_BYTES_FLOOR` undocumented |
| 9 | Low | PLAUSIBLE | `FetchPhaseResponseChunk.java:160` | Redundant null guard and dead per-slot null check |
| 10 | Low | PLAUSIBLE | `SearchHitRamUsageEstimator.java:40` | Duplicate HashMap overhead formula |
