# Code Review: `circuit-breaker/memory-held-gauge`

**Branch:** `circuit-breaker/memory-held-gauge`  
**Date:** 2026-06-18  
**Reviewer:** Claude Code (automated, high-effort)  

---

## Summary

This PR adds three new circuit-breaker metrics:

- `es.breaker.memory.held.usage` — up/down counter tracking live admitted bytes, per breaker type and caller label
- `es.breaker.memory.limit.size` — async gauge of configured limit per breaker
- `es.breaker.memory.estimated.usage` — async gauge of current `getUsed() * overhead` per breaker

The implementation covers `ChildMemoryCircuitBreaker`, `PreallocatedCircuitBreakerService`, `SearchExecutionContext`, and the percolator wrapper. The overall shape is correct, but two correctness bugs were found (one of which can permanently inflate breaker usage), a concurrency race was confirmed, and several secondary issues require attention.

---

## Findings

### 1. `releaseQueryConstructionMemory()` — negative per-label balance inflates the breaker permanently

**File:** `server/src/main/java/org/elasticsearch/index/query/SearchExecutionContext.java`, line 873–874  
**Severity:** Bug — data corruption  

`addCircuitBreakerMemory(long bytes, long heldBreakerBytes, String label)` stores `delta = bytes - heldBreakerBytes` into `queryConstructionMemoryByLabel` (line 843). When the reservation-swap pattern is used (pre-flight holds > actual, i.e. `heldBreakerBytes > bytes`), `delta` is negative and the per-label `AtomicLong` accumulates a negative sum.

In `releaseQueryConstructionMemory()` the guard is `if (held != 0)` (line 873). When `held < 0`, the call becomes:
```java
circuitBreaker.addWithoutBreaking(-held, entry.getKey());  // -held is positive — adds bytes to breaker
```
This permanently inflates the breaker's `used` counter by the reservation-refund amount. Over time this causes the request breaker to trip on allocations that should otherwise succeed.

**Concrete failure:** Query for a wildcard field uses a 500-byte pre-flight reservation but the compiled automaton only needs 100 bytes. The label's accumulated delta is `100 - 500 = -400`. On `releaseQueryConstructionMemory()`, `circuitBreaker.addWithoutBreaking(400, "wildcard")` is called, adding 400 bytes to the breaker instead of releasing them. The breaker usage drifts upward by 400 bytes per such query, eventually causing spurious trips.

**Fix:** Change the guard to `if (held > 0)` — negative net accumulations mean bytes were already returned to the breaker via `addWithoutBreaking(delta, label)` at admission time, so nothing additional should be released. Or equivalently: clamp `held` to zero before the release: `if (held > 0) { circuitBreaker.addWithoutBreaking(-held, ...) }`.

---

### 2. `PreallocatedCircuitBreaker.close()` — "used-all" path transiently spikes `used`, risking spurious parent trips

**File:** `server/src/main/java/org/elasticsearch/common/breaker/PreallocatedCircuitBreakerService.java`, lines 192–193  
**Severity:** Bug — concurrency race  

In the "used-all" branch of `close()`, the PR issues two separate `addWithoutBreaking` calls to correct the held gauge:
```java
next.addWithoutBreaking(preallocated, ChildMemoryCircuitBreaker.UNCATEGORIZED_RELEASE);  // line 192
next.addWithoutBreaking(-preallocated, preallocateLabel);                                 // line 193
```

The accompanying comment states these "net to zero real bytes", but `addWithoutBreaking(long, String)` in `ChildMemoryCircuitBreaker` calls `adjustUsedBytes(bytes)` first (line 264), which does `used.addAndGet(bytes)`. Between lines 192 and 193, the real `used` counter on the underlying breaker is inflated by `preallocated` bytes. Any concurrent `checkParentLimit` that reads `used` in this window will see an artificially elevated total and may fire a `CircuitBreakingException`.

**Concrete failure:** A 10 MB preallocation closes in the "used-all" state. A concurrent request that is 5 MB below the parent limit triggers `checkParentLimit`, reads the transiently inflated `used` (inflated by 10 MB), and gets a spurious `[parent] Data too large` exception.

**Fix:** Avoid touching `used` at all in this branch. One approach is to add a gauge-only re-label method on `ChildMemoryCircuitBreaker` that calls `memoryHeldMeter.add(...)` directly without touching `used`. Alternatively, the comment's intent is simply to drain the gauge: track the preallocate admission label-to-label from construction so no cancellation is needed.

---

### 3. `CircuitBreaker.addWithoutBreaking(long, String)` — default silently drops label in wrapper implementations

**File:** `server/src/main/java/org/elasticsearch/common/breaker/CircuitBreaker.java`, line 96  
**Severity:** Latent gauge imbalance  

The new interface default method falls back to the unlabeled `addWithoutBreaking(bytes)`:
```java
default void addWithoutBreaking(long bytes, String label) {
    addWithoutBreaking(bytes);
}
```

`LocalCircuitBreaker` (ESQL compute) overrides only the single-arg form. Any labeled release routed through a `LocalCircuitBreaker` wrapper silently drops the label and records under `uncategorized` in the underlying `ChildMemoryCircuitBreaker`. The `held.usage` gauge accumulates a permanent positive balance for the original category and a compensating negative under `uncategorized`.

While the current labeled-release call sites (`SearchExecutionContext`, `PreallocatedCircuitBreakerService`) happen to reach `ChildMemoryCircuitBreaker` directly, the silent-fallback default is a structural trap: every new `CircuitBreaker` wrapper written in future faces the same hazard with no compile-time warning.

**Fix:** Either require explicit override (remove the default and add `addWithoutBreaking(long, String)` as an abstract method — binary-incompatible but exposes all gaps), or change the default to forward to `addWithoutBreaking(bytes)` but log a one-shot `DEBUG` warning the first time it is invoked on a non-implementing class, or add a `@SuppressWarnings`-friendly `checkstyle` rule. At minimum, `LocalCircuitBreaker` should explicitly override the two-arg form so the behavior is visible.

---

### 4. `memoryHeldMeter` records raw bytes; `ES_BREAKER_MEMORY_ESTIMATED` applies overhead — inconsistent scales

**File:** `server/src/main/java/org/elasticsearch/common/breaker/ChildMemoryCircuitBreaker.java`, line 180 vs. `server/src/main/java/org/elasticsearch/indices/breaker/HierarchyCircuitBreakerService.java`, line 295  
**Severity:** Semantic inconsistency (misleading metrics)  

`memoryHeldMeter.add(bytes, ...)` records the raw `bytes` argument. `getUsed()` returns raw bytes. But `collectMemoryEstimates()` computes `(long)(breaker.getUsed() * breaker.getOverhead())`, applying the overhead multiplier.

For the fielddata breaker (default overhead 1.03), an allocation of 1 000 000 bytes:
- Shows 1 000 000 in `es.breaker.memory.held.usage`
- Shows 1 030 000 in `es.breaker.memory.estimated.usage`

A user correlating the two gauges in a dashboard will see a systematic 3% discrepancy with no explanation. Neither the gauge descriptions nor the metric names document this difference.

**Fix:** Either apply overhead in `memoryHeldMeter` (recording `bytes * overhead`) so the two gauges are on the same scale, or update the metric descriptions to explicitly state that `held.usage` is raw bytes and `estimated.usage` includes the overhead multiplier.

---

### 5. `releaseQueryConstructionMemory(long, String)` — decrements total counter even when label absent post-clear

**File:** `server/src/main/java/org/elasticsearch/index/query/SearchExecutionContext.java`, line 897  
**Severity:** Latent correctness — negative monitoring counter  

After `releaseQueryConstructionMemory()` (no-arg) has run, `queryConstructionMemoryByLabel` is cleared and `queryConstructionMemoryUsed` is set to 0. If the two-arg `releaseQueryConstructionMemory(bytes, label)` is then called for any reason:
- Line 893: `queryConstructionMemoryByLabel.get(label)` returns `null` → per-label update is skipped
- Line 897: `queryConstructionMemoryUsed.addAndGet(-bytes)` still executes, driving the total counter negative

In the current codebase the ordering invariant is maintained by caller discipline (the percolator wrapper always calls the two-arg form before the source context is closed). But the guard is missing, making this a latent underflow if the ordering is ever violated by a future change.

**Fix:** Add a guard at line 897: only decrement the total if the per-label entry was found (`if (held != null) { ... queryConstructionMemoryUsed.addAndGet(-bytes); }`). This makes the method idempotent regardless of call order.

---

### 6. Hot-path `Map.of(...)` allocation per labeled admit and release in `ChildMemoryCircuitBreaker`

**File:** `server/src/main/java/org/elasticsearch/common/breaker/ChildMemoryCircuitBreaker.java`, lines 180 and 265  
**Severity:** Efficiency — GC pressure on query hot path  

Every call to `addEstimateBytesAndMaybeBreak(bytes, label)` on the success path allocates a fresh two-entry `Map`:
```java
Map.of(BREAKER_METRIC_TYPE_ATTRIBUTE, this.name, CIRCUIT_BREAKER_CATEGORY_ATTRIBUTE, label)
```
Same at line 265 in the labeled `addWithoutBreaking`. The PR already demonstrates awareness of this cost by caching `uncategorizedHeldAttributes` at construction time (lines 91–96) — the unlabeled path reuses the single cached map.

For queries that compile wildcard or regexp automata, both an admit and a release are issued per query. On a busy search node, this generates one extra short-lived map allocation per breaker interaction on the labeled paths.

**Fix:** Cache attribute maps by label in a `ConcurrentHashMap<String, Map<String, Object>>` field, computed on first use per label. The label vocabulary is small (a handful of query-type strings), so the cache stays bounded. This brings the labeled paths in line with the already-cached unlabeled path.

---

### 7. `ConcurrentHashMap` + `AtomicLong` in `PercolateQueryBuilder.perIterationCharges` is unnecessary for a single-threaded context

**File:** `modules/percolator/src/main/java/org/elasticsearch/percolator/PercolateQueryBuilder.java`, line 652  
**Severity:** Simplification  

`perIterationCharges` is a field of an anonymous `SearchExecutionContext` subclass created fresh per-document inside a Lucene collect loop. `SearchExecutionContext` is documented as not thread-safe. All accesses to `perIterationCharges` happen on the same thread. There is no concurrent access path.

`ConcurrentHashMap` carries synchronization overhead for every `computeIfAbsent` and map iteration, and `AtomicLong` uses volatile reads/writes that are unnecessary for single-threaded accumulation.

**Fix:** Replace `ConcurrentMap<String, AtomicLong>` with `HashMap<String, Long>` and use `merge(label, bytes, Long::sum)` for accumulation and `getOrDefault(label, 0L)` for read. The drain loop becomes `forEach` + `clear()`.

---

### 8. Dual tracking structures (`queryConstructionMemoryUsed` + `queryConstructionMemoryByLabel`) that can diverge silently

**File:** `server/src/main/java/org/elasticsearch/index/query/SearchExecutionContext.java`, lines 143–144  
**Severity:** Design — silent divergence risk  

`queryConstructionMemoryUsed` is maintained as a separately-updated cached sum of `queryConstructionMemoryByLabel`. The two are updated in sequence (not atomically) at three sites:
- `addCircuitBreakerMemory` — both updated together
- `releaseQueryConstructionMemory()` — map drained, then total set to 0
- `releaseQueryConstructionMemory(long, String)` — total decremented even if map key is absent (finding #5)

If a future contributor updates one site without the other, the divergence is silently masked by `queryConstructionMemoryUsed.set(0)` at drain time.

**Fix:** Eliminate the redundant total field. Derive `getQueryConstructionMemoryUsed()` by summing `queryConstructionMemoryByLabel.values()` on demand. The label count per query is O(1–3), so the O(n) sum is negligible. This removes the entire class of synchronization errors.

---

### 9. Gauge supplier method references pin `HierarchyCircuitBreakerService` in `MeterRegistry` indefinitely

**File:** `server/src/main/java/org/elasticsearch/indices/breaker/HierarchyCircuitBreakerService.java`, line ~270; `server/src/main/java/org/elasticsearch/indices/breaker/CircuitBreakerMetrics.java`, lines 95–111  
**Severity:** Lifecycle concern  

`metrics.registerMemoryGauges(this::collectMemoryLimits, this::collectMemoryEstimates)` stores captured `Supplier` references in the `MeterRegistry`. The registry is shared node-wide and has no deregistration API exposed here. The captured `this` prevents the `HierarchyCircuitBreakerService` from being garbage-collected for the lifetime of the registry.

In production this is benign (one service per JVM lifetime). In tests that create multiple `HierarchyCircuitBreakerService` instances against the same `RecordingMeterRegistry` (as `HierarchyCircuitBreakerServiceTests` now does), old service instances remain pinned and their stale supplier data is returned on subsequent collects, corrupting test assertions and producing a leak.

**Fix:** Either add a `deregister()` or `close()` method on `CircuitBreakerMetrics` and `HierarchyCircuitBreakerService` that removes the gauge suppliers, or create a fresh `RecordingMeterRegistry` per test method (verify the tests don't share one across cases).

---

### 10. `collectMemoryEstimates()` calls `memoryUsed(0L)` causing a redundant breaker iteration (and JMX poll) per telemetry scrape

**File:** `server/src/main/java/org/elasticsearch/indices/breaker/HierarchyCircuitBreakerService.java`, line 300  
**Severity:** Efficiency  

`collectMemoryEstimates()` first iterates `this.breakers` for child estimates (lines 294–298), then calls `memoryUsed(0L).totalUsage` for the parent entry (line 300). `memoryUsed(0L)` itself loops over `this.breakers` again (adding each `getUsed() * overhead`) and, when `trackRealMemoryUsage=true` (the default), calls `ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed()` — a JNI/JMX call — to obtain real heap usage. This means every telemetry scrape triggers two full breaker iterations and one JMX heap poll just from `collectMemoryEstimates`.

**Fix:** Accumulate the child sum during the first loop (`total += estimated`) and add the JVM adjustment once at the end, without calling `memoryUsed`. This halves the breaker iterations and avoids the redundant JMX call. Alternatively, share one `MemoryUsage` result computed once per scrape across both `collectMemoryLimits` and `collectMemoryEstimates` by caching it behind a `volatile` reference.

---

## Summary Table

| # | File | Line | Severity | Summary |
|---|------|------|----------|---------|
| 1 | `SearchExecutionContext.java` | 873–874 | **Bug** | Negative per-label balance causes `addWithoutBreaking(positive)` on release, inflating breaker permanently |
| 2 | `PreallocatedCircuitBreakerService.java` | 192–193 | **Bug** | "Used-all" close() transiently spikes `used` by `preallocated` bytes; concurrent `checkParentLimit` may spuriously trip |
| 3 | `CircuitBreaker.java` | 96 | Bug (latent) | Default `addWithoutBreaking(long, String)` silently drops label in all non-overriding wrapper classes |
| 4 | `ChildMemoryCircuitBreaker.java` / `HierarchyCircuitBreakerService.java` | 180 / 295 | Semantic | `held.usage` reports raw bytes; `estimated.usage` applies overhead — different scales, no documentation |
| 5 | `SearchExecutionContext.java` | 897 | Latent | Two-arg `releaseQueryConstructionMemory` decrements total even when label absent, can produce negative counter |
| 6 | `ChildMemoryCircuitBreaker.java` | 180, 265 | Efficiency | Labeled admit/release allocates a fresh `Map.of(...)` per call; uncategorized path already caches its map |
| 7 | `PercolateQueryBuilder.java` | 652 | Simplification | `ConcurrentHashMap` + `AtomicLong` used in a single-threaded per-document context; plain `HashMap<String, Long>` suffices |
| 8 | `SearchExecutionContext.java` | 143–144 | Design | `queryConstructionMemoryUsed` is a redundant cached sum of the label map; divergence is silently masked at drain |
| 9 | `HierarchyCircuitBreakerService.java` | ~270 | Lifecycle | Gauge supplier method references pin the service in the registry indefinitely; leaks in multi-instance tests |
| 10 | `HierarchyCircuitBreakerService.java` | 300 | Efficiency | `collectMemoryEstimates` calls `memoryUsed(0L)`, causing a redundant full breaker loop + JMX poll per scrape |
