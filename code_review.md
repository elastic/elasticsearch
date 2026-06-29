# Code Review: `circuit-breaker/memory-held-gauge`

**Branch**: `circuit-breaker/memory-held-gauge`  
**Compared to**: `main`  
**Effort level**: max (7 angles × parallel finders, 6 independent verifiers)

## Summary

This PR introduces three new circuit breaker telemetry metrics (`es.breaker.memory.held.usage`, `es.breaker.memory.limit.size`, `es.breaker.memory.estimated.usage`). The design — bounded categories, pre-allocated attribute maps, label-to-category routing — is solid. Three confirmed bugs and four lower-severity issues follow.

---

## Confirmed Bugs

### 1. `CircuitBreakingOperations.java:205` — unlabeled release after labeled admit permanently skews held gauge

**Severity: High**

`CircuitBreakingOperations.determinize` admits bytes under a caller-supplied label via:
```java
// line ~170
circuitBreaker.addEstimateBytesAndMaybeBreak(estimatedNewBytes, label);
```
The label is `"wildcard"` or `"regexp"` (both `KNOWN_CATEGORIES`), so the admission is recorded on `es.breaker.memory.held.usage` under `category=wildcard` or `category=regexp`.

The `finally` block then releases via the *unlabeled* overload:
```java
// line 205
circuitBreaker.addWithoutBreaking(-totalReserved);
```
After this PR, the unlabeled `addWithoutBreaking(long)` records the delta under `CATEGORY_UNCATEGORIZED`. The result: every wildcard/regexp automaton build (i.e., every such query execution) permanently inflates the `wildcard`/`regexp` category and deflates `uncategorized` by `totalReserved` bytes. Over time the gauge diverges arbitrarily from actual held memory.

**Fix**: Change line 205 to pass the label:
```java
circuitBreaker.addWithoutBreaking(-totalReserved, label);
```

---

### 2. `PreallocatedCircuitBreakerService.java:198–199` — else-branch transiently inflates `used`, risking spurious parent trip

**Severity: Moderate**

In `PreallocatedCircuitBreaker.close()`, the "used all" state has two branches. The `ChildMemoryCircuitBreaker` branch (lines 195–196) correctly uses `recordHeldDelta` — a gauge-only write that does not touch the `used` counter — specifically to avoid a transient inflation race (the comment on line 190 explains this). The `else` branch for non-`ChildMemoryCircuitBreaker` delegates (lines 198–199) does not apply the same care:

```java
// lines 198-199 — reintroduces the exact race the instanceof branch avoids
next.addWithoutBreaking(preallocated, ChildMemoryCircuitBreaker.CATEGORY_UNCATEGORIZED);
next.addWithoutBreaking(-preallocated, preallocateLabel);
```

`addWithoutBreaking(long, String)` calls `adjustUsedBytes(bytes)`, which atomically modifies `used`. Between these two calls, `used` is transiently inflated by `preallocated` bytes. A concurrent `checkParentLimit` that observes the inflated value may throw a `CircuitBreakingException` for a usage level that was never actually reached.

In production `next` is always a `ChildMemoryCircuitBreaker`, so the else-branch is dead code today. But it is a latent correctness hazard for any test or out-of-tree caller using a custom delegate.

**Fix**: Replace the else-branch with a symmetric pair of `recordHeldDelta` calls via a cast, or expose `recordHeldDelta` on the interface/abstract class, or change the comment to explicitly note that the else-branch is unsafe for multi-threaded callers.

---

### 3. `HierarchyCircuitBreakerService.java:300` — `collectMemoryEstimates()` issues a JMX heap-introspection call on every gauge collection tick

**Severity: Low / Performance**

`collectMemoryEstimates()` computes the parent breaker's estimated usage by calling `memoryUsed(0L).totalUsage`:

```java
// line 300
memoryUsed(0L).totalUsage,
```

`memoryUsed` delegates to `currentMemoryUsage()` → `realMemoryUsage()` → `MEMORY_MX_BEAN.getHeapMemoryUsage().getUsed()` when `trackRealMemoryUsage` is `true` — the production default (`USE_REAL_MEMORY_USAGE_SETTING` defaults to `true`).

`allStats()` at line 375 already calls `memoryUsed(0L)`, but that method is only invoked on user-triggered node-stats requests. Registering `collectMemoryEstimates` as a gauge callback adds this JMX call to every background telemetry collection cycle. At high collection frequencies, or on JVMs where `getHeapMemoryUsage()` triggers safepoint coordination, this adds recurring latency and GC pressure.

**Fix**: Cache the last computed value or deduplicate the call with the existing `allStats()` path. Alternatively, sum child `getUsed()` values (without the real-memory adjustment) for the gauge, reserving the full JVM query for the node-stats REST API where it has always lived.

---

## Lower-severity / Cleanup

### 4. `SearchExecutionContext.java:145` — `ConcurrentHashMap` unnecessary for a single-threaded object

**Severity: Low / Cleanup**

`SearchExecutionContext` is a per-shard, per-query-phase object accessed by a single thread. `queryConstructionMemoryByLabel` is declared as:

```java
private final ConcurrentMap<String, AtomicLong> queryConstructionMemoryByLabel = new ConcurrentHashMap<>();
```

`ConcurrentHashMap` carries volatile reads and lock-free CAS on every `computeIfAbsent` call (line 844). A plain `HashMap<String, Long>` with simple `merge` would be equivalent and cheaper. Additionally, the combination of `getAndSet(0)` iteration and subsequent `clear()` in `releaseQueryConstructionMemory()` (lines 872–878) has a latent race if the single-threaded contract is ever violated: a concurrent `addCircuitBreakerMemory` could add bytes between the `getAndSet(0)` and `clear()`, losing those bytes from accounting permanently.

**Fix**: Replace with `HashMap<String, Long>` and use `merge` for accumulation.

---

### 5. `PercolateQueryBuilder.java:653` — per-document `ConcurrentHashMap` allocation adds GC pressure on the percolation hot path

**Severity: Low / Performance**

`perIterationCharges` was changed from a single `AtomicLong` to a `ConcurrentHashMap<String, AtomicLong>`. Each `addCircuitBreakerMemory` call now triggers `computeIfAbsent` (potentially allocating an `AtomicLong`), and each document iteration clears the entire map. For high-throughput percolation workloads this multiplies short-lived heap allocation compared to the prior single-slot design. Since the percolate wrapper itself is single-threaded, there is no benefit from the concurrency overhead.

**Fix**: Use a plain `HashMap<String, Long>` with `merge`, or accumulate a `long` per-label without an intermediate `AtomicLong`.

---

### 6. `HierarchyCircuitBreakerService.java:280–305` — gauge callbacks allocate fresh collections on every tick

**Severity: Low / Efficiency**

Both `collectMemoryLimits()` and `collectMemoryEstimates()` allocate a new `ArrayList` and one `LongWithAttributes`/`Map.of` pair per breaker on every invocation. For the default four breakers this is eight short-lived heap objects per collection cycle per node. Pre-computing immutable attribute maps (analogous to `categoryAttributeCache` in `ChildMemoryCircuitBreaker`) and reusing a fixed-size list would eliminate this allocation entirely.

---

### 7. `HierarchyCircuitBreakerTelemetryIT.java:111` — IT test does not exercise `ES_BREAKER_MEMORY_HELD`

**Severity: Low / Test coverage**

`testCircuitBreakerMemoryGauges` verifies `ES_BREAKER_MEMORY_LIMIT` and `ES_BREAKER_MEMORY_ESTIMATED` but not `ES_BREAKER_MEMORY_HELD` — the primary new metric introduced by this PR. The unit tests in `HierarchyCircuitBreakerServiceTests` exercise the held gauge in isolation, but the integration test should confirm that the metric is registered and reported in a real cluster with at least one admission/release cycle to validate end-to-end wiring.

---

## Actions Required

| # | File | Action |
|---|------|--------|
| 1 | `CircuitBreakingOperations.java:205` | **Must fix**: pass `label` to `addWithoutBreaking` |
| 2 | `PreallocatedCircuitBreakerService.java:198–199` | **Should fix**: align else-branch with ChildMemoryCircuitBreaker path to avoid transient used inflation |
| 3 | `HierarchyCircuitBreakerService.java:300` | **Should fix**: avoid JMX call in gauge callback; cache or use child-sum instead |
| 4 | `SearchExecutionContext.java:145` | **Consider**: downgrade to `HashMap<String, Long>` |
| 5 | `PercolateQueryBuilder.java:653` | **Consider**: downgrade to `HashMap<String, Long>` |
| 6 | `HierarchyCircuitBreakerService.java:280–305` | **Consider**: pre-allocate attribute maps in gauge callbacks |
| 7 | `HierarchyCircuitBreakerTelemetryIT.java:111` | **Should add**: cover `ES_BREAKER_MEMORY_HELD` in the IT test |
