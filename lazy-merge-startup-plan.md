# Plan: Lazy Merge Node Startup

## Review Comment Summary

Currently `SubPlansExecutor` starts **all** merge drivers upfront in Phase 2, before any leaf
is dispatched. Each merge driver "bursts" briefly (polling the exchange), yields, and then
sleeps until a leaf supplies data. Two problems:

1. With many merge nodes the burst increases rejection likelihood.
2. All those drivers appear in the task list, cluttering production diagnostics.

Proposed fix: don't start a merge driver until the first leaf *under* it is dispatched.
Before dispatching a leaf, start its ancestors (top-down, if not already started).
This may also let us collapse the three phases into two.

---

## Current Architecture

### Relevant files
- `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plugin/SubPlansExecutor.java`
  (all changes are here; ~1 225 lines)
- Tests: same package

### Three-phase flow in `execute()`
- **Phase 1** (`buildSubPlanContext`, lines 312–332): walks the `SubPlan` tree, registers an
  `ExchangeSourceHandler` per merge node and an eager `ExchangeSinkHandler` per nested-merge
  child. Leaf children get a lazy `ParentSink` (only a keep-alive ref on the parent source;
  no sink registered yet). Builds the `MergeContext`/`LeafContext` tree.
- **Phase 2** (`startMerge`, lines 498–635): recursive top-down walk that calls
  `ComputeService.runCompute` for **every** merge node and collects leaves into the flat
  `scheduledLeaves` list. All merge drivers start here.
- **Phase 3** (lines 247–258): dispatches up to `branchParallelDegree` leaves initially;
  each completion refills its slot via `tryExecuteNextLeaf`.

### Key inner classes
| Class | Role |
|---|---|
| `MergeContext` | coordinator segment; owns `ExchangeSourceHandler`, `started` CAS, list of children |
| `LeafContext` | producer leaf; owns a lazy `ParentSink` |
| `ScheduledLeaf` | pairs `LeafContext` with its `childListener` ref from the parent's `ComputeListener` |
| `ParentSink` | eager (nested merge) or lazy (leaf) exchange sink wrapper |

### Existing `MergeContext.started` CAS (line 1133)
Already used for two purposes: launch gate (`startMerge` sets it) and abort guard
(`abortUnstartedMergeContext` skips nodes where it is already set). This CAS is the right
hook for lazy startup.

---

## Proposed Architecture: Two Phases

### New Phase 1 — setup (unified tree walk)

Merge what Phase 1 and Phase 2 *allocate* into one pass. The tree walk in
`buildSubPlanContext` already visits every node; extend it to also create each merge's
`ComputeListener` and acquire all refs, but **not** call `runCompute`.

For each `MergeContext`:
- Create `ComputeListener(cancelOnFailure, terminalListener)`.
- Acquire `guard = computeListener.acquireAvoid()`.
- Acquire `segmentListener = computeListener.acquireCompute()` — **store in `MergeContext`**.
- For each child, acquire `childListener = computeListener.acquireCompute()`:
  - Leaf child → `ScheduledLeaf(leaf, childListener)` as now; also record ancestor chain in `LeafContext`.
  - Nested merge child → recurse, passing `childListener` as `completionListener`.
- Store `childListeners` list in `MergeContext` (needed for error recovery in Phase 2).
- `guard.onResponse(null)` + close `computeListener` (drops the initial ref). Ref tree is frozen.
- **Do not call `runCompute`.**

Rollback (`cleanupUnstarted`): same exchange teardown as now, plus complete any stored
`segmentListeners` with the failure error.

### New Phase 2 — lazy dispatch

Replace the old Phase 2 (`startMerge` call) + Phase 3 with a single dispatch loop.

Before dispatching each leaf (`executeLeaf`), call `ensureAncestorsStarted(leaf)`:
```
for ancestor in leaf.ancestors  // top-down: root first, direct parent last
    if ancestor.started.compareAndSet(false, true):
        runComputeFor(ancestor)   // may throw; see error handling below
```

Then proceed with `parentSink.attach()` and `executePlan` as today.

---

## Data Structure Changes

### `LeafContext` — add one field
```java
// Ancestor chain from root MergeContext down to direct parent, built in Phase 1.
private final List<MergeContext> ancestors;
```
Built by passing a growing list down through the recursive `buildSubPlanContext` call. The
list is immutable once the leaf is constructed.

### `MergeContext` — add four fields
```java
// Stored in Phase 1; consumed lazily when the first leaf under this node is dispatched.
private volatile ActionListener<DriverCompletionInfo> segmentListener;
private List<ActionListener<DriverCompletionInfo>> childListeners; // parallel to children
private Runnable cancelOnFailure;
private PlanTimeProfile segmentPlanTimeProfile;
```
`started` and `sourceRemoved` are unchanged.

---

## Method Changes

### `buildSubPlanContext` (currently Phase 1 only)
After creating `MergeContext`, immediately allocate its `ComputeListener` (inline the
ref-allocation logic currently in Phase 2's `startMerge`). Pass the growing ancestor list
as a parameter so leaf children can record it.

### `buildChildContext`
For nested-merge children: instead of recursing through a no-op path, pass the
`childListener` as `completionListener` to the recursive `buildSubPlanContext` call (which
now also allocates refs). For leaf children: pass the current ancestor list when constructing
`LeafContext`.

### `execute()` — remove Phase 2 trigger
Delete the `startMerge(root, ...)` call. The Javadoc block describing Phase 2 is replaced
with a description of lazy ancestor startup.

### Add `ensureAncestorsStarted(LeafContext leaf)` — called from `executeLeaf`
```java
private void ensureAncestorsStarted(LeafContext leaf) {
    for (MergeContext ancestor : leaf.ancestors) {
        if (ancestor.started.compareAndSet(false, true)) {
            try {
                runComputeFor(ancestor);
            } catch (Exception e) {
                // settle this ancestor's refs and abort its subtree; re-throw
                handleAncestorStartFailure(ancestor, e);
                throw e;
            }
        }
    }
}
```

### Add `runComputeFor(MergeContext mc)`
Extracted from the `startMerge` body; calls `computeService.runCompute(...)` using
`mc.segmentListener`, `mc.computeSessionId`, `mc.segmentPlanTimeProfile`, etc.
The `noMoreLeaves` wrapper on the root's `segmentListener` moves here.

### Add `handleAncestorStartFailure(MergeContext mc, Exception e)`
When `runComputeFor` throws synchronously (before `runCompute` absorbs `segmentListener`):
1. `mc.segmentListener.onFailure(e)` (notifyOnce makes it a no-op if already absorbed).
2. `mc.childListeners.forEach(l -> l.onFailure(e))` — settles all child refs.
3. `abortChildrenWithSettledListeners(mc, e)` — marks leaf sinks finished; aborts nested merges.

### `cleanupUnstarted` — extend rollback
After the existing exchange teardown, also complete any `segmentListeners` on
`unstartedMergeContexts` that were allocated but never consumed by `runCompute`.

### `startMerge` — refactor or remove
The method can be decomposed into `allocateRefs` (called in Phase 1) and `runComputeFor`
(called lazily in Phase 2), then deleted. The error-handling inner catch in `startMerge`
moves into `handleAncestorStartFailure`.

---

## Error Handling Details

### Phase 1 failure (unchanged rollback path)
`cleanupUnstarted` tears down exchange registrations and fails parent sinks, as now.
Extension: walk `unstartedMergeContexts` reversed and call
`mc.segmentListener.onFailure(failure)` for any `segmentListener` that was allocated.
This is safe because `notifyOnce` makes it idempotent, and no `runCompute` has started.

### `runCompute` failure during `ensureAncestorsStarted` (new path)
The failed ancestor's `started` is already `true`, so `abortUnstartedMergeContext` will
no-op for it (correct — the node attempted to start). Its siblings that have not yet started
are aborted through `abortChildrenWithSettledListeners` called from the parent's handler.
For the failed ancestor itself, `handleAncestorStartFailure` settles all child refs, which:
- Marks each leaf's `parentSink` as finished → `executeLeaf` will skip undispatched leaves.
- Aborts each nested merge child via `abortUnstartedMergeContext`.

The exception propagates out of `ensureAncestorsStarted`, is caught by `executeLeaf`'s
existing `catch (Exception e)` block, and routed to `settleLeafAndRefill` as a failure.

### Already-running ancestors when an error fires
They self-clean through their own `terminalListener` (deregister source, finishEarly, signal
parent sink) once the exchange delivers the error to their drivers — same as today.

---

## Concurrency

With `branchParallelDegree > 1`, two leaves under the same merge can both enter
`ensureAncestorsStarted` concurrently. The `compareAndSet(false, true)` CAS ensures exactly
one calls `runComputeFor`. The other sees `true` and skips. Since `runCompute` is
non-blocking (submits drivers to the compute executor) and the exchange is buffered, the
second leaf may start writing before the merge driver has "run" — the same timing window
that exists today with Phase 2 submitting and Phase 3 dispatching concurrently.

---

## Phases: Why Two Instead of Three

The reason Phase 1 and Phase 2 were separate was that Phase 1 "has no async side effects,
so any exception can be cleanly rolled back." With the unified walk, ref allocation is also
free of async side effects (in-memory only, no drivers started). The rollback extension
(completing `segmentListeners`) is equally clean. The reviewer's intuition that phases
can collapse is correct.

After the change, `execute()` Javadoc describes:
- **Setup** (one synchronous tree walk): register exchanges, allocate ref trees, build leaf ancestor lists.
- **Dispatch** (async): dispatch leaves; before each leaf, start unstarted ancestors.

---

## Files to Change

| File | Changes |
|---|---|
| `SubPlansExecutor.java` | All of the above (~150–200 lines modified/added) |
| `SubPlansExecutorTests.java` (or equivalent) | Update Phase 2 assertions; add tests for lazy startup ordering |

No other files need to change — `MergeExec`, `PlannerUtils.buildSubPlan`, `ComputeService`, and
the exchange layer are untouched.

---

## Estimated Complexity

**Medium.** The ref-counting invariants and CAS guards already exist; we are mostly moving
*where* `runCompute` is called (Phase 2 → just before leaf dispatch) and adding an ancestor
list to `LeafContext`. The trickiest part is the error path in `handleAncestorStartFailure`:
it must settle all pre-allocated `childListeners` (stored in `MergeContext`) before aborting
child sinks, matching the ordering invariant that `settleThenAbortChildren` currently
enforces.

The existing exhaustive Javadoc on `startMerge` and `buildSubPlanContext` will need
corresponding updates to reflect the new two-phase model.

---

# Follow-up: Lazy Nested-Merge Exchange Sink

Applies on top of the implemented plan above. Lazy merge startup left one piece eager —
the nested merge's `ExchangeSinkHandler` — and that turns out to be a correctness problem,
not just an inefficiency.

## Why: lazy startup invalidated a documented invariant

`ParentSink`'s Javadoc justified eager merge sinks with:

> Nested-merge sinks are **eager**: their handler is registered in phase 1 and their
> `runCompute` attaches an `ExchangeSink` to it **synchronously in phase 2**, so the
> `InactiveSinksReaper` sees them as active.

True under three-phase execution, false once phase 2 stopped starting every merge.

`InactiveSinksReaper` (`ExchangeService:344-362`):

```java
if (sink.hasData() && sink.hasListeners()) continue;   // skip only if BOTH
if (nowInMillis - sink.lastUpdatedTimeInMillis() > keepAlive) finishSinkHandler(key, timeout);
```

`hasData()` is `outstandingSinks.get() > 0 || buffer.size() > 0` (`ExchangeSinkHandler:177`),
and `outstandingSinks` is incremented only by `createExchangeSink()` — which for a nested
merge happens inside `runComputeFor`, i.e. **at lazy start**. So an unstarted nested merge has
`hasData() == false`, fails the skip test regardless of listeners, and its `lastUpdatedInMillis`
is frozen at the parent fetcher's first `fetchPageAsync`. After
`esql.exchange.sink_inactive_interval` (**default 5 minutes**) the handler is reaped with an
`ElasticsearchTimeoutException`, failing the query.

Reachable in practice: 100 leaves at `branchParallelDegree=2`, ~10s each ≈ 8 minutes, so a
merge registered at t=0 but not started until t=6min is killed before it runs.

This is precisely the hazard already documented for leaves — *"a leaf can sit in
`scheduledLeaves` behind `branchParallelDegree` for longer than the reaper's inactive
interval, and an idle registered handler would be reaped"* — which lazy startup extended to
merges without extending the mitigation.

## The change

Nested merges stop having their own sink flavor and reuse the lazy constructor and `attach()`
that leaves already use. This **removes** a code path rather than adding one.

### `buildChildContext`

```java
if (child instanceof SubPlan.Merge merge) {
    var childSink = new ParentSink(childSessionId, parentSource);   // lazy ctor
    unstartedParentSinks.add(childSink);
    parent.children.add(buildSubPlanContext(merge, childPath, childSink, collectedPages, ancestors));
}
```

The `createSinkHandler` + `addRemoteSink` pair moves into `attach()`, which already performs
that sequence in the correct order.

### `runComputeFor`

Attach at start instead of dereferencing a pre-built handler:

```java
Supplier<ExchangeSink> sinkSupplier = mergeContext.parentSink == null
    ? null
    : mergeContext.parentSink.attach();
```

`attach()` already returns the `() -> handler.createExchangeSink(...)` supplier that
`ComputeContext` expects. The root keeps `parentSink == null`.

### `ParentSink`

Delete the eager constructor; `parentSource` and `pendingRef` lose `@Nullable` and are always
set; `handler` is always published by `attach()`.

### Re-justify `attach()`'s precondition

The assert `finished.get() == false` held for leaves because *"aborts happen synchronously in
phase 2, before any leaf is dispatched"*. That rationale does **not** survive for merges started
at arbitrary later times.

The assert itself is still sound, so it was kept and its justification rewritten rather than
converted into a runtime check. What guarantees it now is the `MergeContext.started` CAS:
`runComputeFor` is reached only by the caller that won that CAS, and every path that can finish
a merge's sink must claim the same CAS first — `abortUnstartedMergeContext`, the
`drainUnstartedMerges` failure path, and (transitively) `abortChildrenWithSettledListeners`.
`mergeTerminalListener` can only fire once `segmentListener` is settled, which likewise requires
either `runCompute` to have absorbed it or an abort to have won the CAS. `cleanupUnstarted` runs
in phase 1, before any start. So `finished == true` is unreachable at `attach()` time, and adding
a fallback would be defensive code for an impossible state.

## Ordering is already safe

`attach()` calls `addRemoteSink` on the *parent's* source, which is registered eagerly in
phase 1 and held open by this child's own `pendingRef` until `attach()` swaps it for the
`addRemoteSink` ref. Neither caller ordering breaks:

- `ensureAncestorsStarted` walks top-down — parent started first.
- `drainUnstartedMerges` walks `reversed()` (children first) — the parent may be unstarted, but
  its *source object* exists and holds refs, which is all `addRemoteSink` needs.

Failure handling already works: `attach()` publishes `handler` *before* `addRemoteSink`, so a
throw from either point still lets `finishParentSink` release `pendingRef` (`releaseOnce`) and
deregister the handler if one exists. Both call sites already wrap `runComputeFor` in a catch
routing to `handleAncestorStartFailure`.

## Javadoc that asserts the old invariant

| Location | Correction |
|---|---|
| `ParentSink` class doc | The eager/lazy split disappears; all sinks are lazy |
| `execute()` phase-1 doc + example | "2 eager sinks, 4 lazy keep-alive refs" → 6 lazy refs |
| `buildSubPlanContext` doc + example | `sink1` leaves the phase-1 registration list; "Leaf sinks are registered lazily" now covers all sinks |
| `cleanupUnstarted` | Its ordering rationale rests on an eager handler racing a live fetch; since it runs only in phase 1, no handler exists at all once merges are lazy |

## Test impact

One existing test changed. `testFinishSessionEarlyUnblocksQueuedNestedLeaves` asserted
`sinkKeys()` had size 3 — "innerA sink, innerB sink, one leaf" — while its own comment says
innerB *"starts lazily after STOP fires"*. That third key was innerB's eagerly registered
handler, i.e. exactly the reapable sink this change removes, so the expectation drops to 2 and
gains an explicit assertion that an unstarted nested merge registers no sink at all.

Everything else passes unchanged: 21/21 in `SubPlansExecutorTests`, 420/420 across 20 seeds,
and 435/435 for the whole `org.elasticsearch.xpack.esql.plugin.*` package.

## Deliberately out of scope

The `drainUnstartedMerges` success path keeps calling `runComputeFor`. It could stop once
merges hold no `addRemoteSink` ref, but that changes PROFILE output (a never-run merge would
vanish from it) and interacts with still-queued leaves under the drained merge. Separate
change, separate test pass.

---

# Approach Comparison: Three Side-by-Side

## The Three Approaches

| | **A — Current (non-recursive)** | **B — Lazy merge startup (this plan)** | **C — Recursive (`MergeLevelExecutor`)** |
|---|---|---|---|
| **Branch** | `nested-unionall-non-recursive` | this plan (in `nested-unionall-non-recursive`) | `nested-unionall-recursive` |
| **Key files** | `SubPlansExecutor.java` (1 225 lines) | same file, modified | `MergeLevelExecutor.java` (710 L) + `SubPlanTaskRunner.java` (~300 L); `SubPlansExecutor` deleted |
| **Plan tree rep.** | Sealed `SubPlan.Leaf` / `SubPlan.Merge` | same | Raw `List<PhysicalPlan>` from `breakPlanIntoSubPlansAndMainPlan`; `SubPlan` class removed |
| **Phases** | Three: register, wire merges, dispatch leaves | Two: setup (register + alloc refs), dispatch (lazy merge start) | None explicitly; recursive descent with per-level window + global runner |
| **When does a merge driver start?** | Phase 2: ALL merges before any leaf | Just before the FIRST LEAF under it is dispatched | When the dispatcher reaches that merge's branch slot (level-by-level) |
| **Concurrency model** | Self-refilling slot per leaf; one atomic `nextLeafIndex` | Same self-refilling slots; `ensureAncestorsStarted` added before each leaf | Per-level `branchParallelDegree` window in `MergeLevelExecutor`; global `SubPlanTaskRunner` enforces overall cap |
| **Sink for leaves** | Lazy (`attach()` at dispatch time) | Same | Strictly lazy; only opened inside `SubPlan.execute()` on the runner thread |
| **Session ID derivation** | Nested (each child uses parent's session prefix) | Same | Flat (all sessions derived from query root); `QueryContext` passed unchanged |

---

## Pros and Cons

### A — Current (non-recursive)

**Pros**
- Battle-tested; the most reviewed and documented.
- Clean invariant: all consumers (merge drivers) are running before any producer (leaf) starts.
- Single synchronous Phase 2 makes failure handling straightforward: rollback ledgers cover everything that ran.
- Flat `scheduledLeaves` list is easy to reason about; `nextLeafIndex` CAS is simple.
- Error path (`settleThenAbortChildren`, `abortUnstartedMergeContext`) is well-established.

**Cons**
- **All merge drivers burst at query start** — reviewer concern #1. With N merge nodes, N `runCompute` calls happen back-to-back before Phase 3 begins.
- **All merge drivers appear in the task list immediately** — reviewer concern #2. Even deeply nested merges whose leaves won't run for seconds are visible.
- Three-phase structure adds conceptual overhead and a long Phase 2 `startMerge` method (137 lines with extensive Javadoc).

---

### B — Lazy merge startup (this plan)

**Pros**
- Merge drivers start JIT: only when the first leaf under them is dispatched. Directly addresses both reviewer concerns.
- Burst is spread across leaf dispatch events (bounded by `branchParallelDegree`).
- Task list only contains merge drivers that are actively serving running leaves.
- Stays within `SubPlansExecutor` — minimal new surface area; no new classes.
- Existing `started` CAS already serves as the launch gate; no new concurrency primitive.
- Two-phase model is conceptually cleaner than three.

**Cons**
- **`handleAncestorStartFailure` is a new, tricky error path.** If `runComputeFor(ancestor)` throws at depth D of an ancestor chain, ancestor D is partially started (its `started=true` but drivers may not have been submitted), ancestors above D are running, ancestors below D are not started. The abort must settle pre-allocated `childListeners` before marking leaf sinks finished — the same ordering invariant as `settleThenAbortChildren`, now in a new call site.
- **`MergeContext` grows 4 fields** (`segmentListener`, `childListeners`, `cancelOnFailure`, `segmentPlanTimeProfile`); `LeafContext` grows 1 (`ancestors`). The inner classes are already large.
- **Hanging `segmentListener` edge case.** If all leaves under a merge are skipped before `ensureAncestorsStarted` runs (e.g. LIMIT fires before any leaf dispatches), the pre-allocated `segmentListener` is never passed to `runCompute` and never completed — the merge's `ComputeListener` terminalListener never fires, hanging the query. Requires explicit detection (e.g. when `noMoreLeaves` fires, complete all unstarted `segmentListeners` as EMPTY). This adds another subtle lifecycle path.
- **Ancestor traversal per leaf** is O(depth) — negligible in practice but requires the ancestor list to be built and stored in every `LeafContext`.
- Conceptually: merge startup is now hidden inside `executeLeaf` rather than an explicit phase, which may surprise future contributors.

---

### C — Recursive (`MergeLevelExecutor` + `SubPlanTaskRunner`)

**Pros**
- **Clean separation of concerns**: `MergeLevelExecutor` owns one merge level; `SubPlanTaskRunner` owns the global concurrency cap. Each class is smaller and independently testable.
- **No pre-allocated ref tree** across levels — each `MergeLevelExecutor` creates its own `ComputeListener` when it starts, so there is no "hanging `segmentListener`" edge case. Unstarted levels simply don't exist yet.
- **`SubPlanTaskRunner` provides simpler concurrency control** than the self-refilling slot model: explicit `fail(e)` and `finish()` methods drain the queue atomically, making the "stop everything on LIMIT/error" path easier to audit.
- Naturally handles "a merge's leaves are all skipped": the merge still starts (when its branch slot fires), exchanges just complete immediately — no special case.
- `SubPlan` sealed class removed — plan tree representation is simpler.
- Session IDs are flat: all child sessions derived from root, so profile output is cleaner.

**Cons**
- **Does NOT fully address the reviewer's concern.** Merges start "level-lazy" (when the dispatcher reaches that branch) not "leaf-lazy" (when the first leaf actually runs). With `branchParallelDegree=2` and a two-level tree, if both of the root's branches are merges, both nested merges start synchronously on the same call chain before any leaf runs. The burst is reduced vs approach A but not eliminated.
- **Much larger diff**: deletes 1 225 lines (`SubPlansExecutor`), adds ~1 010 lines (two new classes), modifies `ComputeService` and several supporting files. Harder to review.
- **Deadlock risk is explicitly documented** and must be maintained by every future contributor: merge sub-plans must bypass `SubPlanTaskRunner`; putting one in the queue while holding a permit deadlocks at `branchParallelDegree=2`.
- `ComputeService` fields must be made `package-private` to allow the new classes to access them — a minor layering concern.
- No existing tests cover the recursive class; test surface is entirely new.
- Recursive synchronous expansion of a merge branch means a deeply nested plan can grow the call stack proportionally to depth (though depth is typically small).

---

## Summary Verdict

| Goal | A (current) | B (lazy) | C (recursive) |
|---|---|---|---|
| No upfront burst of merge drivers | ✗ | ✓ | Partial |
| Merge drivers absent from task list until needed | ✗ | ✓ | Partial |
| Conceptual simplicity | Medium | Medium | Lower (two interacting classes + deadlock constraint) |
| Implementation risk | None (existing) | Medium (new error path + edge case) | High (large rewrite) |
| Code size change | — | +~150–200 lines | Net −215 lines but new surface area is larger |
| Phase count | 3 | 2 | 0 (recursive, no named phases) |

**Recommendation**: Approach B most directly addresses both reviewer concerns with the smallest diff. The two non-trivial risks (the `handleAncestorStartFailure` ordering and the hanging-`segmentListener` edge case) are well-scoped and testable. Approach C is architecturally interesting but is a large bet with partial benefit on the stated concerns. Approach A is the baseline to beat.
