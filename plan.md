# Fix: SMILE query clause uncontrolled resource consumption

Tracking issue: elastic/security#8690

---

## Root cause

`BoolQueryBuilder.fromXContent` uses `ObjectParser.declareObjectArrayOrNull` to stream and
allocate each inner `QueryBuilder` object during XContent parsing. All clause objects land in
heap before any clause-count validation runs.

`AbstractObjectParser.parseArray` builds a full `ArrayList` as it goes. A throw from the
item parser stops the loop early; a check in the `clauses.forEach(...)` consumer (as the
original plan proposed) is already too late — all objects are allocated by then.

The check that does exist — `ContextIndexSearcher.verifyQueryLimit` →
`MaxClauseCountQueryVisitor` — fires at shard-level weight construction, after:

1. HTTP layer decodes compact SMILE (~3.5 MB) with no size alarm (`http.max_content_length`
   defaults to 100 MB)
2. Coordinating-node parse builds the full `QueryBuilder` tree (e.g. 500k `TermQueryBuilder`
   objects at ~264 B each → ~130 MB of heap)
3. Tree is serialised and fanned out to every shard
4. Each shard calls `toQuery()` (which runs `MaxClauseCountQueryVisitor`) → then
   `ContextIndexSearcher.rewrite()` → `verifyQueryLimit()` → finally rejects

Both `MaxClauseCountQueryVisitor` invocations (`toQuery` and `verifyQueryLimit`) are still
post-allocation and too late. `createWeight` runs after `rewrite()`, not before.

---

## What NOT to do

- **Do not add anything to `XContentParserConfiguration`.**
  `XContentParserConfiguration` is an interface; the live implementation is
  `XContentParserConfigurationImpl` in `libs/x-content`, which has no search/clause concepts.
  `XContentParser` exposes `getRestApiVersion()` / `getDeprecationHandler()` but no
  `getParserConfiguration()`. The node-wide `parserConfig` is built once in
  `AbstractHttpServerTransport` and shared across all concurrent requests — a mutable counter
  on it would race.

- **Do not modify `RestSearchAction` or `SearchSourceBuilder` to reconfigure the parser.**
  `RestSearchAction` does not create a parser; it uses
  `request.withContentOrSourceParamParserOrNull(...)`. Search uses instance `parseXContent`,
  which already calls `parseTopLevelQuery`.

- **Do not patch only `BoolQueryBuilder.PARSER` lambdas.**
  This misses `DisMaxQueryBuilder` and any other compound query builder.

- **Do not use `INDICES_MAX_CLAUSE_COUNT_SETTING` as the limit.**
  `indices.query.bool.max_clause_count` (default 4096, `DeprecatedWarning`) is not read
  anywhere except `ClusterSettings` registration. The live limit is set in `NodeConstruction`:

  ```java
  // NodeConstruction.java:621
  IndexSearcher.setMaxClauseCount(SearchUtils.calculateMaxClauseValue(threadPool));
  ```

  Gate the parse-time check on `IndexSearcher.getMaxClauseCount()`.

---

## Plan

### Step 1 — Fix: count `QueryBuilder` named objects in `parseTopLevelQuery`

The right extension point is `AbstractQueryBuilder.parseTopLevelQuery`. It already wraps the
parser in a `FilterXContentParserWrapper` that counts nesting depth for
`indices.query.bool.max_nested_depth`. Add a width counter in the same wrapper.

#### Critical implementation constraints

**1. Do NOT call `super.namedObject()`.**
`FilterXContentParser.namedObject` forwards to `delegate().namedObject(...)`, passing the raw
unwrapped parser into every `fromXContent`. All inner queries would then call
`parseNamedObject` through the raw parser, never re-entering the override. Both `clauseCount`
and `nestedDepth` would fire exactly once (for the outermost query), making the check
completely ineffective. The correct call is:

```java
getXContentRegistry().parseNamedObject(categoryClass, name, this, context)
```

(`this` = the wrapper, so nested queries re-enter the override.)

**2. Preserve the two-block structure from the existing `nestedDepth` implementation.**
The live code has a pre-parse block (increment + guard) and a post-parse block (decrement +
telemetry). `clauseCount` goes in the pre-parse block only and must **never be decremented**
— it is a running total, not a stack depth. A comment is required to prevent a future
contributor from adding `clauseCount--` by analogy with `nestedDepth--`.

**3. Snapshot `IndexSearcher.getMaxClauseCount()` once.**
Read it into a local `int max` before the comparison; use `max` in both the guard and the
exception message. Reading the static field twice risks a self-contradictory error message if
a concurrent test restores it between the two reads.

**4. Throw `ParsingException` with `parser.getTokenLocation()`**, not bare
`IllegalArgumentException`. `ParsingException` maps to HTTP 400 and includes source location,
matching the pattern of all other parse errors.

#### Correct sketch

```java
// AbstractQueryBuilder.java — inside parseTopLevelQuery's anonymous FilterXContentParserWrapper
// alongside existing: int nestedDepth;
int clauseCount; // running total; intentionally NOT decremented (unlike nestedDepth)

@Override
public <T> T namedObject(Class<T> categoryClass, String name, Object context)
        throws IOException {
    if (categoryClass.equals(QueryBuilder.class)) {
        nestedDepth++;
        if (nestedDepth > maxNestedDepth) { /* existing check, unchanged */ }

        int max = IndexSearcher.getMaxClauseCount();
        if (++clauseCount > max) {
            throw new ParsingException(
                getTokenLocation(),
                "query has too many clauses [" + clauseCount + "], max is [" + max + "]"
            );
        }
    }
    // parseNamedObject with 'this' so inner queries re-enter this override
    T result = getXContentRegistry().parseNamedObject(categoryClass, name, this, context);
    if (categoryClass.equals(QueryBuilder.class)) {
        queryNameConsumer.accept(name);
        nestedDepth--;
    }
    return result;
}
```

#### Counting semantics (intentional tightening)

`clauseCount` counts every `QueryBuilder` named object — including compound containers
(`bool`, `dis_max`) themselves — while `MaxClauseCountQueryVisitor` counts only Lucene leaf
queries. At the limit boundary, parse-time rejection is slightly stricter than runtime. This
is an intentional security tradeoff; document it in the exception message and a short comment.

Use "query has too many clauses" (not "field expansion or bool query…" — field expansion has
not run at parse time).

#### Scope: what this covers and what it does not

**Covered:** All HTTP/SMILE paths that call `parseTopLevelQuery` — `query`, `post_filter`,
rescorers, highlight queries, percolator index-time parsing, `dis_max` and every other
compound query.

**Not covered (out of scope for this CVE):** Native-transport `SearchRequest`
deserialization via `StreamInput`. `QueryBuilder` objects are read directly without passing
through `parseTopLevelQuery`. The existing `verifyQueryLimit` / `toQuery` visitor still stops
execution at the shard, but coordinator-node heap is unprotected on this path. Note this
explicitly in a comment.

**Note on per-call counters:** Each call to `parseTopLevelQuery` creates a fresh wrapper with
its own `clauseCount`. A request with multiple independent query positions (query +
post_filter + rescorers + highlight queries) gets a fresh budget for each. This is acceptable
for the SMILE/HTTP CVE vector, which targets the query body; document it rather than trying
to share a counter across calls (which would require non-trivial threading).

**Note on `WrapperQueryBuilder`:** `WrapperQueryBuilder.fromXContent` stores raw bytes and
repopulates in `doRewrite` via a fresh `parseTopLevelQuery` call with its own counter. This
is an existing gap; documenting it is sufficient for this change.

**Keep `verifyQueryLimit` and `toQuery`'s `MaxClauseCountQueryVisitor` in place** as a
fallback safeguard for query paths that bypass `parseTopLevelQuery`.

**Update the Javadoc for `parseTopLevelQuery`** to mention the clause-count cap alongside the
existing nested-depth documentation.

### Step 2 — Repro / regression test

**Location:** `BoolQueryBuilderTests` (goes through `parseTopLevelQuery` via
`AbstractQueryTestCase`); add a `DisMaxQueryBuilderTests` case too.

**Design:**
- Save `IndexSearcher.getMaxClauseCount()` before; restore in `finally`.
  (Note: `setMaxClauseCount` writes a plain static int — not thread-safe across parallel test
  classes. This is the existing convention in `IntervalQueryBuilderTests` etc.; accept it.)
- Set `IndexSearcher.setMaxClauseCount(N)` for a small N (e.g. 5).
- Build a `bool/should` query with N inner clauses (N+1 total named objects: 1 bool + N
  terms); serialise as both **JSON and SMILE** using `XContentFactory`.
- Call `parseTopLevelQuery` (not `BoolQueryBuilder.fromXContent` directly — that bypasses
  the wrapper where the check lives).
- Assert `ParsingException` is thrown **from parse**, before `toQuery` is called.
- Add a `dis_max` variant with N inner queries (N+1 total named objects).
- Assert that a bool with N−1 inner clauses (N total named objects: 1 bool + N−1 terms)
  parses without error.
- Do **not** assert heap deltas — TLAB/GC makes them flaky in `ESTestCase`.
- Do **not** parse 500k clauses — a small N is sufficient and fast.

### Step 3 — Changelog and disclosure (to be done by human only)

- Changelog entry goes in `docs/changelog/<pr-number>.yaml` (not root `CHANGELOG.md`).
- Coordinate with the security team before opening a public PR; an embargo may be in place.

---

### Stage 2 — Memory-based circuit-breaker cap at parse time ✅ DONE

**Goal:** charge the REQUEST circuit breaker for every `QueryBuilder` constructed during
`parseTopLevelQuery`, so that large-payload queries (e.g. a `terms` query with 500k values)
trip the breaker before the full tree lands in heap.

#### Design (as implemented)

**`parseTimeBreakerEstimate()` on `AbstractQueryBuilder`**

```java
public static final long QUERY_BUILDER_SIZE_ESTIMATE_BYTES = 256L;

protected long parseTimeBreakerEstimate() {
    return QUERY_BUILDER_SIZE_ESTIMATE_BYTES;
}
```

Default: 256 bytes per clause — the structural overhead shared by every `QueryBuilder`. This
also drives the clause-count breaker in tests (limit = `(n+1) * 256`).

**Single post-construction charge in `namedObject`**

After `parseNamedObject` returns, every `QueryBuilder` — including the root — is charged
`aqb.parseTimeBreakerEstimate()`. No pre-charge; no root exemption. The charge happens:

- Outside `ObjectParser` for the root query → raw `CircuitBreakingException`.
- Inside `ObjectParser` for non-root clauses → wrapped in `XContentParseException`.

**`setQueryParsingBreaker` / global static breaker**

`queryParsingBreaker` is a `static volatile CircuitBreaker` — a global reference, not
thread-local. A test setter (`setQueryParsingBreaker`) injects a `LimitedBreaker` globally
and restores `null` in `finally`. Production code calls `addEstimateBytesAndMaybeBreak`;
on close (search context teardown) it releases via `addWithoutBreaking` with a negative amount.

#### Payload-aware overrides (done)

| Class | Override | Formula |
|---|---|---|
| `TermsQueryBuilder` | ✅ | `256 + Σ(value.length * 2 + 64)` per String/BytesRef; `+64` per other |
| `IdsQueryBuilder` | ✅ | `256 + Σ(id.length * 2 + 64)` |

#### Tests added/updated

- `BoolQueryBuilderTests.testTooManyClausesRejectedAtParseTime` — limit = `(max+1) * 256`;
  expects raw `CircuitBreakingException` (root charged outside `ObjectParser`).
- `DisMaxQueryBuilderTests.testTooManyClausesRejectedAtParseTime` — same pattern.
- `SearchSourceBuilderTests`: `testQueryParsingBreakerHeldAfterParseReleasedOnClose`,
  `testQueryAndPostFilterBreakerChargesAreCumulative`,
  `testPartialBreakerChargeReleasedOnParseFailure`,
  `testBreakerReleasedWhenSubsequentFieldFailsAfterSuccessfulQuery`.
- `TermsQueryBuilderTests.testPayloadBreakerTripsOnLargeTermsList`
- `IdsQueryBuilderTests.testPayloadBreakerTripsOnLargeIdsList`

**Committed:** `0c5a0537a50f` ("payload-aware CB estimates for terms/ids; charge all clauses post-construction")

#### Lifecycle regression tests (leak-fix coverage) ✅ DONE

**`RestSearchActionTests`** (3 new tests):
- `testBreakerReleasedOnUnknownRestParam` — `dispatched == false` close path (unknown URL param)
- `testBreakerReleasedOnDispatchFailure` — `runAfter` completion-listener path (node client rejects)
- `testBreakerHeldDuringOutstandingSearch` — charge > 0 while search in-flight; == 0 after completion

**`BulkByPaginatedSearchParallelizationHelperTests`** (4 new tests):
- `testBreakerReleasedOnAutoSlicesInitFailure` — `startSlicedAction` closingListener on init failure
- `testBreakerReleasedOnWorkerCancellationBeforeSearch` — worker `l` path on pre-search cancellation
- `testBreakerHeldUntilFinalSliceCompletes` — leader path holds charge until all slices complete
- `testBreakerHeldDuringWorkerExecution` — worker holds charge during in-flight search

#### Leak fixes (post-commit)

Two release-path leaks were identified and fixed:

**REST cleanup** (`RestSearchAction.prepareRequest`): Two release paths, both required:

- *Abandonment* (e.g., unknown URL params): `BaseRestHandler.handleRequest` wraps `prepareRequest`
  in `try (var action = ...)` and calls `action.close()` on every exit path. The returned
  `RestChannelConsumer` tracks a `dispatched` flag; `close()` releases charges only when
  `accept()` was never called.
- *Completion / filter rejection*: action filters can reject the request before
  `TransportSearchAction.doExecute` runs, so relying on `TransportSearchAction`'s own `runAfter`
  alone is insufficient. `accept()` wraps the completion listener with
  `ActionListener.runAfter(completionListener, parsedSource::close)`, which fires on any
  pipeline outcome — success, search failure, or filter rejection.

`SearchSourceBuilder.close()` is synchronized and idempotent, so the two paths are safe to
overlap.

**Sliced bulk-by-query** (`BulkByPaginatedSearchParallelizationHelper`): Two release points,
both needed:

- *Slice completion* (`sendSubRequests`): `shallowCopy()` sources carry no `queryParsingReleasables`,
  so `TransportSearchAction`'s `runAfter` is a no-op for all slices. `sendSubRequests` wraps its
  `listener` with `ActionListener.runAfter(listener, originalSource::close)` so charges are released
  when the entire sliced operation completes (success or failure).
- *Init-phase failure* (`startSlicedAction`): with `slices=auto`, a `ClusterSearchShardsRequest`
  shard lookup happens before `sendSubRequests` is called. If it fails (e.g., missing index), the
  listener path never reaches `sendSubRequests` and the `sendSubRequests` wrapper is never installed.
  `startSlicedAction` therefore installs its own `closingListener` wrapper before `initTaskState`,
  covering this failure path.
- *Unsliced worker path* (`startSlicedAction` → `workerAction`): when the request resolves to a
  single worker (slices = 1), `executeSlicedAction` calls `workerAction` instead of `sendSubRequests`.
  The original `Runnable` form captured the outer `listener` at construction time, bypassing
  `closingListener`. If the worker is cancelled before its first `TransportSearchAction` call,
  neither path releases the charge. Fixed by changing `workerAction` to
  `Consumer<ActionListener<BulkByPaginatedSearchResponse>>`: `startSlicedAction` passes `l`
  (the wrapped `closingListener`) into the worker at call time; callers use `wrappedListener`
  instead of the outer `listener` when constructing `AsyncIndexBySearchAction` /
  `AsyncDeleteByQueryAction`.

`SearchSourceBuilder.close()` is idempotent, so the double-wrap (one from each site) on the
successful multi-slice path is safe.

**Note:** the PR description still reflects the earlier design (non-root, fixed-256-byte
accounting). It should be updated to describe: root included, payload-aware estimates, and
the two leak fixes.

---

### Stage 3 — Payload-aware overrides for remaining variable-size clause types

**Important constraint:** `parseTimeBreakerEstimate()` fires *after* the `QueryBuilder` is
fully constructed. This means large payload arrays (`terms`, `ids`, etc.) are already
allocated before the breaker charge is assessed. The hook provides **accounting** (the charge
is held for the request lifetime and released on close, preventing the same byte budget from
being used again), not **prevention** (the allocation already happened). True before-allocation
protection for variable-size fields would require incremental charging during XContent token
parsing, which is a separate effort.

**Goal:** all `QueryBuilder` subclasses that can carry unbounded per-clause payload should
override `parseTimeBreakerEstimate()` so that the REQUEST breaker accurately reflects the
allocated heap and trips on sustained large-payload requests. Currently only `TermsQueryBuilder`
and `IdsQueryBuilder` have overrides; the base 256-byte estimate is too small for the following.

#### Formula convention (follow this in all overrides)

```
estimate = QUERY_BUILDER_SIZE_ESTIMATE_BYTES  // always start with base
         + Σ(entry cost per element)
```

For string entries: `s.length() * 2L + 64L` (UTF-16 chars + object header + field refs).
For `GeoPoint`: `~40L` (two `double` fields + object header).
Round up; exact precision is not required — the goal is order-of-magnitude accuracy.

#### Candidates

| Class | Payload | Override formula sketch |
|---|---|---|
| `TermsSetQueryBuilder` | `terms` list (strings) | same as `TermsQueryBuilder`: `256 + Σ(s.length * 2 + 64)` |
| `MoreLikeThisQueryBuilder` | `likeTexts`, `unlikeTexts`, `fields`, and `Item` list | `256 + Σ text.length * 2 + Σ field.length * 2 + Σ item cost`; items carry id + index strings |
| `GeoPolygonQueryBuilder` | `points` list (`GeoPoint[]`) | `256 + points.size() * 40L` |
| `GeoShapeQueryBuilder` | geometry (can be a large polygon/multi-polygon) | `256 + estimated WKB/GeoJSON bytes`; `ShapeBuilder` has a `numPoints()` or similar |
| `IntervalQueryBuilder` | nested `IntervalsSource` rules — typically small but recursive | `256` (base) for now; revisit if exploit vector is identified |
| `SpanNearQueryBuilder` | span clause list (each is itself a `QueryBuilder`, charged separately) | `256` base is fine; children already charged individually |
| `FunctionScoreQueryBuilder` | `FilterFunction[]` list (each holds a score function + optional filter) | `256 + functions.size() * 128L` (rough estimate per function object) |
| `QueryStringQueryBuilder` | query string + fields map | `256 + queryString.length * 2 + Σ field.length * 2` |
| `SimpleQueryStringBuilder` | query string + fields map | same shape as `QueryStringQueryBuilder` |
| `RegexpQueryBuilder` | regexp value string | `256 + value.length * 2` |
| `MultiMatchQueryBuilder` | fields map | `256 + Σ field.length * 2` |
| `ScriptQueryBuilder` | `Script` (source + params) | `256 + script.idOrCode.length * 2 + params map estimate` |

#### Implementation notes

- Add override + unit test (in the corresponding `*Tests` class) for each.
- Test pattern mirrors `TermsQueryBuilderTests.testPayloadBreakerTripsOnLargeTermsList`:
  set a `LimitedBreaker` tight enough that one entry fits but two do not; assert
  `CircuitBreakingException` on the big case and no exception on the ok case.
- `MoreLikeThisQueryBuilder` and `GeoShapeQueryBuilder` are the highest priority — they can
  hold the largest unbounded payloads in a single clause.

---

## Resolved questions

| Item | Resolution |
|---|---|
| Transport version | Not needed. Parser config and `QueryBuilder` wire format are unchanged. |
| Nested bools | `parseTopLevelQuery`'s wrapper counter accumulates across all nesting levels via `this` re-entry. |
| Default limit | Use `IndexSearcher.getMaxClauseCount()` (snapshot to a local `int max`), not `INDICES_MAX_CLAUSE_COUNT_SETTING`. |
| Percolator | Already uses `parseTopLevelQuery`. Cap applies at index time too — no exemption needed. |
| `should()` at line 164 | Java builder API, not the HTTP parse path. Irrelevant. |
| `WrapperQueryBuilder` | Repopulates via fresh `parseTopLevelQuery` in `doRewrite` — out-of-scope gap; document only. |
| Per-call counters | Each `parseTopLevelQuery` call gets its own counter; acceptable for the HTTP CVE vector. |
| `TooManyNestedClauses` label | Fires during `toQuery()` (`MaxClauseCountQueryVisitor`) and also in `ContextIndexSearcher.rewrite()` → `verifyQueryLimit()`. Not during weight construction; `createWeight` runs after `rewrite()`. |
