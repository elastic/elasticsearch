# Full-Text / Search Functions: External Data Source Compatibility

## Executive Summary

External sources use `ExternalRelation` (logical) / `ExternalSourceExec` (physical). They have **no Lucene index**, **no shardContexts** (`EmptyIndexedByShardId.instance()`), **no IndexSearcher**, and produce no `DocBlock` in pages.

All full-text functions (`match`, `match_phrase`, `multi_match`, `kql`, `qstr`, `score`, `knn`) are **incompatible** with external sources. The failure mode depends on the specific function:
- `kql()` and `qstr()`: **Verifier error** (blocked at analysis time)
- `match()`, `match_phrase()`, `multi_match()`, `knn()`: **Runtime crash** (assertion failure / NPE / IndexOutOfBoundsException)
- `score()`: **Runtime crash** (same as above, through ScoreMapper)
- `LIKE`, `RLIKE`, `contains()`, `starts_with()`, `ends_with()`: **All work** (pure compute evaluators, no Lucene dependency)

---

## Detailed Function-by-Function Analysis

### 1. match() — NO

**Class**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/expression/function/fulltext/Match.java`

**Hierarchy**: `Match extends SingleFieldFullTextFunction extends FullTextFunction`

**Failure mode**: Runtime crash (assertion + IndexOutOfBoundsException)

**Trace**:
1. `Match` has no `toEvaluator()` override — inherits from `FullTextFunction`
2. `FullTextFunction.toEvaluator()` (line 444-445):
   ```java
   return new LuceneQueryExpressionEvaluator.Factory(toShardConfigs(toEvaluator.shardContexts()));
   ```
3. `toEvaluator.shardContexts()` returns `EmptyIndexedByShardId.instance()` for coordinator-only execution (external sources)
4. `toShardConfigs()` (line 464) calls `contexts.map(...)` — on empty instance, returns itself (empty)
5. `LuceneQueryEvaluator` constructor (line 58):
   ```java
   assert shards != null && shards.isEmpty() == false : "LuceneQueryEvaluator requires shard information";
   ```
   **Assertion fails** with assertions enabled.
6. If assertions disabled, `executeQuery()` (line 64-74) searches for `DocBlock` in page — external sources don't produce DocBlocks. **Assertion fails** at line 73:
   ```java
   assert docBlock != null : "LuceneQueryExpressionEvaluator expects a DocBlock";
   ```
7. If both assertions disabled, `DocVector docs = (DocVector) docBlock.asVector()` — **NullPointerException**.

**Verifier checks**: The general full-text function verifier at `FullTextFunction.java:270-280` only blocks Limit/Aggregate/UnionAll/MvExpand below the function — it does NOT check for ExternalRelation. So the query passes verification.

**Key files**:
- `FullTextFunction.java:444-445` — toEvaluator calls shardContexts()
- `FullTextFunction.java:464-466` — toShardConfigs maps over shardContexts
- `LuceneQueryEvaluator.java:58` — assertion on non-empty shards
- `LuceneQueryEvaluator.java:73` — assertion on DocBlock presence
- `ComputeService.java:502` — EmptyIndexedByShardId.instance() for coordinator-only

---

### 2. match_phrase() — NO

**Class**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/expression/function/fulltext/MatchPhrase.java`

**Hierarchy**: `MatchPhrase extends SingleFieldFullTextFunction extends FullTextFunction`

**Failure mode**: Identical to `match()` — runtime crash. Same code path through `FullTextFunction.toEvaluator()`.

**Key files**: Same as match(). `MatchPhrase` inherits `toEvaluator()` from `FullTextFunction`.

---

### 3. multi_match() — NO

**Class**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/expression/function/fulltext/MultiMatch.java`

**Hierarchy**: `MultiMatch extends FullTextFunction`

**Failure mode**: Identical to `match()` — runtime crash via `FullTextFunction.toEvaluator()`.

**Key files**: Same as match().

---

### 4. kql() — NO (Verifier Error)

**Class**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/expression/function/fulltext/Kql.java`

**Hierarchy**: `Kql extends FullTextFunction`

**Failure mode**: **Verifier error at analysis time** — blocked before execution.

**Trace**:
`FullTextFunction.java:257-267` — special check for `QueryString.class` and `Kql.class`:
```java
List.of(QueryString.class, Kql.class).forEach(functionClass -> {
    checkCommandsBeforeExpression(
        plan, condition, functionClass,
        lp -> (lp instanceof Filter || lp instanceof OrderBy || lp instanceof EsRelation),
        ...
    );
});
```
The predicate requires all plan nodes below the Filter to be `Filter`, `OrderBy`, or **`EsRelation`**. An `ExternalRelation` is NOT an `EsRelation`. The check fails, producing an error like:
```
[KQL] function cannot be used after EXTERNAL "s3://..."
```

**Key file**: `FullTextFunction.java:263` — `lp instanceof EsRelation` check excludes ExternalRelation.

---

### 5. qstr() (QueryString) — NO (Verifier Error)

**Class**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/expression/function/fulltext/QueryString.java`

**Hierarchy**: `QueryString extends FullTextFunction`

**Failure mode**: **Verifier error at analysis time** — same check as `kql()`.

**Key file**: `FullTextFunction.java:263` — identical predicate blocks QSTR with ExternalRelation.

---

### 6. score() — NO

**Class**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/expression/function/fulltext/Score.java`

**Failure mode**: Runtime crash

**Trace**:
1. `Score.toEvaluator()` at line 90-92:
   ```java
   ScoreOperator.ExpressionScorer.Factory scorerFactory = ScoreMapper.toScorer(children().getFirst(), toEvaluator.shardContexts());
   return driverContext -> new ScorerEvaluatorFactory(scorerFactory).get(driverContext);
   ```
2. `toEvaluator.shardContexts()` returns `EmptyIndexedByShardId` for external sources.
3. `ScoreMapper.toScorer()` (`ScoreMapper.java:23-42`) passes shardContexts to child expression's `toScorer()`.
4. For the child FullTextFunction, `FullTextFunction.toScorer()` (line 460-461):
   ```java
   return new LuceneQueryScoreEvaluator.Factory(toShardConfigs(toScorer.shardContexts()));
   ```
5. Same crash path as `match()` — empty shards + no DocBlock.

Additionally, `score()` wraps a full-text function. So if the inner function is `kql()` or `qstr()`, it would be blocked at the verifier stage first.

**Key files**:
- `Score.java:91` — calls shardContexts()
- `ScoreMapper.java:23-38` — passes shardContexts through
- `FullTextFunction.java:460-461` — toScorer uses LuceneQueryScoreEvaluator

---

### 7. knn() — NO

**Class**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/expression/function/vector/Knn.java`

**Hierarchy**: `Knn extends SingleFieldFullTextFunction extends FullTextFunction`

**Failure mode**: Runtime crash — same as `match()`.

`Knn` inherits `toEvaluator()` from `FullTextFunction`. Additionally, `Knn.evaluatorQueryBuilder()` (line 280-288) creates an `ExactKnnQueryBuilder` which requires a Lucene `IndexSearcher` to execute.

**Key files**:
- Inherits `FullTextFunction.toEvaluator()` at line 444-445
- `Knn.java:280-288` — evaluatorQueryBuilder creates ExactKnnQueryBuilder

---

### 8. LIKE operator (WildcardLike) — YES

**Class**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/expression/function/scalar/string/regex/WildcardLike.java`

**Hierarchy**: `WildcardLike extends RegexMatch<WildcardPattern>`

**Works with external sources**: YES

**Trace**:
`RegexMatch.toEvaluator()` at line 54-61:
```java
return AutomataMatch.toEvaluator(
    source(),
    toEvaluator.apply(field()),
    pattern().pattern().isEmpty() ? Automata.makeEmptyString() : pattern().createAutomaton(caseInsensitive())
);
```
This creates a pure automaton-based evaluator (`AutomataMatch`) that operates on `BytesRef` values. No Lucene index, no shardContexts, no IndexSearcher needed. The `translatable()` method at line 142-143 checks `isPushableAttribute(field())` — for external sources this returns `NO` (not pushable), so it won't be pushed to Lucene. The evaluator runs in-memory on the page data.

**Key file**: `RegexMatch.java:54-61` — AutomataMatch evaluator (pure compute)

---

### 9. RLIKE operator — YES

**Class**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/expression/function/scalar/string/regex/RLike.java`

**Hierarchy**: `RLike extends RegexMatch<RLikePattern>`

**Works with external sources**: YES

Same code path as LIKE — inherits `RegexMatch.toEvaluator()` which uses `AutomataMatch`. Pure compute, no Lucene dependency.

**Key file**: `RegexMatch.java:54-61` — same AutomataMatch evaluator

---

### 10. contains() — YES

**Class**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/expression/function/scalar/string/Contains.java`

**Works with external sources**: YES

**Trace**:
`Contains.toEvaluator()` at line 135-139:
```java
ExpressionEvaluator.Factory strExpr = toEvaluator.apply(str);
ExpressionEvaluator.Factory substrExpr = toEvaluator.apply(substr);
return new ContainsEvaluator.Factory(source(), strExpr, substrExpr);
```
Pure `@Evaluator`-annotated function operating on `BytesRef`. No Lucene dependency.

**Key file**: `Contains.java:117-121` — process() is pure string comparison

---

### 11. starts_with() — YES

**Class**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/expression/function/scalar/string/StartsWith.java`

**Works with external sources**: YES

**Trace**:
`StartsWith.toEvaluator()` at line 134-136:
```java
return new StartsWithEvaluator.Factory(source(), toEvaluator.apply(str), toEvaluator.apply(prefix));
```
Pure `@Evaluator`-annotated function operating on `BytesRef` byte array comparison. No Lucene dependency.

Note: `StartsWith` implements `TranslationAware.SingleValueTranslationAware` for Lucene pushdown optimization, but the evaluator itself is independent. The `translatable()` check (line 139-141) would return `NO` for external sources, so it just runs the pure evaluator.

**Key file**: `StartsWith.java:116-121` — process() is pure byte array comparison

---

### 12. ends_with() — YES

**Class**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/expression/function/scalar/string/EndsWith.java`

**Works with external sources**: YES

Same pattern as `starts_with()`. Pure `@Evaluator` function operating on byte arrays.

**Key file**: `EndsWith.java:112-124` — process() is pure byte array comparison

---

## Other Functions with Lucene/shardContexts/IndexSearcher Dependencies

### Functions in the fulltext package (ALL incompatible):
All classes in the `expression/function/fulltext/` package depend on Lucene through `FullTextFunction.toEvaluator()`:
- `Match` — NO
- `MatchPhrase` — NO
- `MatchOperator` (extends Match) — NO
- `MultiMatch` — NO
- `Kql` — NO (verifier error)
- `QueryString` — NO (verifier error)
- `Score` — NO (shardContexts dependency)

### Knn (in vector package):
- `Knn` (extends SingleFieldFullTextFunction) — NO

### Functions that are safe (partial list):
All `@Evaluator`-annotated scalar functions work fine. This includes the entire `scalar/` package: string functions, math functions, date functions, conditional functions, IP functions, spatial functions, etc. Their evaluators operate on Block/Vector data without any Lucene dependency.

### Key dependency chain:
```
FullTextFunction.toEvaluator()
  → toShardConfigs(toEvaluator.shardContexts())
    → contexts.map(sc -> new ShardConfig(sc.toQuery(queryBuilder), sc.searcher()))
      → LuceneQueryExpressionEvaluator.Factory(shardConfigs)
        → LuceneQueryEvaluator(blockFactory, shards)
          → assert shards.isEmpty() == false  ← CRASH
          → executeQuery(page) expects DocBlock  ← CRASH
            → shards.get(shard)  ← IndexOutOfBoundsException
```

---

## Summary Table

| Function | Works? | Failure Mode | Where |
|----------|--------|-------------|-------|
| `match()` | NO | Runtime crash: assertion / NPE | `FullTextFunction.java:444`, `LuceneQueryEvaluator.java:58,73` |
| `match_phrase()` | NO | Runtime crash: assertion / NPE | Same as match() |
| `multi_match()` | NO | Runtime crash: assertion / NPE | Same as match() |
| `kql()` | NO | Verifier error (analysis time) | `FullTextFunction.java:263` |
| `qstr()` | NO | Verifier error (analysis time) | `FullTextFunction.java:263` |
| `score()` | NO | Runtime crash: assertion / NPE | `Score.java:91`, `ScoreMapper.java:23` |
| `knn()` | NO | Runtime crash: assertion / NPE | Same as match() |
| `LIKE` | YES | N/A | `RegexMatch.java:54` (AutomataMatch) |
| `RLIKE` | YES | N/A | `RegexMatch.java:54` (AutomataMatch) |
| `contains()` | YES | N/A | `Contains.java:135` (ContainsEvaluator) |
| `starts_with()` | YES | N/A | `StartsWith.java:134` (StartsWithEvaluator) |
| `ends_with()` | YES | N/A | `EndsWith.java:137` (EndsWithEvaluator) |

## Root Cause

The fundamental issue is that `FullTextFunction.toEvaluator()` assumes Lucene infrastructure (shardContexts, IndexSearcher, DocBlock in pages). External sources provide none of this. The only full-text functions that work are the string-matching operators (LIKE, RLIKE) and scalar string functions (contains, starts_with, ends_with) which use pure compute evaluators.

For `kql()` and `qstr()`, there's a verifier check at `FullTextFunction.java:263` that explicitly requires `EsRelation` below the function, which blocks usage with ExternalRelation at analysis time. For all other full-text functions (`match`, `match_phrase`, `multi_match`, `knn`), the verifier does NOT block them against ExternalRelation — they silently pass analysis and then crash at execution time.

## Recommendation

The verifier should be updated to block ALL full-text functions (not just kql/qstr) when used against ExternalRelation, producing a clear error message like: "[MATCH] function is not supported for external data sources." This would convert the runtime crashes into clean analysis-time errors.
