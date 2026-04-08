# JOIN, ENRICH, and Cross-Source Queries with External Data Sources

## 1. LOOKUP JOIN

### 1.1 Parsing and Resolution

LOOKUP JOIN is parsed in `LogicalPlanBuilder.visitJoinCommand()`:
- **File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/parser/LogicalPlanBuilder.java`
- **Lines 817-867**: The parser creates a `LookupJoin` node with:
  - **Left side**: the incoming pipeline (`p`)
  - **Right side**: an `UnresolvedRelation` with `IndexMode.LOOKUP` (line 853)
  - The parser **hardcodes the right side as an index pattern** (`visitIndexPattern`), not an arbitrary relation. There is no way to specify an external source on the right side of a LOOKUP JOIN via the grammar.

Key constraint at line 829-831:
```java
var target = ctx.joinTarget();
var rightPattern = visitIndexPattern(List.of(target.index));
```
The right side must be an Elasticsearch index pattern (no wildcards, no remote clusters). It creates:
```java
UnresolvedRelation right = new UnresolvedRelation(
    source(target), new IndexPattern(..., rightPattern),
    false, emptyList(), IndexMode.LOOKUP, null);
```

During analysis, `resolveLookupJoin()` resolves the USING columns between left and right:
- **File**: `Analyzer.java`, **lines 966-1006**
- The resolution is purely attribute-based - it matches field names from left output and right output. No `EsRelation` or `ExternalRelation` type check here.

The `LookupJoin.surrogate()` method (line 68-71 of `LookupJoin.java`) converts the `LookupJoin` into a plain `Join` during analysis, which is then handled by the mapper.

### 1.2 Physical Mapping: External Source on the LEFT Side

When an external source is the **left side** of a LOOKUP JOIN (e.g., `EXTERNAL "s3://data.parquet" | LOOKUP JOIN lookup_idx ON key`):

**Mapper.mapBinary()** (`Mapper.java` lines 183-233):
1. Line 190: `join.isRemote()` check -- if remote, wraps as `FragmentExec`. For external sources this is false.
2. Line 198: `PhysicalPlan left = mapInner(bp.left())` -- ExternalRelation maps to `ExternalSourceExec` via `MapperUtils.mapLeaf()` (not `FragmentExec`).
3. **Line 201**: `if (left instanceof FragmentExec)` -- **This check FAILS** because the left is `ExternalSourceExec`, not `FragmentExec`.
4. Line 205: Maps the right side. The right side is `EsRelation` with `IndexMode.LOOKUP` which becomes `FragmentExec`.
5. **Lines 217-230**: Checks `if (right instanceof FragmentExec fragment)` and `isIndexModeLookup(fragment)` -- This SUCCEEDS.
6. Returns `LookupJoinExec` with left=ExternalSourceExec, right=FragmentExec.

**Verdict**: LOOKUP JOIN with external source on the LEFT side **works** in the coordinator Mapper. The `ExternalSourceExec` flows through as the left input to `LookupJoinExec`.

**LocalMapper.mapBinary()** (`LocalMapper.java` lines 118-171):
When the same plan hits the LocalMapper (for data-node execution), the path is similar:
1. Line 126: `PhysicalPlan left = map(binary.left())` -- ExternalRelation maps to `ExternalSourceExec`.
2. Line 128: `if (binary.right() instanceof LocalRelation)` -- typically false for lookup joins.
3. **Lines 143-148**: Checks `if (binary.right() instanceof EsRelation esRelation)` or `Filter(EsRelation)`.
4. **Line 149-151**: `if (rightRelation == null)` -- throws `EsqlIllegalArgumentException("Unsupported right plan for lookup join [...]")`.

This means if the plan reaches the LocalMapper with ExternalRelation on the left, it would work as long as the right side is EsRelation (which it always is for LOOKUP JOIN since the parser forces it).

### 1.3 Physical Mapping: External Source on the RIGHT Side

**Not possible via syntax.** The parser hardcodes the right side of LOOKUP JOIN as an `UnresolvedRelation` with `IndexMode.LOOKUP` (a lookup index in Elasticsearch). There is no grammar production that allows `LOOKUP JOIN external_source ON key`.

Even if you could construct such a plan programmatically:
- **Mapper.java lines 235-244**: `isIndexModeLookup()` explicitly checks `fragment.fragment() instanceof EsRelation relation && relation.indexMode() == IndexMode.LOOKUP`. An `ExternalRelation` would not match this check.
- **LocalMapper.java lines 143-151**: Checks `binary.right() instanceof EsRelation` -- an `ExternalRelation` would fail this check and throw `EsqlIllegalArgumentException`.

**Verdict**: External source on the RIGHT side of LOOKUP JOIN is **impossible** -- blocked by both grammar and runtime checks.

### 1.4 Hardcoded EsRelation/EsIndex Checks in JOIN Path

| Location | File | Line | Check | Impact |
|----------|------|------|-------|--------|
| Mapper.isIndexModeLookup() | `Mapper.java` | 235-244 | `fragment.fragment() instanceof EsRelation relation && relation.indexMode() == IndexMode.LOOKUP` | Blocks non-EsRelation right sides |
| LocalMapper.mapBinary() | `LocalMapper.java` | 143-148 | `binary.right() instanceof EsRelation esRelation` and `Filter(EsRelation)` | Blocks non-EsRelation right sides |
| LocalMapper.mapBinary() | `LocalMapper.java` | 152-154 | `rightRelation.indexMode() != IndexMode.LOOKUP` | Requires lookup index mode |
| Parser.visitJoinCommand() | `LogicalPlanBuilder.java` | 829 | `visitIndexPattern(List.of(target.index))` | Right side must be ES index pattern |
| Parser.visitJoinCommand() | `LogicalPlanBuilder.java` | 848-855 | `new UnresolvedRelation(..., IndexMode.LOOKUP, ...)` | Right side hardcoded to lookup mode |
| Join.postOptimizationVerification() | `Join.java` | 367-389 | Checks `ExecutesOn.COORDINATOR` in ancestors for remote joins | ExternalRelation being COORDINATOR blocks remote join above it |

### 1.5 Remote Join Restriction

`Join.java` lines 367-389, `checkRemoteJoin()`:
```java
if (u instanceof PipelineBreaker || (u instanceof ExecutesOn ex && ex.executesOn() == ExecuteLocation.COORDINATOR)) {
    fails.add(u.source());
}
```
Since `ExternalRelation` implements `ExecutesOn.Coordinator`, any **remote** LOOKUP JOIN placed **after** an external source pipeline would be blocked by this check. The error message would be: `"LOOKUP JOIN with remote indices can't be executed after [EXTERNAL ...]"`.

This only applies to remote joins (where `isRemote()` is true). For local joins, this check is skipped.

---

## 2. ENRICH

### 2.1 Three Modes

Defined in `Enrich.java` lines 83-101:

| Mode | Syntax | `executesOn()` | Description |
|------|--------|----------------|-------------|
| `ANY` | `ENRICH policy` | `ExecuteLocation.ANY` | Default. Can execute anywhere -- on data nodes or coordinator. |
| `COORDINATOR` | `ENRICH _coordinator:policy` | `ExecuteLocation.COORDINATOR` | Runs only on the coordinating node. |
| `REMOTE` | `ENRICH _remote:policy` | `ExecuteLocation.REMOTE` | Runs only on remote data nodes. |

Mode parsing: `LogicalPlanBuilder.parsePolicyName()` at lines 741-773. The prefix `_coordinator:`, `_remote:`, or `_any:` before the policy name determines the mode.

### 2.2 ENRICH with ExternalRelation as Source

**Mode.ANY** (default):
- When the Mapper encounters `Enrich` with `mode == ANY`, it is treated as a streaming operator.
- In `Mapper.mapUnary()` line 100: `if (unary instanceof Enrich enrich && enrich.mode() == Enrich.Mode.COORDINATOR)` -- for ANY mode, this check is false.
- Line 105: `if (unary instanceof PipelineBreaker == false ...)` -- Enrich is NOT a PipelineBreaker, so it passes this check.
- **But** the child `ExternalRelation` maps to `ExternalSourceExec` (not `FragmentExec`), so the `mappedChild instanceof FragmentExec` check at line 98 fails.
- The code falls through to the pipeline operators section at line 180: `return MapperUtils.mapUnary(unary, mappedChild)`.
- `MapperUtils.mapUnary()` at line 130: creates `EnrichExec` with the mapped child.
- **Verdict**: ENRICH with `_any` mode **works** with ExternalRelation. The EnrichExec runs on the coordinator since the source executes on coordinator.

**Mode.COORDINATOR**:
- Same flow as ANY, except:
- `Mapper.mapUnary()` line 100: `enrich.mode() == Enrich.Mode.COORDINATOR` is true.
- Line 101: `mappedChild = addExchangeForFragment(enrich.child(), mappedChild)` -- since `mappedChild` is `ExternalSourceExec` (not `FragmentExec`), `addExchangeForFragment` returns `mappedChild` unchanged (line 273: `if (child instanceof FragmentExec)` is false).
- Line 102: `return MapperUtils.mapUnary(unary, mappedChild)` -- creates `EnrichExec`.
- **Verdict**: ENRICH with `_coordinator` mode **works** with ExternalRelation. Both execute on coordinator.

**Mode.REMOTE**:
- The REMOTE mode has a **post-optimization verification** check that blocks coordinator-only sources.
- `Enrich.java` lines 322-327:
  ```java
  public void postOptimizationVerification(Failures failures) {
      if (this.mode == Mode.REMOTE) {
          checkForPlansForbiddenBeforeRemoteEnrich(failures);
      }
  }
  ```
- `checkForPlansForbiddenBeforeRemoteEnrich()` at lines 280-289:
  ```java
  this.forEachDown(LogicalPlan.class, u -> {
      if (u instanceof ExecutesOn ex && ex.executesOn() == ExecuteLocation.COORDINATOR) {
          failures.add(
              fail(this, "ENRICH with remote policy can't be executed after [" + u.source().text() + "]")
          );
      }
  });
  ```
- Since `ExternalRelation` implements `ExecutesOn.Coordinator`, this check **will fire** and produce an error.
- **Verdict**: ENRICH with `_remote` mode is **BLOCKED** when the source is ExternalRelation. The error message would be: `"ENRICH with remote policy can't be executed after [EXTERNAL \"...\"]"`.

### 2.3 Exact Blocking Location for _remote Mode

The blocking check is in:
- **File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/logical/Enrich.java`
- **Line 284**: `if (u instanceof ExecutesOn ex && ex.executesOn() == ExecuteLocation.COORDINATOR)`
- **Lines 285-287**: Error message generation
- **Line 323-326**: `postOptimizationVerification()` invokes this only for `Mode.REMOTE`

This check traverses the plan **downward** from the Enrich node. Since ExternalRelation is the leaf source node, it will always be found by this traversal. The check is a **semantic conflict**: remote ENRICH needs data to be on remote data nodes, but ExternalRelation data is on the coordinator.

### 2.4 Additional ENRICH Restrictions

`Enrich.java` lines 293-298, `postAnalysisVerification()`:
- For REMOTE mode, also checks `checkMvExpandAfterLimit` (lines 311-320): MV_EXPAND after LIMIT is incompatible with remote ENRICH.

`LogicalPlanBuilder.java` lines 691-694: When ENRICH is in REMOTE mode, the parser transforms all upstream LookupJoin nodes to be remote-only:
```java
if (mode == Mode.REMOTE) {
    child = child.transformDown(LookupJoin.class, lj -> new LookupJoin(..., true));
}
```

---

## 3. Cross-Source Queries

### 3.1 FROM with Multiple Sources

**Syntax**: `FROM es_index, (subquery)` using subqueries (parenthesized sub-pipelines).

The `visitRelation()` method in `LogicalPlanBuilder.java` (lines 368-419) handles the FROM clause:
1. Parses index patterns and subqueries separately (lines 369-380).
2. Creates an `UnresolvedRelation` for the index pattern (line 395).
3. If subqueries exist, wraps everything in a `UnionAll` (line 416):
   ```java
   return new UnionAll(source, mainQueryAndSubqueries, List.of());
   ```

**Can you write `FROM es_index, external_source`?**

**No.** The grammar's `indexPatternOrSubquery` production allows either an index pattern or a subquery. The `EXTERNAL` command is a separate **source command**, not a processing command or an index pattern. You cannot mix `FROM` with `EXTERNAL` in a single FROM clause.

However, you **could** use a subquery:
```
FROM es_index, (EXTERNAL "s3://data.parquet" | ...)
```
This would create a `UnionAll` with two branches: one `UnresolvedRelation` (es_index) and one `Subquery` wrapping the EXTERNAL pipeline. But this depends on the SUBQUERY_IN_FROM_COMMAND capability being enabled.

### 3.2 FORK/UnionAll with External Sources

**Fork** (`Fork.java` lines 39):
- `Fork extends LogicalPlan` and implements `ExecutesOn.Coordinator`.
- Fork is a coordinator-only operation.
- Each branch is an independent sub-plan.
- Fork does not check what type of source each branch uses -- it only aligns output columns across branches.
- **Verdict**: FORK branches could theoretically contain external sources (each branch runs independently), but this would need each branch to separately source data from the external relation. In practice, FORK operates on an existing pipeline -- the source is shared.

**UnionAll** (`UnionAll.java`):
- `UnionAll extends Fork` -- also `ExecutesOn.Coordinator`.
- Created when FROM has multiple index patterns or subqueries (line 416 in LogicalPlanBuilder.java).
- Each child is mapped independently via `Mapper.mapUnionAll()` (lines 254-267).
- In `mapUnionAll`, each child is mapped with `mapInner()`. If a child produces an `ExternalSourceExec`, it would NOT be a FragmentExec, so the `if (child instanceof FragmentExec)` check at line 261 would be false, and no `ExchangeExec` would be added.
- **Verdict**: UnionAll can theoretically contain a mix of `FragmentExec` (ES indices) and `ExternalSourceExec` (external sources) children. The MergeExec produced at line 266 would merge both.

### 3.3 Practical Cross-Source Limitations

While UnionAll can structurally contain mixed sources, there are practical limitations:

1. **No syntax for mixing EXTERNAL and FROM**: As noted above, the grammar does not allow `FROM es_index, EXTERNAL "..."`. You would need subquery support.

2. **Schema alignment**: UnionAll requires all branches to output the same columns (checked in `checkUnionAll()` lines 72-101). External sources and ES indices would need compatible schemas.

3. **Serialization**: `ExternalRelation.writeTo()` throws `UnsupportedOperationException` (line 76). If any cross-node serialization is attempted (e.g., for remote execution), it would fail.

4. **LOOKUP JOIN across sources**: Cannot use an external source as the lookup table (right side). The left side can be external.

---

## Summary Table

| Operation | External on Left | External on Right | Works? | Blocking Location |
|-----------|-----------------|-------------------|--------|-------------------|
| LOOKUP JOIN | Yes (coordinator maps it) | No (grammar + runtime block) | Left only | `LogicalPlanBuilder.java:829`, `Mapper.java:238`, `LocalMapper.java:143-151` |
| ENRICH _any | Yes | N/A (unary op) | Yes | -- |
| ENRICH _coordinator | Yes | N/A | Yes | -- |
| ENRICH _remote | No (ExecutesOn.Coordinator conflict) | N/A | No | `Enrich.java:284` |
| FROM multi-source | N/A (syntax doesn't allow mixing) | N/A | No syntax | `LogicalPlanBuilder.java:368-419` |
| FORK | Shared source only | N/A | Coordinator-only, no cross-source | `Fork.java:39` |
| UnionAll (subquery) | Each branch independent | Each branch independent | Structurally possible via subquery | Schema alignment required |
