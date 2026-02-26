# PromQL Vector Matching

Tracking issue: https://github.com/elastic/elasticsearch/issues/142596

## Background

PromQL binary operations between vectors with different label sets.
`on`/`ignoring` select matching labels; `group_left`/`group_right` allow
many-to-one cardinality.

```promql
errors{code="500"} / ignoring(code) requests
```

Left `{method, code}`, right `{method}` → match on `{method}`.

## Current state

| Layer | Status |
|-------|--------|
| Grammar | Done — parses all vector matching syntax |
| Parser | Done — builds `VectorMatch` on `VectorBinaryOperator` |
| Verifier | Allows `on`/`ignoring` without group modifiers (Phase 1) |
| Translator | Folds compatible groupings into single aggregate (Phase 1) |
| Capability | `PROMQL_VECTOR_MATCHING` in `EsqlCapabilities` |

Different groupings and group modifiers still blocked.

## Design

Vector matching = JOIN on matching labels between two aggregation results.

### Matching keys

Let `L` = label list in `on()`/`ignoring()`, `G_l` = left groupings,
`G_r` = right groupings. Step is always an implicit grouping on both sides.

**`on(L)`**: matching keys = `L ∪ {step}`.
Every label in `L` must be present in both `G_l` and `G_r`. A label
absent from one side has no values to join on — error at plan time.

**`ignoring(L)`**: matching keys = `(G_l ∩ G_r) \ L`.
Only labels present in both sides can be matching keys. Labels in one
side only are the extra dimensions requiring `group_left`/`group_right`.
Step is in both sides' groupings and never ignored.

Examples with `G_l = {step, cluster, pod}`, `G_r = {step, cluster}`:

| Expression | Keys | Extra dims |
|-----------|------|-----------|
| `on(cluster)` | `{step, cluster}` | pod (left only) |
| `on(cluster, pod)` | error: pod ∉ G_r | — |
| `ignoring(pod)` | `{step, cluster}` | pod (left only) |
| `ignoring(cluster)` | `{step}` | cluster+pod (left), cluster (right) |

### Join type

Prometheus drops unmatched rows — INNER JOIN semantics.

`InlineJoin` is LEFT JOIN. Both sides share a single `EsRelation` (one
`index=` parameter, can be a pattern resolving to many backing indices).

**Known bug**: label matchers from both sides are currently merged with
AND into a single filter (`TranslatePromqlToEsqlPlan.labelFilterConditions`).
`errors{service="frontend"} / requests{service="backend"}` filters by
`service="frontend" AND service="backend"` — matches nothing. Each side
should filter independently.

With the current merged filter, both sides see identical documents, so
LEFT = INNER in practice. Once per-side filtering is fixed, a driving-side
key may have no match on the inline side. LEFT JOIN produces NULLs where
Prometheus drops the row. Fix: `Filter[rhs IS NOT NULL]` after the join.

### Driving vs inline side

Execution rule for join-based vector matching:

- Keep the higher-cardinality vector as the **driving** side.
- Keep the lower-cardinality vector as the **inline** (lookup) side.
- `InlineJoin` is LEFT JOIN, so the driving side must be on the left.
- If needed (`group_right`), swap operands in the plan to keep this shape.

Decision table:

| Case | Action |
|------|--------|
| 1:1 compatible (`on`/`ignoring`, no group modifier) | no join; fold into one aggregate |
| n:1 (`group_left`) | left drives, right inline |
| 1:n (`group_right`) | swap; right drives, left inline |
| no group modifier with n:1 or 1:n cardinality | error |

### Cardinality cases

#### 1:1 — no group modifier, compatible groupings

```promql
sum by (cluster)(a) / on(cluster) sum by (cluster)(b)
```

Both sides group by `{step, cluster}`. Same groupings → fold into single
aggregate. No join needed (same cardinality, same matching keys).

```
Eval[ratio = sum_a / sum_b]
  TimeSeriesAggregate[sum(a), sum(b) BY step, cluster]
    EsRelation
```

#### 1:1 — no group modifier, incompatible groupings

```promql
sum by (cluster, pod)(a) / ignoring(pod) sum by (cluster)(b)
```

Left produces multiple rows per matching key. Without `group_left` this is a cardinality error in Prometheus. We throw `QlIllegalArgumentException` instead of choosing a driving side implicitly.

#### n:1 — `group_left`

```promql
sum by (cluster, pod)(a) / ignoring(pod) group_left sum by (cluster)(b)
```

Left `{step, cluster, pod}`, right `{step, cluster}`.
Keys: `{step, cluster}`. Left drives (many), right is inline (one).

Deterministic execution has two phases:
1) Materialize inline subplan (right side) on coordinator.
2) Run main plan with materialized right side as local hash table.

Logical plan (before subplan execution):

```
Eval[ratio = lhs_value / rhs_value]
  Filter[rhs_value IS NOT NULL]
    InlineJoin[LEFT, keys={step, cluster}]
      ├─ drivingPlan[left side translated independently]
      │   └─ ... (typically TimeSeriesAggregate/Aggregate + optional Eval)
      └─ inlinePlan[right side translated independently]
          └─ StubRelation
```

Logical plan (after inline materialization):

```
Eval[ratio = lhs_value / rhs_value]
  Filter[rhs_value IS NOT NULL]
    InlineJoin[LEFT, keys={step, cluster}]
      ├─ drivingPlan[left output includes lhs_value, step, cluster, pod]
      └─ LocalRelation[rhs_value, step, cluster]
```

Physical shape:

```
EvalExec
  FilterExec[is_not_null(rhs_value)]
    HashJoinExec[leftKeys={step,cluster}, right=LocalSourceExec]
```

Each side keeps its own aggregation chain; no new `TimeSeriesAggregate` is
created above `InlineJoin`. The NULL filter drops rows where the inline side
had no match (INNER JOIN semantics).

#### 1:n — `group_right`

```promql
sum by (cluster)(a) / ignoring(pod) group_right sum by (cluster, pod)(b)
```

Right has more dimensions → right drives. Swap sides in the plan so the
driving (many) side stays left in `InlineJoin`, while keeping operand order
in the expression.

Logical plan (before subplan execution):

```
Eval[ratio = lhs_value / rhs_value]   // preserve original operand order
  Filter[lhs_value IS NOT NULL]
    InlineJoin[LEFT, keys={step, cluster}]
      ├─ drivingPlan[right side translated independently]
      └─ inlinePlan[left side translated independently]
          └─ StubRelation
```

Logical plan (after inline materialization):

```
Eval[ratio = lhs_value / rhs_value]   // preserve original operand order
  Filter[lhs_value IS NOT NULL]
    InlineJoin[LEFT, keys={step, cluster}]
      ├─ drivingPlan[right output includes rhs_value, step, cluster, pod]
      └─ LocalRelation[lhs_value, step, cluster]
```

Same structure as n:1 with sides swapped. `lhs_value` comes from the inline
side, `rhs_value` from the driving side. NULL filter drops unmatched
rows.

### Files touched

`PromqlCommand.java` (lift blockers), `TranslatePromqlToEsqlPlan.java`
(join path), `EsqlCapabilities.java` (capability flag).

No new plan nodes, no compute-layer changes, no grammar changes.

## Phases

### Phase 1 — compatible groupings ✓

`on`/`ignoring` without group modifiers where both sides have same
groupings. Folded into single `Aggregate`. Done.

### Phase 2 — `group_left`/`group_right`

Incompatible groupings require `InlineJoin`.

1. Lift verifier blocker for `Joining != NONE`.
2. In `foldBinaryExpressionAggregates` when groupings differ:
   - Translate both operands independently into stable instant-vector subplans.
   - `group_left` → left drives, right inlined.
   - `group_right` → swap: right drives, left inlined.
   - Build matching keys from side outputs (`step` + label keys).
   - Stub inline-side source via `InlineJoin.stubSource`.
   - Build `InlineJoin(drivingPlan, inlinePlan, LEFT, matchingKeys)`.
   - Apply `Filter[inlineValue IS NOT NULL]` to enforce INNER semantics.
   - Apply `Eval[lhsValue <op> rhsValue]` on top.
3. Rely on `EsqlSession` subplan path: execute right subplan first,
   replace with `LocalRelation`, then map final join to `HashJoinExec`.
4. Project extra labels from group-modifier list into inline output.

### Phase 2 critical assessment

This pivot is safer than building a new `TimeSeriesAggregate` above
`InlineJoin`, but it has trade-offs:

- **Pros**: keeps `TranslateTimeSeriesAggregate` rewrites side-local and avoids
  cross-side `step` identity collisions.
- **Correctness risks**: join keys are matched by output names from independently
  rewritten subplans; naming drift (especially `step`) must be guarded by tests.
- **Optimizer risks**: operator-phase rules must not move/merge evals across
  join sides in ways that create missing references.
- **Performance risks**: inline-side materialization can grow coordinator memory
  pressure for high-cardinality right sides.
- **Acceptance criteria**: add plan-shape tests asserting (a) no
  `TimeSeriesAggregate` is introduced above `InlineJoin` in Phase 2, (b)
  pre-materialization uses `StubRelation`, and (c) post-materialization uses
  `LocalRelation` with stable join keys.

### Phase 3 — raw instant selectors

`metric_a / on(cluster) metric_b` without aggregation wrappers.
Requires decomposing `_timeseries` into individual labels. TBD.

### Phase 4 — set operators (`and`, `or`, `unless`)

Set operators filter/combine vectors without computing values. Different
join semantics from arithmetic matching.

| Operator | PromQL semantics | SQL equivalent |
|----------|-----------------|----------------|
| `a and b` | keep `a` where match in `b` | semi-join |
| `a unless b` | keep `a` where no match in `b` | anti-join |
| `a or b` | all `a` + unmatched `b` | left union |

Same matching-key infrastructure, different plan structure. Separate effort.

## Open questions

1. **One-to-one uniqueness**: Prometheus errors when matching key hits
   multiple rows without group modifier. Data-dependent — cannot validate
   at plan time. Defer runtime check.

2. **InlineJoin optimizer isolation**: lock this with tests that assert
   `group_left`/`group_right` plans do not introduce a `TimeSeriesAggregate`
   above `InlineJoin`, and that optimizer rewrites keep references side-local
   (no cross-side missing refs after `PropagateInlineEvals`/`CombineEvals`).

3. **Implicit matching**: no `on`/`ignoring` + different groupings →
   empty result in Prometheus. Ensure translator matches.
