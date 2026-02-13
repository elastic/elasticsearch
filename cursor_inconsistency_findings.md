# Cursor Inconsistency Findings

## What the inconsistency is

- In several `SubstituteRoundToGoldenTests` local physical plans, the same intermediate aggregate fields appear with different `NameId`s across an `ExchangeExec` boundary.
- Typical pattern:
  - `ExchangeExec` output uses IDs like `#2/#3`
  - direct child (often `EsStatsQueryExec`) exposes the same logical fields as `#4/#5`
- `AggregateExec FINAL` then follows the `ExchangeExec` side IDs, so the mismatch looks like an Exchange boundary drift.

## Why this happens (production path)

- This is not a golden-print normalization issue; it is produced by plan construction.
- The split-agg path creates intermediate attributes more than once in different planning phases:
  1. `Mapper` builds split aggregate intermediates and stores them on `ExchangeExec` output.
  2. During local planning (`PlannerUtils.localPlan`), fragment replanning (`LocalMapper`) rebuilds intermediate attrs, allocating fresh `NameId`s.
- `ExchangeExec.replaceChild(...)` keeps its existing `output` list, so parent-side IDs and child-side IDs can diverge.

## Why it still exists

- The plan remains executable, so this drift can survive verification and only shows up as consistency noise in golden plan structure.
- There is also an explicit TODO in `PushStatsToSource` noting recreated attributes can get wrong `NameId`s, which indicates this class of issue is known in planning code.

## Why it is not a trivial fix

- A naive fix (for example, always forcing `ExchangeExec` output to child output IDs) can break split-aggregation runtime semantics.
- We already saw this kind of direct wiring change cause spec IT failures (including `TopIpsGrouping`) and assertion failures.
- A safe fix needs careful ID propagation/reconciliation across planner phases (Mapper/local mapper/exchange boundaries), not just a 1-2 line tweak.
- Any robust change likely touches planner identity flow and can cause broad golden churn, so it needs targeted validation.
