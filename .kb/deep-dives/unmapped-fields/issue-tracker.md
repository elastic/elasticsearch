# Unmapped Fields Tech Preview (esql-planning#196)

**Parent**: elastic/esql-planning#196 — "Support unmapped fields LOAD: tech preview" (OPEN)
**Meta-issue**: elastic/elasticsearch#138888 — "ESQL: support for mapping-unavailable fields meta-issue" (CLOSED)

The feature adds `SET unmapped_fields="load"` support to ES|QL, allowing queries to read unmapped field values from `_source` instead of failing. The `nullify` mode (substitutes NULL) shipped first; `load` is in tech preview.

---

## Closed Issues (14)

| # | Title | PR | Summary |
|---|-------|-----|---------|
| **141907** | unmapped_fields shouldn't nullify/load metadata, esp. _score | [#143155](https://github.com/elastic/elasticsearch/pull/143155) | Bug fix: metadata fields (`_score`, `_id`, etc.) were incorrectly being nullified/loaded by the unmapped_fields setting. Should be excluded since they require explicit `METADATA` clause. |
| **141925** | unmapped_fields="load" can lead to wrong sort order | [#143460](https://github.com/elastic/elasticsearch/pull/143460) | Non-issue/fix: loading unmapped fields could produce wrong sort order when unmapped fields were pushed down into sort/filter operations. Fix prevents pushdown of unmapped fields in filters and sorts. |
| **141920** | unmapped_fields="load" can lead to 0 pages emitted | [#143460](https://github.com/elastic/elasticsearch/pull/143460) | Non-issue/fix: queries could return 0 results when unmapped fields were pushed down into Lucene filters. Same fix as #141925 — prevent pushdown of unmapped fields. |
| **142367** | "load" should disallow branching commands (FORK, subqueries, views, LOOKUP JOIN) | [#144115](https://github.com/elastic/elasticsearch/pull/144115) | Non-issue/fix: branching commands have complex semantics with `load`. Analyzer now forbids them until semantics are finalized (see #142033). |
| **141927** | Disallow unmapped_fields="load" with full-text search/MATCH | [#143810](https://github.com/elastic/elasticsearch/pull/143810) | Non-issue/fix: full-text search on unmapped loaded fields returns empty sets silently. Now fails explicitly since MATCH/KNN cannot operate on unmapped fields. |
| **143494** | "load" should not try to load subfields of flattened fields | [#144190](https://github.com/elastic/elasticsearch/pull/144190) | Non-issue/fix: subfields of flattened fields (e.g. `foo.bar` where `foo` is flattened) were being treated as unrelated unmapped fields. Now excluded until flattened field semantics are settled. |
| **142127** | Disallow "load"/"nullify" for functions that implicitly refer to @timestamp (like TBUCKET) | [#144239](https://github.com/elastic/elasticsearch/pull/144239) | Non-issue/fix: functions like `TBUCKET` that implicitly reference `@timestamp` could silently produce wrong results if that field was nullified/loaded. Now explicitly forbidden. |
| **143390** | Double check interaction with PromQL queries | [#144791](https://github.com/elastic/elasticsearch/pull/144791) | Bug fix: PromQL always nullifies unmapped fields regardless of setting. `SET unmapped_fields="load"` with PromQL is now rejected since load never actually loads from _source for PromQL. |
| **141911** | Add tests and identify not-yet working cases for "load" | [#143693](https://github.com/elastic/elasticsearch/pull/143693) | Test/investigation issue: systematically tested existing spec tests with various fields unmapped to discover edge cases. Spawned several child issues. Closed after investigation complete. |
| **142220** | "load" should work for simple views that de-sugar into non-branching queries | [#144661](https://github.com/elastic/elasticsearch/pull/144661) | Non-issue/fix: views that desugar into simple non-branching queries should support `load`. Tests added for views, fork, and subqueries interaction with unmapped_fields. |
| **144833** | Rename unmapped_fields default from `fail` to `default` | [#144869](https://github.com/elastic/elasticsearch/pull/144869) | Enhancement: `fail` was a misnomer since PromQL's default behavior is to nullify, not fail. Renamed to `default` for flexibility. Breaking change (setting name). |
| **141912** | "load" with partially mapped field not as KEYWORD but cast explicitly | [#143693](https://github.com/elastic/elasticsearch/pull/143693) | Non-issue/fix: when a field is mapped as `long` in one index and unmapped in another, using `::DOUBLE` cast should work. Type conflict resolution added for unmapped-fields load. |
| **143916** | Load for unmapped numerics fails during synthetic source loading | [#144112](https://github.com/elastic/elasticsearch/pull/144112) | Bug: ClassCastException when loading unmapped numeric fields from synthetic `_source` (Double couldn't be cast to BytesRef). Fix handles non-KEYWORD types during synthetic source loading. |
| **144799** | Add test cases for index aliases for unmapped_fields=load | [#145095](https://github.com/elastic/elasticsearch/pull/145095) | Test coverage: added tests verifying that `load` works correctly with index aliases. Merged 2026-03-30. |

---

## Open Issues (4)

### #141994 — unmapped_fields="load" doesn't load partially unmapped fields unless mentioned in expressions
- **State**: OPEN
- **Assignee**: alex-spies
- **Labels**: non-issue
- **PR**: [#144228](https://github.com/elastic/elasticsearch/pull/144228) (OPEN) — makes load auto-trigger for partially mapped KEYWORD fields and allows projecting partially mapped non-KEYWORD fields
- **Problem**: When a field is mapped in index1 but unmapped in index2, `load` only loads the unmapped field's values from _source if the field is explicitly used in an expression (e.g., EVAL, WHERE). A bare `KEEP foo` does not trigger loading.
- **What remains**: PR #144228 needs review and merge. Semantics for auto-triggering load on partially mapped fields must be finalized.

### #143218 — Disallow unmapped_fields="load" for partially mapped non-KEYWORD fields used without cast
- **State**: OPEN
- **Assignee**: shmuelhanoch
- **Labels**: non-issue
- **PRs**: [#144109](https://github.com/elastic/elasticsearch/pull/144109) (OPEN), [#144228](https://github.com/elastic/elasticsearch/pull/144228) (OPEN)
- **Problem**: When a field is `long` in one index and unmapped in another, loading from _source returns a string/keyword value that doesn't match the mapped type. Without an explicit cast, this is ambiguous.
- **What remains**: Decide whether to disallow (restrictive) or allow with implicit coercion. PR #144109 takes the restrictive approach; PR #144228 is more permissive. One needs to be chosen and merged.

### #142369 — De-SNAPSHOT unmapped_fields="load" and add documentation
- **State**: OPEN
- **Assignee**: mouhc1ne
- **Labels**: enhancement
- **PR**: [#145052](https://github.com/elastic/elasticsearch/pull/145052) (OPEN) — de-snapshots the "load" setting
- **Problem**: The `load` setting is still behind a snapshot/capability gate. Needs to be de-snapshotted for tech preview release.
- **What remains**: Merge PR #145052, update docs, regenerate Kibana definition JSON (`settings/unmapped_fields.json`). Must have `test-release` label.

### #145206 — unmapped_fields="load" optimized incorrectly for partially unmapped non-KEYWORD fields
- **State**: OPEN
- **Assignee**: (unassigned)
- **Labels**: bug
- **PR**: None yet
- **Problem**: Optimizer produces incorrect results for partially unmapped non-KEYWORD fields. When `foo` is `long` in index1 and unmapped in index2, the optimizer may incorrectly push down or transform operations.
- **What remains**: Needs investigation, root cause analysis, fix, and tests. Newly filed (2026-03-30), no PR yet.

---

## Summary

**14 of 18 issues closed.** The remaining 4 open issues cluster around two themes:

1. **Partially mapped non-KEYWORD fields** (#141994, #143218, #145206): The hardest semantic question — what happens when a field is `long` in one index and unmapped (loaded as keyword from _source) in another? Three issues remain open around type coercion, auto-triggering, and optimizer correctness. Two competing PRs (#144109 restrictive vs #144228 permissive) need a design decision.

2. **De-snapshot for tech preview** (#142369): Mechanical — remove the snapshot gate, update docs, regenerate Kibana JSON. PR #145052 is already open.

**Critical path**: #145206 (optimizer bug) is unassigned and has no PR. The partially-mapped type issues (#141994, #143218) are interdependent and blocked on a semantic decision. #142369 (de-snapshot) is the gate for shipping to tech preview.
