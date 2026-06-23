# HIGHLIGHT PR #151653 — Review Comments & Plan

Untracked scratch file. Delete when done. Two reviewers: **ioanatia** (human, binding)
and **Copilot** (AI). Several Copilot items were already fixed in earlier commits of the
PR; those are marked **ALREADY ADDRESSED (verify only)**.

Recommended execution order is the numbered list below.

---

## 1. Remove the `HIGHLIGHT after STATS/INLINE STATS/LOOKUP JOIN` validation — DONE
- **Source:** ioanatia — `Highlight.java:245`
- **Comment:** "we don't need this check at all. we can highlight on any column, no
  matter if it's coming from STATS, from an Elasticsearch index, external data source etc."
- **Verdict:** AGREE. The operator highlights against a per-row in-memory `MemoryIndex`
  built from the field value itself, not a backing shard doc, so the restriction is
  artificial.
- **Action:** Delete `postAnalysisVerification(...)`, drop `PostAnalysisVerificationAware`
  + now-unused imports in `Highlight.java`; remove the 3 rejection tests in `VerifierTests`.
- **Independent / low risk.**
- **Resolution:** Removed `postAnalysisVerification(...)` and the
  `PostAnalysisVerificationAware` interface from `Highlight`, and pruned the now-unused
  imports (`PostAnalysisVerificationAware`, `Failures`, `Join`, static `Failure.fail`).
  Deleted `testHighlightAfterStatsIsRejected`, `testHighlightAfterInlineStatsIsRejected`,
  and `testHighlightAfterLookupJoinIsRejected` from `VerifierTests`. No remaining references
  to the "HIGHLIGHT cannot be used after" message anywhere.

## 2. `fields` should be `List<NamedExpression>`, not `List<Expression>` — DONE
- **Source:** ioanatia — `LogicalPlanBuilder.java:1441`
- **Comment:** "`fields` here should be a `NamedExpression` - `Expression` is too wide...
  it will also help with `Highlight.generatedAttributesFor` if we pass a
  `List<NamedExpression>` for `fields`."
- **Verdict:** AGREE. Tightens typing and feeds directly into the generated-name fix
  (Copilot #7 below). Do this before the big refactor so the operator/plan signatures
  settle once.
- **Action:** Change `fields` type in `LogicalPlanBuilder`, `Highlight`, `HighlightExec`,
  and `generatedAttributesFor`. Touches serialization read/write of `fields`.
- **Resolution:** Changed `fields` to `List<NamedExpression>` in `Highlight`, `HighlightExec`,
  and the `LogicalPlanBuilder.visitHighlightCommand` builder (cast to `NamedExpression`).
  Serialization read switched to `readNamedWriteableCollectionAsList(NamedExpression.class)`
  (write side is generic, unchanged). `generatedAttributesFor` now takes
  `List<NamedExpression>` and uses `f.name()` directly, dropping the
  `instanceof Attribute ... : Expressions.name(f)` fallback. Updated both serialization test
  fixtures (`HighlightSerializationTests`, `HighlightExecSerializationTests`). Compiles and
  the highlight unit/serialization/golden/verifier suites pass.

## 3. Refactor: lean planner + config object + move operator to compute + scratch
Bundle of four tightly-coupled comments — do together (the package move dictates where
the config/query-building code must live).
- **3a — ioanatia `LocalExecutionPlanner.java:1203`:** "way too much highlight specific (Done)
  logic in `LocalExecutionPlanner` - push this to `HighlightExec` and `HighlightOperator`." 
- **3b — ioanatia `LocalExecutionPlanner.java:1223`:** "pass in the `HighlightOptions` Done
  directly to the `HighlightOperator` ... see `FuseScoreEval`/`RrfConfig`/`LinearConfig`."
- **3c — ioanatia `HighlightOperator.java:8`:** "wrong place — should be in
  `compute/.../operator` with the rest of the operators ... move `HighlightOperatorTests` too."
- **3d — ioanatia `HighlightOperator.java:192`:** "pass `scratch` as an argument to
  `highlightField` instead of initializing a new one."
- **Verdict:** AGREE on all four.
- **Action:** New compute-side `HighlightConfig` record (RrfConfig-style); move
  `HighlightOperator`(+tests) to `org.elasticsearch.compute.operator`; build the Lucene
  `Query`/`Analyzer`/`PassageFormatter` inside the operator; `planHighlight` reduces to
  evaluators + layout + factory; thread one `scratch` through `process`.
- **Notes:** compute is a JPMS module — needs `requires org.apache.lucene.highlighter;`
  and `requires org.apache.lucene.memory;`. Operator must use `IllegalArgumentException`
  (not the esql-only `EsqlIllegalArgumentException`).

## 4. Add a test where HIGHLIGHT is pushed to data nodes — DONE
- **Source:** ioanatia — `highlight.csv-spec:207`
- **Comment:** Existing query gets an implicit `SORT ... LIMIT` so HIGHLIGHT runs on the
  coordinator. Add a query that forces pushdown (sort on `highlight_*` after HIGHLIGHT) so
  serialization/deserialization is exercised end-to-end.
- **Verdict:** AGREE. Especially valuable now that the plan nodes carry generated fields.
- **Action:** Add a csv-spec test using the suggested shape (`... | HIGHLIGHT ... | SORT
  highlight_title | LIMIT 1000 | KEEP ...`).
- **Resolution:** Added `highlightPushedToDataNodes` to `highlight.csv-spec`. Sorting on the
  `highlight_title` column forces HIGHLIGHT to execute before the final TopN, so it is pushed
  down to the data nodes. Verified end-to-end with the distributed `CsvIT` runner:
  `./gradlew :x-pack:plugin:esql:internalClusterTest --tests
  "org.elasticsearch.xpack.esql.CsvIT.*highlightPushedToDataNodes*"` (passes,
  exercising `HighlightExec` serialization/deserialization).

## 5. Fix the failing `highlightNumberOfFragments` csv-spec test — TODO
- **Source:** Pre-existing failure surfaced while verifying #1/#2 (not raised by a reviewer).
- **Symptom:** `CsvIT.test {csv-spec:highlight.highlightNumberOfFragments}` fails — with
  `number_of_fragments: 2` the operator returns the whole text as a single fragment instead
  of the two expected sentence fragments:
  - expected `["<em>Elasticsearch</em> is fast.", "<em>Elasticsearch</em> is scalable."]`
  - actual `"<em>Elasticsearch</em> is fast. <em>Elasticsearch</em> is scalable. <em>Elasticsearch</em> is open."`
- **Confirmed pre-existing:** Reproduces identically on the clean branch baseline (changes
  stashed) with the same seed, so it is unrelated to #1/#2. Not listed in `muted-tests.yml`.
- **Verdict:** Real bug in fragmentation — `number_of_fragments` is not capping/splitting the
  output. Likely in `planHighlight` / `HighlightOperator` (passage formatter, fragment count,
  or the `BreakIterator`/sentence-boundary wiring), independent of locale.
- **Action:** Investigate `HighlightOperator` + `CustomPassageFormatter` fragment handling so
  `number_of_fragments` produces N bounded sentence fragments; re-enable / confirm the test
  with `./gradlew :x-pack:plugin:esql:internalClusterTest --tests
  "org.elasticsearch.xpack.esql.CsvIT.*highlightNumberOfFragments*"`. Until fixed, consider a
  `muted-tests.yml` entry referencing the tracking issue so CI is not blocked.
