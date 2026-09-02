# HIGHLIGHT Part C — Implementation Guide

**Feature:** implicit query from upstream `WHERE`, bare `HIGHLIGHT` (no `ON`), and `HIGHLIGHT ON *`
**Audience:** an engineer or coding agent with no prior context on this feature. Everything needed is in this document plus the referenced files.
**Repo:** `elastic/elasticsearch`, branch off `main`. All paths are relative to the repo root.
**Revision note:** v5 — keeps `HIGHLIGHT_V6` for the existing command and introduces the
`HIGHLIGHT_IMPLICIT_QUERY_AND_FIELDS` capability for Part C tests. It also incorporates three review rounds.
Round 1 (`highlight-part-c-review.md`):
resolution trigger via `derivationPending`, removal of the identity-based predicate guard, QSTR derivation
reduced to `default_field`, negations excluded from implicit collection, corrected `ON *` expansion.
Round 2 (`highlight-part-c-review-2.md`): named pattern variables instead of `case X _` (does not compile
under `--release 21`), the masked-error caveat for the "requires a query" message, `Not` pruned from field
derivation, the `derivationPending` serialization stance. Round 3 (`highlight-part-c-review-3.md`):
reverted the round-2 synthesized-`Source` idea back to `Predicates.combineOr` (with corrected facts about
where the query text actually surfaces — see §3.2), and gated the `requireOnField` check on a user-written
`ON` clause via the new `derivedFields` flag (fixes a regression where `Not`-pruned derivation made
`HIGHLIGHT MATCH(a,..) AND NOT MATCH(b,..)` with no `ON` fail citing an ON list the user never wrote).

Read `AGENTS.md` at the repo root before starting — it contains binding rules on formatting, imports,
switch statements, capabilities, and commit messages. In particular: **read the javadoc of
`EsqlCapabilities.Cap`** (in `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/action/EsqlCapabilities.java`)
before touching capabilities or csv-spec tests. That rule comes from AGENTS.md and is not optional.

---

## 1. What you are building

The ES|QL `HIGHLIGHT` command exists today (snapshot-only, behind the `DEV_HIGHLIGHT` grammar token and the
`HIGHLIGHT_V6` capability). Its current form requires **both** a query and an `ON` field list:

```esql
FROM books
| WHERE MATCH(title, "Return")
| HIGHLIGHT MATCH(title, "Return") ON title WITH {"pre_tags": "<b>", "post_tags": "</b>"}
| KEEP title, highlight_title
```

Part C removes the two "required" constraints, per the design doc ("Highlighting in ES|QL – Plan", §3.1, §5, §5.1):

| New form | Behavior |
| --- | --- |
| `HIGHLIGHT` (no query, no `ON`) | Query is derived from upstream full-text `WHERE` clauses; the column set is derived from the fields the query references. |
| `HIGHLIGHT ON title, body` (no query) | Query derived from upstream `WHERE`; columns are exactly the listed fields. A listed field the query doesn't target yields `null` (or a `no_match_size` prefix). |
| `HIGHLIGHT "fox"` (query, no `ON`) | Column set derived from the query. A plain string literal has no field references, so it falls back to **all text/keyword fields**. |
| `HIGHLIGHT ON *` | All text/keyword fields of the incoming rows, with or without an explicit query. |

Worked example of the flagship use case (Kibana Discover):

```esql
FROM books
| WHERE MATCH(title, "Return")
| HIGHLIGHT
| KEEP title, highlight_title
```

produces a `highlight_title` keyword column with `<em>Return</em>` wrapped fragments, without the user
re-stating the query or the field.

### Non-goals (do NOT build these)

- Stage 2 / data-node fetch-phase highlighting.
- Semantic highlighter, `semantic_text`-aware chunk selection.
- Multiple `pre_tags`/`post_tags` values (separate TODO in `HighlightOptions`).
- Arbitrary field *patterns* in `ON` (e.g. `ON title*`). Only concrete names and a sole `*` are in scope.
- Rewriting collected predicates across RENAMEs (see §3.4 accepted limitation).
- Changing `TOP_SNIPPETS` in any way.

---

## 2. How HIGHLIGHT works today (read this before coding)

Pipeline walkthrough — open each file and skim it top to bottom once:

1. **Grammar** — `x-pack/plugin/esql/src/main/antlr/EsqlBaseParser.g4`, rule `highlightCommand`:
   ```antlr
   highlightCommand
       : DEV_HIGHLIGHT (prefixKeyword=identifier ASSIGN prefix=string)? queryExpression=booleanExpression ON highlightFields=qualifiedNames commandNamedParameters
       ;
   ```
   The command is listed under `processingCommand` behind `{this.isDevVersion()}?`. Files under
   `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/parser/EsqlBaseParser*.java` are
   **generated** — never hand-edit them; regenerate with `./gradlew :x-pack:plugin:esql:regen`.

2. **Parse → logical plan** — `LogicalPlanBuilder.visitHighlightCommand(...)` in
   `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/parser/LogicalPlanBuilder.java`
   (search for `visitHighlightCommand`). It builds a
   `Highlight` node with: `prefix` (default `"highlight_"`), `query` (an `Expression`), `fields`
   (`List<NamedExpression>` of `UnresolvedAttribute`), `options` (`MapExpression` from `WITH {...}`), and
   `generatedFields` — one `ReferenceAttribute` per field named `<prefix><field>`, type KEYWORD, built by
   `Highlight.generatedAttributesFor(...)`. Two TODO comments in this method mark exactly the Part C work.

3. **Logical node** — `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/logical/Highlight.java`.
   Note: `output()` = child output merged with `generatedFields` (name collisions shadow, which is how
   `prefix = ""` overwrites the source column); `computeReferences()` = the ON fields only (the query's
   fields are *not* runtime inputs — the query is translated to a Lucene query from names/literals, the
   operator never needs the query fields' values); `postAnalysisVerification(...)` validates options and the
   query shape; `expressionsResolved()` tolerates a `null` query.

4. **Analysis** — `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/analysis/Analyzer.java`.
   There is **no Highlight-specific rule today**: the `ResolveRefs` rule's `default ->` branch resolves the
   `UnresolvedAttribute`s in `fields`/`query` generically (see the `switch (plan)` inside
   `ResolveRefs.rule(...)`, around line 1110). Part C adds a `case Highlight`.

   **Critical mechanic** (this shaped the design in Step 3): analyzer rules skip nodes that already report
   themselves resolved. `AnalyzerRules.ParameterizedAnalyzerRule.apply(...)`
   (`.../esql/analysis/AnalyzerRules.java`) is
   `plan.transformUp(typeToken(), t -> t.analyzed() || skipResolved() && t.resolved() ? t : rule(t, context))`,
   `skipResolved()` defaults to `true`, and `ResolveRefs` does not override it. A bare `HIGHLIGHT` node —
   null query, empty field list — reports `expressionsResolved() == true` today, so without the
   `derivationPending` flag introduced in Step 3 **the rule would never run for exactly the forms Part C
   adds**. Do not "simplify" the flag away.

5. **Query validation & translation** —
   `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/planner/HighlightQueryBuilders.java`.
   `verify(query, onFields, analyzer)` is called from `Highlight.postAnalysisVerification` and checks the
   query is a supported shape: `Match` (also covers the `:` operator via `MatchOperator extends Match`),
   `MatchPhrase`, `QueryString`, `Kql`, `And`/`Or`/`Not` combinations, or a foldable string literal
   (treated as `query_string` over the ON fields). It currently **requires every query field to be listed
   in ON** (`requireOnField` → `"HIGHLIGHT query field [x] is not in ON fields [...]"`). `translate(...)`
   converts to a Lucene query once at local-planning time, against a synthetic
   `RuntimeSearchExecutionContext` that only knows the ON fields — **a query field that is not an ON field
   resolves to a null `MappedFieldType` and becomes an unmapped-field match-none query**, i.e. it is safe
   and yields null highlight columns rather than an error. Part C relies on this twice (§3.2, §3.4).

6. **Physical plan & execution** — `MapperUtils` maps `Highlight` → `HighlightExec`;
   `LocalExecutionPlanner.planHighlight(...)` folds `HighlightOptions`, translates the query, and builds a
   `HighlightOperator` (in `x-pack/plugin/esql/compute/src/main/java/org/elasticsearch/compute/operator/HighlightOperator.java`).
   Per row it indexes the ON-field text into a `MemoryIndex` and runs one `CustomUnifiedHighlighter` per ON
   field against the same translated query. **A field the query doesn't target yields `null`** (unless
   `no_match_size > 0`) — this already gives us the §3.1 "listed but untargeted field → null" semantics for
   free. You should not need to change the operator at all.

7. **Capability & gating** — the existing command remains behind
   `EsqlCapabilities.Cap.HIGHLIGHT_V6(Build.current().isSnapshot())`. Part C adds
   `HIGHLIGHT_IMPLICIT_QUERY_AND_FIELDS(Build.current().isSnapshot())`. Existing tests continue to use
   `highlight_v6`; only tests for implicit queries, derived fields, and `ON *` use
   `highlight_implicit_query_and_fields`. This keeps mixed-version/BWC CI clusters from running new-form
   queries against old nodes without renaming the existing capability.

8. **Tests today** (use these as templates):
   - Parser: `x-pack/plugin/esql/src/test/java/org/elasticsearch/xpack/esql/parser/StatementParserTests.java` (`grep -n HIGHLIGHT`)
   - Verifier: `x-pack/plugin/esql/src/test/java/org/elasticsearch/xpack/esql/analysis/VerifierTests.java` (`grep -n HIGHLIGHT`)
   - Node serialization: `.../plan/logical/HighlightSerializationTests.java`, `.../plan/physical/HighlightExecSerializationTests.java`
   - Options: `.../plan/logical/HighlightOptionsTests.java`
   - Query building: `.../planner/HighlightQueryBuildersTests.java`
   - Physical-plan golden tests: `.../optimizer/rules/physical/local/HighlightGoldenTests.java`
   - Random query generator: `x-pack/plugin/esql/qa/testFixtures/src/main/java/org/elasticsearch/xpack/esql/generator/command/pipe/HighlightGenerator.java`
   - Usage telemetry YAML: `x-pack/plugin/src/yamlRestTest/resources/rest-api-spec/test/esql/60_usage.yml`
   - End-to-end: `x-pack/plugin/esql/qa/testFixtures/src/main/resources/highlight.csv-spec` (~1900 lines;
     `ROW`-based tests at the top, `FROM books` / `FROM semantic_text` tests from ~line 1040).

---

## 3. Behavior specification (normative)

### 3.1 Grammar

```antlr
highlightCommand
    : DEV_HIGHLIGHT (prefixKeyword=identifier ASSIGN prefix=string)? queryExpression=booleanExpression? (ON highlightFields=qualifiedNamePatterns)? commandNamedParameters
    ;
```

Both the query and the `ON` clause become optional; `qualifiedNames` becomes `qualifiedNamePatterns` so `*`
lexes (same rule KEEP/DROP use). All four combinations are valid syntax:
`HIGHLIGHT`, `HIGHLIGHT <query>`, `HIGHLIGHT ON <fields>`, `HIGHLIGHT <query> ON <fields>` — each optionally
with `prefix = "..."` and `WITH {...}`.

Parser-level (not analysis-level) rejections, thrown as `ParsingException` from `LogicalPlanBuilder`:

- A name **pattern** other than a lone `*` (i.e. any `UnresolvedNamePattern`, e.g. `ON title*`):
  `line X:Y: Invalid pattern [title*] in HIGHLIGHT ON, expected field names or [*]`
- `*` mixed with other entries (`ON title, *`):
  `line X:Y: HIGHLIGHT ON [*] cannot be combined with other fields`

The parser also sets `derivationPending = true` on the `Highlight` node whenever the query or the `ON`
clause is absent (see Step 3 for why this flag must exist).

### 3.2 Query resolution (design doc §5)

- **Explicit query present** → exactly today's behavior. Upstream `WHERE`s are ignored for highlighting.
- **No query** → during analysis, walk **down the child chain** of the `Highlight` node (downward = upstream
  commands) and collect *searchable conjuncts* from every `Filter` node in the same **doc-level slice**:
  - Split each `Filter.condition()` with `Predicates.splitAnd(...)`
    (`org.elasticsearch.xpack.esql.expression.predicate.Predicates`).
  - A conjunct is *searchable* iff its entire tree is built from `Match` / `MatchPhrase` / `QueryString` /
    `Kql` / `And` / `Or` — **positive** full-text combinations only. `Not` is deliberately excluded from
    implicit collection: a negated predicate matches by *absence* and contributes nothing highlightable, and
    OR-ing in a pure negation (e.g. `... OR NOT MATCH(b,"y")`) would make the highlight query match nearly
    everything while marking nothing. A conjunct containing a `Not` anywhere is dropped whole. (Explicit
    queries still accept `Not` — there is an existing verifier test for
    `HIGHLIGHT NOT MATCH(title, "fox") ON title` around `VerifierTests.java:4694` that must keep passing.)
    Likewise `MATCH(a,"x") OR b > 3` is not searchable (drop the whole conjunct);
    `MATCH(a,"x") AND b > 3` contributes `MATCH(a,"x")` because `splitAnd` separates them first.
  - Collect the conjunct **as-is, with no liveness guard on its attributes**. A predicate whose field was
    later dropped or renamed (`WHERE MATCH(title,..) | DROP title | HIGHLIGHT ON body`) is safe: the
    highlight query is translated against a synthetic context that only knows the ON fields, so the stale
    field becomes an unmapped-field match-none query and the affected column is simply `null` (§2.5).
    Do **not** add an `outputSet().containsAll(references)` guard here — `AttributeSet` membership is
    `NameId`-identity-based (`AttributeMap.AttributeWrapper` uses `semanticEquals`), so a RENAME or
    MV_EXPAND minting a fresh id would silently drop the predicate and turn the documented null-column
    behavior (§3.4) into a confusing "requires a query" error.
  - The walk passes through nodes that keep rows 1:1 doc-bound — allowlist by class:
    `Filter`, `Eval`, `Project` (covers `Keep` and resolved KEEP/RENAME projections), `Rename`, `Drop`,
    `RegexExtract` (covers `Dissect`/`Grok`), `Limit`, `OrderBy`, `TopN`, `MvExpand`, `Highlight`.
    **Any other node type stops the walk** (that includes `Aggregate`/`InlineStats`/joins/`Fork`/
    `Completion`/`Rerank`/`Sample`/relations — being conservative is correct here).
  - OR the collected conjuncts together with `Predicates.combineOr(...)`; duplicates are harmless.
  - Mark the node as carrying an **implicit** query (new `implicitQuery` boolean, see Step 3).
- OR-combining uses plain `Predicates.combineOr(...)`. Known cosmetic consequence, accepted for Part C:
  `combineOr` builds `new Or(l.source(), l, r)`, so the combined expression carries the *first conjunct's*
  `sourceText()`, and `HighlightQueryBuilders.translate` uses `sourceText()` as the `queryText` that
  `HighlightConfig.describe()` renders into PROFILE operator descriptions. A two-WHERE implicit query
  therefore *describes* itself as `query=MATCH(title,"x")` while running the OR of both. This affects
  PROFILE output only — `HighlightGoldenTests` render physical-plan `toString()`, and `HighlightConfig` is
  built later in `LocalExecutionPlanner`, so golden files never contain `queryText`. Do **not** try to fix
  this by hand-building the OR chain with a synthesized `Source`: the repo treats synthetic source text as
  test-only (`Source.synthetic` is `@Deprecated` — "can't be correctly deserialized"), `Source.readFrom`
  reconstructs text by slicing the original query string (mismatched text overruns and throws), and
  `BinaryLogic.writeTo` is `final` and hardcodes `Source.EMPTY`, discarding whatever you attach if the
  expression is ever serialized. It happens to work today only because HIGHLIGHT executes on the
  coordinator (`HighlightExec` sits above `ExchangeExec` — limits/TopN push below it, see the
  `HighlightGoldenTests` expectations), so the expression never crosses the wire — but Stage 2 plans to
  move it, at which point the text silently degrades to `""`. The honest fix, if ever wanted, is threading
  a display string from the analyzer through `Highlight`/`HighlightExec` into `HighlightConfig` — a
  deferred follow-up, not Part C.
- **No query and nothing collected** → leave `query == null`; post-analysis verification fails with:
  `HIGHLIGHT requires a query or a preceding full-text WHERE (MATCH, MATCH_PHRASE, QSTR or KQL)`

  **Error-masking caveat (pre-existing ES|QL behavior, newly reachable through Part C):**
  `Verifier.verify` runs an unresolved-attribute sweep first and bails out before any post-analysis plan
  check (`Verifier.java`, "in case of failures bail-out"). When derivation fails, `fields` and
  `generatedFields` stay empty, so `Highlight.output()` has no `highlight_*` column — and any downstream
  reference to it is an unresolved attribute that fires first. So the flagship mistake, written the way a
  real user writes it:

  ```esql
  FROM books | HIGHLIGHT | KEEP title, highlight_title
  ```

  reports only `Unknown column [highlight_title]`; the "requires a query" message surfaces only when no
  downstream command references a generated column. Accept and document this in Part C (pin both messages
  with tests — §5.3); improving it (e.g. minting placeholder generated attributes on failed derivation so
  the HIGHLIGHT-specific failure is the only one) is a behavior change beyond Part C — raise it with the
  team as a follow-up rather than building it in.

The `requireOnField` check in `HighlightQueryBuilders` is enforced **only when the user wrote both an
explicit query and a concrete `ON` list** (`enforceOnFields = implicitQuery == false && derivedFields == false`;
`derivedFields` is set by the parser for the bare and `ON *` forms — Step 3/4). The check exists to catch a
mismatch between two *user-written* lists; against a derived list it is vacuous at best and harmful at
worst: `verifyQueryStructure` recurses into `Not`, so with a derived field set (which prunes `Not`
subtrees, §3.3) the query `MATCH(title,"x") AND NOT MATCH(description,"y")` with no `ON` would derive
`[title]` and then fail `requireOnField("description", [title])` — an error citing an ON list the user
never typed. Implicit queries skip the check for the design-doc reason (§3.1: a listed field the query
doesn't target must yield `null`, not an error); derived field sets skip it for the reason above.

Note the implicit-shape check is **intentionally narrower** than the explicit-shape check in
`verifyQueryStructure` (no `Not`, no string `Literal` — a bare literal in a WHERE is not a predicate). Keep
both checks side by side in `HighlightQueryBuilders` with a comment cross-referencing them; they diverge on
purpose, so don't force one to delegate to the other.

### 3.3 Column-set resolution (design doc §3.1, §5.1)

- `ON f1, f2` → exactly those (today's behavior).
- `ON *` → all **text/keyword** attributes of the child output (`DataType.isString(...)`, which is exactly
  `KEYWORD || TEXT` — the same filter `Highlight.verifyFieldTypes` uses), excluding `MetadataAttribute`s,
  in child-output order, deduped by name keeping the **last** occurrence (later columns shadow earlier ones
  elsewhere in ES|QL). Two deliberate consequences to be aware of and pin with tests, not "fix":
  - **Multi-field sub-fields are included.** `mapping-books.json` maps `author` as `text` with an
    `author.keyword` sub-field; `ON *` over `FROM books` therefore generates both `highlight_author` and
    `highlight_author.keyword`. This mirrors Query DSL `"fields": {"*": {}}`, which also expands to
    sub-fields; with require-field-match semantics the `.keyword` column will usually be null.
  - **`semantic_text` fields are included.** ES|QL surfaces `semantic_text` as `DataType.TEXT` (field-caps
    reports it as text; see the `semantic_text` → `TEXT` entry in the type-loading map and the existing
    `FROM semantic_text` csv-spec tests whose column header is `semantic_text_field:text`). So `isString`
    matches them and `*` picks them up — highlighting stays lexical, which is consistent with the design.
  - Columns synthesized by earlier commands (EVAL, a previous HIGHLIGHT) are included too, same reasoning.
- **No `ON`** → derive from the resolved query (explicit or implicit):
  - `Match`/`MatchPhrase` → the target field's name.
  - `QueryString` → the folded `default_field` option if present (a value of `"*"` or any wildcard-bearing
    value = "no specific fields"); otherwise "no specific fields". **QSTR has no `fields` option in ES|QL**
    — `QueryString.ALLOWED_OPTIONS` contains `default_field` but no `fields` entry, and
    `Options.populateMap` rejects unknown keys, so `default_field` is the only derivable option. (The
    `@MapParam` javadoc in `QueryString.java` *does* describe a `fields` entry — that is a documentation
    bug being tracked separately; do not build on it.)
  - `Kql` → "no specific fields" (fields live inside the query string; not derivable).
  - String literal query → "no specific fields".
  - `And`/`Or` → union of both sides.
  - `Not` → **contributes nothing: the whole negated subtree is pruned** before any of the rules above
    apply (including the fallback triggers inside it). Same rationale as §3.2 — a negated predicate
    matches by absence, and with require-field-match semantics its column could only ever be null. This
    only matters for *explicit* queries (implicit collection already dropped negations): the bare form
    `HIGHLIGHT NOT MATCH(title, "x")` prunes everything, derives no fields, and fails verification with
    the "found no text or keyword fields" message below — which is the right nudge, because with an
    explicit `ON` a pure-negative query is still legal (pinned by the existing verifier test
    `HIGHLIGHT NOT MATCH(title, "fox") ON title`, ~`VerifierTests.java:4694`). The **mixed** shape
    `HIGHLIGHT MATCH(title,"x") AND NOT MATCH(description,"y")` with no `ON` derives `[title]` only and
    analyzes cleanly — this works because `requireOnField` is not enforced against derived field sets
    (§3.2); the negated field simply gets no column.
  - If any surviving part said "no specific fields" → fall back to the full `ON *` expansion above.
  - Map the collected names to child-output attributes; silently skip names that are missing or not
    text/keyword. (E.g. `WHERE MATCH(title,...) | KEEP body | HIGHLIGHT` skips `title`.)
- If the final field set is **empty** (star matched nothing, or every derived name was skipped), post-analysis
  verification fails with: `HIGHLIGHT found no text or keyword fields to highlight; add an explicit ON clause`
- `generatedFields` (the `<prefix><name>` output attributes) are recomputed from the final field list, once.

### 3.4 Semantics recap (what csv-spec must pin, from design §3.1)

Given `FROM books | WHERE MATCH(title, "Return")`:

| Statement | Result |
| --- | --- |
| `HIGHLIGHT` | one column `highlight_title`, populated on every row (all rows matched the WHERE). |
| `HIGHLIGHT ON title` | same. |
| `HIGHLIGHT ON description` | `highlight_description` exists but is `null` (query targets `title` only). |
| `HIGHLIGHT ON *` | one `highlight_<field>` per text/keyword field — including `.keyword` sub-fields — and only `highlight_title` populates. |
| `HIGHLIGHT "Tolkien"` (explicit, no ON) | falls back to all text/keyword fields; upstream WHERE ignored. |
| `WHERE MATCH(title,"x") | WHERE MATCH(description,"y") | HIGHLIGHT` | implicit query = `MATCH(title,"x") OR MATCH(description,"y")`; columns `highlight_title` and `highlight_description`; each populates only where its own field matched. |
| `WHERE MATCH(title,"x") | STATS c = COUNT(*) BY title | HIGHLIGHT ON title` | **error** — the walk stops at STATS, no query found. |
| `WHERE MATCH(title,"x") | DROP title | HIGHLIGHT ON description` | predicate is still collected; `title` is unknown to the highlight context → match-none → `highlight_description` is `null`. Not an error. |
| `WHERE MATCH(title,"x") | RENAME title AS t | HIGHLIGHT ON t` | same mechanism: the collected predicate still says `title`, the ON field is `t` → no terms for `t` → `null`. Accepted limitation; pin with a test, do not rewrite predicates. |

---

## 4. Implementation steps

Work in this order; each step compiles and has its own tests. If you must split into multiple PRs, split on
these boundaries and add a distinct semantic capability for each independently shipped behavior change.

### Step 1 — Part C capability

File: `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/action/EsqlCapabilities.java`

Keep `HIGHLIGHT_V6(Build.current().isSnapshot())` unchanged and add:

```java
/**
 * Support for deriving the {@code HIGHLIGHT} query and target fields, including {@code ON *}.
 */
HIGHLIGHT_IMPLICIT_QUERY_AND_FIELDS(Build.current().isSnapshot()),
```

Use the new capability only for Part C coverage:

| File | What to change |
| --- | --- |
| `.../esql/action/EsqlCapabilities.java` | add the enum constant and javadoc; retain `HIGHLIGHT_V6` |
| `.../esql/parser/StatementParserTests.java` | gate only the new Part C methods on `Cap.HIGHLIGHT_IMPLICIT_QUERY_AND_FIELDS` |
| `.../esql/analysis/AnalyzerTests.java` | gate the new Part C methods on `Cap.HIGHLIGHT_IMPLICIT_QUERY_AND_FIELDS` |
| `.../esql/analysis/VerifierTests.java` | gate only the new Part C methods on `Cap.HIGHLIGHT_IMPLICIT_QUERY_AND_FIELDS` |
| `x-pack/plugin/esql/qa/testFixtures/src/main/resources/highlight.csv-spec` | use `required_capability: highlight_implicit_query_and_fields` only in the appended Part C section |

Do not change existing `HIGHLIGHT_V6` or `required_capability: highlight_v6` references.

### Step 2 — grammar

File: `x-pack/plugin/esql/src/main/antlr/EsqlBaseParser.g4` — apply the rule from §3.1 (make
`queryExpression` optional, wrap `ON ...` in `(...)?`, switch to `qualifiedNamePatterns`). Then:

```bash
./gradlew :x-pack:plugin:esql:regen
```

This rewrites the generated `EsqlBaseParser*.java`/listener/visitor files. Commit the generated diff as-is;
never hand-edit those files. If `regen` produces unrelated churn, your ANTLR change is wrong — re-check.

### Step 3 — `Highlight` logical node

File: `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/logical/Highlight.java`

1. Add three `private final boolean` fields (constructor parameters after `query`, in this order):
   - `implicitQuery` — true once the analyzer filled the query from upstream WHEREs; disables the
     `requireOnField` check.
   - `derivedFields` — true when the user did not write a concrete `ON` list (bare form or `ON *`); set
     by the **parser**, immutable afterwards. Also disables the `requireOnField` check — that check
     compares two user-written lists, and against a derived list it produces errors citing an ON clause
     the user never typed (§3.2).
   - `derivationPending` — true from parse time whenever the query or the ON clause was absent; cleared
     (exactly once) by the analyzer rule. **Why it must exist:** analyzer rules skip resolved plans
     (`AnalyzerRules.ParameterizedAnalyzerRule.apply` checks `skipResolved() && t.resolved()`, and
     `skipResolved()` defaults to true). Without this flag a bare `HIGHLIGHT` — null query, empty fields —
     reports `resolved() == true` straight out of the parser, `ResolveRefs` never fires for it, and the
     feature's two headline forms fail with "requires a query". The flag makes the node report itself
     unresolved until derivation has been *attempted*; clearing it unconditionally afterwards (even when
     nothing was collectible) lets the plan settle so post-analysis verification can emit the intended
     message instead of a generic unresolved-plan error.

   Thread both through `replaceChild`, `withOptions`, `withGeneratedNames`, `info()` (the
   `NodeInfo.create` call **must** list parameters in constructor order), `equals`, `hashCode`.
2. Extend `expressionsResolved()`:
   ```java
   @Override
   public boolean expressionsResolved() {
       if (derivationPending) {
           return false; // ResolveRefs must still visit this node to derive the query/fields (see field javadoc)
       }
       // ... existing body unchanged ...
   }
   ```
3. Serialization: read/write all three booleans right after `query` (`in.readBoolean()` ×3 /
   `out.writeBoolean(...)` ×3, same order as the constructor). `derivationPending` can never be `true` on
   the wire — the parser sets it, the analyzer clears it, and only analyzed plans are serialized. We
   round-trip it anyway (decided, don't relitigate in review): treating it as transient would force the
   serialization tests to special-case it and would make `equals` asymmetric around a round trip. Preempt
   the inevitable reviewer question with one javadoc line on the field: *"Analysis-time only; always false
   in serialized (post-analysis) plans."* **Explicit deviation from the AGENTS.md
   backwards-compatibility rule** ("for changes to a Writeable, add a new TransportVersion"): no new
   `TransportVersion` is added here, because (a) HIGHLIGHT is snapshot-only and has never shipped in a
   release, so no released node can ever receive this node, and (b) the Step 1 capability rename prevents
   mixed-version CI clusters from planning any HIGHLIGHT across the version boundary. State this deviation
   and both reasons in the PR description; if a reviewer still wants a TransportVersion, follow
   `docs/internal/Versioning.md` rather than hand-editing resource files.
4. Add the copy-method the analyzer rule uses. Do **not** name it `resolved(...)` — that overloads
   `LogicalPlan.resolved()`, the no-arg resolution predicate, and reads terribly:
   ```java
   /** Copy with the analyzer's derivation results applied; clears {@code derivationPending}. */
   public Highlight withResolved(Expression newQuery, boolean newImplicitQuery,
                                 List<NamedExpression> newFields, List<Attribute> newGeneratedFields) {
       return new Highlight(
           source(), child(), prefix, newQuery, newImplicitQuery, derivedFields, false, newFields, options, newGeneratedFields
       );
   }
   ```
   (`derivedFields` is parser-set state and passes through unchanged; only `derivationPending` is cleared.)
5. In `postAnalysisVerification(Failures failures)` add, before the option checks (exactly one of the two
   can fire — keep the `else if`):
   ```java
   if (query == null) {
       failures.add(fail(this, "HIGHLIGHT requires a query or a preceding full-text WHERE (MATCH, MATCH_PHRASE, QSTR or KQL)"));
   } else if (fields.isEmpty()) {
       failures.add(fail(this, "HIGHLIGHT found no text or keyword fields to highlight; add an explicit ON clause"));
   }
   ```
6. In `verifyQuery(...)`, extend the early-return guard so an empty field list can't produce a *second*
   failure on top of the one above (translation over zero fields is at best meaningless and at worst
   throws, and `VerifierTests.error(...)` asserts the combined failure text):
   ```java
   if (query == null || query.resolved() == false || fields.isEmpty()) {
       return;
   }
   ```
   and pass the strictness flag through:
   `HighlightQueryBuilders.verify(query, fieldNames, analyzer, implicitQuery == false && derivedFields == false /* enforceOnFields */)`.
   Both flags matter: `implicitQuery` covers the derived-query case, `derivedFields` covers the
   explicit-query-with-derived-columns case (§3.2's `AND NOT` example). `implicitQuery == false` alone
   reintroduces the regression.

Update `HighlightQueryBuilders.verify` and `verifyQueryStructure` to accept `boolean enforceOnFields` and
only call `requireOnField(...)` when it is true.

Also add to `HighlightQueryBuilders` the implicit-shape check. It is **narrower on purpose** than
`verifyQueryStructure` (no `Not`, no `Literal`) — keep them adjacent with a cross-referencing comment
instead of merging them:

```java
/**
 * True when {@code expr} is a positive full-text combination HIGHLIGHT can collect as an implicit query
 * from a WHERE clause. Deliberately narrower than {@link #verifyQueryStructure}: negations match by
 * absence and contribute nothing highlightable, and a bare string literal in a WHERE is not a predicate.
 */
public static boolean isSupportedImplicitPredicate(Expression expr) {
    return switch (expr) {
        case Match match -> true;               // MatchOperator extends Match, so ":" is covered
        case MatchPhrase matchPhrase -> true;
        case QueryString queryString -> true;
        case Kql kql -> true;
        case And and -> isSupportedImplicitPredicate(and.left()) && isSupportedImplicitPredicate(and.right());
        case Or or -> isSupportedImplicitPredicate(or.left()) && isSupportedImplicitPredicate(or.right());
        // default is the negative answer over an open Expression domain, not a "forgotten case" —
        // see the AGENTS.md switch rule; enumerating every Expression subtype here is neither
        // possible nor desirable.
        default -> false;
    };
}
```

Use **named** pattern variables even where unused, as above. Unnamed patterns (`case Match _`) are JEP 456,
final only in Java 22 — but Elasticsearch compiles main sources with `--release 21`
(`build-tools-internal/src/main/resources/minimumCompilerVersion` and `minimumRuntimeVersion` are both
`21`, and `ElasticsearchJavaBasePlugin` sets the compiler `release` from that), so `case Match _` **does
not compile** regardless of the JDK you run Gradle with. The named-but-unused style matches the adjacent
`verifyQueryStructure` (`case Kql kql -> { }`).

`MapperUtils`' `Highlight → HighlightExec` mapping and `LocalExecutionPlanner.planHighlight` need **no**
changes (post-analysis the query is always non-null and fields non-empty, or verification already failed).
`HighlightExec` does not need either flag.

### Step 4 — parser builder

File: `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/parser/LogicalPlanBuilder.java`,
method `visitHighlightCommand` (delete the two Part C TODO comments there — this is that work).

```java
@Override
public PlanFactory visitHighlightCommand(EsqlBaseParser.HighlightCommandContext ctx) {
    Source source = source(ctx);
    final String prefix = highlightPrefix(ctx);
    // Optional; when absent the analyzer derives the query from upstream full-text WHERE clauses.
    Expression query = ctx.queryExpression == null ? null : expression(ctx.queryExpression);
    // Optional; empty means "derive from the query". A sole [*] means "all text/keyword fields".
    List<NamedExpression> fields = ctx.highlightFields == null
        ? List.of()
        : visitQualifiedNamePatterns(ctx.highlightFields, ne -> {
            if (ne instanceof UnresolvedNamePattern up) {
                throw new ParsingException(ne.source(), "Invalid pattern [{}] in HIGHLIGHT ON, expected field names or [*]", up.pattern());
            }
        });
    if (fields.size() > 1 && fields.stream().anyMatch(f -> f instanceof UnresolvedStar)) {
        throw new ParsingException(source, "HIGHLIGHT ON [*] cannot be combined with other fields");
    }
    // "The user did not write a concrete ON list" — the bare and [*] forms. Doubles as: generated
    // attributes are deferred to analysis, and requireOnField must not be enforced (see Highlight javadoc).
    boolean derivedFields = fields.isEmpty() || fields.getFirst() instanceof UnresolvedStar;
    List<Attribute> generatedFields = derivedFields ? List.of() : Highlight.generatedAttributesFor(source, prefix, fields);
    // derivationPending keeps the node "unresolved" until ResolveRefs has attempted query/field derivation.
    boolean derivationPending = query == null || fields.isEmpty();
    List<NamedExpression> finalFields = fields;
    return p -> applyHighlightOptions(
        new Highlight(source, p, prefix, query, false, derivedFields, derivationPending, finalFields, null, generatedFields),
        ctx.commandNamedParameters()
    );
}
```

Note the `derivationPending` condition is `query == null || fields.isEmpty()` and **deliberately does not
include the star case**: `HIGHLIGHT <query> ON *` has a non-empty field list whose sole `UnresolvedStar`
already reports `resolved() == false`, which keeps the node unresolved and gets `ResolveRefs` to fire
without the flag. Do not "correct" the condition to cover it — the flag exists only for the two shapes
that would otherwise look fully resolved (absent query, absent ON).

Notes: `visitQualifiedNamePatterns(ctx, Consumer<NamedExpression>)` already exists in `ExpressionBuilder`
(KEEP/DROP use it) and produces `UnresolvedAttribute` / `UnresolvedStar` / `UnresolvedNamePattern` entries;
`UnresolvedNamePattern` has a `pattern()` accessor. Imports:
`org.elasticsearch.xpack.esql.core.expression.UnresolvedStar`,
`org.elasticsearch.xpack.esql.expression.UnresolvedNamePattern`.

### Step 5 — analyzer resolution (the core of Part C)

File: `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/analysis/Analyzer.java`, class
`ResolveRefs`. Add to the `switch (plan)` (next to `case Rerank r -> resolveRerank(...)`):

```java
case Highlight h -> resolveHighlight(h, childrenOutput);
```

**Placement of helpers (decided, not a choice):** the plan-walking parts (`resolveHighlight`,
`collectImplicitQuery`, `isDocLevelTransparent`) live in `ResolveRefs`; the query-shape and field-derivation
parts (`isSupportedImplicitPredicate` from Step 3, plus `deriveFields`, `allHighlightableFields`,
`collectQueryFieldNames`) live in `HighlightQueryBuilders` as **public** statics — `Analyzer` is in package
`...esql.analysis` and `HighlightQueryBuilders` in `...esql.planner`, so package-private does not work
across them. The private `queryStringDefaultField` already in `HighlightQueryBuilders` stays private;
`collectQueryFieldNames` calls it from inside the same class.

```java
private LogicalPlan resolveHighlight(Highlight highlight, List<Attribute> childrenOutput) {
    // 1. Ordinary attribute resolution for explicit ON fields and the explicit query — this is what the
    //    default branch used to do for Highlight.
    Highlight h = (Highlight) highlight.transformExpressionsOnly(
        UnresolvedAttribute.class, ua -> maybeResolveAttribute(ua, childrenOutput)
    );

    // 2. Implicit query: only when the user wrote no query and we haven't derived one yet.
    Expression query = h.query();
    boolean implicit = h.implicitQuery();
    if (query == null && h.derivationPending()) {
        query = collectImplicitQuery(h.child());
        implicit = query != null;
    }

    // 3. Column set: expand a sole [*], or derive from the query in the bare form. Runs at most once —
    //    afterwards fields is non-empty (or derivation has been attempted and derivationPending cleared),
    //    which is what makes the rule converge.
    List<NamedExpression> fields = h.fields();
    List<Attribute> generated = h.generatedAttributes();
    boolean star = fields.size() == 1 && fields.getFirst() instanceof UnresolvedStar;
    if (star || (fields.isEmpty() && query != null && query.resolved())) {
        List<NamedExpression> derived = star
            ? HighlightQueryBuilders.allHighlightableFields(childrenOutput)
            : HighlightQueryBuilders.deriveFields(query, childrenOutput);
        if (derived.isEmpty() == false) {
            fields = derived;
            generated = Highlight.generatedAttributesFor(h.source(), h.prefix(), fields);
        } else if (star) {
            fields = List.of(); // let postAnalysisVerification report "no text or keyword fields"
        }
    }

    // Clear derivationPending exactly once, even when nothing was derivable, so the node settles and the
    // verifier can produce the intended error instead of a generic unresolved-plan failure.
    if (h.derivationPending() || query != h.query() || fields != h.fields()) {
        return h.withResolved(query, implicit, fields, generated);
    }
    return h;
}
```

The walk (private, next to `resolveHighlight`):

```java
/**
 * Design doc §5: collect searchable predicates from every WHERE in the same doc-level slice, OR them.
 * The walk moves DOWN the child chain (children are upstream commands) and stops at any node that
 * aggregates or replaces rows. Predicates are collected as-is: a predicate whose field was later dropped
 * or renamed translates to an unmapped-field match-none query, which is the documented null-column
 * behavior — do not filter on attribute liveness here (AttributeSet is NameId-based and would silently
 * drop predicates across RENAME/MV_EXPAND).
 * <p>
 * Combined with plain Predicates.combineOr: the resulting Or carries the first conjunct's Source, so
 * PROFILE descriptions of a multi-conjunct implicit query show only that conjunct's text (§3.2 accepted
 * cosmetic). Do NOT swap in a hand-built Or with a synthesized Source to "fix" that — synthetic source
 * text is a test-only mechanism in this codebase (Source.synthetic is deprecated for exactly this
 * reason) and BinaryLogic.writeTo discards the Source anyway if the expression is ever serialized.
 */
private static Expression collectImplicitQuery(LogicalPlan child) {
    List<Expression> searchable = new ArrayList<>();
    LogicalPlan current = child;
    while (current instanceof UnaryPlan unary) {
        if (current instanceof Filter filter) {
            for (Expression conjunct : Predicates.splitAnd(filter.condition())) {
                if (HighlightQueryBuilders.isSupportedImplicitPredicate(conjunct)) {
                    searchable.add(conjunct);
                }
            }
        } else if (isDocLevelTransparent(current) == false) {
            break;
        }
        current = unary.child();
    }
    return searchable.isEmpty() ? null : Predicates.combineOr(searchable);
}
```

```java

/** Nodes that keep each row bound 1:1 to a source doc (design doc §5 "doc-level slice"). */
private static boolean isDocLevelTransparent(LogicalPlan plan) {
    return plan instanceof Filter
        || plan instanceof Eval
        || plan instanceof Project        // Keep extends Project; resolved RENAME/KEEP become projections
        || plan instanceof Rename
        || plan instanceof Drop
        || plan instanceof RegexExtract   // Dissect and Grok
        || plan instanceof Limit
        || plan instanceof OrderBy
        || plan instanceof TopN
        || plan instanceof MvExpand
        || plan instanceof Highlight;
}
```

The derivation helpers (public statics on `HighlightQueryBuilders`):

```java
/**
 * ON * expansion: every text/keyword column of the incoming rows, deduped by name (last wins).
 * Includes multi-field sub-fields (e.g. author.keyword) and semantic_text columns (which ES|QL surfaces
 * as TEXT) — deliberate Query DSL parity, see the implementation guide §3.3. MetadataAttributes are
 * excluded; anything non-string (including UnsupportedAttribute, whose type is UNSUPPORTED) already
 * fails the isString filter.
 */
public static List<NamedExpression> allHighlightableFields(List<Attribute> childrenOutput) {
    Map<String, NamedExpression> byName = new LinkedHashMap<>();
    for (Attribute attr : childrenOutput) {
        if (DataType.isString(attr.dataType()) && attr instanceof MetadataAttribute == false) {
            byName.remove(attr.name()); // re-insert so the LAST occurrence wins, matching shadowing order
            byName.put(attr.name(), attr);
        }
    }
    return List.copyOf(byName.values());
}

/**
 * Design doc §5.1: fields the query mentions; falls back to all text/keyword fields when the query has
 * no derivable field references (string literal, KQL, QSTR without a default_field option).
 */
public static List<NamedExpression> deriveFields(Expression query, List<Attribute> childrenOutput) {
    Set<String> names = new LinkedHashSet<>();
    boolean derivable = collectQueryFieldNames(query, names); // false means "no specific fields"
    if (derivable == false) {
        return allHighlightableFields(childrenOutput);
    }
    List<NamedExpression> result = new ArrayList<>(names.size());
    for (String name : names) {
        for (Attribute attr : childrenOutput) {   // skip silently when missing or not text/keyword
            if (attr.name().equals(name) && DataType.isString(attr.dataType())) {
                result.add(attr);
                break;
            }
        }
    }
    return result;
}
```

`collectQueryFieldNames` walks the query per §3.3: `Match`/`MatchPhrase` add
`Expressions.name(f.field())`; `QueryString` reads the existing private `queryStringDefaultField(...)`
(a `"*"` or wildcard-bearing value, or no option at all → return `false`); `Kql`/`Literal` → return
`false`; `And`/`Or` recurse into both children and AND the results (any "no specific fields" verdict
poisons the whole derivation into the fallback); `Not` → **skip the subtree entirely and return `true`**
— it contributes no names and must not trigger the fallback either, per §3.3 (a pure-negative query then
derives an empty field set and fails verification with the "found no text or keyword fields" message).
Do **not** attempt to read a QSTR `fields` option — it does not exist (§3.3).

**Convergence rules — read twice.** The analyzer runs rules to a fixpoint; a rule that returns a changed
node every iteration hangs analysis (max-iteration failure):

- The rule only fires while the node is unresolved (`derivationPending`, an unresolved star, or unresolved
  attributes); once `withResolved` clears the flag, `ParameterizedAnalyzerRule.apply` skips the node
  entirely on later passes.
- `Highlight.generatedAttributesFor` mints fresh `NameId`s. Call it **only inside the fill branch** — never
  unconditionally — or every iteration produces a different plan.
- When nothing changed and the flag is already clear, return the same instance (`h`), not a copy.
- Clearing `derivationPending` is unconditional after one derivation attempt with resolved children —
  failed derivation must not keep the node unresolved forever (that would surface as a generic
  unresolved-plan error instead of the §3.2/§3.3 messages).

### Step 6 — done-ness sweep

- Delete the two `TODO` comments in `visitHighlightCommand` (that work now exists).
- **`HighlightGenerator`** (`x-pack/plugin/esql/qa/testFixtures/src/main/java/.../generator/command/pipe/HighlightGenerator.java`):
  its class javadoc says *"Queries only reference fields in the `ON` clause, as required by HIGHLIGHT"* —
  that constraint is exactly what Part C removes for implicit queries, and the generator unconditionally
  emits `ON <fields>`. Minimum: fix the javadoc and the `Cap.HIGHLIGHT_V6` gate (Step 1). Better: teach it
  to occasionally emit the bare form / `ON *` after an upstream full-text WHERE, so the random-query suite
  covers the new paths. If you defer the "better" part, leave a TODO referencing the Part C PR.
- `./gradlew :x-pack:plugin:esql:spotlessApply` (also prunes/orders imports).
- Docs: no action. The command is dev-only; the generated
  `docs/reference/query-languages/esql/kibana/generated/.../commands/highlight.json` stub is produced by
  DocsV3Support and does not encode the grammar shape. Do not hand-edit anything under `generated/`.

---

## 5. Test plan

Every bullet is required. Follow the style of neighboring tests in each file; use the ES test framework
(`ESTestCase` subclasses already in place). Gate new Part C tests on
`HIGHLIGHT_IMPLICIT_QUERY_AND_FIELDS`; new csv-spec tests use
`required_capability: highlight_implicit_query_and_fields`.

### 5.1 Parser — `StatementParserTests`

Find the existing HIGHLIGHT tests (`grep -n HIGHLIGHT StatementParserTests.java`) and add:

1. `FROM a | HIGHLIGHT` → `Highlight` with `query() == null`, `fields().isEmpty()`,
   `derivationPending() == true`, prefix `highlight_`.
2. `FROM a | HIGHLIGHT ON title, body` (no query) → null query, two unresolved fields, `derivationPending()`.
3. `FROM a | HIGHLIGHT "fox"` (no ON) → literal query, empty fields, `derivationPending()`.
4. `FROM a | HIGHLIGHT ON *` → single `UnresolvedStar` field.
5. Error: `FROM a | HIGHLIGHT ON title*` → `Invalid pattern [title*] in HIGHLIGHT ON, expected field names or [*]`.
6. Error: `FROM a | HIGHLIGHT ON title, *` → `HIGHLIGHT ON [*] cannot be combined with other fields`.

### 5.2 Analyzer — `AnalyzerTests` (`x-pack/plugin/esql/src/test/java/org/elasticsearch/xpack/esql/analysis/`)

Use the existing test mapping/analyzer utilities in that package (see how other command tests call
`AnalyzerTestUtils.analyze(...)`). Assert on the **analyzed plan**:

1. Implicit query materialized: `FROM test | WHERE match(first_name, "x") | HIGHLIGHT ON first_name` →
   `Highlight.query()` is a `Match`, `implicitQuery() == true`, `derivationPending() == false`.
2. Two WHEREs OR-ed: `... WHERE match(a,"x") | WHERE match(b,"y") | HIGHLIGHT ON a` → query is
   `Or(Match, Match)` (structural check only — do not assert `sourceText()`, which is just the first
   conjunct's text per the §3.2 accepted cosmetic).
3. Mixed conjunct: `WHERE match(a,"x") AND salary > 3 | HIGHLIGHT ON a` → query is just the `Match`.
4. Non-searchable OR dropped: `WHERE match(a,"x") OR salary > 3 | HIGHLIGHT ON a` → query null → verification error.
5. Negation excluded: `WHERE match(a,"x") AND NOT match(b,"y") | HIGHLIGHT ON a` → query is just
   `Match(a,"x")`; and `WHERE NOT match(a,"x") | HIGHLIGHT ON a` alone → verification error (nothing collectible).
6. Walk stops at STATS: `WHERE match(a,"x") | STATS c = count(*) BY a | HIGHLIGHT ON a` → verification error.
7. Walk passes EVAL/KEEP/SORT/LIMIT/DISSECT: one test chaining several transparent commands.
8. Bare form derives columns from the query: `WHERE match(first_name,"x") | HIGHLIGHT` → fields =
   `[first_name]`, generated = `[highlight_first_name]` (check `output()` contains it, type KEYWORD).
9. Bare form fallback: `HIGHLIGHT "fox"` on the test index → fields = all text/keyword columns of the mapping.
10. Explicit beats implicit: `WHERE match(a,"x") | HIGHLIGHT match(b,"y") ON b` → query is `Match(b,...)`,
    `implicitQuery() == false`.
11. Mixed positive/negative explicit query, no ON:
    `HIGHLIGHT match(first_name,"x") AND NOT match(last_name,"y")` → analyzes **cleanly** with fields
    `[first_name]` (the `Not` subtree derives nothing, and `requireOnField` is not enforced because
    `derivedFields` is true). This pins the round-3 regression fix — with `implicitQuery == false` alone
    gating the check, this query fails with an error citing an ON list the user never wrote.
12. Dropped-field predicate still collected: `WHERE match(a,"x") | DROP a | HIGHLIGHT ON b` → **no error**;
    `query()` is the `Match` on the dropped field, `implicitQuery() == true`. (Runtime yields nulls — csv
    test 5.5.12 pins that end.) Bare-form variant: `WHERE match(a,"x") | DROP a | HIGHLIGHT` → derivation
    skips the missing name → "found no text or keyword fields" verification error.
13. Fixpoint sanity: analyzing any of the above twice yields equal plans (guards NameId regeneration bugs),
    and every analyzed Highlight has `derivationPending() == false`.

### 5.3 Verifier — `VerifierTests`

Exact-message tests (copy the assertion style of existing HIGHLIGHT verifier tests):

1. Bare HIGHLIGHT with no upstream full-text WHERE →
   `HIGHLIGHT requires a query or a preceding full-text WHERE (MATCH, MATCH_PHRASE, QSTR or KQL)`.
   **The test query must not reference any generated `highlight_*` column** (no `KEEP highlight_title`):
   failed derivation leaves the output without those columns, the verifier's unresolved-attribute sweep
   runs first and bails out, and the test would assert `Unknown column [...]` instead — leaving you
   debugging perfectly-working `derivationPending` wiring. See the §3.2 error-masking caveat.
2. `HIGHLIGHT ON *` when no text/keyword field exists (e.g. `ROW i = 1 | HIGHLIGHT "x" ON *`) →
   `HIGHLIGHT found no text or keyword fields to highlight; add an explicit ON clause` — and **only** that
   failure (the `verifyQuery` guard from Step 3.6 must keep the query check from adding a second one).
3. Bare pure-negative explicit query: `HIGHLIGHT NOT MATCH(title, "x")` (no ON) → same "found no text or
   keyword fields" message (§3.3: `Not` subtrees are pruned from derivation).
4. Explicit-query strictness retained: `HIGHLIGHT MATCH(title,"x") ON body` still fails with
   `HIGHLIGHT query field [title] is not in ON fields [body]`.
5. Explicit `Not` with explicit ON retained: the existing `HIGHLIGHT NOT MATCH(title, "fox") ON title` test
   (~`VerifierTests.java:4694`) still passes unchanged.
6. Implicit-query leniency: `WHERE MATCH(title,"x") | HIGHLIGHT ON body` does **not** fail (columns just
   come out null at runtime).

### 5.4 Serialization — `HighlightSerializationTests`

Add all three new booleans (`implicitQuery`, `derivedFields`, `derivationPending`) to the random instance
generator and to the mutate method (flip each as separate mutation choices). Run:

```bash
./gradlew :x-pack:plugin:esql:test --tests org.elasticsearch.xpack.esql.plan.logical.HighlightSerializationTests
```

### 5.5 End-to-end — `highlight.csv-spec`

Append a clearly-commented Part C section. Model the data-backed tests on the existing `FROM books` block
(around line 1040). Every test: `required_capability: highlight_implicit_query_and_fields`, plus the
capability of any full-text function used in WHERE (copy from neighboring tests, e.g. `match_function`).
Full-text functions in WHERE require a real index — use `FROM books`, not `ROW`. Minimum set, mirroring §3.4:

1. `implicitQueryBareForm` — `FROM books | WHERE MATCH(title, "Return") | HIGHLIGHT | KEEP title, highlight_title | SORT title`.
2. `implicitQueryUntargetedOnFieldIsNull` — same WHERE, `HIGHLIGHT ON description`, assert nulls.
3. `implicitQueryTwoWheresOrTogether` — two full-text WHEREs on different fields, bare HIGHLIGHT, assert both columns and per-field population.
4. `highlightOnStar` — `WHERE MATCH(title, "Return") | HIGHLIGHT ON * | KEEP highlight_*`. This pins the
   expansion, **including** `highlight_author.keyword` (books maps `author` with a `.keyword` sub-field —
   expect it present and null) and the expansion order.
5. `explicitQueryNoOnFallsBackToAllFields` — `HIGHLIGHT "Tolkien"` with no WHERE.
6. `implicitOnSemanticTextViaStar` — `FROM semantic_text | WHERE match(semantic_text_field, "prosper") | HIGHLIGHT ON *`
   → semantic_text columns are included in the expansion (they surface as `text`).

Writing expected outputs: run the suite once, eyeball the actual results for correctness (are the right
terms wrapped in `<em>`? are nulls where §3.4 says?), then paste them in. Never paste output you have not
sanity-checked against the spec.

```bash
# fast local loop (runs csv-spec in-JVM):
./gradlew :x-pack:plugin:esql:test --tests org.elasticsearch.xpack.esql.CsvTests
# full pass incl. cluster-backed run before opening the PR:
./gradlew :x-pack:plugin:esql:internalClusterTest --tests "org.elasticsearch.xpack.esql.CsvIT.*highlight*"
```

### 5.6 Whole-suite gates (run all before opening the PR)

```bash
./gradlew :x-pack:plugin:esql:spotlessJavaCheck
./gradlew :x-pack:plugin:esql:test
./gradlew :x-pack:plugin:esql:internalClusterTest --tests "org.elasticsearch.xpack.esql.CsvIT.*highlight*"
```

If a test class seems to vanish from results rather than fail, check `muted-tests.yml` first (see AGENTS.md,
"Debugging Missing Tests"). Golden/expected-output tests (`HighlightGoldenTests`, optimizer tests) may need
regenerated expectations if node `toString`s changed — follow the regeneration instructions in those test
files' javadoc rather than hand-editing expectations. Golden files never contain the operator's
`queryText` (`HighlightGoldenTests` render physical-plan `toString()`; `HighlightConfig` is built later,
in `LocalExecutionPlanner`) — so nothing golden-related changes for implicit queries. The only place the
first-conjunct-text cosmetic (§3.2) is visible is PROFILE operator descriptions; there is nothing to
assert about it here.

---

## 6. Pitfalls (each of these has bitten someone)

1. **Analyzer rules skip resolved nodes.** `skipResolved()` defaults to true, and a Highlight with a null
   query and empty fields is "resolved" the moment it is parsed. Without `derivationPending` the bare and
   query-only forms silently never reach `resolveHighlight` and die in verification with "requires a
   query". This is a general trap for any plan node whose expressions are all optional.
2. **Analyzer non-convergence.** Any unconditional `generatedAttributesFor` call or unconditional node copy
   in `resolveHighlight` makes analysis loop to max-iterations. The fill branches must be guarded exactly as
   written in Step 5, and the settled path must return the same object.
3. **Do not "sanity-filter" collected predicates by attribute liveness.** `AttributeSet`/`AttributeMap`
   membership is `NameId`-identity based (`AttributeWrapper` uses `semanticEquals`); RENAME and MV_EXPAND
   mint fresh ids, so a `containsAll(references)` guard silently drops predicates and turns the documented
   null-column behavior into a "requires a query" error. Stale field names are already safe: the highlight
   translation context knows only the ON fields, and unknown fields become match-none (null columns).
   Relatedly, `Highlight.computeReferences()` deliberately excludes the query, so stale query attributes
   cannot trip plan-consistency checks either.
4. **Editing generated parser files.** Anything under `.../esql/parser/EsqlBase*` is ANTLR output. Grammar
   changes go in the `.g4` file + `./gradlew :x-pack:plugin:esql:regen`.
5. **`NodeInfo.create` argument order** must match the constructor exactly, or plan-tree transforms will
   silently scramble fields (usually caught by serialization tests, sometimes not).
6. **Wildcard imports / switch defaults / pattern variables.** AGENTS.md: no wildcard imports; in switches
   over known sets, enumerate cases and reserve `default` for genuinely unexpected values.
   `isSupportedImplicitPredicate`'s `default -> false` is a justified exception (open `Expression` domain) —
   keep the inline comment saying so. Use **named** pattern variables even when unused
   (`case Match match ->`): unnamed patterns (`case Match _`, JEP 456) are final only in Java 22, and main
   sources compile with `--release 21`, so `case Match _` fails to compile no matter which JDK runs Gradle.
7. **`ROW` + full-text WHERE doesn't work.** `MATCH`/`QSTR` in WHERE need an index; analyzer/csv tests for
   the implicit path must use an indexed source (`FROM books`, or the AnalyzerTests test index).
8. **The walk direction.** "Upstream WHERE" = *children* of the Highlight node. You walk `child()`
   downward. Do not attempt to walk parents.
9. **Don't relax `requireOnField` for explicit queries.** Only the implicit path skips it. There are
   existing verifier tests pinning the strict message — they must keep passing.
10. **QSTR has no `fields` option.** Its `@MapParam` javadoc claims one, but `ALLOWED_OPTIONS` rejects it —
    derive from `default_field` only. (The doc/implementation mismatch is tracked as a separate issue.)
11. **Capability rename scope.** Seven files (see the Step 1 table), including one under
    `qa/testFixtures/src/main` and one YAML under `x-pack/plugin/src/yamlRestTest` — a stale reference makes
    CI fail with "unknown capability". `sed -i ''` is macOS-only; use `perl -pi` or your editor.
12. **Star dedup order.** Child output can contain shadowed duplicates of a name; keep the last one.
13. **The "requires a query" message is masked by downstream column references.** The verifier's
    unresolved-attribute sweep bails out first, so `FROM books | HIGHLIGHT | KEEP highlight_title` reports
    `Unknown column [highlight_title]`, not the HIGHLIGHT message (§3.2 caveat). Write verifier tests
    without generated-column references (test 5.3.1) and pin the masked shape (test 5.3.2) — do not spend
    time "debugging" `derivationPending` when you hit it.
14. **Do not synthesize `Source` text — it is a test-only mechanism.** `Source.synthetic` is deprecated
    ("can't be correctly deserialized"), `Source.readFrom` reconstructs text by slicing the original query
    string at an offset (mismatched text overruns and throws), and `BinaryLogic.writeTo` is `final` and
    writes `Source.EMPTY`, discarding anything you attach to an `Or`. A hand-built OR chain with a
    synthesized Source *appears* to work only because HIGHLIGHT currently executes on the coordinator and
    never serializes its query expression — Stage 2 plans to change exactly that. Use
    `Predicates.combineOr` and accept the §3.2 PROFILE cosmetic (first conjunct's text).

---

## 7. Review checklist (self-review before requesting human review)

- [ ] Grammar: all four form combinations parse; generated files regenerated, not edited; `EXPLAIN` on each form shows a `HighlightExec`.
- [ ] `git diff` contains no unrelated files; no changes under `compute/` (operator untouched).
- [ ] `derivationPending` exists, is set by the parser for absent query/ON, is cleared exactly once by `resolveHighlight`, and every analyzed plan reports it false (test 5.2.13).
- [ ] `Highlight` serialization: field order in reader/writer matches; `HighlightSerializationTests` covers both new flags; PR text names the explicit AGENTS.md TransportVersion deviation and both reasons (snapshot-only + capability rename).
- [ ] `resolveHighlight` convergence guards present (Pitfalls 1–2); AnalyzerTests double-analyze test passes.
- [ ] Implicit collection: positive-only allowlist (no `Not`), whole-conjunct drop for mixed trees, no attribute-liveness guard, plain `Predicates.combineOr` (no synthesized `Source` — Pitfall 14); STATS/joins/Fork stop the walk (tests 5.2.5–6).
- [ ] Field derivation prunes `Not` subtrees (consistent with §3.2); bare pure-negative explicit query fails with the "found no text or keyword fields" message (test 5.3.4); mixed positive/negative explicit query with no `ON` analyzes cleanly (test 5.2.11b).
- [ ] `requireOnField` enforcement is gated on `implicitQuery == false && derivedFields == false` — not on `implicitQuery` alone (that reintroduces the round-3 regression); `derivedFields` is parser-set and passes through `withResolved` unchanged.
- [ ] Error messages match §3.2/§3.3 strings verbatim; VerifierTests assert them verbatim without referencing generated columns (test 5.3.1), the masked `Unknown column` shape is pinned (test 5.3.2), and the empty-fields case produces exactly one failure.
- [ ] No `case X _ ->` anywhere in the change — main sources compile with `--release 21` (Pitfall 6).
- [ ] Explicit-query behavior is byte-for-byte unchanged: the pre-existing highlight.csv-spec tests pass with only the capability-name diff; the explicit-`Not`-with-ON verifier test passes unchanged.
- [ ] All §3.4 table rows are pinned by a csv-spec test, including the DROP/RENAME null-column rows and the `ON *` sub-field row.
- [ ] `HighlightGenerator` javadoc no longer claims ON-field containment is required; its capability gate is updated.
- [ ] `spotlessJavaCheck` clean; new public methods have "why"-focused javadoc; no comments removed from existing code.
- [ ] Commit messages: 50-char summary, 72-char body wrap, no AI attribution trailers (AGENTS.md rule).

---

## 8. Quick reference

| What | Where |
| --- | --- |
| Grammar | `x-pack/plugin/esql/src/main/antlr/EsqlBaseParser.g4` (`highlightCommand`) |
| Parse → plan | `.../esql/parser/LogicalPlanBuilder.java` (`visitHighlightCommand`) |
| Logical node | `.../esql/plan/logical/Highlight.java`, `HighlightOptions.java` |
| Analysis | `.../esql/analysis/Analyzer.java` (`ResolveRefs`), `.../esql/analysis/AnalyzerRules.java` (`skipResolved`) |
| Query shapes & translation | `.../esql/planner/HighlightQueryBuilders.java`, `.../esql/planner/RuntimeSearchExecutionContext.java` |
| Predicate utils | `.../esql/expression/predicate/Predicates.java` (`splitAnd`, `combineOr`) |
| Full-text functions | `.../esql/expression/function/fulltext/{Match,MatchPhrase,QueryString,Kql,MatchOperator}.java` |
| Physical mapping | `.../esql/planner/mapper/MapperUtils.java`, `.../esql/planner/LocalExecutionPlanner.java` (`planHighlight`) |
| Operator (do not touch) | `x-pack/plugin/esql/compute/src/main/java/org/elasticsearch/compute/operator/HighlightOperator.java` |
| Capability | `.../esql/action/EsqlCapabilities.java` (`HIGHLIGHT_V6` plus `HIGHLIGHT_IMPLICIT_QUERY_AND_FIELDS`; see Step 1) |
| csv-spec | `x-pack/plugin/esql/qa/testFixtures/src/main/resources/highlight.csv-spec` |
| Books mapping (sub-field pin) | `x-pack/plugin/esql/qa/testFixtures/src/main/resources/index/mappings/mapping-books.json` |
| Design doc | "Highlighting in ES|QL – Plan", §3.1 (forms), §5 (query source), §5.1 (column derivation) |

Commands:

```bash
./gradlew :x-pack:plugin:esql:regen                 # after grammar edits
./gradlew :x-pack:plugin:esql:spotlessApply         # format + import cleanup
./gradlew :x-pack:plugin:esql:test --tests org.elasticsearch.xpack.esql.parser.StatementParserTests
./gradlew :x-pack:plugin:esql:test --tests org.elasticsearch.xpack.esql.analysis.AnalyzerTests
./gradlew :x-pack:plugin:esql:test --tests org.elasticsearch.xpack.esql.analysis.VerifierTests
./gradlew :x-pack:plugin:esql:test --tests org.elasticsearch.xpack.esql.CsvTests
./gradlew :x-pack:plugin:esql:internalClusterTest --tests "org.elasticsearch.xpack.esql.CsvIT.*highlight*"
```

### Decisions already made (do not reopen without team sign-off)

1. **RENAME/DROP between WHERE and HIGHLIGHT** yields null columns via the unmapped-field match-none path;
   predicates are never rewritten and never liveness-filtered. Pinned by tests 5.5.11–12.
2. **`ON *` includes multi-field sub-fields, `semantic_text` (as TEXT), and columns synthesized by earlier
   commands** — Query DSL `"*"` parity. Pinned by tests 5.5.6 and 5.5.13.
3. **Negations contribute nothing anywhere**: implicit collection drops any conjunct containing `Not`, and
   field derivation prunes `Not` subtrees (a pure-negative bare query is an error steering the user to an
   explicit `ON`). Explicit queries keep supporting `Not`. Pinned by tests 5.2.5, 5.3.4 and 5.3.6.
4. **No new TransportVersion** for the `Highlight` wire-format change — explicit, documented deviation from
   the AGENTS.md rule (snapshot-only feature + capability rename). Named in the PR description.
5. **All three booleans are serialized** (`implicitQuery`, `derivedFields`, `derivationPending`) even
   though `derivationPending` is always false post-analysis (keeps round-trip equality and the
   serialization tests uniform); the field javadocs say so.
6. **The implicit query's combined `Or` keeps `combineOr`'s first-conjunct `Source`.** PROFILE operator
   descriptions of multi-conjunct implicit queries therefore show only the first conjunct's text — a
   documented cosmetic (§3.2). Synthesizing `Source` text is off the table (test-only mechanism,
   discarded by `BinaryLogic` on serialization); the honest fix is a display string threaded through
   `Highlight`/`HighlightExec` into `HighlightConfig`, deferred as a follow-up.
7. **`requireOnField` compares user-written lists only**: enforced iff the query is explicit AND the `ON`
   list was typed by the user (`derivedFields == false`). Derived field sets (bare form, `ON *`) never
   enforce it — that is what makes `MATCH(a,..) AND NOT MATCH(b,..)` with no `ON` legal. Pinned by tests
   5.2.11b and 5.3.5.
8. **The masked "requires a query" error is accepted for Part C** (§3.2 caveat): when the user references a
   generated column after failed derivation, they see `Unknown column [...]`. Pinned by test 5.3.2;
   improving it (placeholder generated attributes) is a follow-up product decision, not part of this change.
