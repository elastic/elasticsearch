# Plan: Teach generative tests that MATCH / MATCH_PHRASE work on expressions

Status: draft, reviewed against product + generator code (Sep 2026). Call path, both gates, emitted shapes, `indexMapped` tracking, and the product runtime-search rules (MATCH/MATCH_PHRASE `isRuntimeSearch`, options-on-non-TEXT rejection, query-value type check, INLINE STATS allowed for MATCH but not KQL/QSTR, ROW/MV_EXPAND behaviour) all verified accurate. Corrections folded in below are marked inline. Do not implement until sign-off.
Out of scope: HIGHLIGHT (`HighlightGenerator.buildQuery` already broadens MATCH to text-or-mapped fields — it is the working precedent for this change; leave it alone here, adopt shared helpers in a follow-up).

## Problem

`MATCH`, the `:` operator, and `MATCH_PHRASE` can search columns that are **not** Lucene-mapped index fields: EVAL / GROK / DISSECT / RENAME / STATS outputs, and other expressions. The search then runs row-by-row (`isRuntimeSearch()`), not as a pushed-down Lucene query.

The ES|QL generative tests (`GenerativeIT` and friends) still generate `match` / `match_phrase` / `:` only against `Column.indexMapped() == true`. Random pipelines therefore never exercise the runtime evaluators. The only match clauses they emit are the old Lucene-pushdown shape.

That gap is in the **WHERE** path (`WhereGenerator` → `booleanExpression` → `FullTextFunctionGenerator.fullTextFunction`). FORK branch WHEREs use the same helper.

## Product vs generator (today)

| Target | Product | Generator |
|---|---|---|
| Mapped keyword/text/numeric/date/boolean | `MATCH` / `:` pushed to Lucene | Generated |
| Mapped keyword/text | `MATCH_PHRASE` pushed to Lucene | Generated |
| EVAL / GROK / DISSECT / RENAME **keyword or text** | Runtime search | **Not generated** |
| Runtime **text** + options (`slop`, `fuzziness`, …) | Allowed | **Not generated** |
| Runtime **keyword** + options | Rejected (`non-TEXT` error) | N/A (never tried on purpose) |
| Runtime numeric/date/boolean + string query | Rejected (query type mismatch) | N/A |
| `mv_expand <same field>` then match that field | Runtime search, legal | Forbidden twice (placement + `indexMapped=false`) |
| After INLINE STATS | `MATCH` / `MATCH_PHRASE` / `:` legal | Forbidden (same list as STATS) |
| After STATS / LIMIT / DEDUP / FORK | Still illegal | STATS/LIMIT/DEDUP skipped; FORK still emitted and swallowed as allowed error |
| ROW source | Runtime search legal | `isFullTextAllowed` requires FROM |

Verifier coverage already exists, though the test name now lags the behaviour: `VerifierTests.testFullTextFunctionsRejectEvalColumns` actually *asserts acceptance* of runtime search on EVAL columns (the "Reject" name is stale). See also `testFieldBasedFullTextFunctions`, `testFullTextFunctionsRejectRenamedNonIndexFields`, and `testFullTextFunctionsRuntimeAnalyzerOptionOnNonTextExpression` (the last pins the options-on-non-TEXT rejection). Csv-spec / IT cover the evaluators (`MatchRuntimeSearchEvaluatorTests`, `MatchPhraseRuntimeSearchEvaluatorTests`). Generative tests do not.

## Findings

### Call path

`WhereGenerator.randomExpression` → `EsqlQueryGenerator.booleanExpression` cases 11–12 → `FunctionGenerator.fullTextFunction` → `FullTextFunctionGenerator.fullTextFunction`.

That is the only non-HIGHLIGHT emission site. STATS does not emit them. `GenerativeFunctionCatalog.EXCLUDED_FUNCTIONS` correctly keeps them out of composite EVAL (they are not general EVAL scalars).

### Two independent gates, both mapped-field-era

1. **Placement** (`isFullTextAllowed`): source must be FROM; pipeline must not contain LIMIT, LIMIT BY, STATS, INLINE STATS, CHANGE_POINT, MV_EXPAND, or DEDUP.
2. **Field origin** (`indexFieldColumns`): `columns.stream().filter(Column::indexMapped)`.

If (2) is empty, generation does **not** fall through to “match on a computed column”. It either emits `qstr`/`kql` (only after FROM/WHERE/SORT) or returns `null`. After EVAL/GROK/DISSECT/RENAME, `qstr`/`kql` are already illegal, so missing mapped fields means no full-text clause at all.

`matchFunction` / `matchPhraseFunction` do not inspect `indexMapped`. They only pick a type. The mapped-field rule lives entirely in the caller.

`Column` javadoc still says match / match_phrase / `:` require index-mapped fields. `updateIndexMapped` in `GenerativeRestTest` exists to keep that filter true: EVAL/GROK/DISSECT/ENRICH/STATS created columns, MV_EXPAND, and RENAME targets are marked `indexMapped=false`.

### What the generator actually emits (unsafe to reuse blindly on runtime columns)

`match(...)` / `field : "..."`:

- Types: keyword, text, boolean, date, datetime, double, integer, ip, long, unsigned_long, version.
- Query: always a string word (`"fox"`, `"world"`, …).
- Options (~20%): `operator`, `fuzziness`, `lenient`, `boost`, `zero_terms_query`. Never `analyzer`.

`match_phrase(...)`:

- Types: keyword, text only.
- Query: always `"word word"`.
- Options (~20%): `slop`, `boost`, `zero_terms_query`.

That shape is what **lenient Lucene match** can tolerate on a mapped numeric/date/boolean. Runtime search cannot:

- Runtime non-text `MATCH` needs a query value convertible to the column type. A string word on an EVAL `integer` fails: `[MATCH] query value [...] does not match the type ([integer]) of non-index-mapped field`.
- Runtime options are allowed only on **text**. Keyword/boolean/numeric + `{...}` fails: `Options are not supported for [MATCH|MATCH_PHRASE] function call on non-index-mapped, non-TEXT field`.

EVAL in this generator almost never produces `text`. `concat` / `substring` / GROK / DISSECT / `to_string` / `to_lower` come back as **keyword**. `to_text` is not in `TypeConversionFunctionGenerator`. So the realistic runtime columns are keyword → exact match, **no options**.

### Extra gates that hide even legal cases

- **RENAME** forces the new name `indexMapped=false` so `match(..., {options})` is not generated on a `ReferenceAttribute`. Product accepts `match(new_name, "…")` as runtime. If Phase 1 allows runtime keyword/text, RENAME starts working without changing `handleRenameIndexMapped`; keep the flag false so options stay gated on `text || indexMapped`.
- **MV_EXPAND** is gated twice, and the wrong way: no full-text after *any* expand, and the expanded column is marked non-mapped. Product: expand a *different* field then match `title` still fails; expand `title` then `match(title, …)` **works** (runtime). That is the one MV_EXPAND case that became legal.
- **INLINE STATS** is on the same forbidden list as STATS. Product allows `MATCH` / `MATCH_PHRASE` / `:` after INLINE STATS (rows are not collapsed). KQL/QSTR stay forbidden.
- **`SET unmapped_fields="nullify"`**: `indexFieldColumns` returns null, so match is not generated. Those columns are a runtime-search case (`isPotentiallyUnmapped()`).
- **ROW**: `isFullTextAllowed` requires FROM. Product accepts runtime match on ROW.

### Allowed errors are a bandage, not coverage

`GenerativeRestTest.ALLOWED_ERRORS` already contains the runtime type-mismatch and options-on-non-text patterns, commented as “need to refine MATCH / MATCH_PHRASE function generation”. `isFieldFullTextError` still treats “not a field from an index mapping” as allowed when the schema says non-mapped. For MATCH/MATCH_PHRASE that message is largely obsolete (`fieldVerifier` returns early when `isRuntimeSearch()` is true); remaining users are things like KNN / federated sources.

Do not delete those patterns in Phase 1. `indexMapped` tracking can still be wrong (subquery, FORK, name collisions). They are a safety net, not a substitute for generating valid runtime clauses.

## Suggested fix

Do not invent typed numeric/date/boolean query literals. Do not teach EVAL to emit `to_text`. Cover the runtime path that the generator can already produce: **keyword and text expressions**, with options only where the product allows them.

### Phase 1 — WHERE generation (this change)

Edit `FullTextFunctionGenerator` only (plus javadoc on `Column`). Leave HIGHLIGHT, placement lists, RENAME flag propagation, and `ALLOWED_ERRORS` alone.

Candidate columns for `match` / `:`:

```
type in MATCH_FIELD_TYPES AND (indexMapped OR type in {keyword, text})
```

Candidate columns for `match_phrase`:

```
type in {keyword, text}
```

regardless of `indexMapped`.

Pick a `Column`, then decide options from that column:

| | no options | with options |
|---|---|---|
| mapped, any currently allowed type | yes | yes |
| runtime **text** | yes | yes |
| runtime **keyword** | yes (`:` and `match`/`match_phrase` without `{...}`) | **no** |

Implementation sketch: stop passing a pre-filtered `indexColumns` list into `matchFunction` / `matchPhraseFunction`. Build the candidate list from the **full `columns`** list using the rules above, pick the `Column` first (not just the name via `randomName`), then call `maybeOptions` only when `column.indexMapped() || "text".equals(column.type())`. The `:` operator already has no options; keep preferring it half the time. `matchFunction` still has to reproduce `randomName`'s quoting (`needsQuoting`/`quote`) once it formats the picked column's name.

**Precedent — HIGHLIGHT already does most of this.** `HighlightGenerator.buildQuery` already builds `matchFields = functionFields.filter(c -> c.type().equals("text") || indexMappedFields.contains(c))` and calls `matchFunction(matchFields)` / `matchPhraseFunction(indexMappedFields)`. So the "broaden match candidates to text-or-mapped" idea is battle-tested. Two differences matter:
- HIGHLIGHT's `matchFields` is `text || indexMapped` — it deliberately **excludes runtime keyword**, because today's `matchFunction(list)` attaches options unconditionally and options on a runtime keyword are illegal. Since the *primary* new coverage here is exactly runtime keyword (`concat`/GROK/DISSECT → keyword, exact match, no options), copying HIGHLIGHT's filter verbatim would miss the main case. The per-column options gate above is therefore **required**, not optional.
- `matchFunction` / `matchPhraseFunction` are shared with HIGHLIGHT. Moving the options gate *inside* them is backward-compatible: HIGHLIGHT only ever passes text-or-mapped columns, which stay options-eligible under `text || indexMapped`. Prefer extracting the candidate filter + `optionsAllowed(Column)` predicate as small helpers in `FullTextFunctionGenerator` so both the WHERE path (now) and HIGHLIGHT (follow-up) can share them and a unit test can assert them directly.

**Gate carefully — `indexFieldColumns` returns null in three cases, not two.** It is null when (a) `previousCommands` is empty, (b) the source is not FROM or `unmapped_fields="nullify"` is set, **and (c) the FROM source has zero index-mapped columns left** (e.g. `FROM x | EVAL c = concat(a,b) | KEEP c`). The common target case — computed columns matched while the original mapped fields *survive* — is **not** null, so gating on `indexFieldColumns != null` and drawing candidates from the full `columns` list already covers it. But case (c) is an all-runtime pipeline the plan wants to reach, and "skip when null" silently drops it. Two options:
- **Preferred:** add a dedicated placement predicate (`FROM source && !nullify`, independent of whether any mapped column survives) and use it as the gate; build candidates from full `columns`. This also covers case (c) for free and removes the null conflation.
- **Acceptable:** keep gating on `indexFieldColumns != null` but state explicitly that all-mapped-columns-removed pipelines (case c) are a deferred Phase-1 limitation, not just nullify/non-FROM.

Either way, keep skipping under `unmapped_fields="nullify"` / non-FROM so nullify columns are never treated as mapped for options.

Keep the FROM-only placement gate. Keep STATS / LIMIT / DEDUP / CHANGE_POINT / MV_EXPAND / INLINE STATS forbidden. Keep `qstr`/`kql` on their current allow-list.

Update `Column` and `fullTextFunction` javadoc so they no longer say match requires an index-mapped field.

### Phase 1 tests

There is no `FullTextFunctionGenerator` unit test today. Generator unit tests already live in testFixtures (`GenerativeFunctionCatalogTests`, `CompositeFunctionGeneratorTests`, `SpecialFunctionGeneratorRegistryTests` under `qa/testFixtures/src/test/.../generator/`), so add `FullTextFunctionGeneratorTests` there — no need to fall back to `GenerativeRestTestTests`. Assert:

- a non-mapped keyword column is eligible for `match` / `match_phrase` / `:`
- a non-mapped integer column is **not** eligible for `match`
- a non-mapped **text** column is options-eligible; a non-mapped **keyword** column is not (assert the extracted `optionsAllowed(Column)` helper directly rather than seed-hunting over `maybeOptions`' randomness)

Run `GenerativeIT` a few times locally after the change. New failures that are real product bugs get issues + `ALLOWED_ERRORS`. Failures that are just “options on keyword EVAL” mean the options gate is wrong — fix generation, do not add another allowed pattern.

### Follow-ups (not this change)

- Lift MV_EXPAND placement for runtime search on the **expanded** field only.
- Allow `MATCH` / `MATCH_PHRASE` / `:` after INLINE STATS; keep KQL/QSTR forbidden.
- Generate match under `unmapped_fields="nullify"` / `"load"` as runtime search, with the same options gate.
- ROW source once placement no longer requires FROM.
- Typed query literals for runtime numeric/boolean/date (only if we want that coverage; EVAL already produces those types).
- Emit `to_text` in EVAL if we want random options-on-runtime-text; without it, runtime **text** columns will be rare (mostly mapped `text` fields that were renamed).
- HIGHLIGHT `match_phrase` still assumes mapped fields (`buildQuery` passes `indexMappedFields`). Once the shared helpers land, HIGHLIGHT can adopt the same runtime-keyword coverage. Separate change.
- Product-side cleanup (not generator): `FullTextFunction`'s javadoc (~the `isFieldFromFederatedSource` block) still claims `MatchPhrase.isRuntimeSearch()` is `false` and that only `Match` returns early from `fieldVerifier`. That is stale — `MatchPhrase.isRuntimeSearch()` overrides to `true` for non-mapped fields (with its own evaluator + tests). Harmless to the plan, but worth fixing if that file is touched.

## Files (Phase 1)

- `x-pack/plugin/esql/qa/testFixtures/src/main/java/org/elasticsearch/xpack/esql/generator/function/FullTextFunctionGenerator.java` — generation rules
- `x-pack/plugin/esql/qa/testFixtures/src/main/java/org/elasticsearch/xpack/esql/generator/Column.java` — javadoc
- new unit test under `x-pack/plugin/esql/qa/testFixtures/src/test/java/.../generator/`

Do not touch `HighlightGenerator`, `GenerativeRestTest.updateIndexMapped`, or `ALLOWED_ERRORS` unless GenerativeIT shows a new genuine gap.

## Risks

- Opening match to every keyword column makes runtime exact-match common (EVAL concat, GROK, DISSECT). That is intended; it is also a different semantic than Lucene phrase/match. Queries will often not match. That is fine: generative tests care that the engine does not throw.
- `maybeOptions` on a renamed mapped `text` field is legal. On a renamed mapped `keyword` field it is not. The `text || indexMapped` gate is required; `indexMapped` alone is no longer enough because RENAME clears it.
- Lookup-side fields still fail (`non-STANDARD mode [lookup]`). Keep that allowed error.
- Subquery + outer match still has known placement bugs (`isFullTextAfterSubqueryInFromBug`). Unchanged.
