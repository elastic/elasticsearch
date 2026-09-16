# How ES|QL `HIGHLIGHT` gets an analyzer, vs Query DSL highlighting

Investigation notes (Sep 2026). Product docs already state the user-facing limitations; this traces the actual resolution paths.

## Short answer

`HIGHLIGHT` never reads a field mapping. It picks **one named analyzer for the whole command** and uses that same instance to (1) rewrite the highlight query and (2) tokenize every ON-field value into a per-row `MemoryIndex`.

Resolution order:

1. `WITH { "analyzer": "<name>" }` if the user set it.
2. Else, if every named full-text leaf in the highlight query (explicit, or borrowed from `WHERE`) agrees on one `analyzer` option, copy that name into `WITH`.
3. Else `"standard"`, loaded from the node-level `AnalysisRegistry`.

Query DSL highlighting does the opposite: **no `highlight.analyzer` option**, per-field **index-time** analyzers from the mapping, and often **no re-analysis at all** because offsets already live in postings or term vectors.

That mapping-vs-`standard` mismatch is the problem this work is aiming to solve: `HIGHLIGHT` on a mapped field should highlight with the same analyzer the `_search` highlighter would use, so a hit from `WHERE MATCH` is not painted with a different tokenizer.

## ES|QL path

```
parse WITH.analyzer
        │
        ▼
ResolveHighlight.withUniformAnalyzer
  if WITH.analyzer unset:
    copy HighlightSupport.uniformAnalyzerOf(query)   // null if none named, or leaves disagree
        │
        ▼
Highlight.postAnalysisVerification
  requireUniformAnalyzer(query, commandAnalyzerName)
  PlannerUtils.resolveAnalyzer(name | "standard", AnalysisRegistry)
  HighlightQueryBuilders.verify(...)                 // rewrite against a synthetic SearchExecutionContext
        │
        ▼
LocalExecutionPlanner.planHighlight
  HighlightQueryBuilders.translate(query, fields, analyzerName, registry)
        │  same Analyzer instance goes into both:
        │    • Lucene Query (via RuntimeSearchExecutionContext)
        │    • HighlightConfig.analyzer
        ▼
HighlightOperator
  MemoryIndex.addField(..., analyzer.tokenStream(field, text))
  UnifiedHighlighter.builder(searcher, analyzer)     // OffsetSource.POSTINGS on the memory index
```

### 1. Name selection (analysis)

`Highlight.ANALYZER` is a command-level `WITH` option (`Highlight.java`). `HighlightOptions.from` only stores the string; it does not load a Lucene `Analyzer`.

`ResolveHighlight.withUniformAnalyzer` fills `WITH.analyzer` when the user omitted it and the query has a single named leaf analyzer:

- Leaves inspected: `HighlightSupport.analyzerNameOf` switches on `SingleFieldFullTextFunction` (which is `MATCH`, `MATCH_PHRASE`, and also `KNN`), `QSTR`, and `KQL`. The walk itself is over every `FullTextFunction`; a leaf whose type is not in that switch yields `null`. In practice only `MATCH`, `MATCH_PHRASE`, `QSTR` and `KQL` carry an `analyzer` option, but the code's predicate is the supertype, not that list.
- Unlabeled leaves do not constrain the result.
- Disagreement is **not** resolved here; `HighlightSupport.requireUniformAnalyzer` fails later:

  - mixed leaf names → `HIGHLIGHT full-text functions use different analyzers [...]`
  - `WITH` vs a leaf → `HIGHLIGHT WITH analyzer [x] does not match analyzer [y] specified by the query`

Implicit `HIGHLIGHT` (no query) borrows the upstream `WHERE` predicate first (`HighlightSupport.collectImplicitQuery`), then runs the same copy. So

```esql
FROM books
| WHERE MATCH(title, "shadow", { "analyzer": "english" })
| HIGHLIGHT
```

highlights with `english` even though `WITH` was never written. A `WHERE MATCH` **without** an `analyzer` option contributes no name, so step 3 (`standard`) applies — even if the mapping's `search_analyzer` is `english`.

Csv-spec coverage: `highlightUniformAnalyzerFromExplicitMatch` (`ROW` + explicit leaf option), `highlightUniformAnalyzerFromImplicitWhere` (`FROM books` + borrowed `WHERE`), `highlightAnalyzerDefaultMissesStem` (`ROW`, no `analyzer` anywhere → `standard` does not stem).

Note what that last test is and is not: it runs on a `ROW` literal, so it proves only that the **default** is `standard`. It does not exercise a mapped field at all — see [Practical mismatches](#practical-mismatches).

### 2. Name → Lucene `Analyzer` (node registry, not the index)

`PlannerUtils.resolveAnalyzer` calls `AnalysisRegistry.getAnalyzer(name)`:

- Prebuilt analyzers come back as `NamedAnalyzer` with the text-field position increment gap already baked in (`PreBuiltAnalyzerProvider`, and `getAnalyzer` re-applies it via `overridePositionIncrementGap`). `PreBuiltAnalyzers` is a short list: `standard`, `default`, `keyword`, `stop`, `whitespace`, `simple`, `classic`.
- Everything else registered on the node — including the **language analyzers** (`english`, `german`, …) from `CommonAnalysisPlugin#getAnalyzers`, and third-party `AnalysisPlugin#getAnalyzers` — takes the `analyzers.get(name)` branch and comes back as a **bare** Lucene analyzer (e.g. `EnglishAnalyzerProvider.get()` returns a plain `EnglishAnalyzer`). `resolveAnalyzer` wraps those as `NamedAnalyzer(..., TextFieldMapper.Defaults.POSITION_INCREMENT_GAP)` so phrases do not match across multi-value boundaries. Test: `HighlightQueryBuildersTests.testResolvedPluginAnalyzerCarriesTextFieldPositionIncrementGap`.

So `english` — the analyzer most of this document's examples use — is in the second group, not the first. It is resolvable on any node that loads `analysis-common`, but the gap comes from `resolveAnalyzer`, not the registry.

Unknown name → `[name] is not a registered analyzer`. No registry → cannot resolve.

**Index-settings analyzers are invisible here.** `AnalysisRegistry` is the node catalog. Custom analyzers defined under `index.analysis` exist only on `IndexAnalyzers` / `SearchExecutionContext.getIndexAnalyzers()`. Product docs: *"The `analyzer` option only supports built-in and node-level plugin analyzers. Analyzers configured in index settings are not supported."*

`HighlightQueryBuilders.DEFAULT_ANALYZER_NAME` is `"standard"`. A `null` command name still ends up as `"standard"`; it does **not** fall through to a mapping default. Note where the substitution happens, though: `PlannerUtils.resolveAnalyzer(null, registry)` returns `null`. It is the two callers that never pass `null` — `HighlightQueryBuilders.translate` does `analyzerName != null ? analyzerName : DEFAULT_ANALYZER_NAME`, and `Highlight.defaultAnalyzer` resolves `DEFAULT_ANALYZER_NAME` explicitly. Anything new calling `resolveAnalyzer` has to supply the default itself.

### 3. Query rewrite and value analysis share that analyzer

`HighlightQueryBuilders.translate`:

- Builds a `RuntimeSearchExecutionContext` whose synthetic text fields all have `TextSearchInfo(searchAnalyzer, searchAnalyzer)` equal to the resolved analyzer, and whose `IndexAnalyzers` map contains that same name (so `MATCH(..., { "analyzer": "whitespace" })` can validate the name).
- Rewrites the highlight query through Query DSL builders (`Match.asLexicalQueryBuilder`, etc.) against those synthetic fields — **column names, not mapped Lucene names**, so `semantic_text` inference is not invoked.
- Returns `TranslatedQuery(queryText, luceneQuery, analyzer)`.

`LocalExecutionPlanner.planHighlight` comments: *"The query and MemoryIndex must use the same analyzer."* It stuffs `translated.analyzer()` into `HighlightConfig`.

`HighlightOperator` then:

- Tokenizes each row value with `config.requiredAnalyzer()` into a `MemoryIndex(true)` (offsets stored).
- Builds `CustomUnifiedHighlighter` with `OffsetSource.POSTINGS` against that memory index.
- `OffsetSource.ANALYSIS` is never used; the TODO on the operator and planner is *"use real index offsets and per-field analyzers when highlighting can run against shard data."*

One analyzer covers every ON field. There is no per-field override, no `matched_fields`, and no read of `TO_TEXT`'s `analyzer` / `ReferenceAttribute.valuesAnalyzer()`.

### What `HIGHLIGHT` deliberately does not use

| Source | Used by | Used by `HIGHLIGHT`? |
|---|---|---|
| Mapping `analyzer` (index-time) | DSL highlighter re-analysis; Lucene indexing | No |
| Mapping `search_analyzer` | DSL `match` / `match_phrase` | No |
| Mapping `search_quote_analyzer` | DSL phrase queries | No |
| Index-settings named analyzers | DSL `match.analyzer` via `getIndexAnalyzers()` | No |
| Stored postings offsets / term vectors | DSL unified / FVH | No (always re-index the row) |
| `TO_TEXT` / `AnalyzedTextExpression.valuesAnalyzer()` | ES\|QL runtime `MATCH` values side | No |
| Per-field analyzer | DSL (wrapper over `MappingLookup.indexAnalyzer`) | No (one command-wide analyzer) |

Runtime `MATCH` is closer to DSL's split than `HIGHLIGHT` is: `SingleFieldFullTextFunction` resolves `queryAnalyzer` from the function's `analyzer` option and `valuesAnalyzer` from `TO_TEXT`, defaulting query → values → `StandardAnalyzer` (`RuntimeSearch.textEvaluatorForQuery`). `HIGHLIGHT` collapses that pair into one name and requires every leaf to match it.

## Query DSL path

There is **no** `highlight.analyzer` (or per-field equivalent) on `AbstractHighlighterBuilder`. Analyzer choice is entirely a mapping / query concern.

### Query terms

`MatchQueryParser.getAnalyzer`:

- Explicit `match.analyzer` → `SearchExecutionContext.getIndexAnalyzers().get(name)` (**index** analyzer catalog, including custom index-settings analyzers).
- Else the field's `TextSearchInfo.searchAnalyzer()` (or `searchQuoteAnalyzer()` for phrases).

`TextParams.Analyzers` defaults:

- `analyzer` → `indexAnalyzers.getDefaultIndexAnalyzer()`, i.e. the index `default` analyzer.
- `search_analyzer` → index `default_search` analyzer **first, but only when the field-level `analyzer` is itself unconfigured**; otherwise the field's `analyzer`. The order matters: setting `analyzer` on the field suppresses `default_search` for that field.
- `search_quote_analyzer` → index `default_search_quoted` only when **both** `search_analyzer` and `analyzer` are unconfigured; otherwise `search_analyzer`.

So a DSL `match` on `title` uses **search-time** analysis of that field.

### Highlighting the hit

`HighlightPhase` passes the search query (or per-field `highlight_query`) into `FieldHighlightContext`. It does not pick an analyzer.

**Unified** (`DefaultHighlighter.buildHighlighter`) and **plain** (`PlainHighlighter`) both do:

```java
context.getSearchExecutionContext().getIndexAnalyzer(f -> Lucene.KEYWORD_ANALYZER)
```

`SearchExecutionContext.getIndexAnalyzer` is a `DelegatingAnalyzerWrapper` over `MappingLookup.indexAnalyzer(field, ...)`.

`MappingLookup.indexAnalyzer` is the map built from each `FieldMapper.indexAnalyzers()`:

- `TextFieldMapper` → that field's **index-time** `analyzer` (plus `_phrase` / `_prefix` wrappers when those options are on).
- `KeywordFieldMapper` → the field **normalizer** (keyword analyzer if none).
- Unmapped / unindexed name → `Lucene.KEYWORD_ANALYZER`.

So when the unified highlighter **re-analyzes** a field, it uses the **index** analyzer, not `search_analyzer`.

It often does **not** re-analyze. `DefaultHighlighter.getOffsetSource`:

1. Source loader reorders values → `ANALYSIS`.
2. Else if the field indexed offsets → `POSTINGS` (or `POSTINGS_WITH_TERM_VECTORS`).
3. Else if term vectors with offsets → `TERM_VECTORS`.
4. Else `ANALYSIS` (in-memory re-analysis with the index analyzer).

FVH always uses term vectors (`term_vector: with_positions_offsets`); it does not run an analyzer at highlight time.

`highlight_query` changes **which Lucene query** is used to extract terms. It does not change which analyzer tokenizes the stored field. `matched_fields` is how DSL overlays another field's tokens (typically a multi-field with a different `analyzer`, e.g. `comment.english`) onto the field being displayed.

Worked example from `docs/reference/elasticsearch/rest-apis/how-es-highlighters-work-internally.md`: mapping `content.analyzer: english`, no offsets/term vectors → unified highlighter re-analyzes with `english`, query `only fox` becomes tokens `onli` + `fox`, snippet tags `only` / `fox` in the original text.

### DSL keyword vs ES|QL keyword

DSL highlight on `keyword` uses the keyword/normalizer analyzer: the whole value is always exactly one token. A normalizer can change the token's text (lowercase, fold accents) but cannot split it — `KeywordFieldMapper` throws `IllegalStateException` if the normalization token stream yields anything other than 1 token.

ES|QL `HIGHLIGHT` on a keyword column still runs `standard` (or whatever `WITH` says) and **tokenizes like a text field**. Docs call this out. `WITH { "analyzer": "keyword" }` is the way to get DSL-like exact-value highlighting (`highlightAnalyzerKeywordExactMatch` / `highlightAnalyzerKeywordPartialMiss`).

## Side-by-side

| | ES\|QL `HIGHLIGHT` | Query DSL `highlight` |
|---|---|---|
| Where the analyzer name comes from | `WITH.analyzer`, else uniform full-text leaf option, else `"standard"` | Mapping; no highlight option |
| Registry | Node `AnalysisRegistry` (built-in + plugin) | Index `IndexAnalyzers` + per-field `MappingLookup` |
| Custom index-settings analyzers | Unsupported | Used |
| Query analysis | Same named analyzer as values | Field `search_analyzer` / explicit query `analyzer` |
| Value / offset analysis | Same named analyzer, always, via `MemoryIndex` | Field **index** `analyzer`; skipped if postings/TV offsets exist |
| Per-field analyzers | No; one for all ON fields | Yes (`DelegatingAnalyzerWrapper`) |
| `matched_fields` / multi-analyzer overlay | No | Unified + FVH |
| Keyword fields | Tokenized (standard) unless `analyzer: keyword` | Keyword/normalizer; always exactly one token |
| `semantic_text` | Lexical only (`asLexicalQueryBuilder`) | Semantic highlighter by default (`SemanticFieldMapper.getDefaultHighlighter` → `SemanticTextHighlighter`) |
| `max_analyzed_offset` | Command option; index setting ignored; truncates at 1M (the operator reads `MAX_ANALYZED_OFFSET_SETTING.get(Settings.EMPTY)`, i.e. the setting's *default*) | Index setting `index.highlight.max_analyzed_offset` **plus** a same-named highlight request-body option; too-long fields error unless the request option is set. (ES's own error text calls it a "query parameter"; it is not a URL param.) |
| Position increment gap | Forced to text-field default (`100`) | Field's `position_increment_gap` **only when explicitly configured**; otherwise the `NamedAnalyzer`'s own gap, which is also `100` for prebuilt analyzers. So the two agree in the common unset case and diverge only on an explicit non-default gap. |
| Can highlight computed columns | Yes (re-analyzes the loaded value) | No (needs a mapped field + `_source`/stored) |

## Practical mismatches

These are the cases where `WHERE MATCH(...) | HIGHLIGHT` will not paint the same tokens the search used. Closing this gap — especially the mapping-vs-`standard` miss — is the goal.

**Mapping analyzer ≠ `standard`.**
`FROM idx | WHERE MATCH(title, "ring") | HIGHLIGHT "ring" ON title` uses the mapping for `WHERE` and `standard` for `HIGHLIGHT`. If `title` is mapped `english`, `WHERE` matches `Rings` via stemming and `HIGHLIGHT` returns null. Fix: `WITH { "analyzer": "english" }`, or put `{ "analyzer": "english" }` on the `MATCH` (copied into `WITH` when omitted).

> **This case is currently untested.** `highlightAnalyzerDefaultMissesStem` is easy to mistake for coverage of it, but that test is `ROW title = "The Lord of the Rings" | HIGHLIGHT "ring" ON title` — no index, no mapping, no `WHERE`. It only pins the default to `standard`. No csv-spec dataset mapping sets a non-`standard` analyzer either: `mapping-books.json` maps `title` as plain `text`. So the mismatch that motivates this whole work has no failing test to point at, and no test that would turn green once it is fixed. Adding one (a dataset whose field is mapped `english`, highlighted without `WITH`) should come before the fix.

**Same name, different catalog.**
`WHERE MATCH(title, "x", { "analyzer": "english" })` resolves `english` from **index** `IndexAnalyzers`. `HIGHLIGHT` resolves `english` from the **node** registry. Equal for built-ins; not equal if the index redefines `english`.

**`search_analyzer` ≠ `analyzer` (DSL-only split).**
DSL search uses `search_analyzer`; unified re-analysis uses index `analyzer`. ES|QL `HIGHLIGHT` cannot express that split. Runtime `MATCH` can (`queryAnalyzer` vs `valuesAnalyzer`); `HIGHLIGHT` cannot.

**`TO_TEXT(..., { "analyzer": "whitespace" })` then `HIGHLIGHT`.**
Runtime `MATCH` on that column indexes values with `whitespace`. `HIGHLIGHT ON` that column still uses `standard` unless `WITH` / a leaf names `whitespace`. `valuesAnalyzer` is not consulted.

**Offsets in the index.**
DSL unified can highlight from postings without re-tokenizing, so highlight tokens are exactly the indexed tokens. ES|QL always re-tokenizes the loaded string; it cannot see index-time filters that dropped or injected tokens unless the chosen named analyzer recreates them.

## Code map

| Piece | File |
|---|---|
| `WITH` copy from leaves | `ResolveHighlight.withUniformAnalyzer` |
| Leaf name collection / disagreement | `HighlightSupport.uniformAnalyzerOf`, `requireUniformAnalyzer` |
| Analysis-time resolve + verify | `Highlight.postAnalysisVerification` |
| Name → `Analyzer` | `PlannerUtils.resolveAnalyzer` |
| Query rewrite + default name | `HighlightQueryBuilders` (`DEFAULT_ANALYZER_NAME = "standard"`) |
| Synthetic mapped-like fields | `RuntimeSearchExecutionContext` |
| Planner wiring | `LocalExecutionPlanner.planHighlight` |
| MemoryIndex + unified highlighter | `HighlightOperator` |
| Runtime `MATCH` dual analyzers (not used here) | `SingleFieldFullTextFunction`, `RuntimeSearch` |
| DSL per-field index analyzer | `SearchExecutionContext.getIndexAnalyzer`, `MappingLookup.indexAnalyzer` |
| DSL unified / plain | `DefaultHighlighter`, `PlainHighlighter` |
| DSL match analyzer | `MatchQueryParser.getAnalyzer` |
| Mapping defaults | `TextParams.Analyzers` |
| Product docs | `docs/reference/query-languages/esql/_snippets/commands/layout/highlight.md` |
| DSL highlighter internals | `docs/reference/elasticsearch/rest-apis/how-es-highlighters-work-internally.md` |

## Follow-ups already marked in code

Goal: eliminate the analyzer mismatch with DSL highlighting.

- `HighlightOperator` / `LocalExecutionPlanner.planHighlight`: run against shard data so real postings offsets and per-field mapping analyzers can be used.
- `PlannerUtils.resolveAnalyzer`: share analyzer verification with `TOP_SNIPPETS`, `MATCH`, and `MATCH_PHRASE`.

See also: `HIGHLIGHT_INDEX_ANALYZER_RETRIEVAL.md` (paths to learn the mapping's index analyzer name, given only predefined analyzers).
