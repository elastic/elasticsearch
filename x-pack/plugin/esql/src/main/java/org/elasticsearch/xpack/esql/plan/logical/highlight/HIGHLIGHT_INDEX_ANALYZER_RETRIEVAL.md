# Paths to retrieve each mapped field's index analyzer for ES|QL `HIGHLIGHT`

Investigation notes (Sep 2026). Companion to `HIGHLIGHT_ANALYZER_VS_DSL.md`.

## Goal

`HIGHLIGHT` tokenizes each ON-field value into a `MemoryIndex` and rewrites the highlight query against those same fields. Each ON field must use **that field's** index-time analyzer — the name the mapping would give `_search` highlighting — so `WHERE MATCH(title, …)` and `HIGHLIGHT … ON title` paint the same tokens, even when `body` on the same command uses a different analyzer.

This phase stores **an analyzer per ON field**, in ON-list order:

```
ON title, body, tags
     │      │     └─ analyzers[2]  // tags
     │      └─────── analyzers[1]  // body
     └────────────── analyzers[0]  // title
```

`HighlightConfig` / `HighlightOperator` take `List<Analyzer>` (or names resolved at plan time) of that length. `MemoryIndex.addField(name, value, analyzers[i])` uses the matching entry. The synthetic `RuntimeSearchExecutionContext` used to rewrite the highlight query exposes the same per-field analyzers, so `MATCH(title, …)` inside the highlight query is analyzed with `title`'s analyzer, not `body`'s.

**This round only cares about predefined Elasticsearch analyzers** — names that resolve on both the coordinating node and every data node via `AnalysisRegistry.getAnalyzer(name)` (built-ins plus `analysis-common` language analyzers such as `english`). Custom `index.analysis` analyzers stay out of scope. Retrieval is **"learn each field's analyzer name, then resolve that name the way we already resolve a known name."**

A field with no mapping analyzer (computed `TO_TEXT` without an option, `ROW`, unmapped) still resolves `"standard"`. `WITH { "analyzer": "<name>" }`, when present, overrides that field's mapping name the way an explicit `MATCH` `{ "analyzer": … }` already does; it is an override, not the source of the array.

## What "index analyzer" means here

Query DSL highlighting re-analyzes each field with that field's **index-time** analyzer (`SearchExecutionContext.getIndexAnalyzer` → `MappingLookup.indexAnalyzer` → `FieldMapper.indexAnalyzers()`). A DSL `match` on a field uses that field's **`search_analyzer`**.

Those are different mapping parameters. They are equal on most mappings; they diverge when the user sets `search_analyzer` (or index `index.analysis.analyzer.default_search`). The fallback order for `search_analyzer` is not simply "`analyzer` if unset": `TextParams.Analyzers` consults the index `default_search` analyzer **first**, and only when the field-level `analyzer` is itself unconfigured; otherwise it uses the field's `analyzer`. So setting `analyzer` on a field suppresses `default_search` for that field.

For closing the paint gap against `_search` highlighting, each array slot is the **index** analyzer of that ON field (`analyzer` in the mapping, defaulting to `"default"`). Do not fill the array from `MappedFieldType.getTextSearchInfo().searchAnalyzer()`.

| Mapping | Typical `NamedAnalyzer.name()` | Used by |
|---|---|---|
| `"analyzer"` / omitted | `"english"`, `"standard"`, or `"default"` | DSL unified/plain re-analysis; Lucene indexing; **each HIGHLIGHT slot** |
| `"search_analyzer"` / omitted | same as index analyzer, unless overridden | DSL `match` / Lucene-pushed ES\|QL `MATCH` |
| keyword field | `"keyword"`, or the **normalizer** name | DSL highlight on `keyword`; that field's HIGHLIGHT slot |

`"default"` and `"standard"` are the same Lucene `StandardAnalyzer` in `PreBuiltAnalyzers`. A mapping that omitted `analyzer` reports `"default"`. Either name resolves on `AnalysisRegistry`; they are not the same string. Canonicalize `"default"` → `"standard"` when filling a slot so `TO_TEXT` / `MATCH` options that say `"standard"` line up.

Language analyzers (`english`, `german`, …) are not in `PreBuiltAnalyzers`. They are registered by `CommonAnalysisPlugin#getAnalyzers` and still come back from `AnalysisRegistry.getAnalyzer` on every node that loads `analysis-common` (default distribution, including coordinating-only nodes).

## What we already have (and do not)

**Already on the coordinating node at analysis**

- `EsIndex.mapping` → `EsField` / `TextEsField` / `KeywordEsField`, built by `IndexResolver` from **field-caps**.
- `FieldAttribute` wraps that `EsField`. No analyzer name on it.
- `IndexProperties` is per concrete index: `IndexMode` + shard count. No analyzer.
- `AnalysisRegistry` for resolving a **name we already know**.
- Cluster state `IndexMetadata.mapping()` (`MappingMetadata` compressed JSON) for **local** indices.
- `QueryBuilderResolver` rewrites the `QueryBuilder` of any `RewriteableAware` expression that needs a coordinator rewrite (`Match` among them) — not Lucene-pushed `MATCH` specifically. It **explicitly skips HIGHLIGHT expressions**, which matters for path 9. Analyzer choice still happens later, on the data node, inside `MatchQueryParser.getAnalyzer`.
- `AnalyzedTextExpression.valuesAnalyzer()` on `TO_TEXT` / `ReferenceAttribute` — a per-column name, unused by `HIGHLIGHT` today.

**Already on a data node when a shard is open**

- `LocalExecutionPlannerContext.shardContexts` → `EsPhysicalOperationProviders.DefaultShardContext` → `SearchExecutionContext`.
- `MappingLookup.indexAnalyzer(field, fallback)` — the real index-time `NamedAnalyzer` **keyed by mapping field name**.
- `MappedFieldType.getTextSearchInfo().searchAnalyzer()` — search-time, **not** the highlighter.
- The same `AnalysisRegistry` as the coordinator.

**Never available from Lucene files**

- `FieldInfos` do not store analyzer names. Offsets/term vectors tell you *tokens*, not *which analyzer produced them*.

**Field-caps today**

`FieldCapabilitiesFetcher` copies `MappedFieldType.meta()` (the user-defined mapping `meta` parameter) onto `IndexFieldCapabilities.meta`. It does **not** copy `analyzer` / `search_analyzer`. `IndexResolver.createField` for `text` builds `new TextEsField(name, new HashMap<>(), false, isAlias, timeSeriesFieldType)` — an always-empty properties map, `hasDocValues` hardcoded `false` (this is what surfaces as aggregatable), alias flag, time-series role. Nothing analyzer-related.

`EsqlResolveFieldsAction` is a fork of field-caps whose javadoc says it will decouple. Right now it still delegates to `TransportFieldCapabilitiesAction` and wraps the same `FieldCapabilitiesResponse`.

## Where `HIGHLIGHT` actually runs

`HighlightExec` is a unary physical op. The coordinator/data-node split is the first `ExchangeExec`: a node **above** the exchange runs on the coordinator, **below** it on the data node.

**`HIGHLIGHT` normally runs on the coordinator.** That is the opposite of what "the optimizer pushes TopN under `HIGHLIGHT`" suggests, so the inference is worth spelling out: pushing TopN *below* Highlight is exactly what leaves Highlight *above* the exchange. `Highlight` is also not a `PipelineBreaker`, so nothing drags it into the data-node fragment.

The golden plans for the two shapes:

- Typical `FROM idx | WHERE MATCH(...) | HIGHLIGHT | SORT ... | LIMIT n` (`testTopNIsPushedBelowHighlight`): `HighlightExec > TopNExec > ExchangeExec > … > EsQueryExec`. Highlight is on the **coordinator**, and `ComputeService` builds that driver's `ComputeContext` with `EmptyIndexedByShardId.instance()` — `shardContexts` is **empty**. The limit and sort do reach Lucene (`EsQueryExec … limit[10], sort[[FieldSort[emp_no…]]]`), just in a different driver from the highlighter.
- Sort on a generated snippet column (`testTopNOnGeneratedSnippetIsNotPushedBelowHighlight`): the sort cannot move below the highlight, which pins `HighlightExec` **below** `ExchangeExec`, between `FieldExtractExec` and `EsQueryExec`. This is the one common shape where Highlight runs on a **data node** with non-empty `shardContexts`.
- `HIGHLIGHT` after `STATS`, `FORK`, over `ROW`, datasets, or any coordinator-only fragment: coordinator, empty `shardContexts`, only `AnalysisRegistry`. (`STATS`/`FORK` also block implicit-query borrowing entirely, and `HIGHLIGHT` after `STATS` requires the ON column to survive the aggregation.)
- Computed ON columns (`EVAL` / `TO_TEXT`): no mapping. Fill that slot from `AnalyzedTextExpression.valuesAnalyzer()`, else `"standard"`.

**`planHighlight` does not read `shardContexts` today at all.** `LocalExecutionPlannerContext` carries them, but the method only touches `foldCtx()` and `analysisRegistry()`. There is no existing shard-side hook to extend; a shard-based path starts from zero.

Consequence for the paths below: a path that only works next to a shard does not merely leave "some" slots wrong — it is inoperative for the **default** query shape. The array has to be populated from plan-time metadata (or `"standard"`) for coordinator execution, and that is the common case rather than the fallback.

## How the array is filled

For each ON expression `fields.get(i)`, pick a name, then `PlannerUtils.resolveAnalyzer(name, registry)`:

| ON expression | Name for `analyzers[i]` |
|---|---|
| `FieldAttribute` with a mapping analyzer | that field's index-analyzer name |
| `FieldAttribute` keyword, no normalizer | `"keyword"` |
| `FieldAttribute` keyword with a normalizer | the normalizer name (predefined only; unknown → `"keyword"` / `"standard"` per policy) |
| `AnalyzedTextExpression` (`TO_TEXT`, `ReferenceAttribute`) | `valuesAnalyzer()`, else `"standard"` |
| anything else | `"standard"` |
| `WITH.analyzer` set | that name for every slot it is meant to override |

Query rewrite uses the same list: the synthetic context's field `title` is built with `analyzers[i]` for the ON field named `title`. A highlight `MATCH` on a field that is not in ON still needs a slot or a fallback (`"standard"`) — same as today when the query names a field the user did not highlight.

`HighlightSupport.requireUniformAnalyzer` across **different** ON fields goes away as a product rule. A leaf may still be required to match **its own** field's slot (`MATCH(title, {analyzer: english})` vs `title` mapped as `whitespace`).

## Shared design questions every path must answer

1. **Index vs search analyzer.** Each slot is index-time, to match DSL highlighting.
2. **Array alignment.** `analyzers.size() == fields.size()`, same order as ON. Generated `highlight_*` columns stay in that order already.
3. **Multi-index `FROM logs-*`.** Disagreement is **per field**: `title` may be `english` on every index while `body` conflicts. Field-caps already merges type conflicts into `InvalidMappedField`. Analyzer disagreement on one field does not poison the others. Policy for a conflicting field: error, `"standard"`, or resolve per shard (only paths that run on the shard can do the last).
4. **CCS / mixed-version.** New field-caps / plan fields need a `TransportVersion` and a fallback when the remote is old (`"standard"` in that slot).
5. **Keyword.** No `analyzer`; DSL uses the keyword analyzer or a normalizer. That field's slot is `"keyword"` / the normalizer name, not `"standard"`.
6. **Name vs instance.** For predefined analyzers, `name` + `AnalysisRegistry.getAnalyzer` is enough on any node. If the code is already sitting on `MappingLookup`, the `NamedAnalyzer` instance is strictly better (and happens to work for custom analyzers later). Re-resolving names on the coordinator is how coordinator-side `HIGHLIGHT` stays in this round's scope.
7. **Query vs values.** Each field's query analysis (synthetic context) and value analysis (`MemoryIndex`) use **that field's** slot. They stay the same name for predefined analyzers; we still do not model `search_analyzer ≠ analyzer` in this round.

## Path 1 — Field-caps / `EsqlResolveFields` → per-field name on `TextEsField`

**What it is.** At field-caps time, each shard already has `MapperService` / `FieldMapper`. Add the index-analyzer **name** to the per-field payload, merge it in `IndexResolver` onto `TextEsField` (and keyword normalizer onto `KeywordEsField`). At analysis / `planHighlight`, walk ON fields and build `List<String>` / `List<Analyzer>` from `FieldAttribute.field()`.

Two ways to carry the string:

- **1a. Public `IndexFieldCapabilities`.** New optional field next to `isInference` / `metricType`, gated by a transport version. `FieldCapabilitiesFetcher` reads it from `FieldMapper.indexAnalyzers().get(field).name()`, not from `MappedFieldType.meta()` and not from `TextSearchInfo.searchAnalyzer()`.
- **1b. ES|QL-only payload on `EsqlResolveFieldsResponse`.** Same fetch, but do not change REST `_field_caps`. Matches the stated direction of `EsqlResolveFieldsAction`. Requires actually decoupling the fork instead of wrapping the same record.

Do **not** stuff the name into `MappedFieldType.meta()`. That map is the user mapping parameter `meta`; Kibana and clients already merge it. Colliding on a key named `analyzer` would be a user-visible break.

**How it works end to end**

```
data node FieldCapabilitiesFetcher
  FieldMapper.indexAnalyzers().get(path).name()   // "english" / "default" / …
        │
        ▼
IndexFieldCapabilities (or EsqlResolveFields extra)
        │  merge across indices (per-field conflict policy)
        ▼
IndexResolver.createField → TextEsField(analyzerName)
                           → KeywordEsField(normalizerName)
        │
        ▼
analysis / planHighlight
  for i, field in ON:
      names[i] = analyzerNameOf(field)   // mapping, TO_TEXT, or "standard"
      analyzers[i] = PlannerUtils.resolveAnalyzer(names[i], registry)
        │
        ▼
HighlightQueryBuilders.translate(query, fields, analyzers)
  RuntimeSearchExecutionContext with per-field TextSearchInfo
HighlightConfig.analyzers == analyzers
HighlightOperator
  MemoryIndex.addField(fields[i], value, analyzers[i])
```

**Ease of implementation.** Medium. The consume side is a loop over ON fields (the array this phase is adding). The produce side touches field-caps serialization, mapping-hash compression (`FieldCapabilitiesIndexResponse` already groups by mapping hash, so the extra string is paid once per unique mapping, not per index), `IndexResolver` merge, `TextEsField` / `KeywordEsField` `Writeable` BWC (`generateTransportVersion`), CCS tests, and a **per-field** conflict policy. 1b is more work up front (fork the response) and less work for every other field-caps client forever.

`HighlightQueryBuilders` / `RuntimeSearchExecutionContext` must stop assuming a single `NamedAnalyzer` for every synthetic field. That change is required for this phase regardless of retrieval path.

**Performance.** Negligible CPU: a `String` already in `NamedAnalyzer`. Payload: one short string per text/keyword field per unique mapping. Field-caps is already on the ES|QL query path; this is not a new RPC. Resolve is one `AnalysisRegistry` lookup per **distinct** name in the ON list, not per row.

**Time.** Roughly **1–2 weeks** for 1a including the config/operator array wiring. Add several days if 1b includes a real `EsqlResolveFields` decoupling. BWC tests and mixed-cluster CCS dominate.

**Maintainability.** High. The name lives on the field, which is where DSL keeps it. `HIGHLIGHT`, `TOP_SNIPPETS`, and runtime `MATCH` can all read `TextEsField` (or path 6). 1a taxes every `_field_caps` caller; 1b keeps that tax inside ES|QL.

**Other software concerns**

- **API surface.** 1a is a wire-format change to a public action. Treat it like `FIELD_CAPS_INFERENCE_FIELD`.
- **Correctness vs DSL.** You get names, not the index's analyzer *instances*. For predefined analyzers they match. If an index *redefines* `english` under `index.analysis`, the companion doc's "same name, different catalog" hole remains — accepted for this round.
- **Default name.** Map omitted `analyzer` to `"default"`, canonicalize to `"standard"` when filling a slot.
- **Security.** Names are mapping metadata, not document values. Same privilege as field-caps (`indices:data/read/esql/resolve_fields`).
- **Testing.** `ON title, body` with `english` vs `standard`; `FROM` two indices where only `body` disagrees; keyword slot vs text slot; `ON *`; `TO_TEXT` mixed with a mapped field; CCS old node omitting the new slot.

## Path 2 — Cluster-state `MappingMetadata` on the coordinator

**What it is.** Skip field-caps. After `IndexResolver` returns concrete index names, walk `ClusterService.state()` → `IndexMetadata.mapping().sourceAsMap()` and pick `properties.<field>.analyzer` (and nested `fields.`) for each ON field.

**How it works.** `MappingMetadata` is compressed mapping JSON that every node in the local cluster already has, including coordinating-only nodes. It is not a parsed `MapperService`. There is no `IndexAnalyzers` on a node that does not hold the shard. You only get the string the mapping serialized, and only if it was serialized: omitted `analyzer` means "use default", which will not appear in the map unless you request `include_defaults`.

**Ease of implementation.** Easy for one local index with explicit `"analyzer"` on each field. Quickly gets messy: multi-fields, objects, aliases, copy_to, runtime fields, omitted defaults. You reimplement a slice of mapper parsing, once per ON field.

**Performance.** No network. CPU is JSON walk of mappings already in RAM. Worst case is `FROM logs-*` with thousands of concrete indices; field-caps already paid a similar cost compressed by mapping hash. Easy to get accidentally O(indices × mapping size) if you don't group by `MappingMetadata.getSha256()`.

**Time.** **2–4 days** for local-only explicit analyzers. **A week+** to handle omitted defaults, multi-fields, and index patterns. CCS still unfinished.

**Maintainability.** Low. Mapping JSON is a published-but-evolving shape; mapper code is the real source of truth. CCS and datasets never work.

**Other software concerns.** Remote index mappings are not in local cluster state — that is why field-caps exists. Views / datasets / lookup are easy to miss. Use this only as a prototype to prove the array wiring, not as the product path.

## Path 3 — Extra `GetMappings` / `GetIndex` RPC

**What it is.** After (or beside) field-caps, send `GetMappingsRequest` for the resolved indices, parse per-field analyzer names, attach them to `EsField`. Unlike path 2 this can be asked of remote clusters.

**Ease of implementation.** Same parsing pain as path 2, plus a second fan-out in `EsqlSession` next to `resolveMainIndicesVersioned`. You duplicate index-options, security, CCS, views, and failure handling that `IndexResolver` already owns.

**Performance.** Extra coordinator round-trip. Mappings responses are large; field-caps was invented to avoid shipping full mappings. Latency and heap both move the wrong way.

**Time.** **1 week** to wire, parse, and test locally; **another week** to make CCS / failures / `skip_unavailable` behave like field-caps.

**Maintainability.** Low–medium. Two resolution channels will drift (field-caps says `text`, mappings say the field vanished). Do not do this when path 1 can ride the RPC we already make.

**Other software concerns.** Privilege checks must match field-caps. Closed / frozen / unavailable indices. Mapping size limits.

## Path 4 — Data-node `MappingLookup` at `planHighlight`

**What it is.** `LocalExecutionPlanner.planHighlight` already receives `shardContexts` and `analysisRegistry`. On a data node, expose `SearchExecutionContext` from `ShardContext` and, for each ON field, `mappingLookup.indexAnalyzer(luceneFieldName, fallback)`. Take `.name()`, confirm `AnalysisRegistry.getAnalyzer(name)` succeeds (predefined), put that `Analyzer` in `analyzers[i]`. Unknown name (custom) → `"standard"` in that slot.

**How it works.** Only when `HighlightExec` is in the data-node fragment — which, per [Where `HIGHLIGHT` actually runs](#where-highlight-actually-runs), is the *uncommon* case (essentially just sort-on-generated-snippet). Coordinator `HIGHLIGHT`, including the default `WHERE MATCH … | HIGHLIGHT … | SORT … | LIMIT` shape, still sees empty `shardContexts` and must fill the array from path 1 / `"standard"`.

Note also that `planHighlight` does not touch `shardContexts` today, so this is new wiring rather than an extension of an existing lookup.

If the node holds shards from two indices, each ON field may disagree across shards. `planHighlight` still builds **one** array for the driver. Agreement is per field: `title` can be `english` on every shard while `body` is `standard` on every shard — that is success. `title` is `english` on shard A and `standard` on shard B — that field's slot cannot be correct without path 5.

**Ease of implementation.** Medium-low for "each ON field agrees across this node's shards": loop ON fields, look up, resolve. No field-caps, no `EsField` BWC. Still requires the synthetic context and `HighlightConfig` to take the array. `EsPhysicalOperationProviders.ShardContext` already exposes `mappingLookup()` publicly, so no new accessor is needed — the raw `SearchExecutionContext` stays private in `DefaultShardContext`, and nothing here needs it.

**Performance.** Zero extra network. `MappingLookup` is already loaded for field extract. One map lookup per ON field at driver start, not per row.

**Time.** **3–5 days** of planner wiring on top of the array change if we accept coordinator slots staying `"standard"` without path 1. **A week** if we detect per-field disagreement across shards and error.

**Coverage, which is the real problem.** Cheap to build, but it fixes almost nothing on its own: the default query shape puts `HighlightExec` on the coordinator, so this path would only fire for sort-on-generated-snippet plans. Treat it as a refinement of path 1, never a substitute.

**Maintainability.** Medium. The lookup is the correct API, but it is hidden inside compute planning. Explain output will not show per-field mapping analyzers unless names also live on the plan (path 1). Custom analyzers accidentally start working on data nodes and still fail on the coordinator — refuse custom names unless path 5 keeps the instance.

**Other software concerns**

- **Driver placement.** If `HIGHLIGHT` sits above a node-reduce that mixed shards, `planHighlight` sees many mappings while each page is already mixed. Confirm with the golden local physical plan.
- **Lucene field name vs column name.** `MappingLookup` keys are **mapping** names (`FieldAttribute.fieldName()`). Column names miss renamed / union-type / `::` converted attributes.
- **Analysis-time verification** still needs path 1 (or it verifies every slot as `"standard"`).

## Path 5 — Per-shard lookup inside `HighlightOperator`

**What it is.** Give `HighlightOperator` an `IndexedByShardId<ShardContext>` the way `ValuesSourceReaderOperator` has one. Each page still carries a doc/shard channel from Lucene source. For ON field `i` on a row from shard `s`, `mappingLookup.indexAnalyzer(fields[i])` on **that** shard. Mixed `FROM logs-*` is correct even when `title` is `english` on one index and `standard` on another.

This is the execution model `DefaultHighlighter` already uses (`DelegatingAnalyzerWrapper` over `MappingLookup.indexAnalyzer`). The remaining TODO on the operator is real postings offsets (path 10), not per-field analyzers.

Same coverage caveat as path 4, and it bites harder here because the cost is higher: on the default plan shape there is no shard next to the highlighter at all, so the shard channel this path builds would be absent exactly when the query is most ordinary. Getting per-row correctness for mixed `FROM logs-*` also implies moving `HighlightExec` below the exchange on purpose, which is a planner decision (see the open TODO on `HighlightExec` about whether HIGHLIGHT should always run on the coordinator) rather than a consequence of this path.

**Ease of implementation.** Hard relative to path 4. `HighlightOperator` is an `AbstractPageMappingOperator` with no doc channel today; it only sees loaded `BytesRef` values. You must:

- Keep a shard/doc channel alive through field extract until highlight (layout / `PruneColumns` / exchange).
- Fall back to the planned array when there is no channel (coordinator, computed column, `ROW`).
- Tokenize `MemoryIndex` field `i` with the shard-local analyzer, not only `analyzers[i]` from plan time.

The planned array is still the coordinator fallback and the analysis-time default. Path 5 refines it per row when a shard is present.

**Performance.** Still no extra RPC. Per-row cost is unchanged (still `MemoryIndex`). Extra indirection per field is noise compared with highlighting. Memory: `NamedAnalyzer` references already resident in each shard's `MappingLookup`.

**Time.** **1.5–3 weeks** including layout/channel work. That work is the risky part, not Lucene.

**Maintainability.** High once done. Operator complexity goes up; tests must cover mixed indices, missing shard channel, mapped+`TO_TEXT` in the same ON list.

**Other software concerns.** Exchange: shard ids are local to a data node. After `ExchangeSink`, the coordinator cannot look up `MappingLookup`. Coordinator `HIGHLIGHT` keeps the planned array. Computed columns keep `AnalyzedTextExpression`.

## Path 6 — `AnalyzedTextExpression` on `FieldAttribute` (consumption, not a source)

**What it is.** Runtime `MATCH` already reads `AnalyzedTextExpression.valuesAnalyzerOf(field)` (`TO_TEXT` / `ReferenceAttribute`). `FieldAttribute` does not implement that interface. Once some source (path 1, 2, 4) has a name, putting it on `FieldAttribute` / `TextEsField` and implementing `AnalyzedTextExpression` makes each ON slot a single call: `valuesAnalyzerOf(fields.get(i))`.

The interface's own javadoc already says the long-term cleaner model is `text(analyzer=...)` as a parameterized type.

**Ease of implementation.** Small **given a name**. `ReferenceAttribute` serialization (`ESQL_TO_TEXT_VALUES_ANALYZER`) is the template. `FieldAttribute` is sealed, widely serialized, and "treat as final" in its class javadoc — adding a field is a transport-version event and a lot of test constructor churn.

**Performance.** One optional string on every field attribute in the plan. Tiny.

**Time.** **2–4 days** on top of path 1. Not a substitute for a retrieval path. Makes the array fill loop uniform for mapped and `TO_TEXT` columns.

**Maintainability.** High for consumers; watch double sources of truth (`TextEsField.analyzerName` vs `valuesAnalyzer()`). Prefer one.

## Path 7 — Dedicated "resolve analyzers" transport action

**What it is.** A skinny RPC: index names + field list → map of field → analyzer name. Something like field-caps without types, aggregatable, inference, etc.

**Ease of implementation.** You still need a per-shard fetch, merge, CCS, BWC, security — i.e. most of field-caps. Then you still merge into `EsField` and build the array.

**Performance.** Extra RPC unless you replace field-caps (you will not). Worse than path 1.

**Time.** **1–2 weeks** to reach what path 1 gets by adding a field.

**Maintainability.** Another action to keep in sync with mapping changes. Only justified if product/security refuses to extend field-caps **and** `EsqlResolveFields` cannot be extended either. Unlikely.

## Path 8 — Index-level default only (`IndexProperties`)

**What it is.** Field-caps already ships per-index `IndexMode` and shard count (`IndexProperties`, `SHARD_COUNTS`). Add `index.analysis.analyzer.default`. Put that same name in every ON slot.

**Ease of implementation.** Easy. Same BWC pattern as shard count.

**Performance.** One string per concrete index.

**Time.** **1–2 days**.

**Maintainability.** A trap. `title: english` and `body: standard` on the same mapping is the case this phase exists to get right. An index default fills every slot with the same name and misses per-field mapping. `NamedAnalyzer.name()` on each field already returns `"default"` when the field omitted `analyzer`.

Not a product path.

## Path 9 — Rewrite the highlight query with the real `SearchExecutionContext`

**What it is.** Lucene-pushed `MATCH` already becomes a `QueryBuilder` and, on the data node, `DefaultShardContext.toQuery` → `MatchQueryParser.getAnalyzer` → that field's **search** analyzer. If `HighlightQueryBuilders.translate` ran against the real context, query terms would follow mapping search analysis per queried field.

**This does not retrieve index analyzers for the value array.** `MemoryIndex` slots would still need path 1/4/5. It also uses **search** analysis for the query, which is what `MATCH` wants and **not** what DSL highlighting uses when `search_analyzer ≠ analyzer`.

**Ease of implementation.** Medium. Real `SearchExecutionContext` is per shard; unmapped / computed ON fields break (today's synthetic context exists so those work). The synthetic context with **per-field** analyzers from the array is the coordinator-safe version of this idea.

**Performance.** Fine.

**Time.** **~1 week**, and the MemoryIndex slots remain.

**Maintainability.** Two contexts (synthetic vs real) forever unless HIGHLIGHT is split into "mapped fields on a shard" vs "computed columns". Better as a piece of path 10 than as a standalone.

Treat as a query-side trick, not an analyzer-retrieval path.

## Path 10 — Run highlighting against shard data (DSL unified highlighter)

**What it is.** Stop building a `MemoryIndex` from loaded strings. On the data node, call the same `DefaultHighlighter` / `CustomUnifiedHighlighter` path `_search` uses: `OffsetSource.POSTINGS` / term vectors when present, else `ANALYSIS` with `getIndexAnalyzer` **per field**. Custom analyzers, index-time filters that drop tokens, `max_analyzed_offset` index setting — all fall out. The array of names becomes unnecessary on the shard path; `MappingLookup` is the array.

A MemoryIndex + planned array remains for computed columns and coordinator `HIGHLIGHT`.

**Ease of implementation.** Hard. Needs stored/source or indexed offsets, fetch-phase-like access, `matched_fields` decisions, `semantic_text`. This is a feature, not a name lookup.

**Performance.** Best when offsets exist (no re-analysis). Worst when they do not (same as today, plus reader access).

**Time.** **Several weeks to a quarter**, depending on how complete vs `_search` it must be.

**Maintainability.** Highest long-term alignment with DSL. Highest short-term risk (two highlighters). Out of scope for "learn predefined names into an array".

**Lucene files still do not give you the name.** They give you tokens. That is enough to highlight, which is why this path can skip retrieval entirely.

## Path 11 — `_analyze` API / `TransportAnalyzeAction`

**What it is.** `POST _analyze` with `field` uses that field's analyzer and returns tokens. With `explain=true` the response *does* include `detail.analyzer.name`, so the name is technically recoverable — the earlier claim that it is not was wrong. What kills the path is cost and shape: a round-trip per ON field, tokenizing user-controlled text, on an API built for debugging rather than planning.

**Not a retrieval path.** Mentioned so it is not "discovered" later. Rejected on performance and security, not on capability.

## Path 12 — `MappedFieldType.getTextSearchInfo().searchAnalyzer()` via existing `ShardContext.fieldType()`

**What it is.** The compute `ShardContext` already exposes `fieldType(name)`. `TextSearchInfo` already has `searchAnalyzer` as a `NamedAnalyzer`. Filling `analyzers[i]` from that is the easiest data-node loop we have. (`MappingLookup` is *not* on that interface — it lives on the ES|QL `EsPhysicalOperationProviders.ShardContext` subclass, which is what path 4 uses.)

**This is the wrong analyzer for DSL highlighting** whenever `search_analyzer` is set. It is the right analyzer for matching `WHERE MATCH` query terms on that field.

**Ease.** Easiest data-node hook we have.

**Time.** **1–2 days** to wire `.name()` into each slot.

**Maintainability.** We would encode the wrong DSL semantics in the first patch and have to unwind it. Only acceptable if product explicitly wants "highlight with the search analyzer so MATCH hits always paint", which is **not** what `_search` highlight does.

Do not take this path by accident. If we take it, document the split **per field**.

## Comparison

| Path | Per-field index names? | Works on coordinator? | CCS | Custom later | Ease | Perf | Time | Long-term |
|---|---|---|---|---|---|---|---|---|
| 1 Field-caps → `TextEsField` | Yes | Yes | Yes (with TV) | Name only; instance still node registry | Medium | ~0 extra RPC | 1–2 w | **Best source of truth** |
| 1b ES\|QL-only resolve_fields | Yes | Yes | Yes | Same | Medium+ | ~0 | 1.5–3 w | Best if we will not touch public field-caps |
| 2 Cluster-state JSON | Explicit names only | Local only | No | Name only | Easy→messy | No RPC; duplicate walk | 2–4 d local | Rotten parser |
| 3 GetMappings RPC | Yes | Yes | If asked | Name only | Medium | Extra large RPC | 1–2 w | Duplicate resolver |
| 4 `planHighlight` + `MappingLookup` | Yes (or instance) | No — and coordinator is the **default** shape | n/a | Instance if kept | Medium-low | ~0 | 3–5 d | Incomplete alone; fires only on sort-on-snippet plans |
| 5 Operator + shard channel | Yes, per field **and** shard | No — needs HIGHLIGHT moved below the exchange on purpose | n/a | Instance | Hard | ~0 | 1.5–3 w | Right execution model, wrong current placement |
| 6 `AnalyzedTextExpression` | No (propagation) | Yes if name exists | — | — | Small add-on | ~0 | 2–4 d | Uniform slot fill |
| 7 New transport action | Yes | Yes | Yes | Name only | Hard | Extra RPC | 1–2 w | Redundant |
| 8 Index default in `IndexProperties` | No (same name every slot) | Yes | Yes | No | Easy | ~0 | 1–2 d | Wrong granularity |
| 9 Real `SearchExecutionContext` rewrite | Search analyzer, query side | No | n/a | Query side only | Medium | ~0 | ~1 w | Not retrieval |
| 10 Shard highlighter | N/A (uses instance/offsets) | No | n/a | Yes | Hard | Best with offsets | weeks–months | End state |
| 11 `_analyze` | No | — | — | — | — | Bad | — | Reject |
| 12 `fieldType().searchAnalyzer()` | **Search** name per field | No | n/a | Instance | Easiest | ~0 | 1–2 d | Wrong semantics |

## Recommendation for this round (predefined analyzers, per-field array)

**Ship path 1 (prefer 1b if `EsqlResolveFields` can grow a field without waiting on public field-caps). Fill `analyzers[i]` from each ON field's `TextEsField` / `KeywordEsField` / `valuesAnalyzer()` / `"standard"`. Resolve each distinct name with `PlannerUtils.resolveAnalyzer`.**

Why:

- Analysis, implicit `HIGHLIGHT`, coordinator execution, and data-node execution all need **names on the fields**, not a shard.
- Predefined analyzers exist on `AnalysisRegistry` everywhere, so name → instance is already implemented and tested (`HighlightQueryBuildersTests`, plugin position-increment gap).
- Field-caps is the metadata channel ES|QL already trusts for CCS, mapping-hash compression, and type conflicts. Analyzer disagreement is the same merge, **scoped to that field**.
- No second RPC, no mapping-JSON parser, no accidental `search_analyzer`.

The array wiring (`HighlightConfig`, `RuntimeSearchExecutionContext`, `HighlightOperator`) is in-scope for this phase and is not optional: retrieval without per-field consume still paints `body` with `title`'s tokenizer.

Add **path 6** in the same change if `TextEsField` plumbing is going in anyway, so mapped fields and `TO_TEXT` share `valuesAnalyzerOf` when building the array. YAGNI if `HIGHLIGHT` is the only consumer this quarter — then read `TextEsField` in the fill loop only.

**Do not block on path 4/5** for the first predefined-analyzer fix. Path 4 is a data-node-only shortcut, and since `HighlightExec` sits above the exchange on the default plan shape, "data-node-only" means it would not fire for ordinary queries at all — it leaves the coordinator slots wrong in precisely the common case. Path 5 is the next execution step when mixed indices must be correct **per row**, or when custom analyzers must keep the shard instance, and it additionally depends on a planner decision to place HIGHLIGHT next to the shard.

**Do not take path 8 or path 12.** Path 8 cannot represent two ON fields with two mapping analyzers. Path 12 fills each slot with the search analyzer.

**Conflict policy for path 1 (ponytail default):** if a field's analyzer name agrees across indices and resolves on `AnalysisRegistry`, use it in that slot. If the field disagrees across indices, or the name is unknown (custom), that slot is `"standard"` and optionally a header warning naming the field. Other slots are unaffected. Erroring is more honest and more breaking.

**Canonicalize `"default"` → `"standard"`** when filling a slot.

## Testing that actually proves the path

**There is no existing test of the gap this work closes.** `highlightAnalyzerDefaultMissesStem` looks like one but is not: it runs on `ROW title = "The Lord of the Rings"`, with no index and no mapping, so path 1 cannot change its result — it will still return `null` afterwards, correctly. And no csv-spec dataset mapping sets a non-`standard` analyzer (`mapping-books.json` maps `title` as plain `text`), so nothing currently reaches the mapping-vs-`standard` path at all.

So the first task is a **new** fixture: a dataset field mapped `analyzer: english`, highlighted without `WITH`, asserting `null` today and a painted snippet after path 1. That is the test that proves the feature. Then:

- `ON title, body` where `title` is `english` and `body` is `standard` — title stems, body does not; both slots independent.
- Mapping omits `analyzer` (expect `standard`/`default` equivalence in that slot).
- `FROM` two indices, `title` agrees (`english`), `body` disagrees → only `body`'s slot falls back.
- Keyword ON field next to a text ON field (`"keyword"` vs `"english"`).
- `TO_TEXT(..., { "analyzer": "whitespace" })` and a mapped `english` field in the same ON list.
- CCS mixed version: old remote omits the new field-caps slot → `"standard"` in that field's slot, no crash.
- `HIGHLIGHT` after `STATS` (coordinator) still uses the planned array from `EsField`, not a shard.
- Unknown name (custom) in one field → that slot `"standard"` (or the existing error if the user wrote `WITH`).

## Code map (retrieval)

| Piece | File |
|---|---|
| Field-caps per-field payload | `server/.../action/fieldcaps/IndexFieldCapabilities.java` |
| Where meta is filled today | `FieldCapabilitiesFetcher` (`ft.meta()`) |
| ES\|QL fork | `EsqlResolveFieldsAction` |
| `EsField` construction | `IndexResolver.createField` |
| Text field (no analyzer today) | `TextEsField` |
| Keyword (`normalized` flag unused for the name) | `KeywordEsField` |
| Attribute wrapping the mapping | `FieldAttribute` |
| TO_TEXT / EVAL analyzer propagation | `AnalyzedTextExpression`, `ReferenceAttribute` |
| Name → Lucene analyzer | `PlannerUtils.resolveAnalyzer` → `AnalysisRegistry.getAnalyzer` |
| Prebuilt names | `PreBuiltAnalyzers` (`standard`/`default`/`keyword`/…) |
| Language names | `CommonAnalysisPlugin#getAnalyzers` (`english`, …) |
| Mapping index analyzer (the real one) | `FieldMapper.indexAnalyzers`, `MappingLookup.indexAnalyzer` |
| DSL highlight analyzer (per field) | `SearchExecutionContext.getIndexAnalyzer`, `DefaultHighlighter` |
| DSL match analyzer (per field) | `MatchQueryParser.getAnalyzer` (search) |
| Data-node shard handle | `LocalExecutionPlannerContext.shardContexts`, `DefaultShardContext` |
| Synthetic rewrite context (today: one analyzer for every field) | `RuntimeSearchExecutionContext` |
| Config / operator to take the array | `HighlightConfig`, `HighlightOperator`, `planHighlight` |
| Mapping JSON on cluster state | `IndexMetadata.mapping()`, `MappingMetadata` |
| Companion (behavior mismatch) | `HIGHLIGHT_ANALYZER_VS_DSL.md` |
