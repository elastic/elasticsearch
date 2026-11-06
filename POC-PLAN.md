# POC plan: `TYPEOF` / `TYPED` over union-typed fields

Worktree `/Users/oleglvovitch/prompts/esql-union-typed-poc/elasticsearch`, branch `esql-union-typed-poc`,
grounded at `eb33d855e3fdcbb64a7125f1abf1209b441f529d` (= `origin/main` when written). Every claim
below was read off the implementation at that commit, not off javadoc. Where a claim rests on a
comment or was not traced, it says so.

Hard constraints: no pull request is created or proposed; everything verifies locally. Commits and
pushes to the branch are fine.

What the POC settles: whether two functions on the existing per-index conversion machinery give
union-typed fields a usable, cheap narrowing path — and whether the one thing that would make the
cheap design worth having over the expensive one (predicate pushdown) survives.

---

## 0. Findings at this commit that change the framing

Read these first; several increments below exist because of them.

**F1. Folding is not an implementation route. An evaluator is required.**
`EsPhysicalOperationProviders.fieldLoader` takes the per-type conversion expression out of the
`UnionTypeEsField` and does `new TypeConverter((EsqlScalarFunction) conversion)`. A `Literal` or a
bare `FieldAttribute` as the stored expression throws `ClassCastException` on the data node.
`TypeConverter`'s `ToEvaluator` returns the loaded block for *every* child it is asked for and its
`foldCtx()` throws `IllegalStateException("not folding")`. So each per-type expression must be an
`EsqlScalarFunction` whose `toEvaluator` (a) never calls `toEvaluator.apply` on anything but the
field child and (b) folds any constant argument itself with `FoldContext.small()` — exactly what
`FromAggregateMetricDouble.toEvaluator` does with `subfieldIndex`. `SubstituteSurrogateExpressions.rule`
only replaces `SurrogateExpression`s; it folds nothing.

**F2. The pushdown hole is by name, and it is total for synthetic union attributes.**
`LucenePushdownPredicates.from(SearchStats, …).isQueryable` for an attribute whose field is a
`UnionTypeEsField` returns `stats.isIndexed(new FieldName(attr.name())) && stats.hasDocValues(…)`.
`attr.name()` for a union attribute built by `ResolveUnionTypes.createIfDoesNotAlreadyExist` is the
synthetic `$$<field>$converted_to$<type>`; `SearchContextStats.makeFieldConfig` finds no mapped field
of that name in any shard context and returns `indexed=false, hasDocValues=false`. The
`attr.field().isAggregatable()` shortcut above it never fires because `CompactMultiTypeEsField.resolveFrom`
and `MultiTypeEsField.resolveFrom` both construct the field with `aggregatable=false`. The
value-query fallback in `isPushableValueAttribute` excludes `UnionTypeEsField` explicitly. Net: a
synthetic union attribute is never pushable, on any node, whatever the shards hold. The existing
`LocalPhysicalPlanOptimizerTests.testToDateNanosPushDown` already shows this for
`date_and_date_nanos_and_long::date_nanos >= …` (stays in `FilterExec`). `TYPED` inherits it
unchanged — Increment 2 measures it, Increment 7 is the spike that could close it, and it is sound
for `TYPED` in a way it is not for a cast (see I7).

**F3. Synthetic union attributes dedup on (name, output type), not on the function.**
`createIfDoesNotAlreadyExist` names the attribute `Attribute.rawTemporaryName(fa.name(), "converted_to", type)`
and dedups through `Attribute.IdIgnoringWrapper.equals`, which calls `FieldAttribute.innerEquals` →
`Objects.equals(field, other.field)` → `EsField.equals`, which compares name/type/aggregatable/
properties/timeSeriesFieldType and **not** the conversion expressions (`CompactMultiTypeEsField` has no
`equals` override). So in one query `TYPEOF(x)` and `TO_STRING(x)` (both KEYWORD) collapse to whichever
was created first, as do `TYPED(x, "long")` and `TO_LONG(x)`. Increment 4 fixes the name.

**F4. The explicit-cast-over-an-auto-promoted-field path rebuilds the function with one child.**
For a non-synthetic `FieldAttribute` whose field is already a `UnionTypeEsField` (only
`DateMillisToNanosInEsRelation` and `ResolveTwoLeggedPunksInEsRelation` produce these),
`resolveConvertFunction` calls `unionTypeEsField.rewrapWithCast(expr)` →
`UnionTypeEsField.replaceChildrenWithExpressionField` → `expression.replaceChildren(List.of(innerField))`.
A function with two children (field, type literal) throws there. It also first checks
`convert.isNoop()`, whose default is `field().dataType() == dataType()`; for `TYPED(ts, "date_nanos")`
over a `{datetime, date_nanos}` field promoted to `date_nanos` that returns the promoted union — every
row converted — which is the opposite of "values whose mapped type is date_nanos". Both are design
decisions in §1, not surprises to discover.

**F5. The all-or-nothing refusal is exactly `supportedTypes()` coverage — verified.**
`resolveConvertFunction`: `tcf.types().forEach(type -> if (supportedTypes.contains(type.widenSmallNumeric())) typeResolutions(...))`
then `if (typeResolutions.size() == tcf.getTypesToIndices().size())`. A function whose `supportedTypes()`
is every `DataType` never trips it. There is no other gate on the first branch except
`FoldablesConvertFunction` (rejected), `ToText` with an analyzer option, and `ToGauge` no-op stripping.

**F6. Per-type expressions are pinned to the raw mapped type, and need not be `ConvertFunction`s.**
`typeSpecificConvert` builds the per-type field with `typedEsField(type, tcf)` where `type` iterates
`tcf.types()` unwidened (a `short` leg is `SHORT`). `ImplicitCastAggregateMetricDoubles.countConvert`
stores a plain `MvCount` (a `UnaryScalarFunction`) as a per-type expression — so the only contract on
the stored expression is F1's `EsqlScalarFunction`. `UnionTypeEsField.resolve` throws
`"Resolved data type mismatch"` if the per-type expressions disagree on `dataType()`.

**F7. The dataset path cannot be reached by these functions, by construction.**
`ResolveUnionTypes.doRule` collects union attributes only `if (plan instanceof EsRelation rel)`;
`resolveConvertFunction` only acts on `FieldAttribute` whose field is a `TypeConflictedField`;
`SchemaReconciliation.reconcileUnionByName` emits `new ReferenceAttribute(...)` per unified column and
nothing under `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources` references
`TypeConflictedField` or `InvalidMappedField` (grep, zero hits). Datasets convert on the data node
through `ColumnMapping` casts in `SchemaAdaptingIterator`, not through `EsPhysicalOperationProviders`.
§4 proposes the mechanism.

**F8. The one-way-cast skip is forgone in `ColumnMapping.mapFilters`, not `mapPage`.**
`mapFilters` drops a pushed conjunct for any column whose cast target has `inferFileType(castTo) == null`;
`inferFileType` returns `INTEGER` for a `LONG` target and `null` for everything else. So every
`union_by_name` cast except LONG-over-INTEGER loses file/row-group skipping and relies on the
re-check on the unified page. The KEYWORD fallback is the case that also destroys ordering.

**F9. There is no per-column inference skip — verified.**
`ExternalSourceResolver.isDeclaredSchema` is `declaredMapping.mappings().dynamic() == Dynamic.FALSE`
and routes the whole dataset to `resolveStrictMultiFileAsync`; otherwise
`DeclaredSchemaResolver.overlayNonStrict(inferred.schema(), declaredMapping, …)` takes the inferred
schema as input. The plan builds on no inference saving.

**F10. Registration costs: capability, snapshot gate, and a test-existence gate.**
`EsqlFunctionRegistry.addCapabilities` emits `fn_<name>` for every registered function (and
`fn_<name>_<cap>` for `Builder.capabilities`). A csv-spec test therefore gates on
`required_capability: fn_typeof` with no `EsqlCapabilities.Cap` entry (precedent: `fn_absent` in the
fixtures). Functions in `EsqlFunctionRegistry.snapshotFunctions()` get `fn_<name>` enabled only in
snapshot builds (`Delay`, `TStep`, `Magnitude`); local gradle test builds are snapshot builds.
`EsqlFunctionRegistryTests.testRegisteredFunctionHaveTests` iterates `snapshotRegistry().listFunctions()`
and requires `<Name>Tests` (an `AbstractFunctionTestCase`), `<Name>ErrorTests` when the function takes
parameters, and `<Name>SerializationTests` unless the class is `OnlySurrogateExpression`,
`FoldablesConvertFunction` or `InferenceFunction`; its allowlist comment says adding entries "while
working on something" is fine.

**F11. `fullyLoadedAnalyzer` is `@Deprecated` in code**; analyzer-level tests use
`EsqlTestUtils.analyzer().addIndex(IndexResolution)`, with `TestAnalyzer.query` / `.error` /
`.statementError`. (AGENTS.md lists these; their javadocs were read; the plan relies on the code.)

**F12. Fixtures.** `CsvTestsDataLoader` gives these conflicts and no others relevant here:
`client_ip` `{ip, keyword}` via `sample_data` + `sample_data_str`; `@timestamp` `{date, long}` via
`sample_data` + `sample_data_ts_long`; `{date, date_nanos}` via `sample_data` + `sample_data_ts_nanos`;
three-way `{date, date_nanos, long}`. There is **no** `{long, keyword}` index fixture, so the brief's
`COALESCE(TYPED(flow, long), TO_LONG(TYPED(flow, keyword)))` is run as
`COALESCE(TYPED(@timestamp, "date_nanos"), TO_DATE_NANOS(TYPED(@timestamp, "long")))` over
`sample_data_ts_nanos, sample_data_ts_long`, and the "type with no conversion rule" case as
`TO_LONG(client_ip)` (refused: `ip` has no rule) versus `LENGTH(TYPED(client_ip, "keyword"))` (accepted).
`AnalyzerTestUtils.indexWithDateDateNanosUnionType()` builds `date_and_date_nanos` and
`date_and_date_nanos_and_long` as `InvalidMappedField`s for analyzer/physical tests. For datasets,
`employees_multifile_type_drift` (Parquet; `code` INTEGER in one file, KEYWORD in the other) is
exercised by `parquetUbnFilterTypeDriftedColumn` in `parquet-multifile.csv-spec`.

**F13. The two regression guards, as implemented.**
`ImplicitCastAggregateMetricDoubles.doRule` rewrites `UnaryAggregateFunction` / `TimeSeriesAggregateFunction`
whose field is a bare `FieldAttribute` over a `TypeConflictedField` with `canBeCast` (types contain
`AGGREGATE_METRIC_DOUBLE` and the rest numeric); the metric is chosen by `getMetric` for
`AggregateMetricDoubleNativeSupport` implementors (`Min/Max/Sum/Count/Avg/Present/Absent/Sparkline`
and the `*OverTime` variants) and `DEFAULT` for every other unary aggregate; `Avg` becomes `Div(Sum, Count)`.
It keys on the aggregate's field being a plain attribute, so wrapping it in `TYPED` bypasses it (then
`TYPED` resolves instead). `DateMillisToNanosInEsRelation.rewrite` promotes a `TypeConflictedField` whose
`types()` are all `DATETIME`/`DATE_NANOS` (`DataType.isDate`) and, under an unmapped-loading mode, not
potentially unmapped, into a non-synthetic `UnionTypeEsField` via `ToDateNanos`. Neither reads a
convert function's `supportedTypes()`, so the new functions cannot change what they accept; what they
*can* affect is F4's rewrap path, which both increments below pin with tests.

**F14. Scope limit, as implemented.** `doRule` sets `state.isAfterAggregate = true` after an `Aggregate`
and returns the plan untouched above it; `UnionTypesCleanup.cleanTypeConflicts` then turns any leftover
`TypeConflictedField` into an `UnsupportedAttribute` via `FieldAttribute.flagTypeConflicts`
("Cannot use field [x] due to ambiguities …"). So both functions work only between the scan and the
first `STATS`. `KEEP x` on an unresolved conflict returns `x:unsupported` with nulls
(`multiIndexSortIpString` in `union_types.csv-spec`).

---

## 1. Design decisions (made here so the increments do not re-litigate them)

**D1. Base class: `EsqlScalarFunction implements ConvertFunction, AnyNullIsNull`** — the shape of
`FromAggregateMetricDouble`, not `AbstractConvertFunction` (whose `factories()` map is keyed per
source type and whose evaluator closes the child block, which the identity branch must not do), and
not `OnlySurrogateExpression` (F1: a surrogate that returned a literal or the field would break the
data-node cast; a surrogate to another function buys nothing). `ToIp`/`ToLongSurrogate`/`ToIntegerSurrogate`
are surrogate wrappers that resolve to `AbstractConvertFunction`s before serialization — not the model
here.

**D2. Type argument of `TYPED` is a keyword string literal: `TYPED(x, "long")`**, resolved with
`DataType.fromTypeName` at construction and held as a **non-child** `DataType` property. Two-child
functions break F4's `replaceChildren(List.of(field))`; a non-child property survives it
(`NodeInfo.create(this, Typed::new, field, typeLiteral)` with `replaceChildren` replacing only the
field). No grammar change: a bare `long` would need parser work the POC does not want. Serialization:
`DataType.writeTo` as `CompactMultiTypeEsField.writeTo` does.

**D3. `isNoop()` returns `false` for both functions** (precedent `FromAggregateMetricDouble.isNoop`).
F4 otherwise makes `TYPED(ts, "date_nanos")` over an auto-promoted field return every row.

**D4. `supportedTypes()` is `DataType.types()` (all)**, `dataType()` is the target type for `TYPED`
and `KEYWORD` for `TYPEOF`. `resolveType` uses `TypeResolutions.isTypeOrUnionType(field, t -> true, …)`
— its union branch requires `tcf.types().stream().allMatch(predicate)`, trivially true.

**D5. Evaluator semantics, decided at factory-construction time from `field().dataType()`** (which
`typeSpecificConvert` pins per leg, F6; the same code serves the ordinary `EVAL` path on a
single-typed field):
- `TYPED`: `sameType(field.dataType(), target)` → identity: `eval(page)` returns the child block
  **without closing it** (ownership passes through, as `TypeConverter.LoadBlockForConversion` and
  the `AbstractConvertFunction` evaluator's `try (Block block = next().eval(page))` contract imply:
  the parent owns the child's result). Otherwise → close the child block, return
  `blockFactory.newConstantNullBlock(positionCount)`.
- `TYPEOF`: close the child block, return a `BytesRef` block holding `field.dataType().typeName()`
  where the child is non-null and null where it is null. (Alternative — a constant block regardless of
  nulls — is cheaper; the POC takes the null-mirroring one so `WHERE TYPEOF(x) == "long"` reads as
  "has a long value". Note it in the csv-spec header; it is a one-line change either way.)

**D6. "Exact" is defined over `widenSmallNumeric()`**: `sameType(a, b)` is
`a.widenSmallNumeric() == b.widenSmallNumeric()`. Reason: F6 pins the leg to the raw mapped type
(`SHORT`), while `resolveConvertFunction` itself keys support on `type.widenSmallNumeric()` and a
`short` loads as an `IntBlock`; treating `TYPED(x, "integer")` as null on a `short` leg would be a
trap nobody asked for. `integer` vs `long` stays **exact** in the POC (null for the integer leg);
Increment 5 documents it with a test so Oleg's "assignable" decision is answerable, and the plan
does not pin assignability to `TypeWidening.widenLossless` (its `widenOrdered` includes
`DATETIME→DATE_NANOS`, which `ToDateNanos` can null per value).

**D7. Registration under `EsqlFunctionRegistry.snapshotFunctions()`**, names `typeof` / `typed`
(neither exists in the registry or the grammar at this commit). Tests gate on
`required_capability: fn_typeof` / `fn_typed`. `testRegisteredFunctionHaveTests` is satisfied by
allowlist entries in the first increment (throwaway) and real test classes if the POC survives.

**D8. Synthetic attribute name carries the function** (fixes F3): `ConvertFunction` gets a default
`String unionAttributeDiscriminator()` returning `"converted_to"`; `Typeof`/`Typed` return
`"typeof"` / `"typed"`; `createIfDoesNotAlreadyExist` uses it. Existing names are unchanged, so no
golden file moves.

**Shippable vs throwaway.** Shippable shape: D1–D6, D8, the evaluator classes, the csv-spec tests,
the `LocalPhysicalPlanOptimizerTests` cases. Throwaway: the allowlist entries (D7), the pushdown
spike (I7) as written, the dataset conflict marker (I8) in the form proposed.

---

## 2. Increments (each independently committable)

Run `./gradlew :x-pack:plugin:esql:spotlessApply` before every commit. Commit messages: 50-column
summary, no attribution trailers (AGENTS.md).

### I1 — `TYPED` skeleton: function, evaluator, registration

**Changes**
- New `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/expression/function/scalar/convert/Typed.java`:
  D1/D2/D3/D4/D5/D6. `ENTRY` + `getWriteableName`/`writeTo`/stream constructor; `DEFINITION =
  FunctionDefinition.def(Typed.class).binary(Typed::new).name("typed")`. `@FunctionInfo` with `returnType`,
  `briefSummary`, `description` and `@Param`s, and no `appliesTo` (the snapshot-only `Delay` declares
  none) — the registry reads the annotation off the constructor in `EsqlFunctionRegistry.functionInfo`.
- `ScalarFunctionWritables`: add `Typed.ENTRY` next to `FromAggregateMetricDouble.ENTRY`.
- `EsqlFunctionRegistry.snapshotFunctions()`: add `Typed.DEFINITION`.
- `EsqlFunctionRegistryTests.testRegisteredFunctionHaveTests`: add allowlist items for
  `…convert.TypedTests`, `TypedErrorTests`, `TypedSerializationTests` (throwaway; replaced in I6).

**Question answered:** does a `ConvertFunction` declaring every type resolve a conflicted field
without the all-or-nothing refusal (F5), and does the stored per-type expression survive the data-node
path (F1)?

**Verify**
```
./gradlew :x-pack:plugin:esql:test --tests "org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistryTests"
./gradlew :x-pack:plugin:esql:test --tests "org.elasticsearch.xpack.esql.analysis.AnalyzerTests.testTypedResolvesConflictedField"
```
`testTypedResolvesConflictedField` (new, modelled on `testProjectionForUnionTypeResolution`): build an
`EsIndex` with `InvalidMappedField("id", {keyword→union_index_1, integer→union_index_2})`, analyze
`FROM union_index* | EVAL t = TYPED(id, "integer")`, assert the `Eval` holds a synthetic
`FieldAttribute` whose field is a `UnionTypeEsField`, whose `getConversionExpressions()` are two `Typed`
instances with `field().dataType()` `KEYWORD` and `INTEGER`, and whose `dataType()` is `INTEGER`.
Then the first end-to-end run, before any spec file exists:
```
./gradlew ":x-pack:plugin:esql:internalClusterTest" --tests "org.elasticsearch.xpack.esql.CsvIT.*union_types*multiIndexIpString*"
```
(an existing test; proves the build and the fixtures are healthy before I3 adds cases).

**If no:** the analyzer test tells you which branch of `resolveConvertFunction` rejected it; the
only plausible one is `skipMultiTypeForPotentiallyUnmappedKeyword`, which cannot fire while
`supportedTypes` contains `KEYWORD`. If `UnionTypeEsField.resolve` throws "Resolved data type
mismatch", `dataType()` is not constant across legs — it must be the target, never the field type.

### I2 — Pushdown measurement (the day-one result)

**Changes**
- `LocalPhysicalPlanOptimizerTests`: new `testTypedFilterIsNotPushedDown`, copied from
  `testToDateNanosPushDown` (same `indexWithDateDateNanosUnionType()`, same
  `EsqlTestUtils.statsForExistingField("date_and_date_nanos", "date_and_date_nanos_and_long")`), query
  ```
  from index*
  | where TYPED(date_and_date_nanos_and_long, "long") > 5
  ```
  Print the plan (`plan.toString()` to stdout under `-Dtests.output=always`) and assert: the `FilterExec`
  condition is a `GreaterThan` over a `FieldAttribute` with a `UnionTypeEsField`, the `EsQueryExec.query()`
  is `null` (no pushed query), and a `FieldExtractExec` for the union attribute sits between them.
- A sibling `testTypedFilterOnSingleTypedFieldIsPushedDown`: `from test | where TYPED(emp_no, "integer") > 5`
  on the default mapping. Expected: **not** pushed either — `TYPED` is an ordinary scalar over a plain
  field, nothing translates it. Record this; it is the baseline the I7 spike must beat.

**Question answered:** Q3 — does `WHERE TYPED(x, T) > 5` push down? Expected answer at this commit:
no, for the F2 reason (name), and the test prints the plan that proves it.

**Verify**
```
./gradlew :x-pack:plugin:esql:test --tests "org.elasticsearch.xpack.esql.optimizer.LocalPhysicalPlanOptimizerTests.testTypedFilter*" -Dtests.output=always
```

**If no (it pushes):** something other than F2 is in play — read the pushed `QueryBuilder`: a range
on the real field name over a `{date, date_nanos, long}` field would be *wrong* on the date legs, and
that is a correctness finding, not a win.

**Decision gate:** if I7 later cannot recover pushdown for the single-type-per-node case, the cheap
design's advantage over the expensive one is only "no new type"; say so in the final report rather
than continuing to polish.

### I3 — `TYPEOF`, and the csv-spec answers to Q1 and Q2

**Changes**
- New `Typeof.java` (D1/D3/D4/D5), `ENTRY` in `ScalarFunctionWritables`, `DEFINITION` in
  `snapshotFunctions()`, allowlist entries as in I1.
- New spec file `x-pack/plugin/esql/qa/testFixtures/src/main/resources/union_types_typed.csv-spec`
  (a new file rather than appending to `union_types.csv-spec`, so the whole POC surface is one
  `git rm` away). Header comment states D5's null-mirroring choice. Every test carries
  `required_capability: fn_typeof` / `fn_typed` as needed (F10) plus `union_types`. Cases:
  1. `FROM sample_data, sample_data_str METADATA _index | EVAL t = TYPEOF(client_ip) | STATS c = COUNT(*) BY _index, t | SORT _index`
     → `sample_data | ip | 7`, `sample_data_str | keyword | 7`.
  2. `FROM sample_data, sample_data_str | EVAL ip = TYPED(client_ip, "ip"), k = TYPED(client_ip, "keyword") | WHERE ip IS NOT NULL OR k IS NOT NULL | STATS ips = COUNT(ip), ks = COUNT(k)` → `7 | 7`.
  3. The narrowing from the brief, on the fixtures that exist (F12):
     `FROM sample_data_ts_nanos, sample_data_ts_long | EVAL ts = COALESCE(TYPED(@timestamp, "date_nanos"), TO_DATE_NANOS(TYPED(@timestamp, "long"))) | STATS mn = MIN(ts), mx = MAX(ts), c = COUNT(ts)`
     → 14 rows counted, min/max matching `sample_data.csv`'s first/last timestamps in nanos.
  4. The "type with no conversion rule" contrast: `FROM sample_data, sample_data_str | EVAL l = TO_LONG(client_ip)`
     is already refused at this commit (the `ip` leg has no `long` rule); add
     `FROM sample_data, sample_data_str | EVAL n = LENGTH(TYPED(client_ip, "keyword")) | STATS c = COUNT(n)` → `7`.
  5. Three-way field, mention two types: `FROM sample_data, sample_data_ts_nanos, sample_data_ts_long | EVAL t = TYPEOF(@timestamp) | STATS c = COUNT(*) BY t | SORT t`
     → `date 7`, `date_nanos 7`, `long 7`.
  6. Auto-promoted field, F4 path: `FROM sample_data, sample_data_ts_nanos | EVAL t = TYPEOF(@timestamp) | STATS c = COUNT(*) BY t | SORT t`
     → `date 7`, `date_nanos 7` (**not** `date_nanos 14`): `rewrapWithCast` replaces the `ToDateNanos`
     leg with `TYPEOF(original leg)`, so the original mapped types come back. And
     `EVAL d = TYPED(@timestamp, "datetime") | STATS c = COUNT(d)` → `7` (D3 makes this exact).
  7. After an aggregate (F14): `FROM sample_data, sample_data_str | STATS c = COUNT(*) BY client_ip | EVAL t = TYPEOF(client_ip)`
     → expected **error** containing `Cannot use field [client_ip] due to ambiguities`. Document the
     limit in the spec; do not try to lift it.
- `AnalyzerTests.testTypeofOverTwoLeggedPunk` (unit, no data): with `SET unmapped_fields` in loading
  mode and a single-type potentially-unmapped field, assert `getUnmappedConversionExpression()` is a
  `Typeof` over a `KEYWORD` leg — the unmapped docs report `keyword`, by `typeSpecificConvert(convert, KEYWORD, tcf)`.
  If building that fixture costs more than an hour, drop it and note it.

**Question answered:** Q1 and Q2.

**Verify**
```
./gradlew ":x-pack:plugin:esql:internalClusterTest" --tests "org.elasticsearch.xpack.esql.CsvIT.*union_types_typed*"
./gradlew :x-pack:plugin:esql:test --tests "org.elasticsearch.xpack.esql.analysis.AnalyzerTests.testTypeof*"
```
A `Capability is not included in the enabled list` assertion means the `fn_` name is misspelled or
the function is not in `snapshotFunctions()` (`CsvTestUtils.checkTestCapabilities`).

**If no:** case 6 failing with `IndexOutOfBoundsException` from `replaceChildren` means D2 was not
followed (type literal became a child). Case 6 returning `date_nanos 14` means D3 was not applied.
Wrong counts in case 1 with correct types means the D5 null-mirroring branch is inverted.

### I4 — Synthetic-name collision (F3)

**Changes**
- `ConvertFunction`: `default String unionAttributeDiscriminator() { return "converted_to"; }`;
  `Typeof`/`Typed` override.
- `Analyzer.ResolveUnionTypes.createIfDoesNotAlreadyExist`: use it in place of the literal.
- `AnalyzerTests.testTypeofAndToStringAreDistinctUnionAttributes`: analyze
  `FROM union_index* | EVAL a = TYPEOF(id), b = id::keyword, c = TYPED(id, "keyword")` on the I1
  fixture and assert three distinct synthetic attribute names, `$$id$typeof$keyword`,
  `$$id$converted_to$keyword`, `$$id$typed$keyword`.
- Append to `union_types_typed.csv-spec`: `FROM sample_data, sample_data_str | EVAL t = TYPEOF(client_ip), s = TO_STRING(client_ip) | WHERE t == "ip" | STATS c = COUNT(*), d = COUNT_DISTINCT(s)`
  → `7 | 3` (without the fix `s` would silently be `t`, giving `d = 1`).

**Question answered:** can both functions coexist with the existing casts in one query.

**Verify**
```
./gradlew :x-pack:plugin:esql:test --tests "org.elasticsearch.xpack.esql.analysis.AnalyzerTests.testTypeofAndToString*"
./gradlew ":x-pack:plugin:esql:internalClusterTest" --tests "org.elasticsearch.xpack.esql.CsvIT.*union_types_typed*"
./gradlew :x-pack:plugin:esql:test --tests "org.elasticsearch.xpack.esql.analysis.*" --tests "org.elasticsearch.xpack.esql.optimizer.*Golden*"
```
The last line is the regression net for the rename: existing names must not move (no golden
`.expected` file should change; `git status` after the run is the check).

**If no:** if a golden file changes, the default-method route altered an existing name — revert to a
`convert instanceof Typeof || convert instanceof Typed` check in `createIfDoesNotAlreadyExist` (uglier,
but the POC only needs the collision gone).

### I5 — Exact vs assignable, made answerable (Q4)

**Changes**
- `AnalyzerTests.testTypedIsExactOverIntegerAndLongLegs`: fixture `InvalidMappedField("n", {integer→i1, long→i2})`;
  analyze `EVAL l = TYPED(n, "long")`; assert the per-type expression for the `integer` leg is a
  `Typed` whose `field().dataType()` is `INTEGER` and whose `toEvaluator` — exercised directly with an
  `IntBlock` of three values through a `DriverContext` from `TestBlockFactory` — returns a
  `ConstantNullBlock`; the `long` leg's returns the input block instance. A second method
  `testTypedTreatsShortAsInteger` asserts the D6 widening (`short` leg, `"integer"` target → identity).
- In the spec header of `union_types_typed.csv-spec`, one paragraph: "exact over `widenSmallNumeric`;
  `integer` is not `long`; assignability undecided — flip `Typed.sameType` and this test to change it."

**Question answered:** Q4 is not decided here; the test is the place the decision lands.

**Verify**
```
./gradlew :x-pack:plugin:esql:test --tests "org.elasticsearch.xpack.esql.analysis.AnalyzerTests.testTyped*"
```

### I6 — Replace the allowlist with real function tests (shippable shape only)

Do this only if I2/I7 leave the approach alive. `TypedTests` / `TypeofTests` extending
`AbstractFunctionTestCase` modelled on `FromAggregateMetricDoubleTests` (`build(source, args)` with
`args.get(1)` a forced literal; `TestCaseSupplier.TypedData(...).forceLiteral()`), `TypedErrorTests` /
`TypeofErrorTests` extending `ErrorsForCasesWithoutExamplesTestCase`, and the two
`*SerializationTests`. Remove the I1/I3 allowlist items.

**Verify**
```
./gradlew :x-pack:plugin:esql:test --tests "org.elasticsearch.xpack.esql.expression.function.scalar.convert.Type*"
./gradlew :x-pack:plugin:esql:test --tests "org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistryTests"
```
Expect `AbstractFunctionTestCase` to probe every type pair; `TYPED` over a single-typed field is a
plain function (D5), so expected values are "the input" when `sameType` and `null` otherwise.

### I7 — Pushdown spike (throwaway): per-node, type-exact pushdown for `TYPED`

**Why this is sound for `TYPED` and not for a cast.** On a shard that maps `x` as `T`, `TYPED(x, T)`
*is* `x`; on a shard that maps it as anything else, every value is null, so any comparison is false.
A cast converts values, so pushing `x > 5` on a keyword-mapped shard asks Lucene a different question.
`SearchContextStats.isQueryable(SearchExecutionContext, String, DataType, boolean)` already evaluates
`dataType(fieldType) == type` per shard context, and the public `isQueryable(FieldName, DataType, boolean)`
returns false if any context disagrees — so "every shard on this node maps `x` as `T`" is one existing
call away, by the *real* field name (`FieldAttribute.fieldName()`, which for a synthetic attribute
resolves to the underlying `EsField` name).

**Changes (all behind `ble instanceof Typed`-style checks so nothing else moves)**
- `LucenePushdownPredicates.from(SearchStats, …).isQueryable`: when `attr.field()` is a
  `UnionTypeEsField` whose conversion expressions are all `Typed` with the same target
  `T`, return `stats.isQueryable(attr.fieldName(), T, false)` instead of the name-based check.
- `isPushableFieldAttribute` already requires `getExactInfo().hasExact()` (true: `EsField.getExactInfo`
  default) and `isQueryable`; `isPushableValueAttribute`'s union exclusion stays.
- Wherever the translator takes the field name for a `FieldAttribute` (follow
  `PushFiltersToSource` → `EsqlExpressionTranslators`; the test will show which call sees `name()`),
  make sure it is `fieldName()` for this attribute — `testToDateNanosPushDown` shows the auto-promoted
  case already pushes by the real name, so the non-synthetic path is the template.
- Not in scope for the spike: the "no shard maps it as `T`" → `match_none` shortcut.

**Verify**
```
./gradlew :x-pack:plugin:esql:test --tests "org.elasticsearch.xpack.esql.optimizer.LocalPhysicalPlanOptimizerTests.testTypedFilter*" -Dtests.output=always
```
with I2's test changed to expect a pushed `rangeQuery("date_and_date_nanos_and_long").gt(5)` when the
stats say every shard is `long`, and the `FilterExec` form when they do not. `SearchStats.isQueryable(FieldName, DataType, boolean)`
defaults to `hasDocValues(field)` and `TestSearchStats` does not override it, so the stock stub says
"queryable" for any type; the test needs a one-off `TestSearchStats` subclass whose `isQueryable`
returns true only for `(date_and_date_nanos_and_long, LONG)` in the pushed case and false in the mixed
case. End-to-end:
```
./gradlew ":x-pack:plugin:esql:internalClusterTest" --tests "org.elasticsearch.xpack.esql.CsvIT.*union_types_typed*"
```
must still pass (a single-node test cluster holds both `sample_data` and `sample_data_str`, so no push
happens there and results cannot change; a pushed-and-wrong result would need a two-node layout, which
`CsvIT` does not give — say so in the report rather than claiming the end-to-end case covered it).

**If no:** if the only blocker is the translator reading `name()`, that is a small, shippable fix.
If the blocker is that `LocalPhysicalPlanOptimizer` has no per-shard type view beyond `SearchContextStats`,
the cheap design's pushdown story needs the data-node planner to split by mapping — which is most of
what the expensive design builds. Report that as the finding.

### I8 — Dataset half (see §4 for the design): surface the keyword fallback as a conflict

**Changes (spike, throwaway in this form)**
- `SchemaReconciliation.reconcileUnionByName`: where `widenToCommonOrKeyword` would yield `KEYWORD`
  for a column with a non-string contributor (the exact condition `emitKeywordFallbackWarnings` tests:
  unified type `KEYWORD` and `ColumnContributions.hasNonStringContributor()`), keep the column but
  mark it: unified attribute typed `UNSUPPORTED`, and a new `Result.conflicts()`
  `Map<String, Map<StoragePath, DataType>>` built from `contributions`. Numeric promotions and
  `DATETIME→DATE_NANOS` go through `TypeWidening.join` untouched. `computeMapping` already produces
  `casts[i] = null` when file type equals unified type and a cast otherwise; for a conflict column
  make the per-file slot `-1` (null-fill) so a projection returns nulls, matching the index path.
- `emitKeywordFallbackWarnings`: becomes a "column [code] has conflicting types …; use TYPED/TYPEOF
  or set [schema_resolution] to [strict]" warning on the same sink.
- `reconcileStrict`: unchanged (`validateStrictTypeMatch` already throws).
- FIRST_FILE_WINS: unchanged; `completeFirstFileWins` never reconciles types, non-anchor files are read
  under the anchor's schema (`SchemaReconciliation.pinnedOver` / `sharedReadContract`), and a value
  the anchor type cannot hold is a per-value coercion failure under the dataset's `ErrorPolicy`
  (`BlockDecoder#coercionFailure` in the NDJSON decoder). Not traced for every format — state as such.
- Tests: `SchemaReconciliationTests.testUnionByNameIntegerVsKeywordSurfacesConflict` (unified type
  `UNSUPPORTED`, `conflicts()` has both files with their types, no "read as [keyword]" on the sink) and
  `testUnionByNameLongDoubleStillWidensToDouble` (unchanged behaviour, next to the existing
  `testUnionByNameLongToDoubleWidensToDouble`). Update `parquetUbnMixedRepresentationMinMax` and
  `parquetUbnFilterTypeDriftedColumn` in `parquet-multifile.csv-spec` to the new expectation
  (`code:unsupported`, nulls; the `MIN/MAX` case becomes an error, as on the index path).

**Question answered:** can the harmful fallback be replaced by a surfaced conflict without touching
the numeric promotions.

**Verify**
```
./gradlew :x-pack:plugin:esql:test --tests "org.elasticsearch.xpack.esql.datasources.SchemaReconciliationTests"
./gradlew :x-pack:plugin:esql-datasource-parquet:qa:csvSpecTests --tests "org.elasticsearch.xpack.esql.qa.parquet.ParquetFormatSpecIT.*parquetUbn*"
```
`ParquetFormatSpecIT` loads `/parquet-*.csv-spec` and cross-products with storage backends; if a
backend needs credentials you do not have locally, run the local-filesystem variant only and name
the ones skipped.

**Not in I8, by design:** `TYPEOF`/`TYPED` *over a dataset column*. §4 says how; it is a second
plumbing job and the plan does not pretend it is done.

---

## 3. Order and the day-one stop rule

I1 → I2 (same day). If I2 shows the F2 hole and I7's first hour shows no per-node type view beyond
`SearchContextStats`, stop and report. Otherwise I3 → I4 → I5 → I7 → I8 → I6. I8 can run in parallel
with I3–I5; it touches no analyzer code.

---

## 4. Dataset half: the gap and how to close it

**The gap, plainly.** `TYPEOF`/`TYPED` resolve only through `ResolveUnionTypes`, which only reads
`EsRelation`s and only rewrites `FieldAttribute`s over `TypeConflictedField`s (F7). A dataset's
`ExternalRelation` carries `ReferenceAttribute`s whose types were fixed before planning by
`SchemaReconciliation` (`reconcileUnionByName` → `TypeWidening.join` → `KEYWORD` fallback;
`reconcileStrict` → refusal). Nothing on the dataset read path consumes a `UnionTypeEsField`: datasets
convert through `ColumnMapping` casts in `SchemaAdaptingIterator`, keyed per file.

**Two ways to close it.**
- *Make datasets produce a conflict field.* `ExternalRelation` would have to emit `FieldAttribute`s
  over an `EsField`, `ResolveUnionTypes` would have to treat `ExternalRelation` like `EsRelation`, and
  the dataset data-node path would need a consumer of the per-type conversion expressions — a second
  `TypeConverter`-shaped seam inside `SchemaAdaptingIterator`. Three new seams for a path that already
  has per-file type knowledge.
- *Give it its own mechanism on `ColumnMapping`* (recommended). The per-file type is already in
  `FileSchemaInfo.fileSchema()`, and `ColumnMapping` already expresses "column absent in this file →
  null-fill" (`globalToLocal[i] == -1`) and "identity" (`casts[i] == null`). `TYPED(c, T)` over a
  conflict column is, per file, *identity* where the file type is `T` and *absent* elsewhere;
  `TYPEOF(c)` is a per-file constant. So the analyzer-side rewrite is: a conflict column becomes an
  `UNSUPPORTED` `ReferenceAttribute` (I8), and a new rule — before `ResolveUnionTypes`, acting on
  `ExternalRelation` — replaces `TYPED(c, T)` with a fresh `ReferenceAttribute` `$$c$typed$T` that
  `ExternalSourceResolver`/`ColumnMapping` materialize per file as identity-or-absent, and `TYPEOF(c)`
  with `$$c$typeof$keyword` materialized as a constant column per file. Filter pushdown then falls out
  of `ColumnMapping.mapFilters`: a conjunct on `$$c$typed$T` references a column that is "missing" in
  files of other types, which `mapFilters` already drops (the file is still read and re-checked — the
  skip-the-file shortcut is a later optimization), and is pushed unchanged in files where it is
  identity, with no one-way cast to withhold (F8).

**Modes.** `strict` — `reconcileStrict` throws on the first type mismatch and on any within-file
widening; a conflict is a refusal and stays one. `union_by_name` — the only mode that folds to
`KEYWORD`; I8 changes exactly that fold and nothing else (`widenOrdered` promotions still apply).
`first_file_wins` — no reconciliation; the anchor's types are the schema and other files are read
under them; a mismatched value is a per-value error-policy event, not a schema conflict; unchanged.

**Mixed-type JSON array inside one NDJSON row.** `NdJsonSchemaInferrer.inferValueSchema` walks array
elements through `addType` / `addLongType`, each of which sets `runningType = TypeWidening.join(previous, type)`
and records a `Widening` when it lands on `KEYWORD`. So `[1, "a"]` makes the *file's* column `KEYWORD`
during inference, before reconciliation sees it: under `strict` it is refused by
`validateNoWithinFileWidening`; under `union_by_name` the per-file type is already `KEYWORD` and a
file-granular conflict cannot tell a stringified array from a real string column. The mechanism above
is per file; a per-row conflict needs per-value type tags — the expensive design's block — and is out
of scope here. Say that in the report.

**No inference saving** (F9): the plan claims none.

---

## 5. Known limits the plan accepts

- Both functions work only below the first `Aggregate` (F14); `FORK`/`UNION ALL` branches go
  through `ResolveUnionTypesInUnionAll`, untested here.
- `TYPED` over a PUNK (single mapped type, partially unmapped) under a loading `unmapped_fields` mode
  gets a `KEYWORD` unmapped leg through `typeSpecificConvert(convert, KEYWORD, tcf)`; `TYPED(x, "long")`
  is null for those docs and `TYPEOF` says `keyword`. Documented, not changed.
- The FFW per-value coercion path for Parquet/ORC was not traced (named in I8).
- `MultiTypeEsField` (pre-`compact_multi_type_es_field` transport versions) carries the same
  expressions keyed by index; the POC does not run under older versions.

---

## 6. What the POC will have proven or disproven, and the decision for Oleg

**Proven if I1–I5 pass:** a union-typed field can be narrowed per leg with two functions and no new
type, element type, block, `BlockHash` or transport version; the all-or-nothing refusal is a
`supportedTypes()` artefact, not a property of the machinery; a leg with no conversion rule becomes
null rather than a refused query; the existing per-index conversion path evaluates the functions on
the data node with the `FromAggregateMetricDouble` shape; `TYPEOF` on an auto-promoted date field
reports the original mapped types; the two regression guards are untouched because neither reads a
convert function's `supportedTypes()`.

**Measured by I2 and settled by I7:** whether `WHERE TYPED(x, T) …` can push down. At this commit it
cannot (F2). If I7 recovers it for the all-shards-agree-per-node case by routing through
`SearchContextStats.isQueryable(fieldName, T, …)`, the cheap design has a pushdown story the plain
cast does not and never will (a cast changes the question). If I7 fails for want of a per-shard type
view, the cheap design is "no new type, same pushdown hole", and the expensive design's per-row tags
are the only route to pushing on the mixed node.

**Disproven, or bounded, for datasets:** the functions do not reach datasets as designed (F7); the
harmful `KEYWORD` fallback can be replaced by a surfaced conflict in `union_by_name` alone (I8), with
numeric promotions intact; wiring `TYPED`/`TYPEOF` over a dataset column is a separate `ColumnMapping`
job (§4), and row-level mixed arrays are below the mechanism's granularity in either design short of
per-value tags.

**The decision handed back:** (1) exact vs assignable for `TYPED` — the I5 test is where the answer
lands; (2) whether per-node type-exact pushdown (I7) is enough, or the mixed-node case justifies the
expensive branch; (3) whether the dataset half proceeds on `ColumnMapping` (§4) or waits for the
index-side verdict.
