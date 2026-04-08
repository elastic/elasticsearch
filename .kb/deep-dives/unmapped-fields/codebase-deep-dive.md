# Unmapped Fields in ES|QL — Codebase Deep Dive

## 1. The SET Command: Parsing and Storage

### Grammar and Parsing

The `SET` command is parsed in **`LogicalPlanBuilder.java`**. Each `SET` clause produces a `QuerySetting` record:

- **File**: `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/parser/LogicalPlanBuilder.java:181`
  ```java
  public QuerySetting visitSetCommand(EsqlBaseParser.SetCommandContext ctx) {
      return new QuerySetting(source(ctx), visitSetField(ctx.setField()));
  }
  ```

- Multiple `SET` statements are collected into a list within `visitStatements` (line 162-164):
  ```java
  for (EsqlBaseParser.SetCommandContext setCommandContext : ctx.setCommand()) {
      settings.add(visitSetCommand(setCommandContext));
  }
  ```

The result is an `EsqlStatement` (line 168):
```java
return new EsqlStatement(query, settings);
```

### QuerySettings Definition

**File**: `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/QuerySettings.java:89-114`

The `UNMAPPED_FIELDS` setting is defined as:

```java
public static final QuerySettingDef<UnmappedResolution> UNMAPPED_FIELDS = new QuerySettingDef<>(
    "unmapped_fields",
    DataType.KEYWORD,
    false,   // serverlessOnly
    true,    // preview
    false,   // snapshotOnly
    (value) -> {
        String resolution = Foldables.stringLiteralValueOf(value, "Unexpected value");
        UnmappedResolution res = UnmappedResolution.valueOf(resolution.toUpperCase(Locale.ROOT));
        if (res == UnmappedResolution.LOAD && EsqlCapabilities.Cap.OPTIONAL_FIELDS_V4.isEnabled() == false) {
            throw new IllegalArgumentException("'LOAD' is only supported in snapshot builds");
        }
        return res;
    },
    UnmappedResolution.DEFAULT  // default value
);
```

Key observations:
- The setting is marked `preview = true` (tech preview).
- It is NOT snapshot-only; however, `LOAD` mode is gated behind `OPTIONAL_FIELDS_V4` which is `Build.current().isSnapshot()`.
- Valid values for released builds: `DEFAULT`, `NULLIFY`.
- Valid values for snapshot builds: `DEFAULT`, `NULLIFY`, `LOAD`.
- Values are case-insensitive (parsed with `toUpperCase`).

### The UnmappedResolution Enum

**File**: `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/analysis/UnmappedResolution.java:15-32`

```java
public enum UnmappedResolution {
    DEFAULT,   // Standard ESQL: fail on unmapped. PromQL: treat differently.
    NULLIFY,   // Alias unmapped field to null of type DataType.NULL
    LOAD       // Attempt to load unmapped field from _source
}
```

### Setting Retrieval at Execution Time

**File**: `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/session/EsqlSession.java:329`

The setting is extracted from the parsed statement and passed to `analyzedPlan()`:
```java
analyzedPlan(plan, statement.setting(UNMAPPED_FIELDS), finalConfiguration, ...);
```

The `EsqlStatement.setting()` method returns the last occurrence of a setting name (if duplicated):

**File**: `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/EsqlStatement.java:28-52`

---

## 2. Field Resolution: How the Analyzer Handles Unmapped Fields

### Flow Overview

1. `EsqlSession.analyzedPlan()` creates an `AnalyzerContext` with the `unmappedResolution` value.
2. The `Analyzer` runs its batch rules, including the **Resolution** batch.
3. The Resolution batch includes `ResolveRefs`, `ImplicitCasting`, `ResolveUnionTypes`, and **`ResolveUnmapped`** (in that order).

**File**: `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/analysis/Analyzer.java:246-257`
```java
new Batch<>("Resolution",
    new ResolveRefs(),
    new ImplicitCasting(),
    new ResolveUnionTypes(),
    new InsertDefaultInnerTimeSeriesAggregate(),
    new ImplicitCastAggregateMetricDoubles(),
    new InsertFromAggregateMetricDouble(),
    new TimeSeriesGroupByAll(),
    new ResolveUnionTypesInUnionAll(),
    new ResolveUnmapped()
),
```

### The ResolveUnmapped Rule

**File**: `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/analysis/rules/ResolveUnmapped.java`

This is the core rule. It is a `ParameterizedAnalyzerRule` that runs iteratively within the batch.

**Entry point** (line 77-87):
```java
protected LogicalPlan rule(LogicalPlan plan, AnalyzerContext context) {
    if (plan instanceof PromqlCommand) {
        return resolve(plan, false);  // PromQL always nullifies
    }
    return switch (context.unmappedResolution()) {
        case DEFAULT -> plan;          // Do nothing; let it fail in verification
        case NULLIFY -> resolve(plan, false);
        case LOAD    -> resolve(plan, true);
    };
}
```

### DEFAULT Mode

No transformation. `UnresolvedAttribute` instances remain in the plan and the Verifier will fail the query with an error message like "Unknown column [field_name]".

### NULLIFY Mode

**File**: `ResolveUnmapped.java:121-141`

For `EsRelation` sources, the rule adds null-typed `FieldAttribute`s to the relation's output:
```java
List<FieldAttribute> fieldsToNullify = fieldsToNullify(unresolved, ...);
return esr.withAttributes(combine(esr.output(), fieldsToNullify));
```

Each nullified field is created as:
```java
new FieldAttribute(source, null, qualifier, name,
    new MissingEsField(name, DataType.NULL, Map.of(), false, ...))
```

**`MissingEsField`** (`x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/core/type/MissingEsField.java`): A subclass of `EsField` used as a marker for fields that are truly absent. Its `DataType.NULL` type causes the **local logical optimizer** rule `ReplaceFieldWithConstantOrNull` to replace references to this field with `Literal.NULL`.

For non-EsRelation sources (Row, LocalRelation), the rule inserts `Eval` nodes with null assignments:
```java
new Eval(source, source, nullAliases)   // each alias: Alias(name, Literal.NULL)
```

### LOAD Mode

**File**: `ResolveUnmapped.java:170-179`

For EsRelation sources, the rule adds "insisted" `FieldAttribute`s wrapped with `PotentiallyUnmappedKeywordEsField`:
```java
List<FieldAttribute> fieldsToLoad = fieldsToLoad(unresolved, ...);
return esr.withAttributes(combine(esr.output(), fieldsToLoad));
```

Each loaded field is created by `insistKeyword()`:

**File**: `Analyzer.java:1238-1246`
```java
public static FieldAttribute insistKeyword(Attribute attribute) {
    return new FieldAttribute(source, null, qualifier, name,
        new PotentiallyUnmappedKeywordEsField(name));
}
```

**`PotentiallyUnmappedKeywordEsField`** (`x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/core/type/PotentiallyUnmappedKeywordEsField.java`): A subclass of `KeywordEsField` used as a marker for fields that may exist in `_source` but not in the mapping. The field is typed as `KEYWORD`, meaning all loaded-from-source values are treated as keyword (string) values.

### Partially Mapped Fields (LOAD mode only)

When `unmapped_fields="load"`, after `ResolveRefs` resolves attributes, the method `resolvePartiallyMapped()` is called:

**File**: `Analyzer.java:670`
```java
return context.unmappedResolution() == UnmappedResolution.LOAD
    ? resolvePartiallyMapped(resolved, context) : resolved;
```

**File**: `Analyzer.java:1252-1283`

This method inspects every `FieldAttribute` in the plan. If the field is "partially unmapped" (mapped in some indices, not in others), it:
- For **keyword** fields: wraps with `PotentiallyUnmappedKeywordEsField` via `insistKeyword()`.
- For **non-keyword** fields: creates an `InvalidMappedField.potentiallyUnmapped()` which captures the type-to-indices mapping. This becomes a union-type field with the unmapped indices treated as keyword.

The information about which indices have which fields unmapped is computed during index resolution:

**File**: `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/session/IndexResolver.java:320-361`
```java
Map<String, Set<String>> fieldToUnmappedIndices = new HashMap<>();
// ... for each field, compute allIndexNames - mappedIndices
```

This `fieldToUnmappedIndices` map is stored in `EsIndex`:

**File**: `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/index/EsIndex.java:23`
```java
Map<String, Set<String>> fieldToUnmappedIndices  // keyed by field name; Set<String> are concrete index names
```

And queried via:
```java
esIndex.isPartiallyUnmappedField(fieldName)  // line 33
esIndex.getUnmappedIndices(fieldName)         // line 37
```

### Pre-Analysis: Field Name Resolution

When `unmapped_fields="load"`, the `FieldNameUtils.resolveFieldNames()` method is called with `includePrefixFields=true`:

**File**: `EsqlSession.java:853`
```java
FieldNameUtils.resolveFieldNames(parsed, hasEnriches, unmappedResolution == UnmappedResolution.LOAD)
```

**File**: `FieldNameUtils.java:318-328`

This causes **parent prefix fields** to be included in the field_caps request. For example, for field `a.b.c`, it also requests `a` and `a.b`. This is needed to detect **flattened parent fields** — if a parent is flattened, loading its subfield from `_source` is disallowed.

---

## 3. Plan Nodes

### Key Types

There are **no special plan nodes** dedicated to unmapped fields. The feature works through **markers on existing plan nodes**:

1. **`EsRelation`**: Its output attribute list is extended with additional `FieldAttribute` entries whose `field()` is either:
   - `MissingEsField` (for NULLIFY mode, DataType.NULL)
   - `PotentiallyUnmappedKeywordEsField` (for LOAD mode, DataType.KEYWORD)
   - `InvalidMappedField` with `isPotentiallyUnmapped=true` (for LOAD mode with type conflicts)

2. **`Insist`** (`x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/logical/Insist.java`): A `UnaryPlan` with `SurrogateLogicalPlan`. Its `surrogate()` is simply a `Project`. The `INSIST` command is a user-facing syntax for explicitly requesting unmapped field loading on specific fields. It resolves via `resolveInsistAttribute()` in the Analyzer.

3. **`Eval`**: In NULLIFY mode for non-ES sources (Row, LocalRelation), `Eval` nodes with null aliases are inserted.

### The Refresh Mechanism

After `ResolveUnmapped` introduces new fields, it calls `refreshPlan()`:

**File**: `ResolveUnmapped.java:240-256`

This clears the "unresolvable" messages on `UnresolvedAttribute` instances and refreshes their `NameId`s, so that `ResolveRefs` can try to resolve them again in the next iteration of the batch. It also patches `Fork` branches to propagate new attributes through top-level `Project` nodes.

---

## 4. Physical Execution: Loading from _source

### The Block Loader Path

When a `FieldAttribute` with `PotentiallyUnmappedKeywordEsField` reaches physical execution, the `EsPhysicalOperationProviders` handles it specially:

**File**: `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/planner/EsPhysicalOperationProviders.java:233-234`
```java
if (attr instanceof FieldAttribute fa && fa.field() instanceof PotentiallyUnmappedKeywordEsField kf) {
    shardContext = wrapWithUnmappedFieldContext(shardContext, kf);
}
```

### DefaultShardContextForUnmappedField

**File**: `EsPhysicalOperationProviders.java:355-394`

The `wrapWithUnmappedFieldContext()` method creates a `DefaultShardContextForUnmappedField` that overrides `fieldType()`:

```java
static DefaultShardContext wrapWithUnmappedFieldContext(DefaultShardContext ctx, PotentiallyUnmappedKeywordEsField unmappedField) {
    return new DefaultShardContextForUnmappedField(ctx, unmappedField);
}
```

The wrapped context has a **synthetic `MappedFieldType`** for the unmapped field:

```java
@Override
public @Nullable MappedFieldType fieldType(String name) {
    var superResult = super.fieldType(name);
    return superResult == null && name.equals(unmappedEsField.getName())
        ? createUnmappedFieldType(name, this) : superResult;
}
```

The synthetic `KeywordFieldType` is configured with:
- `docValues(false)` — no doc values
- `indexed(false)` — not indexed
- `isSourceSynthetic()` is passed through

This is the critical mechanism: when the real mapping returns `null` for the field (unmapped), the wrapper pretends the field exists as a keyword with no doc values and no index. The block loader then falls back to loading from `_source`, since there are no doc values or stored fields to read from.

### The FieldType Configuration

```java
private static final FieldType UNMAPPED_FIELD_TYPE = new FieldType(KeywordFieldMapper.Defaults.FIELD_TYPE);
static {
    UNMAPPED_FIELD_TYPE.setDocValuesType(DocValuesType.NONE);
    UNMAPPED_FIELD_TYPE.setIndexOptions(IndexOptions.NONE);
    UNMAPPED_FIELD_TYPE.setStored(false);
    UNMAPPED_FIELD_TYPE.freeze();
}
```

### Union-Type Handling for Partially Mapped Fields

For partially mapped non-keyword fields, the execution path goes through `MultiTypeEsField`:

**File**: `EsPhysicalOperationProviders.java:267-274`
```java
Expression potentiallyUnmapped = unionTypes.getPotentiallyUnmappedExpression();
if (!(potentiallyUnmapped instanceof AbstractConvertFunction convert)) {
    return ValuesSourceReaderOperator.LOAD_CONSTANT_NULLS;
}
fieldName = getFieldName((Attribute) convert.field());
shardContext = wrapWithUnmappedFieldContext(shardContext, new PotentiallyUnmappedKeywordEsField(fieldName));
conversion = potentiallyUnmapped;
```

For indices where the field IS mapped, the value is loaded normally and converted. For indices where it is NOT mapped, it falls back to loading as keyword from `_source` and then converting.

### Local Logical Optimizer: Retaining Potentially Unmapped Fields

**File**: `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/optimizer/rules/logical/local/ReplaceFieldWithConstantOrNull.java:83-90`

The `ReplaceFieldWithConstantOrNull` rule (which normally replaces missing fields with `Literal.NULL` based on `SearchStats`) explicitly **retains** potentially unmapped fields:

```java
Predicate<FieldAttribute> shouldBeRetained = f ->
    f instanceof TimeSeriesMetadataAttribute
    || isPotentiallyUnmapped(f)   // <-- THIS
    || EsQueryExec.isDocAttribute(f)
    || localLogicalOptimizerContext.searchStats().exists(f.fieldName())
    || ...;
```

This ensures the block loader has a chance to attempt `_source` loading rather than being short-circuited to null.

---

## 5. Restrictions (LOAD Mode Only)

All restrictions are checked in the **Verifier** after analysis succeeds:

**File**: `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/analysis/Verifier.java:131-135`
```java
if (unmappedResolution == UnmappedResolution.LOAD) {
    checkLoadModeDisallowedCommands(plan, failures);
    checkLoadModeDisallowedFunctions(plan, failures);
    checkFlattenedSubFieldLoad(plan, failures);
}
```

### Disallowed Commands (LOAD mode)

**File**: `Verifier.java:426-441`

| Command | Error Message | Reason |
|---------|--------------|--------|
| **FORK** | `FORK is not supported with unmapped_fields="load"` | Branching complexity; `UnionAll` (internal FORK representation) is excluded from this check |
| **Subqueries / Views** | `Subqueries and views are not supported with unmapped_fields="load"` | Same concern |
| **LOOKUP JOIN** | `LOOKUP JOIN is not supported with unmapped_fields="load"` | Detected via `EsRelation` with `IndexMode.LOOKUP` |
| **PromQL** | `PROMQL is not supported with unmapped_fields="load"` | PromQL has its own unmapped handling |

### Disallowed Functions (LOAD mode)

**File**: `Verifier.java:448-460`

All **full-text search functions** are disallowed:
```java
plan.forEachExpressionDown(FullTextFunction.class, fullTextFunctions::add);
```

This includes: `MATCH`, `MATCH_PHRASE`, `KQL`, `QSTR`, and any future `FullTextFunction` subclasses.

Error message: `unmapped_fields="load" does not support full-text search function [<name>]; use "default" or "nullify"`

The restriction is blanket — it applies even if the FTF does not reference an unmapped field, because functions like KQL can reference unmapped fields inside their query strings.

### Flattened Sub-Field Restriction (LOAD mode)

**File**: `Verifier.java:467-498`

If a `PotentiallyUnmappedKeywordEsField` attribute is a dot-delimited sub-field of a flattened parent field, loading is rejected:

Error message: `Loading subfield [a.b] when parent [a] is of flattened field type is not supported with unmapped_fields="load"`

This is because flattened field sub-field resolution works differently from standard `_source` loading.

### Lucene Pushdown Prevention

**File**: `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/optimizer/rules/physical/local/LucenePushdownPredicates.java:85-94`

Predicates involving `PotentiallyUnmappedKeywordEsField` attributes are **NOT pushed down** to Lucene:
```java
if (exp instanceof FieldAttribute fa
    && fa.field() instanceof PotentiallyUnmappedKeywordEsField == false  // <-- blocks pushdown
    && fa.getExactInfo().hasExact()
    && isIndexedAndHasDocValues(fa)) {
    ...
}
```

This prevents wrong results — e.g., `WHERE unmapped_field == "x"` pushed to Lucene would miss rows where the field is unmapped (absent from the inverted index).

### @timestamp Exception

**File**: `Verifier.java:391-410`

The `unmapped_fields` setting does NOT apply to the implicit `@timestamp` reference used by `TimestampAware` functions like `TBUCKET`. If `@timestamp` is unmapped across all indices, a specific error is emitted:

```
[TBUCKET] requires @timestamp; the [unmapped_fields] setting does not apply to the implicit @timestamp reference
```

---

## 6. Partially Mapped Fields

### Detection

During index resolution (`IndexResolver`), the system computes `fieldToUnmappedIndices`:

**File**: `IndexResolver.java:320-361`

For each field in the field_caps response, it computes `allIndexNames - mappedIndices`. If the result is non-empty, the field is partially unmapped.

### Resolution in LOAD Mode

When `unmapped_fields="load"`, `resolvePartiallyMapped()` in the Analyzer processes fields that are resolved (mapped in at least one index) but partially unmapped:

**File**: `Analyzer.java:1252-1283`

1. If the field's data type is **KEYWORD**: wraps with `PotentiallyUnmappedKeywordEsField` (same as fully unmapped).
2. If the field's data type is **NOT keyword**: creates an `InvalidMappedField.potentiallyUnmapped()` with the type-to-indices map.
   - This flows into the union-type resolution path, where each index either gets its native type conversion or the keyword-from-source fallback.

### Resolution in NULLIFY Mode

Partially mapped fields are NOT explicitly handled in NULLIFY mode. The field resolves normally for the indices where it is mapped. For shards on indices where it is unmapped, the local optimizer's `ReplaceFieldWithConstantOrNull` handles it:
- `SearchStats.exists(fieldName)` returns `false` on that shard.
- The field is replaced with `Literal.NULL`.

This is the standard per-shard null replacement that happens regardless of the `unmapped_fields` setting.

### EsIndex API

**File**: `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/index/EsIndex.java:33-39`
```java
public boolean isPartiallyUnmappedField(String fieldName)   // true if field is missing in at least one index
public Set<String> getUnmappedIndices(String fieldName)      // returns the set of indices where field is unmapped
```

---

## 7. The NULLIFY Mode in Detail

### How It Differs from LOAD

| Aspect | NULLIFY | LOAD |
|--------|---------|------|
| Fully unmapped fields | `null` | Loaded from `_source` as keyword |
| Partially unmapped fields | null on unmapped shards (local optimizer) | `_source` loading on unmapped shards |
| Non-keyword type conflicts | N/A (field is null) | Union-type resolution (keyword from `_source` + conversion) |
| Full-text search | Allowed | **Blocked** |
| FORK / Subqueries / Views | Allowed | **Blocked** |
| LOOKUP JOIN | Allowed | **Blocked** |
| Lucene filter pushdown | Normal (field doesn't exist in plan as a real field) | Blocked for unmapped fields |
| Sort pushdown | Normal | Blocked for unmapped fields |
| Data type of unmapped field | `DataType.NULL` | `DataType.KEYWORD` |
| Snapshot-only | No | Yes (gated by `OPTIONAL_FIELDS_V4`) |

### NULLIFY Mechanism

1. **ResolveUnmapped** adds `FieldAttribute` entries with `MissingEsField(DataType.NULL)` to the `EsRelation` output.
2. The `ResolveRefs` rule resolves the previously-unresolved attributes against these new fields.
3. The **local logical optimizer** rule `ReplaceFieldWithConstantOrNull` sees the `MissingEsField` field has `DataType.NULL`, and since `SearchStats.exists()` returns `false` for it, replaces it with `Literal.NULL`.
4. At execution time, the null literal flows through the compute pipeline as a constant null block.

For non-ES sources (ROW, LocalRelation), NULLIFY inserts `Eval` nodes:
```java
new Eval(source, source, [Alias(name, Literal.NULL)])
```

---

## 8. Configuration: Cluster/Node Settings

### No Cluster/Node-Level Setting for ES|QL Unmapped Fields

There is **NO** cluster-level or node-level setting for the ES|QL `unmapped_fields` behavior. It is exclusively a **query-level setting** via the `SET` command.

The default value is `UnmappedResolution.DEFAULT`, which means standard ES|QL queries fail on unmapped fields.

### Related but Separate: Index-Level Setting

**File**: `server/src/main/java/org/elasticsearch/index/IndexSettings.java:96-100`
```java
public static final Setting<Boolean> ALLOW_UNMAPPED = Setting.boolSetting(
    "index.query.parse.allow_unmapped_fields",
    true,
    Property.IndexScope
);
```

This is a **different feature** — it controls whether the Query DSL parser allows unmapped fields in Elasticsearch search queries (not ES|QL). It defaults to `true` and is not related to the ES|QL `unmapped_fields` setting.

---

## 9. Capability Gates

**File**: `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/action/EsqlCapabilities.java`

| Capability | Description |
|-----------|-------------|
| `OPTIONAL_FIELDS_NULLIFY_TECH_PREVIEW` | Base support for DEFAULT/NULLIFY modes |
| `OPTIONAL_FIELDS_FIX_UNMAPPED_FIELD_DETECTION` | Fix: don't false-positive on attributes already in children's output |
| `OPTIONAL_FIELDS_NULLIFY_SKIP_GROUP_ALIASES` | Fix: don't nullify aliases in Aggregate groupings |
| `OPTIONAL_FIELDS_DETECT_UNMAPPED_FIELDS_IN_AGG_FILTERS` | Fix: handle `STATS agg(field) WHERE field...` |
| `OPTIONAL_FIELDS_V4` | Full support including LOAD mode. **Snapshot-only** (`Build.current().isSnapshot()`) |
| `REJECT_LOADING_FLATTENED_SUBFIELDS` | Gated on `OPTIONAL_FIELDS_V4` — reject flattened sub-field loading |

---

## 10. The INSIST Command

### Syntax

`INSIST` is a command that explicitly marks specific fields for unmapped-field loading:

**File**: `LogicalPlanBuilder.java:433-445`
```java
public PlanFactory visitInsistCommand(EsqlBaseParser.InsistCommandContext ctx) {
    // INSIST doesn't support wildcards
    return input -> new Insist(source, input,
        fields.stream().map(ne -> (Attribute) new UnresolvedAttribute(ne.source(), ne.name())).toList());
}
```

### Resolution

**File**: `Analyzer.java:1204-1221`

The `resolveInsistAttribute()` method:
1. If the field is **not mapped anywhere**: calls `insistKeyword()` to create a `PotentiallyUnmappedKeywordEsField`.
2. If the field is **partially unmapped** and keyword: calls `insistKeyword()`.
3. If the field is **partially unmapped** and non-keyword: calls `invalidInsistAttribute()` to create an `InvalidMappedField.potentiallyUnmapped()`.
4. If the field is **mapped everywhere**: uses the normally resolved attribute.

### Relationship to unmapped_fields="load"

`INSIST` operates independently of the `unmapped_fields` setting. It provides field-level control while `unmapped_fields` provides query-level control. Both use the same underlying mechanism (`PotentiallyUnmappedKeywordEsField`).

---

## 11. Optimizer Rules for Unmapped Fields

### PropagateUnmappedFields

**File**: `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/optimizer/rules/logical/PropgateUnmappedFields.java`

This **logical optimizer rule** ensures that `PotentiallyUnmappedKeywordEsField` attributes referenced in expressions above an `EsRelation` are propagated into the relation's output. Without this, the `FieldExtractExec` would not know to extract these fields.

It scans all expressions for `FieldAttribute` with `PotentiallyUnmappedKeywordEsField` and merges them into the `EsRelation`'s output (if not already present).

### ReplaceFieldWithConstantOrNull (Local Optimizer)

**File**: `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/optimizer/rules/logical/local/ReplaceFieldWithConstantOrNull.java`

This rule runs **per-shard** during local logical optimization. For `PotentiallyUnmappedKeywordEsField` fields, it explicitly retains them (`isPotentiallyUnmapped(f)` returns `true`), preventing them from being replaced with null. This allows the block loader to attempt `_source` loading.

---

## 12. End-to-End Data Flow Summary

### NULLIFY mode

```
SET unmapped_fields="nullify"; FROM index | KEEP mapped_field, unmapped_field
```

1. **Parser**: Extracts `unmapped_fields="nullify"` as a `QuerySetting`.
2. **Pre-analysis**: `FieldNameUtils` collects `mapped_field`, `unmapped_field` for field_caps.
3. **Index resolution**: field_caps returns `mapped_field` mapped, `unmapped_field` not found.
4. **Analysis**: `ResolveRefs` resolves `mapped_field`, leaves `unmapped_field` as `UnresolvedAttribute`.
5. **ResolveUnmapped**: Adds `FieldAttribute("unmapped_field", MissingEsField(NULL))` to `EsRelation` output.
6. **ResolveRefs** (next iteration): Resolves the `UnresolvedAttribute` against the new output.
7. **Local optimizer** (per shard): `ReplaceFieldWithConstantOrNull` replaces the `MissingEsField` field with `Literal.NULL`.
8. **Execution**: Null block flows through the pipeline.

### LOAD mode

```
SET unmapped_fields="load"; FROM index | KEEP mapped_field, unmapped_field
```

1-3. Same as NULLIFY.
4. **Analysis**: `ResolveRefs` resolves `mapped_field`, leaves `unmapped_field` as `UnresolvedAttribute`.
5. **ResolveUnmapped**: Adds `FieldAttribute("unmapped_field", PotentiallyUnmappedKeywordEsField)` to `EsRelation` output.
6. **ResolveRefs** (next iteration): Resolves the `UnresolvedAttribute`.
7. **Verifier**: Checks for disallowed commands/functions.
8. **Logical optimizer**: `PropagateUnmappedFields` ensures the field is in the `EsRelation` output.
9. **Local optimizer** (per shard): `ReplaceFieldWithConstantOrNull` **retains** the field (does NOT replace with null).
10. **Physical execution**: `EsPhysicalOperationProviders.blockLoaderAndConverter()` detects `PotentiallyUnmappedKeywordEsField`, wraps the `ShardContext` with `DefaultShardContextForUnmappedField`.
11. **Block loading**: The synthetic `KeywordFieldType` (no doc values, not indexed) forces the block loader to read from `_source`.
12. **Result**: String values from `_source`, or null if the field is absent from the document's source.
