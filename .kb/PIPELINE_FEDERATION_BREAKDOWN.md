# ES|QL Query Processing Pipeline: Federation Coupling Breakdown

This document provides a stage-by-stage analysis of the ES|QL query processing pipeline, identifying coupling to Elasticsearch/Lucene and the work required for federation support.

> **Note (Feb 2026):** Since this analysis was written, a complete external data sources framework has been merged to main. The framework achieved distributed execution, partition detection, and a dual SPI (file-based + query-based) **within the existing planner and compute infrastructure** — without requiring the large-scale refactoring this document estimated. The coupling analysis below remains factually accurate as a reference for understanding where ES-specific logic lives, but the "refactoring effort" estimates were superseded by a more pragmatic approach: external sources run through their own pipeline path (`ExternalRelation` → `ExternalSourceExec`) that coexists alongside the ES path rather than abstracting it. See [EXTERNAL_SOURCES_OVERVIEW.md](EXTERNAL_SOURCES_OVERVIEW.md) for what was actually built.

---

## Executive Summary

| Stage | LOC | % Coupled | % Generic | Refactoring Effort | Nature of Work |
|-------|-----|-----------|-----------|-------------------|----------------|
| **1. Pre-Analysis (Schema)** | ~950 | 85% | 15% | HIGH | Interface abstraction, new SPI |
| **2. Analysis** | ~3,500 | 35-40% | 60-65% | MEDIUM | Interface extraction, factory pattern |
| **3. Logical Optimization** | ~6,000 | 12% | 88% | LOW | Parameterization, capability interfaces |
| **4. Physical Planning (Mapper)** | ~1,300 | 47% | 53% | MEDIUM | Parallel implementations |
| **5. Physical Local Optimization** | ~2,500 | 90% | 10% | HIGH | Per-connector rule implementations |
| **6. Execution Planning** | ~1,800 | 88% | 12% | CRITICAL | ConnectorOperationProviders SPI |

**Overall**: ~16,000 LOC analyzed, ~45% coupled, ~55% generic

---

## 1. Pre-Analysis (Schema Discovery)

**Files**: `PreAnalyzer.java`, `IndexResolver.java`, `EsqlResolveFieldsAction.java`
**Total LOC**: ~800

### Coupling Analysis

| Component | LOC | Coupling | Description |
|-----------|-----|----------|-------------|
| `PreAnalyzer.java` | 112 | 15% | Walks AST, extracts index patterns - mostly generic |
| `IndexResolver.java` | ~550 | 95% | FieldCaps API calls, ES field mapping |
| `EsqlResolveFieldsAction.java` | ~300 | 95% | Transport action for schema resolution |

### Call Flow
```
EsqlSession.execute()
    → PreAnalyzer.preAnalyze(statement)      // Generic AST walk
    → IndexResolver.resolveAsSeparateClusters() // ES-SPECIFIC
        → EsqlResolveFieldsAction             // ES-SPECIFIC
            → FieldCapabilities API           // ES-SPECIFIC
    → IndexResolution (contains EsIndex)     // ES-SPECIFIC
```

### Key Coupling Points

1. **FieldCaps API Integration** (CRITICAL)
   - `IndexResolver` directly calls Elasticsearch FieldCapabilities API
   - Returns `Map<String, EsField>` - typed to ES field system
   - No abstraction layer exists

2. **EsField Type Hierarchy** (CRITICAL)
   - `EsField`, `KeywordEsField`, `TextEsField`, `DateEsField`
   - `MultiTypeEsField`, `UnsupportedEsField`, `InvalidMappedField`
   - Flows through entire pipeline

3. **Index Mode Detection**
   - `IndexMode.STANDARD`, `TIME_SERIES`, `LOOKUP`
   - Affects optimization paths

### Federation Requirements

| Abstraction Needed | Current | Replacement |
|--------------------|---------|-------------|
| Schema Provider | `IndexResolver` | `SchemaProvider` interface |
| Field Type | `EsField` hierarchy | `IField` interface with variants |
| Index Metadata | `EsIndex` | `IDataSource` interface |
| Resolution Result | `IndexResolution` | `SchemaResolution` interface |

### Refactoring Metrics

- **Lines to change**: ~600-700 (85% of stage)
- **New interfaces**: 4-5
- **Effort**: HIGH
- **Risk**: Medium (well-isolated from rest of pipeline)

---

## 2. Analysis

**Files**: `Analyzer.java` (3,214 LOC), `Verifier.java` (470 LOC), `AnalyzerContext.java` (171 LOC), others
**Total LOC**: ~3,900

### Coupling Analysis

| Component | LOC | Coupling | Description |
|-----------|-----|----------|-------------|
| `ResolveTable` inner class | 67 | 100% | Creates `EsRelation` from `EsIndex` |
| `ResolveUnionTypes` | 238 | 80% | `EsRelation` modification, `MultiTypeEsField` |
| `ImplicitCasting` | 305 | 40% | Deep EsField type system usage |
| `mappingAsAttributes()` | 25 | 100% | `Map<String, EsField>` → attributes |
| `TimeSeriesGroupByAll` | 109 | 30% | `EsRelation` traversal |
| Rest of Analyzer | ~2,700 | 25% | Generic analysis rules |

### Key Coupling Points

1. **EsRelation Creation** (lines 336-340)
   ```java
   EsIndex esIndex = indexResolution.get();
   return new EsRelation(source, esIndex, indexMode, ...)
   ```
   - Hardcoded to `EsRelation` class
   - Needs factory pattern

2. **EsField Type Checks** (~80 occurrences)
   ```java
   if (field instanceof MultiTypeEsField)
   if (field instanceof UnsupportedEsField)
   if (field instanceof InvalidMappedField)
   ```
   - Scattered throughout Analyzer
   - Need `IField.isMultiType()`, `IField.isUnsupported()` etc.

3. **IndexMode Usage** (~30 occurrences)
   - `IndexMode.LOOKUP` checks
   - `IndexMode.TIME_SERIES` for TSDB handling

### Federation Requirements

| Abstraction | Lines Affected | Nature |
|-------------|----------------|--------|
| `ISourceRelation` interface | ~100 | Replace `EsRelation` type checks |
| `IRelationFactory` | ~40 | Factory for relation creation |
| `IField` interface | ~150 | Replace `EsField` instanceof |
| `IIndexMode` interface | ~30 | Parameterize index mode |

### Inner Classes by Coupling

| Class | LOC | Coupling Level |
|-------|-----|----------------|
| ResolveTable | 67 | CRITICAL |
| ResolveUnionTypes | 238 | HIGH |
| ImplicitCasting | 305 | MEDIUM |
| DateMillisToNanosInEsRelation | 29 | HIGH |
| ImplicitCastAggregateMetricDoubles | 161 | MEDIUM |
| ResolveRefs | 1,053 | LOW-MEDIUM |
| Other 12 classes | ~1,000 | LOW |

### Refactoring Metrics

- **Lines to change**: ~600-800 (20-25% of stage)
- **New interfaces**: 4 (`ISourceRelation`, `IRelationFactory`, `IField`, `IIndexMode`)
- **Effort**: MEDIUM
- **Risk**: Medium (core analysis logic unchanged)

---

## 3. Logical Optimization

**Files**: `LogicalPlanOptimizer.java`, 66 rule files in `rules/logical/`
**Total LOC**: ~6,000

### Coupling Analysis

| Category | Rule Count | % of Rules | Examples |
|----------|------------|------------|----------|
| **GENERIC** | 40 | 61% | ConstantFolding, BooleanSimplification, CombineEvals |
| **PARAMETERIZED** | 18 | 27% | PushDownFilters, PushDownLimits (need connector config) |
| **ES-SPECIFIC** | 8 | 12% | TimeSeriesAggregate, InlineStats, RemoteEnrich |

### Generic Rules (No Changes Needed)

These operate on universal query optimization patterns:

- `BooleanSimplification`, `BooleanFunctionEqualsElimination`
- `ConstantFolding`, `FoldNull`
- `CombineBinaryComparisons`, `CombineDisjunctions`
- `CombineEvals`, `CombineProjections`, `CombineLimitTopN`
- `PropagateEquals`, `PropagateNullable`, `PropagateEmptyRelation`
- `PruneFilters`, `PruneRedundantOrderBy`, `PruneEmptyAggregates`
- `LiteralsOnTheRight`, `SubstituteSurrogatePlans`
- ~25 more rules

### Parameterized Rules (Need Connector Config)

| Rule | Parameter Needed |
|------|------------------|
| `PushDownAndCombineFilters` | Filter pushability semantics |
| `PushDownAndCombineLimits` | Local vs distributed limit |
| `PushDownAndCombineOrderBy` | Sort stability guarantees |
| `PushDownAndCombineSample` | Sampling implementation |
| `PushDownEval` | Remote eval capability |
| `SkipQueryOnEmptyMappings` | Schema availability check |
| `PushLimitToKnn` | Vector search support |

### ES-Specific Rules (Need Connector Equivalent)

| Rule | ES Coupling | Federation Impact |
|------|-------------|-------------------|
| `TranslateTimeSeriesAggregate` | `_tsid`, `IndexMode.TIMESERIES` | Time-series sources only |
| `TranslatePromqlToTimeSeriesAggregate` | PromQL dialect | Prometheus-specific |
| `HoistRemoteEnrichLimit/TopN` | Remote ENRICH mode | ES enrich only |
| `PropagateInlineEvals` | `InlineJoin`, `StubRelation` | ES inline stats |
| `ReplaceLookupWithJoin` | LOOKUP JOIN semantics | ES lookup tables |

### Refactoring Metrics

- **Lines to change**: ~500-800 (8-12% of stage)
- **New interfaces**: 2-3 (`ConnectorCapabilities`, `PushdownSemantics`)
- **Effort**: LOW
- **Risk**: Low (most rules untouched)

---

## 4. Physical Planning (Mapper)

**Files**: `Mapper.java` (246 LOC), `LocalMapper.java` (153 LOC), `MapperUtils.java` (206 LOC)
**Related**: `EsSourceExec.java` (132 LOC), `EsQueryExec.java` (412 LOC), `FragmentExec.java` (144 LOC)
**Total LOC**: ~1,300

### Architecture: Dual-Path Planning

```
Logical Plan
    │
    ├─→ Mapper (Coordinator) → FragmentExec wrapper [GENERIC]
    │   - Uses FragmentExec for deferred planning
    │   - Creates ExchangeExec for distribution
    │
    └─→ LocalMapper (Data Node) → EsSourceExec [ES-SPECIFIC]
        - Creates ES-specific source nodes
        - Handles lookup join specifics
```

### Coupling Analysis

| File | LOC | % Coupled | % Generic |
|------|-----|-----------|-----------|
| `Mapper.java` | 246 | 4% | 96% |
| `LocalMapper.java` | 153 | 46% | 54% |
| `MapperUtils.java` | 206 | 0% | 100% |
| `FragmentExec.java` | 144 | 0% | 100% |
| `EsSourceExec.java` | 132 | 100% | 0% |
| `EsQueryExec.java` | 412 | 100% | 0% |

### Key Coupling Point

**LocalMapper.java:66-67** - Only 13 lines of ES-specific code:
```java
private PhysicalPlan mapLeaf(LeafPlan leaf) {
    if (leaf instanceof EsRelation esRelation) {
        return new EsSourceExec(esRelation);  // ES-SPECIFIC
    }
    return MapperUtils.mapLeaf(leaf);
}
```

### Design Insight

- **FragmentExec is the federation boundary** - wraps logical plans generically
- **EsSourceExec/EsQueryExec are not created by mappers** - created by optimizer rules
- **Coordinator path (Mapper.java) is 96% generic**

### Federation Requirements

| Abstraction | Lines Affected | Nature |
|-------------|----------------|--------|
| `ISourceExec` interface | ~20 | Abstract source plan node |
| `LocalMapperFactory` | ~15 | Per-connector mapper creation |
| Remove EsRelation checks | ~50 | Use interface instead |

### Refactoring Metrics

- **Lines to change**: ~200-300 (15-20% of stage)
- **New interfaces**: 2 (`ISourceExec`, `LocalMapperFactory`)
- **Effort**: MEDIUM
- **Risk**: Low (clean separation already exists)

---

## 5. Physical Local Optimization

**Files**: `LocalPhysicalPlanOptimizer.java`, 14 rule files in `rules/physical/local/`
**Total LOC**: ~2,500

### Coupling Analysis

| Category | Rule Count | % of Rules |
|----------|------------|------------|
| **ES-SPECIFIC** | 11 | 79% |
| **GENERIC (with ES variants)** | 2 | 14% |
| **GENERIC** | 1 | 7% |

### Rule-by-Rule Breakdown

| Rule | LOC | Coupling | Key ES Concepts |
|------|-----|----------|-----------------|
| `PushFiltersToSource` | ~200 | HIGH | QueryBuilder, SingleValueQuery |
| `PushStatsToSource` | ~150 | HIGH | EsStatsQueryExec, searchStats() |
| `PushCountQueryAndTagsToSource` | ~180 | HIGH | BoolQueryBuilder, RangeQueryBuilder |
| `PushTopNToSource` | ~150 | MEDIUM | GeoDistanceSort, LucenePushdownPredicates |
| `PushSampleToSource` | ~80 | HIGH | RandomSamplingQueryBuilder |
| `PushLimitToSource` | ~50 | LOW | EsQueryExec pattern matching |
| `ReplaceRoundToWithQueryAndTags` | ~300 | VERY HIGH | Query cost estimation, time-series |
| `ReplaceSourceAttributes` | ~100 | HIGH | IndexMode, time-series fields |
| `EnableSpatialDistancePushdown` | ~150 | HIGH | Lucene spatial queries |
| `SpatialDocValuesExtraction` | ~100 | HIGH | Doc-values, SearchStats |
| `SpatialShapeBoundsExtraction` | ~100 | HIGH | GeometryDocValueWriter |
| `ExtractDimensionFieldsAfterAggregation` | ~80 | VERY HIGH | Time-series dimensions |
| `InsertFieldExtraction` | ~120 | MEDIUM | FieldExtractPreference |
| `LucenePushdownPredicates` | ~100 | VERY HIGH | Core pushability interface |

### ES Concept Dependencies

| ES Concept | Rules Using It | Impact |
|------------|----------------|--------|
| `QueryBuilder` types | 8 rules | Critical - query construction |
| `SearchStats` | 5 rules | Critical - field metadata |
| `IndexMode.TIME_SERIES` | 3 rules | Time-series only |
| `MappedFieldType` | 4 rules | Field type resolution |
| Spatial/Geo types | 4 rules | Spatial queries |

### Federation Requirements

For each connector, implement equivalent optimization rules:

| ES Rule | Connector Equivalent Needed |
|---------|---------------------------|
| `PushFiltersToSource` | Predicate pushdown per connector |
| `PushStatsToSource` | Stats/count API per connector |
| `PushTopNToSource` | Sort pushdown per connector |
| `LucenePushdownPredicates` | `ConnectorPushdownPredicates` interface |

### Refactoring Metrics

- **Lines to change**: ~2,000-2,200 (80-90% of stage)
- **New interfaces**: 5+ per connector
- **Effort**: HIGH
- **Risk**: High (performance-critical rules)

---

## 6. Execution Planning

**Files**: `LocalExecutionPlanner.java` (1,188 LOC), `EsPhysicalOperationProviders.java` (619 LOC)
**Total LOC**: ~1,800

### Coupling Analysis

| Component | LOC | Coupling |
|-----------|-----|----------|
| `EsPhysicalOperationProviders` | 619 | 88% ES-specific |
| `LocalExecutionPlanner` (delegation) | ~200 | Generic but uses ES provider |
| `LocalExecutionPlanner` (generic) | ~984 | Generic operator creation |

### Lucene-Specific Methods

| Method | LOC | Creates |
|--------|-----|---------|
| `sourcePhysicalOperation()` | 60 | LuceneSourceOperator, LuceneTopNSourceOperator, TimeSeriesSourceOperator |
| `fieldExtractPhysicalOperation()` | 20 | ValuesSourceReaderOperator |
| `countSource()` | 15 | LuceneCountOperator |
| `timeSeriesAggregatorOperatorFactory()` | 16 | TimeSeriesAggregationOperator |
| `blockLoaderAndConverter()` | 38 | BlockLoader from field types |
| `querySupplier()` | 18 | Query conversion |
| `ShardContext` implementation | 155 | DefaultShardContext (Lucene searcher) |

### The Critical Coupling Point: ShardContext

```
ShardContext (abstract interface)
    └── DefaultShardContext (ES implementation)
            ├── SearchExecutionContext (ES-specific)
            ├── IndexSearcher (Lucene)
            ├── QueryBuilder → Query conversion
            └── Field type resolution
```

**ShardContext is the central coupling point** - all data access flows through it.

### Federation Requirements (ConnectorOperationProviders SPI)

| Interface Needed | Methods | Purpose |
|------------------|---------|---------|
| `ConnectorSourceFactory` | `createSourceOperator()` | Source operator creation |
| `ConnectorFieldExtractor` | `createFieldExtractor()` | Field extraction |
| `ConnectorShardContext` | Replace `ShardContext` | Data access abstraction |
| `ConnectorQueryProvider` | `provideQueries()` | Query translation |
| `ConnectorAggregatorFactory` | Time-series, stats | Aggregation operators |

### Refactoring Metrics

- **Lines to change**: ~550-650 (30-35% of stage, but CRITICAL path)
- **New interfaces**: 5 (`ConnectorSourceFactory`, `ConnectorFieldExtractor`, `ConnectorShardContext`, `ConnectorQueryProvider`, `ConnectorAggregatorFactory`)
- **Effort**: CRITICAL
- **Risk**: High (performance-critical, data access layer)

---

## Summary: Federation Work by Stage

### Effort Distribution

```
Pre-Analysis (Schema)         ████████████████████  HIGH (85% coupled)
Analysis                      ████████████          MEDIUM (35% coupled)
Logical Optimization          ████                  LOW (12% coupled)
Physical Planning (Mapper)    ████████████          MEDIUM (47% coupled)
Physical Local Optimization   ████████████████████  HIGH (90% coupled)
Execution Planning            ████████████████████  CRITICAL (88% coupled)
```

### Priority Order for Refactoring

1. **Execution Planning** (CRITICAL) - ConnectorOperationProviders SPI
   - Without this, no connector can execute
   - ShardContext abstraction is foundational

2. **Pre-Analysis** (HIGH) - SchemaProvider SPI
   - Connectors need schema discovery
   - Unblocks Analysis stage

3. **Physical Local Optimization** (HIGH) - Per-connector optimization rules
   - Performance depends on pushdown
   - Can start with basic rules, add optimizations iteratively

4. **Analysis** (MEDIUM) - ISourceRelation, IField interfaces
   - Mechanical refactoring
   - Low risk

5. **Physical Planning** (MEDIUM) - LocalMapperFactory
   - Small surface area
   - Clean existing separation

6. **Logical Optimization** (LOW) - ConnectorCapabilities
   - Most rules already generic
   - Parameterization for edge cases

### New Abstractions Required

| Interface | Stage | Complexity | Lines |
|-----------|-------|------------|-------|
| `SchemaProvider` | Pre-Analysis | Medium | ~200 |
| `IField` | Analysis | Medium | ~150 |
| `ISourceRelation` | Analysis | Low | ~100 |
| `IRelationFactory` | Analysis | Low | ~50 |
| `ConnectorCapabilities` | Logical Opt | Low | ~100 |
| `ConnectorPushdownPredicates` | Physical Opt | High | ~200 |
| `ConnectorSourceFactory` | Execution | High | ~150 |
| `ConnectorFieldExtractor` | Execution | Medium | ~100 |
| `ConnectorShardContext` | Execution | High | ~200 |
| `ConnectorQueryProvider` | Execution | Medium | ~100 |
| **Total New Interface Code** | | | **~1,350** |

### Estimated Total Refactoring

| Category | Lines |
|----------|-------|
| Interface definitions | ~1,350 |
| ES implementation of interfaces | ~2,000 |
| Core code refactoring | ~3,500 |
| Test updates | ~2,000+ |
| **Total** | **~9,000+ lines** |

---

## Appendix: Files by Refactoring Priority

### CRITICAL (Must Change for Federation)

| File | LOC | Stage |
|------|-----|-------|
| `EsPhysicalOperationProviders.java` | 607 | Execution |
| `IndexResolver.java` | ~400 | Pre-Analysis |
| `LucenePushdownPredicates.java` | ~100 | Physical Opt |
| `PushFiltersToSource.java` | ~200 | Physical Opt |
| `LocalMapper.java` | 153 | Mapper |

### HIGH (Significant Changes)

| File | LOC | Stage |
|------|-----|-------|
| `Analyzer.java` | 3,184 | Analysis |
| `LocalExecutionPlanner.java` | 1,184 | Execution |
| `PushStatsToSource.java` | ~150 | Physical Opt |
| `ReplaceRoundToWithQueryAndTags.java` | ~300 | Physical Opt |
| `EsqlResolveFieldsAction.java` | ~300 | Pre-Analysis |

### MEDIUM (Moderate Changes)

| File | LOC | Stage |
|------|-----|-------|
| `PreAnalyzer.java` | 112 | Pre-Analysis |
| `Verifier.java` | 464 | Analysis |
| `InsertFieldExtraction.java` | ~120 | Physical Opt |
| `PushTopNToSource.java` | ~150 | Physical Opt |

### LOW (Minor Changes)

| File | LOC | Stage |
|------|-----|-------|
| `Mapper.java` | 246 | Mapper |
| `MapperUtils.java` | 206 | Mapper |
| `LogicalPlanOptimizer.java` | ~300 | Logical Opt |
| Generic logical rules | ~4,000 | Logical Opt |
