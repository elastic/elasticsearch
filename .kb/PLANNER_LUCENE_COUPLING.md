# ES|QL Planner: Lucene Coupling Analysis

This document provides a component-by-component analysis of Lucene/Elasticsearch coupling in the ES|QL planner, identifying where refactoring work is needed for federation.

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Analysis Phase](#1-analysis-phase)
3. [Logical Optimizer](#2-logical-optimizer)
4. [Physical Optimizer](#3-physical-optimizer)
5. [Mapper/Planner](#4-mapperplanner)
6. [Refactoring Priority Matrix](#5-refactoring-priority-matrix)

---

## Executive Summary

### Coupling Intensity by Component

```
Component                    | ES Coupling | Refactoring Effort | Priority
-----------------------------|-------------|--------------------|---------
Analysis (Analyzer.java)     | MEDIUM      | 60%                | P1
Logical Optimizer            | LOW-MEDIUM  | 30%                | P3
Physical Local Optimizer     | VERY HIGH   | 90%                | P1
Mapper                       | LOW         | 20%                | P2
LocalExecutionPlanner        | VERY HIGH   | 95%                | P1
EsPhysicalOperationProviders | CRITICAL    | 95%                | P0
```

### Key Abstraction Points Needed

| Current Class | Needed Abstraction | Complexity |
|---------------|-------------------|------------|
| `EsRelation` | `DataSourceRelation` interface | Medium |
| `EsIndex` / `IndexResolution` | `SchemaProvider` interface | Medium |
| `LucenePushdownPredicates` | `SourcePushdownCapabilities` per connector | High |
| `EsField` hierarchy | `FieldType` interface | High |
| `SearchStats` | `SourceStatistics` interface | Medium |
| `QueryBuilder` | `SourceQuery` abstraction | Very High |
| `EsPhysicalOperationProviders` | `ConnectorOperationProviders` SPI | Critical |
| `ShardContext` | `SourceContext` interface | Critical |

---

## 1. Analysis Phase

**Location:** `src/main/java/org/elasticsearch/xpack/esql/analysis/`

### 1.1 Analyzer.java (~3,184 lines)

**Coupling Level: MEDIUM**

#### ES-Specific Imports
```java
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
```

#### Key Coupling Points

| Lines | Code/Concept | ES-Specific? | Abstraction Needed |
|-------|--------------|--------------|-------------------|
| 283-349 | `ResolveTable` rule creates `EsRelation` | YES | `DataSourceRelation` interface |
| 405-427 | `mappingAsAttributes()` - converts ES mappings to attributes | YES | `SchemaProvider.getFields()` |
| 336-340 | `EsIndex` instantiation with mappings | YES | Generic `SourceMetadata` |
| 2142-2407 | `MultiTypeEsField` handling for union types | YES | Generic union type handling |
| 2437-2489 | `DateMillisToNanosInEsRelation` for TIME_SERIES | YES | Index mode abstraction |
| 2650-2652 | `IndexMode.TIME_SERIES` check for aggregate_metric_double | YES | Capability-based check |

#### Data Flow
```
Query String
    ↓
Parser (generic)
    ↓
PreAnalyzer → IndexResolution (ES-SPECIFIC: uses FieldCaps API)
    ↓
Analyzer.ResolveTable → EsRelation (ES-SPECIFIC)
    ↓
Field Resolution → EsField types (ES-SPECIFIC)
    ↓
Analyzed Plan
```

#### Refactoring Strategy
```java
// Current (ES-specific)
if (p instanceof UnresolvedRelation ur) {
    IndexResolution indexResolution = preAnalysis.indices.get(ur.table().index());
    EsIndex esIndex = indexResolution.get();
    return new EsRelation(ur.source(), esIndex, ur.frozen(), ur.indexMode());
}

// Target (Generic)
if (p instanceof UnresolvedRelation ur) {
    SourceMetadata metadata = context.resolveSource(ur.table());
    return metadata.createRelation(ur.source(), ur.frozen());
}
```

**Estimated Effort:** 60% of file needs refactoring

---

### 1.2 PreAnalyzer.java (~112 lines)

**Coupling Level: LOW-MEDIUM**

#### Key Coupling Points

| Lines | Code/Concept | ES-Specific? |
|-------|--------------|--------------|
| 28-34 | `IndexMode` collection | YES |
| 81 | TIME_SERIES check for aggregate_metric_double | YES |
| 86-95 | Vector function detection (knn, to_dense_vector) | YES |

**Estimated Effort:** 30% refactoring

---

## 2. Logical Optimizer

**Location:** `src/main/java/org/elasticsearch/xpack/esql/optimizer/`

### 2.1 LogicalPlanOptimizer.java (~251 lines)

**Coupling Level: LOW (mostly generic)**

#### Rule Classification

**Generic Rules (No ES Dependency):**
```
Substitution Phase:
├── SubstituteSurrogatePlans
├── SubstituteFilteredExpression
├── ReplaceAggregateNestedExpressionWithEval
├── ReplaceAggregateAggExpressionWithEval
├── SubstituteSurrogateAggregations
├── ReplaceRegexMatch
├── ReplaceTrivialTypeConversions
├── ReplaceAliasingEvalWithProject
├── SubstituteSurrogateExpressions
└── ReplaceOrderByExpressionWithEval

Operator Phase:
├── BooleanSimplification
├── ConstantFolding
├── FoldNull
├── PropagateEquals
├── PropagateNullable
├── CombineBinaryComparisons
├── CombineDisjunctions
├── SimplifyComparisonsArithmetics
├── PruneFilters
├── PruneColumns
├── PushDownAndCombineLimits
├── PushDownAndCombineFilters
├── PushDownAndCombineOrderBy
└── PushDownAndCombineSample
```

**ES-Specific Rules:**
```
├── TranslatePromqlToTimeSeriesAggregate (Line 151) - PromQL/TS
├── TranslateTimeSeriesAggregate (Line 153) - Time-series
├── PruneUnusedIndexMode (Line 154) - IndexMode
├── SkipQueryOnEmptyMappings (Line 171) - ES mappings
├── HoistRemoteEnrichLimit (Line 183) - ES Enrich
├── HoistRemoteEnrichTopN (Line 245) - ES Enrich
├── PushDownEnrich (Line 226) - ES Enrich
├── PushDownConjunctionsToKnnPrefilters (Line 221) - ES KNN
└── PushLimitToKnn (Line 219) - ES KNN
```

**Estimated Effort:** 30% (only ES-specific rules)

---

## 3. Physical Optimizer

**Location:** `src/main/java/org/elasticsearch/xpack/esql/optimizer/rules/physical/local/`

### 3.1 Overview

**Coupling Level: VERY HIGH - Almost all rules are ES/Lucene-specific**

This is the **most critical area** for federation refactoring. These rules decide what computations push down to Lucene.

### 3.2 LucenePushdownPredicates.java (FOUNDATION)

**Coupling Level: CRITICAL**

This interface determines whether expressions can be pushed to Lucene.

#### Key Methods
```java
interface LucenePushdownPredicates {
    boolean isIndexedAndHasDocValues(FieldAttribute);  // Lucene field properties
    boolean isIndexed(FieldAttribute);                  // Lucene indexed check
    boolean hasExactSubfield(FieldAttribute);           // TEXT → KEYWORD subfield
    boolean isPushableFieldAttribute(Attribute);        // Aggregatable check
    boolean isPushableMetadataAttribute(Attribute);     // _score, metadata
}
```

#### ES-Specific Concepts
- `isAggregatable` - ES field capability
- `isIndexed` - Lucene index property
- `hasDocValues` - Lucene doc-values
- `hasExactSubfield` - TEXT/KEYWORD subfield (ES mapping feature)
- `SearchStats` - Per-shard statistics

#### Abstraction Needed
```java
// Target abstraction
interface SourcePushdownCapabilities {
    boolean canPushFilter(Expression filter);
    boolean canPushAggregation(AggregationType type);
    boolean canPushSort(SortType type);
    boolean canPushLimit();
    Set<String> pushableFields();
}
```

**Cannot be made generic** - must be replaced per-connector.

---

### 3.3 Individual Rule Analysis

#### PushFiltersToSource.java

**Coupling Level: VERY HIGH**

| Aspect | Detail |
|--------|--------|
| Purpose | Push WHERE clauses to Lucene queries |
| Lines | ~100 |
| ES Classes | `QueryBuilder`, `EsQueryExec`, `LucenePushdownPredicates` |
| Key Logic | Converts `Expression` → `QueryBuilder` via translator |

**Example transformation:**
```
FilterExec(condition: x > 5)          EsQueryExec(query + RangeQuery(x > 5))
  └─ EsQueryExec(query)         →
```

**Abstraction needed:** `SourceQueryTranslator` interface per connector

---

#### PushTopNToSource.java

**Coupling Level: VERY HIGH**

| Aspect | Detail |
|--------|--------|
| Purpose | Push SORT + LIMIT to Lucene |
| Lines | ~250 |
| ES Classes | `EsQueryExec.Sort`, `FieldSort`, `GeoDistanceSort`, `ScoreSort` |
| Complexity | Handles field sorts, geo-distance sorts, score sorts |

**ES-Specific Features:**
- `GeoDistanceSort` - Lucene spatial sorting
- `ScoreSort` - Lucene relevance scoring
- Multi-field sorts with Lucene sort builders

**Cannot be made generic** - sort capabilities vary wildly by backend.

---

#### PushStatsToSource.java

**Coupling Level: VERY HIGH**

| Aspect | Detail |
|--------|--------|
| Purpose | Push simple aggregations to ES stats queries |
| Lines | ~200 |
| ES Classes | `EsStatsQueryExec`, `EsStatsQueryExec.BasicStat` |
| Scope | Only COUNT(field), no GROUP BY |

**Transformation:**
```
AggregateExec(COUNT(field))           EsStatsQueryExec(COUNT field)
  └─ EsQueryExec             →
```

**Fundamentally ES-specific** - relies on Elasticsearch's stats query execution mode.

---

#### PushCountQueryAndTagsToSource.java

**Coupling Level: VERY HIGH**

| Aspect | Detail |
|--------|--------|
| Purpose | Optimize count with bucketing using query tags |
| ES Classes | `EsStatsQueryExec`, `RangeQueryBuilder`, `TermQueryBuilder` |

**ES-specific optimization** for Lucene's query execution with bucket tagging.

---

#### InsertFieldExtraction.java

**Coupling Level: MEDIUM**

| Aspect | Detail |
|--------|--------|
| Purpose | Insert field materialization at last possible moment |
| Lines | ~150 |
| ES Classes | `FieldExtractExec`, `_doc` attribute |
| Generic Potential | HIGH - pattern is generic |

**This rule's pattern could work for any columnar system** - just needs adaptation for different execution plans.

---

#### ReplaceSourceAttributes.java

**Coupling Level: HIGH**

| Aspect | Detail |
|--------|--------|
| Purpose | Convert `EsSourceExec` → `EsQueryExec` |
| ES Classes | `EsSourceExec`, `EsQueryExec`, `TIME_SERIES_SOURCE_FIELDS` |

**Bridge rule** specific to ES execution model.

---

#### Spatial Rules (3 files)

**Coupling Level: VERY HIGH**

| Rule | Purpose |
|------|---------|
| `EnableSpatialDistancePushdown` | Rewrite `ST_DISTANCE(f, p) <= d` → `ST_INTERSECTS(f, CIRCLE(p, d))` |
| `SpatialDocValuesExtraction` | Use doc-values for spatial aggregations |
| `SpatialShapeBoundsExtraction` | Extract bounds from Lucene's binary encoding |

All three are **deeply tied to Lucene's spatial implementation** and Elasticsearch's spatial types.

---

#### Time-Series Rules

**Coupling Level: VERY HIGH**

| Rule | Purpose |
|------|---------|
| `ExtractDimensionFieldsAfterAggregation` | Optimize dimension field reads in TS mode |
| `ReplaceRoundToWithQueryAndTags` | Push date bucketing to Lucene queries |

Both are **specific to Elasticsearch's time-series index mode**.

---

### 3.4 Physical Optimizer Rule Summary

| Rule | Lines | ES Coupling | Can Abstract? |
|------|-------|-------------|---------------|
| LucenePushdownPredicates | ~200 | CRITICAL | NO - replace per connector |
| PushFiltersToSource | ~100 | VERY HIGH | Partially - need query translator |
| PushTopNToSource | ~250 | VERY HIGH | NO - sort types are backend-specific |
| PushStatsToSource | ~200 | VERY HIGH | NO - stats query is ES-specific |
| PushCountQueryAndTagsToSource | ~150 | VERY HIGH | NO - query+tags is ES-specific |
| PushLimitToSource | ~30 | LOW | YES |
| PushSampleToSource | ~50 | HIGH | Partially |
| InsertFieldExtraction | ~150 | MEDIUM | YES - generic pattern |
| ReplaceSourceAttributes | ~100 | HIGH | NO - ES execution model |
| EnableSpatialDistancePushdown | ~200 | VERY HIGH | NO - spatial specific |
| SpatialDocValuesExtraction | ~150 | VERY HIGH | NO - doc-values specific |
| SpatialShapeBoundsExtraction | ~100 | VERY HIGH | NO - Lucene encoding |
| ExtractDimensionFieldsAfterAggregation | ~150 | VERY HIGH | NO - TS specific |
| ReplaceRoundToWithQueryAndTags | ~300 | VERY HIGH | NO - query+tags specific |

---

## 4. Mapper/Planner

**Location:** `src/main/java/org/elasticsearch/xpack/esql/planner/`

### 4.1 Mapper.java (~247 lines)

**Coupling Level: LOW**

| Lines | Code | ES-Specific? |
|-------|------|--------------|
| 82-83 | `EsRelation` → `FragmentExec` | YES |
| 185-212 | `IndexMode.LOOKUP` check | YES |

**Abstraction:** Replace `EsRelation` check with generic `DataSourceRelation` interface.

---

### 4.2 LocalMapper.java (~154 lines)

**Coupling Level: MEDIUM**

| Lines | Code | ES-Specific? |
|-------|------|--------------|
| 66-67 | `EsRelation` → `EsSourceExec` | YES |
| 131-147 | `IndexMode.LOOKUP` validation | YES |

---

### 4.3 LocalExecutionPlanner.java (~1,184 lines)

**Coupling Level: VERY HIGH**

This is the **critical bridge** between physical plans and executable operators.

#### Lucene/ES Imports (Lines 10-90)
```java
// Lucene
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.lucene.*;

// ES Search
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.lookup.SearchLookup;
```

#### Key ES-Specific Methods

| Method | Lines | Purpose | ES Classes Used |
|--------|-------|---------|-----------------|
| `planEsQueryNode()` | 290-377 | Creates Lucene operators | `LuceneOperator`, `LuceneTopNSourceOperator` |
| `planFieldExtract()` | ~400 | Creates field extraction | `ValuesSourceReaderOperator` |

**Cannot be abstracted** without replacing the entire operator creation layer.

---

### 4.4 EsPhysicalOperationProviders.java (~607 lines)

**Coupling Level: CRITICAL - This is the heart of ES/Lucene integration**

#### Import Analysis

| Category | Count | Examples |
|----------|-------|----------|
| Lucene | 7 | `FieldType`, `DocValuesType`, `IndexSearcher`, `Query` |
| ES Mapper | 10+ | `MappedFieldType`, `MappingLookup`, `SourceLoader`, `BlockLoader` |
| ES Query | 5+ | `QueryBuilder`, `SearchExecutionContext`, various builders |
| ES Search | 5+ | `SearchContext`, `AliasFilter`, `SortBuilder` |

#### Key Methods

| Method | Purpose | Lucene-Specific? |
|--------|---------|------------------|
| `sourcePhysicalOperation()` | Creates `LuceneSourceOperator`, `LuceneTopNSourceOperator`, `TimeSeriesSourceOperator` | CRITICAL |
| `fieldExtractPhysicalOperation()` | Creates `ValuesSourceReaderOperator` with `BlockLoader` | CRITICAL |
| `querySupplier()` | Converts `QueryBuilder` → Lucene `Query` via `ShardContext` | CRITICAL |
| `countSource()` | Creates `LuceneCountOperator` | CRITICAL |

#### ShardContext Inner Class (CRITICAL)

```java
abstract class ShardContext implements RefCounted, Releasable {
    abstract Query toQuery(QueryBuilder queryBuilder);  // QueryBuilder → Lucene Query
    abstract double storedFieldsSequentialProportion();
}

class DefaultShardContext extends ShardContext {
    IndexSearcher searcher();           // Direct Lucene access
    BlockLoader blockLoader(...);       // ES field loading
    MappedFieldType fieldType(String);  // ES field metadata
    Query toQuery(QueryBuilder);        // Builds BooleanQuery
}
```

**This class is the fundamental abstraction point for federation.** Each connector would need its own `ShardContext` equivalent.

---

## 5. Refactoring Priority Matrix

### Priority 0 (Critical Path - Must Do First)

| Component | File | Why Critical |
|-----------|------|--------------|
| Connector SPI | NEW | Define `DataSourceConnector` interface |
| ShardContext abstraction | `EsPhysicalOperationProviders.java` | All execution flows through here |
| Source operator factory | `LocalExecutionPlanner.java` | Creates all source operators |

### Priority 1 (Core Refactoring)

| Component | File | Effort |
|-----------|------|--------|
| Schema resolution | `Analyzer.java` | Replace `EsRelation`/`EsIndex` |
| Pushdown capabilities | `LucenePushdownPredicates.java` | Per-connector implementation |
| Physical rules | `rules/physical/local/*.java` | Make connector-aware |

### Priority 2 (Can Defer)

| Component | File | Notes |
|-----------|------|-------|
| Mapper | `Mapper.java`, `LocalMapper.java` | Minimal changes needed |
| Logical optimizer | `LogicalPlanOptimizer.java` | Most rules are generic |

### Priority 3 (Low Priority)

| Component | Notes |
|-----------|-------|
| Spatial rules | Only needed if external source supports spatial |
| Time-series rules | Only needed if external source supports TS |
| Enrich rules | ES-specific feature, can ignore for federation |

---

## Appendix: Abstraction Interfaces (Draft)

### DataSourceConnector SPI

```java
public interface DataSourceConnector {
    // Identity
    String name();

    // Schema
    SourceMetadata resolveSchema(String pattern);

    // Capabilities
    SourcePushdownCapabilities pushdownCapabilities();

    // Execution
    SourceOperatorFactory createSourceOperatorFactory(SourceConfig config);
    FieldExtractorFactory createFieldExtractorFactory();

    // Query translation (optional)
    Optional<QueryTranslator> queryTranslator();
}
```

### SourcePushdownCapabilities

```java
public interface SourcePushdownCapabilities {
    boolean canPushFilter(Expression filter);
    boolean canPushAggregation(AggregationType type, List<Expression> args);
    boolean canPushSort(List<SortSpec> sorts);
    boolean canPushLimit();
    boolean canPushTopN(List<SortSpec> sorts, int limit);

    // Field-level capabilities
    boolean isFieldPushable(String fieldName, PushdownType type);
}
```

### SourceContext (replaces ShardContext)

```java
public interface SourceContext extends RefCounted, Releasable {
    // Query execution
    Object toNativeQuery(Expression filter);

    // Field access
    FieldReader getFieldReader(String fieldName);
    FieldMetadata getFieldMetadata(String fieldName);

    // Statistics (optional)
    Optional<FieldStatistics> getFieldStatistics(String fieldName);
}
```
