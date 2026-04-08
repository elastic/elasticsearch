# Deep Dive: FROM datasource::expression Syntax

**Issue**: [#143277](https://github.com/elastic/elasticsearch/issues/143277) — ESQL: Data sources: FROM datasource::expression syntax
**Date**: 2026-03-03
**Status**: Investigation complete, implementation not started

---

## 1. Current EXTERNAL Syntax — What Works Today

### 1.1 The Dev Gate

The EXTERNAL command is behind a **snapshot-only gate** that prevents it from appearing in production builds. The gate operates at two levels:

**Lexer level** (`x-pack/plugin/esql/src/main/antlr/lexer/From.g4:18`):
```antlr
DEV_EXTERNAL : {this.isDevVersion()}? 'external' -> pushMode(FROM_MODE);
```

**Parser level** (`x-pack/plugin/esql/src/main/antlr/EsqlBaseParser.g4:48`):
```antlr
sourceCommand
    : fromCommand
    | rowCommand
    | showCommand
    | timeSeriesCommand
    | promqlCommand
    | {this.isDevVersion()}? explainCommand
    | {this.isDevVersion()}? externalCommand   // <-- gated
    ;
```

The `isDevVersion()` predicate is configured by `EsqlConfig` (`x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/parser/EsqlConfig.java`), which defaults to `Build.current().isSnapshot()`. In test code, it can be set to `true` explicitly.

**Capability**: `EsqlCapabilities.Cap.EXTERNAL_COMMAND` (also gated by `Build.current().isSnapshot()`).

### 1.2 Grammar Rules

The EXTERNAL command grammar (`EsqlBaseParser.g4:110-112`):
```antlr
externalCommand
    : DEV_EXTERNAL stringOrParameter commandNamedParameters
    ;
```

Where:
- `stringOrParameter` = `string | parameter` (quoted string literal or `?`/`?name` parameter)
- `commandNamedParameters` = `(WITH mapExpression)?` — optional WITH clause containing a JSON-like map

The lexer pushes into `FROM_MODE` when it sees `external`, and `FROM_WITH` transitions to `EXPRESSION_MODE` when it encounters `WITH`, enabling map expression parsing.

### 1.3 Concrete Syntax Examples That Work Today

```esql
-- Simple path
EXTERNAL "s3://bucket/table"

-- With credentials
EXTERNAL "s3://bucket/table" WITH { "access_key": "AKIA...", "secret_key": "wJal..." }

-- With format override
EXTERNAL "s3://bucket/path/*.ndjson.gz" WITH { "format": "ndjson" }

-- With boolean parameter
EXTERNAL "s3://bucket/table" WITH { "use_cache": true }

-- Parameterized path
EXTERNAL ?

-- With pipeline
EXTERNAL "s3://bucket/table" | WHERE age > 25 | LIMIT 10

-- Glob patterns
EXTERNAL "s3://bucket/data/year=2024/**/*.parquet"

-- Multiple file types
EXTERNAL "s3://bucket/data/file1.parquet,s3://bucket/data/file2.parquet"
```

### 1.4 Parse Tree to Plan Node

The parser creates an `UnresolvedExternalRelation` (`LogicalPlanBuilder.java:781-789`):

```java
public LogicalPlan visitExternalCommand(EsqlBaseParser.ExternalCommandContext ctx) {
    Source source = source(ctx);
    Expression tablePath = expression(ctx.stringOrParameter());
    MapExpression options = visitCommandNamedParameters(ctx.commandNamedParameters());
    Map<String, Expression> params = options != null ? options.keyFoldedMap() : Map.of();
    return new UnresolvedExternalRelation(source, tablePath, params);
}
```

The `UnresolvedExternalRelation` carries:
- `tablePath`: The URI expression (a `Literal` for string literals, a parameter reference for `?`)
- `params`: Key-value map from the WITH clause (e.g., `{"access_key": "AKIA..."}`)

### 1.5 Resolution Pipeline

The resolution flow is:

1. **PreAnalyzer** (`PreAnalyzer.java:73-82`): Walks the plan, extracts string paths from `UnresolvedExternalRelation` nodes into `icebergPaths` list.

2. **EsqlSession.preAnalyzeExternalSources** (`EsqlSession.java:851-871`): Called during pre-analysis. Extracts path-to-params mapping, extracts partition filter hints from WHERE clauses, calls `ExternalSourceResolver.resolve()` asynchronously.

3. **ExternalSourceResolver.resolve** (`ExternalSourceResolver.java:85-125`): Iterates registered `ExternalSourceFactory` instances (from `DataSourceModule`) to find one that `canHandle(path)`. For multi-file globs, expands the glob and resolves schema from the first file. Returns `ExternalSourceResolution` containing `ResolvedSource` (metadata + file set).

4. **Analyzer.ResolveExternalRelations** (`Analyzer.java:456-489`): Replaces `UnresolvedExternalRelation` with `ExternalRelation`, carrying the resolved `SourceMetadata` (schema, config, source type) and `FileSet`.

5. **ExternalRelation.toPhysicalExec()** (`ExternalRelation.java:115-127`): Creates `ExternalSourceExec` for physical execution.

### 1.6 Factory Dispatch

`ExternalSourceResolver` iterates factories from `DataSourceModule.sourceFactories()`:
- **Connector factories** (Flight, etc.): Keyed by scheme (e.g., `grpc`). `canHandle()` checks if the URI scheme matches.
- **Table catalog factories** (Iceberg): Keyed by catalog type. `canHandle()` uses heuristics (S3 scheme + no file extension).
- **FileSourceFactory**: Registered as catch-all fallback. `canHandle()` uses `StoragePath.of()` to parse the URI scheme and checks if the format reader registry has a reader for the file extension.

---

## 2. Target FROM Syntax

### 2.1 From Issue #143277

The target syntax is:
```esql
FROM s3::"s3://bucket/path/*.parquet"
FROM iceberg::mydb.logs
FROM jdbc::mydb.orders
FROM flight::"grpc://host:port/dataset"
```

The `::` separates the **datasource type** from the **expression**. This mirrors the existing index selector syntax (`my_index::data`) but repurposes it for datasource dispatch.

### 2.2 How :: Already Works in the Grammar

`::` is lexed as `CAST_OP` in multiple modes:
- **Expression mode**: `primaryExpression CAST_OP dataType` — inline cast (e.g., `x::integer`)
- **FROM mode**: `FROM_SELECTOR : CAST_OP -> type(CAST_OP)` — index selector (e.g., `FROM my_index::failures`)

In `FROM_MODE`, the `indexPattern` rule (`EsqlBaseParser.g4:127-130`):
```antlr
indexPattern
    : (clusterString COLON)? unquotedIndexString (CAST_OP selectorString)?
    | indexString
    ;
```

Currently, `selectorString` only accepts `UNQUOTED_SOURCE`. Index selectors are restricted values like `data` or `failures`.

### 2.3 Lexer Constraints

The `UNQUOTED_SOURCE_PART` fragment (`From.g4:44-47`):
```antlr
fragment UNQUOTED_SOURCE_PART
    : ~[:"=|,[\]/() \t\r\n]
    | '/' ~[*/]
    ;
```

Excluded characters: `:`, `"`, `=`, `|`, `,`, `[`, `]`, `/`, `(`, `)`, whitespace.

This means:
- `s3://bucket/path` CANNOT be an UNQUOTED_SOURCE (contains `:` and `//`)
- URIs with schemes MUST be quoted: `"s3://bucket/path"`
- Unquoted identifiers like `mydb.logs` CAN be UNQUOTED_SOURCE (`.` is allowed)

### 2.4 What FROM s3::"s3://bucket/*.parquet" Would Parse As

Under the **current** grammar:
- `FROM` pushes into `FROM_MODE`
- `s3` matches `UNQUOTED_SOURCE`
- `::` matches `FROM_SELECTOR` (which is `CAST_OP`)
- `"s3://bucket/*.parquet"` matches `FROM_QUOTED_SOURCE` (which is `QUOTED_STRING`)

But the `indexPattern` rule expects `selectorString` → `UNQUOTED_SOURCE` after `CAST_OP`, not `QUOTED_STRING`. So this would **fail to parse** under the current grammar.

For `FROM s3::mydb.logs`:
- `s3` matches `UNQUOTED_SOURCE`
- `::` matches `CAST_OP`
- `mydb.logs` matches `UNQUOTED_SOURCE`

This **would parse** under the current `indexPattern` rule but would be interpreted as an index selector, not a datasource expression.

---

## 3. What Needs to Change

### 3.1 Grammar Changes

**Option A: Extend indexPattern (minimal change)**

Modify the `indexPattern` rule to accept either an unquoted or quoted expression after `CAST_OP`:

```antlr
indexPattern
    : (clusterString COLON)? unquotedIndexString (CAST_OP datasourceExpression)?
    | indexString
    ;

datasourceExpression
    : UNQUOTED_SOURCE   // existing selector: my_index::data
    | QUOTED_STRING      // new: s3::"s3://bucket/path"
    ;
```

This is backwards-compatible: `FROM my_index::data` still works as an index selector. The parser decides at a higher level whether this is a datasource reference or an index selector based on the left-hand side.

**Option B: New parser rule (cleaner separation)**

Add a dedicated `datasourcePattern` alternative to `indexPatternOrSubquery`:

```antlr
indexPatternOrSubquery
    : indexPattern
    | {this.isDevVersion()}? subquery
    | {this.isDevVersion()}? datasourcePattern
    ;

datasourcePattern
    : datasourceType CAST_OP datasourceExpression commandNamedParameters
    ;

datasourceType
    : UNQUOTED_SOURCE    // s3, iceberg, jdbc, flight
    ;

datasourceExpression
    : UNQUOTED_SOURCE    // mydb.logs
    | QUOTED_STRING      // "s3://bucket/path/*.parquet"
    ;
```

**Recommendation**: Option B is cleaner. It keeps datasource parsing separate from index pattern parsing, avoids ambiguity with index selectors, and can carry its own WITH clause. It also naturally maps to a different plan node.

### 3.2 Parser Changes (LogicalPlanBuilder)

A new visitor method would be needed:

```java
@Override
public LogicalPlan visitDatasourcePattern(EsqlBaseParser.DatasourcePatternContext ctx) {
    Source source = source(ctx);
    String datasourceType = ctx.datasourceType().getText();  // "s3", "iceberg", etc.

    // Expression after :: — could be quoted URI or unquoted identifier
    String expression;
    if (ctx.datasourceExpression().QUOTED_STRING() != null) {
        expression = unquote(ctx.datasourceExpression().QUOTED_STRING().getText());
    } else {
        expression = ctx.datasourceExpression().UNQUOTED_SOURCE().getText();
    }

    MapExpression options = visitCommandNamedParameters(ctx.commandNamedParameters());
    Map<String, Expression> params = options != null ? options.keyFoldedMap() : Map.of();

    // Create the same UnresolvedExternalRelation, but with datasource type information
    return new UnresolvedExternalRelation(source, datasourceType, expression, params);
}
```

### 3.3 Resolver Dispatch Changes

Currently, `ExternalSourceResolver` iterates ALL factories and calls `canHandle(path)`. With explicit datasource types from the syntax, resolution becomes direct lookup:

```java
// Current: iterate and test
for (ExternalSourceFactory factory : dataSourceModule.sourceFactories().values()) {
    if (factory.canHandle(path)) { ... }
}

// With FROM s3::...: direct lookup by type
ExternalSourceFactory factory = dataSourceModule.sourceFactories().get(datasourceType);
if (factory == null) {
    throw new IllegalArgumentException("Unknown datasource type: " + datasourceType);
}
return factory.resolveMetadata(expression, config);
```

This is a significant improvement:
- No ambiguity between Iceberg and file-based paths (the `canHandle()` heuristic in `LazyTableCatalogWrapper` is fragile)
- No unnecessary factory iteration
- Clear error messages ("Unknown datasource type: xyz" vs "No handler found")
- Lazy loading still works — only the targeted factory is initialized

### 3.4 Coexistence During Transition

Both syntaxes can coexist:
- `EXTERNAL "s3://bucket/path"` → creates `UnresolvedExternalRelation` (tablePath-based, no explicit type)
- `FROM s3::"s3://bucket/path"` → creates `UnresolvedExternalRelation` (with explicit datasourceType)

Both resolve through `ExternalSourceResolver`, but the FROM syntax provides the datasource type directly while EXTERNAL relies on `canHandle()` dispatch.

The parser grammar allows both simultaneously:
- `EXTERNAL` is a separate `sourceCommand` alternative (dev-gated)
- `FROM` with `datasourcePattern` is an extension to the existing FROM command (also dev-gated)

---

## 4. Named Datasource Resolution

### 4.1 The User Story

With a CRUD API, users register named datasources:
```
PUT _esql/datasource/my_s3_logs
{
  "type": "s3",
  "settings": {
    "bucket": "my-company-logs",
    "region": "us-east-1",
    "access_key": "AKIA...",
    "secret_key": "..."
  }
}
```

Then query using the name:
```esql
FROM my_s3_logs::"year=2024/**/*.parquet"
```

### 4.2 Resolution Path

The named datasource resolution would follow this path:

1. **Parser**: `FROM my_s3_logs::"year=2024/**/*.parquet"` parses with:
   - `datasourceType` = `my_s3_logs`
   - `datasourceExpression` = `year=2024/**/*.parquet`

2. **PreAnalyzer**: Extracts the datasource reference (type + expression).

3. **Datasource Lookup**: A new `DataSourceConfigResolver` (analogous to `EnrichPolicyResolver`) looks up `my_s3_logs` in cluster state or a dedicated system index. Returns the stored configuration including type (`s3`) and credentials.

4. **ExternalSourceResolver**: Uses the stored type to find the correct factory, combines stored credentials with the expression, and resolves:
   ```
   type = "s3" (from stored config)
   path = "s3://my-company-logs/year=2024/**/*.parquet" (constructed from stored base + expression)
   config = { access_key: "AKIA...", ... } (from stored config, overridable by WITH clause)
   ```

5. **Analyzer**: Same as today — `ResolveExternalRelations` creates `ExternalRelation`.

### 4.3 Key Design Question: Is the Name a Type or a Configuration?

There's an important distinction:
- `FROM s3::"s3://bucket/path"` — `s3` is a **type** (maps to a factory)
- `FROM my_s3_logs::"*.parquet"` — `my_s3_logs` is a **named configuration** (maps to stored settings + a type)

The parser doesn't know which it is. Resolution needs a two-step lookup:
1. Is `my_s3_logs` a registered datasource name? If yes, load its config (which includes its type).
2. If not, is it a known factory type (`s3`, `iceberg`, etc.)? If yes, treat the expression as a full path.
3. If neither, error: "Unknown datasource 'my_s3_logs'."

This is analogous to how `ENRICH` works: the policy name is looked up in cluster state, and the stored policy provides the match type, indices, and field mappings.

### 4.4 WITH Clause Override

Named datasources should support local overrides:
```esql
FROM my_s3_logs::"*.parquet" WITH { "format": "ndjson" }
```

The merge order:
1. Stored datasource config (base)
2. WITH clause parameters (override)

This is already how `ExternalSourceResolver.wrapAsExternalSourceMetadata()` works — it merges metadata config with query-level params, with query-level params taking precedence.

---

## 5. The EXTERNAL to FROM Migration

### 5.1 Migration Plan

| Phase | EXTERNAL | FROM datasource:: | Notes |
|-------|----------|-------------------|-------|
| Current (dev) | Works (snapshot-only) | Not implemented | EXTERNAL is the only path |
| Transition | Deprecated (snapshot-only) | Works (snapshot-only) | Both paths coexist; parser emits deprecation warning for EXTERNAL |
| GA | Removed | Works (GA, no dev gate) | Clean removal |

### 5.2 What Changes for the User

| Today | After |
|-------|-------|
| `EXTERNAL "s3://bucket/path.parquet"` | `FROM s3::"s3://bucket/path.parquet"` |
| `EXTERNAL "s3://bucket/table"` | `FROM iceberg::"s3://bucket/table"` |
| `EXTERNAL "grpc://host:port/ds"` | `FROM flight::"grpc://host:port/ds"` |
| `EXTERNAL "s3://bucket/path" WITH { "key": "val" }` | `FROM s3::"s3://bucket/path" WITH { "key": "val" }` |
| N/A | `FROM my_s3_logs::"*.parquet"` |

Key improvement: The datasource type is **explicit** in the syntax, eliminating the ambiguity in factory dispatch. Today, `EXTERNAL "s3://bucket/table"` could be either an Iceberg table or a file-based source, and the resolver uses fragile heuristics (no file extension → try Iceberg first).

### 5.3 Backward Compatibility

Since EXTERNAL is behind a dev gate (`isDevVersion()`) and has never been in a GA release, there are **no backward compatibility concerns**. No production users depend on it. The migration is purely internal cleanup.

### 5.4 Telemetry

`FeatureMetric.EXTERNAL` currently tracks `ExternalRelation` plan nodes. After the migration:
- Rename to `FeatureMetric.DATASOURCE` or keep `EXTERNAL` as the metric name
- The plan node doesn't change — `ExternalRelation` is the resolved form regardless of syntax

---

## 6. Concrete Syntax Forms

### 6.1 Direct Access (Type + Full URI)

```esql
-- Parquet on S3
FROM s3::"s3://bucket/data/*.parquet"

-- NDJSON.gz on GCS
FROM gcs::"gs://bucket/logs/2024-*.ndjson.gz"

-- Parquet on Azure
FROM azure::"wasbs://container@account.blob.core.windows.net/data/*.parquet"

-- CSV via HTTP
FROM http::"https://data.example.com/dataset.csv"

-- Arrow Flight
FROM flight::"grpc://host:8815/my_dataset"

-- Iceberg table
FROM iceberg::"s3://bucket/warehouse/db/table"
```

### 6.2 Named Datasource Access

```esql
-- Named datasource + expression
FROM my_s3_logs::"year=2024/month=03/**/*.parquet"

-- Named Iceberg catalog
FROM my_iceberg::"analytics.page_views"

-- Named JDBC connection
FROM my_postgres::"orders"
```

### 6.3 With Parameters

```esql
-- Format override
FROM s3::"s3://bucket/data/*.dat" WITH { "format": "ndjson" }

-- Credentials override
FROM s3::"s3://bucket/data/*.parquet" WITH {
    "access_key": "AKIA...",
    "secret_key": "wJal..."
}

-- Named datasource with local override
FROM my_s3_logs::"*.parquet" WITH { "format": "parquet", "hive_partitioning": false }
```

### 6.4 With Pipeline

```esql
FROM s3::"s3://bucket/logs/*.ndjson.gz"
    | WHERE @timestamp > NOW() - 1 day
    | GROK message "%{COMMONAPACHELOG}"
    | STATS count = COUNT(*) BY status
    | SORT count DESC
    | LIMIT 20
```

### 6.5 Edge Cases and Open Questions

**Unquoted expressions after ::**
```esql
-- Should this work? mydb.logs is a valid UNQUOTED_SOURCE
FROM iceberg::mydb.logs

-- What about globs? * is excluded from UNQUOTED_SOURCE_PART
-- So this MUST be quoted:
FROM s3::"s3://bucket/*.parquet"

-- What about simple paths without special chars?
-- year=2024 has = which is excluded from UNQUOTED_SOURCE_PART
-- So this MUST be quoted:
FROM my_s3_logs::"year=2024/**/*.parquet"
```

**Ambiguity with index selectors**:
```esql
-- Is this an index selector or a datasource?
FROM my_index::data

-- Answer: it's an index selector (existing behavior)
-- Datasource names would need to be distinct from index names
-- Resolution: check datasource registry first, fall back to index selector
-- Or: require datasource expressions to always be quoted
```

**Combining with remote clusters**:
```esql
-- Remote cluster + index selector (existing):
-- FROM cluster:my_index::data

-- Remote cluster + datasource? Probably not supported:
-- FROM cluster:s3::"s3://bucket/path"  -- confusing, likely error
```

---

## 7. Implementation Approach

### 7.1 Minimal Grammar Change

The most surgical change adds `datasourcePattern` as a new alternative inside `indexPatternOrSubquery`:

**From.g4** — no changes needed (lexer already handles `::`).

**EsqlBaseParser.g4** — add datasource pattern:

```antlr
indexPatternOrSubquery
    : indexPattern
    | {this.isDevVersion()}? subquery
    | {this.isDevVersion()}? datasourcePattern   // NEW
    ;

datasourcePattern
    : unquotedIndexString CAST_OP indexString commandNamedParameters
    ;
```

This reuses existing token types:
- `unquotedIndexString` for the datasource type (s3, iceberg, my_datasource)
- `CAST_OP` for `::`
- `indexString` for the expression (either `UNQUOTED_SOURCE` or `QUOTED_STRING`)
- `commandNamedParameters` for the optional `WITH { ... }` clause

### 7.2 Parser Ambiguity Resolution

The grammar has a potential ambiguity: `FROM foo::bar` could match either `indexPattern` (with selector) or `datasourcePattern`. ANTLR resolves this by rule order — `indexPattern` comes first in `indexPatternOrSubquery`, so it would win.

Two solutions:
1. **Parser-level detection**: In `visitRelation()`, check if the "selector" is a known datasource type, and redirect.
2. **Grammar restructuring**: Make `datasourcePattern` require the expression to be quoted: `FROM s3::"expression"`. Since index selectors never use quoted strings, there's no ambiguity.

**Recommendation**: Solution 2 (require quoted expression for datasources). This eliminates ambiguity at the grammar level, produces clear error messages, and is the expected UX anyway (URIs need quoting due to `:`, `//`, `*`, `=` characters).

### 7.3 Plan Node Changes

`UnresolvedExternalRelation` needs an optional `datasourceType` field:

```java
public class UnresolvedExternalRelation extends LeafPlan implements Unresolvable {
    private final String datasourceType;  // NEW: "s3", "iceberg", null (for EXTERNAL compat)
    private final Expression tablePath;
    private final Map<String, Expression> params;
    // ...
}
```

When `datasourceType` is present, the resolver uses direct factory lookup instead of `canHandle()` iteration.

### 7.4 What Stays the Same

The following components need **no changes**:
- `ExternalRelation` (resolved plan node) — unchanged
- `ExternalSourceExec` (physical plan node) — unchanged
- `SourceMetadata`, `ExternalSourceMetadata` — unchanged
- `OperatorFactoryRegistry` — unchanged
- `DataSourceModule`, factory registration — unchanged
- All source operator factories — unchanged
- All plugins — unchanged
- Optimizer rules — unchanged
- Physical planner mapping — unchanged

The change is purely in **parsing** (grammar + LogicalPlanBuilder) and **resolution** (ExternalSourceResolver dispatch).

---

## 8. Summary

| Aspect | Current (EXTERNAL) | Target (FROM ::) |
|--------|-------------------|-----------------|
| Syntax | `EXTERNAL "uri"` | `FROM type::"expr"` |
| Gate | `isDevVersion()` on lexer + parser | `isDevVersion()` on parser rule |
| Plan node | `UnresolvedExternalRelation` (tablePath only) | `UnresolvedExternalRelation` (type + expression) |
| Resolution | `canHandle()` iteration over all factories | Direct factory lookup by type |
| Ambiguity | Iceberg vs file heuristic in `canHandle()` | None — type is explicit |
| Named sources | Not supported | Resolve name → stored config → type + base path |
| WITH clause | Supported | Supported (same mechanism) |
| Backward compat | N/A (never in GA) | N/A (dev gate) |
| Grammar change | N/A | Add `datasourcePattern` to `indexPatternOrSubquery` |
| LOC estimate | N/A | ~100 lines grammar + parser, ~50 lines resolver |

---

## Key Files Referenced

| File | Purpose |
|------|---------|
| `x-pack/plugin/esql/src/main/antlr/lexer/From.g4` | FROM mode lexer — DEV_EXTERNAL token, UNQUOTED_SOURCE_PART, FROM_SELECTOR |
| `x-pack/plugin/esql/src/main/antlr/EsqlBaseParser.g4` | Parser grammar — sourceCommand, externalCommand, indexPattern rules |
| `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/parser/LogicalPlanBuilder.java` | visitExternalCommand, visitFromCommand, visitRelation |
| `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/parser/IdentifierBuilder.java` | visitIndexPattern, reassembleIndexName, visitSelectorString |
| `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/parser/EsqlConfig.java` | isDevVersion() configuration |
| `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/logical/UnresolvedExternalRelation.java` | Unresolved plan node |
| `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/logical/ExternalRelation.java` | Resolved plan node |
| `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/analysis/PreAnalyzer.java` | Pre-analysis — extracts icebergPaths |
| `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/session/EsqlSession.java` | preAnalyzeExternalSources, extractIcebergParams |
| `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/ExternalSourceResolver.java` | Factory dispatch — canHandle() iteration |
| `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/DataSourceModule.java` | Factory registration — sourceFactories map |
| `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/OperatorFactoryRegistry.java` | Operator factory dispatch |
| `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/analysis/Analyzer.java` | ResolveExternalRelations analyzer rule |
| `x-pack/plugin/esql/src/test/java/org/elasticsearch/xpack/esql/parser/IcebergParsingTests.java` | EXTERNAL command parsing tests |
| `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/action/EsqlCapabilities.java` | EXTERNAL_COMMAND capability |
| `x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/telemetry/FeatureMetric.java` | EXTERNAL telemetry metric |
| `server/src/main/java/org/elasticsearch/cluster/metadata/IndexNameExpressionResolver.java` | SELECTOR_SEPARATOR = "::" definition |
