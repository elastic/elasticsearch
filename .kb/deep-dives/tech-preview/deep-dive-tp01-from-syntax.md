# Deep Dive: FROM datasource::expression Syntax

## Investigation Date: 2026-03-03
## Branch: esql/connector-spi-v3 (fresh from main)

---

## 1. ANTLR Grammar: How FROM Is Defined

### Parser Grammar

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/antlr/EsqlBaseParser.g4`

The `FROM` command is defined at line 102-104:

```antlr
fromCommand
    : FROM indexPatternAndMetadataFields          // line 103
    ;
```

The `indexPatternAndMetadataFields` rule (line 114-116):

```antlr
indexPatternAndMetadataFields
    : indexPatternOrSubquery (COMMA indexPatternOrSubquery)* metadata?
    ;
```

The `indexPattern` rule (line 127-131) -- this is the critical piece:

```antlr
indexPattern
    : (clusterString COLON)? unquotedIndexString (CAST_OP selectorString)?
    | indexString
    ;
```

### Key Observation: `::` Already Has Meaning in FROM

The `CAST_OP` token (defined in `Expression.g4` line 94 as `'::'`) is already used in the `indexPattern` rule as the **selector separator**. In the grammar, `CAST_OP selectorString` parses expressions like `my_index::data` or `my_index::failures`. These are **index component selectors** (data vs. failures partitions of data streams).

The full pattern for an index pattern in FROM is:
```
[cluster:]index[::selector]
```

Where:
- `cluster:` is optional (remote cluster)
- `index` is the index name/pattern
- `::selector` is optional (must be `data` or `failures`)

### EXTERNAL Command Grammar

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/antlr/EsqlBaseParser.g4`

The EXTERNAL command is defined at line 110-112:

```antlr
externalCommand
    : DEV_EXTERNAL stringOrParameter commandNamedParameters    // line 111
    ;
```

It is gated as a dev-only source command at line 48:

```antlr
sourceCommand
    : fromCommand
    | rowCommand
    | showCommand
    | timeSeriesCommand
    | promqlCommand
    // in development
    | {this.isDevVersion()}? explainCommand
    | {this.isDevVersion()}? externalCommand       // line 48
    ;
```

`stringOrParameter` (line 218-221) accepts either a quoted string or a query parameter:

```antlr
stringOrParameter
    : string
    | parameter
    ;
```

`commandNamedParameters` (line 265-267) accepts an optional `WITH mapExpression`:

```antlr
commandNamedParameters
    : (WITH mapExpression)?
    ;
```

### Lexer Grammar

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/antlr/lexer/From.g4`

```antlr
FROM : 'from'                 -> pushMode(FROM_MODE);    // line 12
TS : 'ts' -> pushMode(FROM_MODE);                        // line 15
DEV_EXTERNAL : {this.isDevVersion()}? 'external' -> pushMode(FROM_MODE);  // line 18
```

All three commands push into `FROM_MODE`. In FROM_MODE:

```antlr
FROM_COLON : COLON -> type(COLON);           // line 22 - single colon
FROM_SELECTOR : CAST_OP -> type(CAST_OP);    // line 23 - double colon ::
```

The `UNQUOTED_SOURCE_PART` fragment (line 44-47) excludes `:` from valid characters:

```antlr
fragment UNQUOTED_SOURCE_PART
    : ~[:"=|,[\]/() \t\r\n]          // line 45 - note : is excluded
    | '/' ~[*/]
    ;
```

**This means `:` cannot appear inside an unquoted source token.** The lexer splits on `:` and `::`, producing separate tokens. When the lexer encounters `my_s3::path`, it would tokenize as: `UNQUOTED_SOURCE("my_s3")`, `CAST_OP("::")`, `UNQUOTED_SOURCE("path")`.

---

## 2. LogicalPlanBuilder: FROM Handler

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/parser/LogicalPlanBuilder.java`

### visitFromCommand (line 451-453)

```java
@Override
public LogicalPlan visitFromCommand(EsqlBaseParser.FromCommandContext ctx) {
    return visitRelation(source(ctx), SourceCommand.FROM, ctx.indexPatternAndMetadataFields());
}
```

### visitRelation (line 368-419)

This is the core method. Key steps:

1. **Line 370-380**: Separates `indexPatternsCtx` from `subqueriesCtx`
2. **Line 381**: Calls `visitIndexPattern(indexPatternsCtx)` -- this goes to `IdentifierBuilder.visitIndexPattern()` which concatenates patterns with commas
3. **Line 382**: Handles subqueries
4. **Line 395**: Creates an `UnresolvedRelation` with an `IndexPattern` (a string wrapper)

```java
IndexPattern table = new IndexPattern(source, visitIndexPattern(indexPatternsCtx));
// ...
UnresolvedRelation unresolvedRelation = new UnresolvedRelation(source, table, false, metadataFields, null, command);
```

**There is zero external/datasource handling in visitRelation.** It always creates an `UnresolvedRelation`, which goes through standard ES index resolution. There is no branching based on the source expression pattern (no `::` datasource detection).

### visitExternalCommand (line 781-789)

```java
@Override
public LogicalPlan visitExternalCommand(EsqlBaseParser.ExternalCommandContext ctx) {
    Source source = source(ctx);
    Expression tablePath = expression(ctx.stringOrParameter());

    MapExpression options = visitCommandNamedParameters(ctx.commandNamedParameters());
    Map<String, Expression> params = options != null ? options.keyFoldedMap() : Map.of();

    return new UnresolvedExternalRelation(source, tablePath, params);
}
```

This creates an `UnresolvedExternalRelation` with:
- `tablePath`: A `Literal` expression containing the quoted string (e.g., `"s3://bucket/table"`)
- `params`: A map from the WITH clause (e.g., `{"access_key": "...", "secret_key": "..."}`)

---

## 3. UnresolvedExternalRelation

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/logical/UnresolvedExternalRelation.java`

### Fields (line 32-34)

```java
private final Expression tablePath;
private final Map<String, Expression> params;
private final String unresolvedMsg;
```

### Constructor (line 43-48)

```java
public UnresolvedExternalRelation(Source source, Expression tablePath, Map<String, Expression> params) {
    super(source);
    this.tablePath = tablePath;
    this.params = params;
    this.unresolvedMsg = "Unknown external table or Parquet file [" + extractTablePathValue(tablePath) + "]";
}
```

### Where Created

Only in `LogicalPlanBuilder.visitExternalCommand()` (line 788).

### Where Resolved

1. **PreAnalyzer** (`/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/analysis/PreAnalyzer.java`, line 73-82): Extracts string paths from `UnresolvedExternalRelation` nodes into `icebergPaths` list.

2. **EsqlSession** (`/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/session/EsqlSession.java`, line 851-872): `preAnalyzeExternalSources()` calls `ExternalSourceResolver.resolve()` with the paths.

3. **Analyzer** (`/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/analysis/Analyzer.java`, line 456-489): `ResolveExternalRelations` rule replaces `UnresolvedExternalRelation` with `ExternalRelation`.

---

## 4. ExternalRelation

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/logical/ExternalRelation.java`

### Fields (line 48-51)

```java
private final String sourcePath;
private final List<Attribute> output;
private final SourceMetadata metadata;
private final FileSet fileSet;
```

### Key Characteristics

- Extends `LeafPlan` and implements `ExecutesOn.Coordinator` (line 46) -- coordinator-only execution
- Not serializable (lines 75-82) -- `writeTo()` and `getWriteableName()` throw `UnsupportedOperationException`
- Has a `toPhysicalExec()` method (line 115-127) that creates `ExternalSourceExec`

### Relationship to EsRelation

`ExternalRelation` and `EsRelation` are siblings -- both extend `LeafPlan`. But they diverge significantly:

- `EsRelation` maps to `FragmentExec` in both `Mapper` and `LocalMapper`, enabling data-node dispatch
- `ExternalRelation` maps via `MapperUtils.mapLeaf()` directly to `ExternalSourceExec`, bypassing FragmentExec/ExchangeExec entirely

In `Mapper.mapLeaf()` (line 85-93):
```java
private PhysicalPlan mapLeaf(LeafPlan leaf) {
    if (leaf instanceof EsRelation esRelation) {
        return new FragmentExec(esRelation);
    }
    // ExternalRelation is handled by MapperUtils.mapLeaf()
    return MapperUtils.mapLeaf(leaf);
}
```

In `MapperUtils.mapLeaf()` (line 68-85):
```java
static PhysicalPlan mapLeaf(LeafPlan p) {
    if (p instanceof LocalRelation local) { ... }
    if (p instanceof ExternalRelation external) {
        return external.toPhysicalExec();
    }
    if (p instanceof ShowInfo showInfo) { ... }
    return unsupported(p);
}
```

---

## 5. EXTERNAL Command: Dev-Gated Details

### Feature Gate: `isDevVersion()`

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/parser/EsqlConfig.java`

```java
public class EsqlConfig {
    private final boolean isDevVersion;

    public EsqlConfig() {
        this(Build.current().isSnapshot());    // line 22
    }
}
```

The EXTERNAL command is gated by `{this.isDevVersion()}?` in both the lexer (From.g4 line 18) and the parser (EsqlBaseParser.g4 line 48). This predicate returns `true` only for snapshot builds (`Build.current().isSnapshot()`).

### Capability

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/action/EsqlCapabilities.java`

Line 2218-2221:
```java
/**
 * Support for the EXTERNAL command (datasource access).
 */
EXTERNAL_COMMAND(Build.current().isSnapshot()),
```

### Telemetry

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/telemetry/FeatureMetric.java`

Line 72:
```java
EXTERNAL(plan -> plan instanceof org.elasticsearch.xpack.esql.plan.logical.ExternalRelation),
```

### Current Syntax

```
EXTERNAL "s3://bucket/table"
EXTERNAL "s3://bucket/table" WITH { "key": "value" }
EXTERNAL ?param
```

Confirmed by tests in `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/test/java/org/elasticsearch/xpack/esql/parser/IcebergParsingTests.java`.

---

## 6. ExternalSourceResolver: Factory Dispatch

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/ExternalSourceResolver.java`

### Dispatch Flow

1. **Entry**: `resolve(paths, pathParams, filterHints, listener)` (line 85-125)
2. For each path, calls `resolveSource(path, config, hints, hivePartitioning)` (line 127-142)
3. `resolveSource` checks `GlobExpander.isMultiFile(path)` and branches:
   - Multi-file: `resolveMultiFileSource()` -- uses `StorageProviderRegistry` for glob expansion
   - Single-file: `resolveSingleSource()` (line 195-235)

### canHandle() Flow (line 195-235)

```java
private SourceMetadata resolveSingleSource(String path, Map<String, Object> config) {
    // Early scheme validation
    StoragePath parsed = StoragePath.of(path);
    if (capabilities.supportsScheme(parsed.scheme()) == false) {
        throw new UnsupportedSchemeException(...);
    }

    // Iterate factories
    for (ExternalSourceFactory factory : dataSourceModule.sourceFactories().values()) {
        if (factory.canHandle(path)) {
            return factory.resolveMetadata(path, config);
        }
    }
    throw new UnsupportedOperationException("No handler found...");
}
```

### How Config Reaches Factories

1. Parser creates `UnresolvedExternalRelation` with `Map<String, Expression> params` from the WITH clause
2. `EsqlSession.extractIcebergParams()` (line 878-888) extracts these into `Map<String, Map<String, Expression>> pathParams`
3. `ExternalSourceResolver.paramsToConfigMap()` (line 286-305) converts `Expression` values to `Map<String, Object>` by folding `Literal` values to strings
4. This `config` map is passed to `factory.resolveMetadata(path, config)`

---

## 7. IdentifierBuilder / Index Name Validation

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/parser/IdentifierBuilder.java`

### visitIndexPattern (line 115-128)

```java
public String visitIndexPattern(List<EsqlBaseParser.IndexPatternContext> ctx) {
    List<String> patterns = new ArrayList<>(ctx.size());
    ctx.forEach(c -> {
        String indexPattern = c.unquotedIndexString() != null ? c.unquotedIndexString().getText() : visitIndexString(c.indexString());
        String clusterString = visitClusterString(c.clusterString());
        String selectorString = visitSelectorString(c.selectorString());
        validate(clusterString, indexPattern, selectorString, c, hasSeenStar.get());
        patterns.add(reassembleIndexName(clusterString, indexPattern, selectorString));
    });
    return Strings.collectionToDelimitedString(patterns, ",");
}
```

### Validation Chain

The `validate()` method (line 186-234) calls `validateSingleIndexPattern()` (line 246-309) which:

1. **Line 266-274**: Tries `splitIndexName()` to extract cluster string from `remote:index`
2. **Line 286-297**: Tries `IndexNameExpressionResolver.splitSelectorExpression()` to extract selector from `index::selector`
3. **Line 299**: Calls `resolveAndValidateIndex()` on the remaining index name
4. **Line 300-308**: Validates the selector against `IndexComponentSelector` values

### What `splitSelectorExpression` Does

**File**: `/Users/oleglvovitch/github/root/elasticsearch/server/src/main/java/org/elasticsearch/cluster/metadata/IndexNameExpressionResolver.java`

Line 2264-2276:
```java
private static <V> V splitSelectorExpression(String expression, BiFunction<String, String, V> bindFunction) {
    int lastDoubleColon = expression.lastIndexOf(SELECTOR_SEPARATOR);  // "::"
    if (lastDoubleColon >= 0) {
        String suffix = expression.substring(lastDoubleColon + SELECTOR_SEPARATOR.length());
        IndexComponentSelector selector = resolveAndValidateSelectorString(() -> expression, suffix);
        // ...
    }
}
```

Line 2282-2291:
```java
private static IndexComponentSelector resolveAndValidateSelectorString(Supplier<String> expression, String suffix) {
    IndexComponentSelector selector = IndexComponentSelector.getByKey(suffix);
    if (selector == null) {
        throw new InvalidIndexNameException(
            expression.get(),
            "invalid usage of :: separator, [" + suffix + "] is not a recognized selector"
        );
    }
    return selector;
}
```

### Valid Selectors

**File**: `/Users/oleglvovitch/github/root/elasticsearch/server/src/main/java/org/elasticsearch/action/support/IndexComponentSelector.java`

```java
public enum IndexComponentSelector implements Writeable {
    DATA("data", (byte) 0),
    FAILURES("failures", (byte) 1);
}
```

Only `data` and `failures` are valid selectors.

### What Would Break If `my_s3::path` Were Used as FROM Source

If a user writes `FROM my_s3::path`:

1. **Lexer**: In FROM_MODE, `my_s3` becomes `UNQUOTED_SOURCE`, `::` becomes `CAST_OP`, `path` becomes `UNQUOTED_SOURCE`. The grammar parses this as `indexPattern: unquotedIndexString(my_s3) CAST_OP selectorString(path)`.

2. **IdentifierBuilder**: `visitIndexPattern()` would get `indexPattern="my_s3"`, `selectorString="path"`.

3. **Validation at line 300-308**: `IndexNameExpressionResolver.SelectorResolver.validateIndexSelectorString("my_s3", "path")` would throw `InvalidIndexNameException` because `"path"` is not a recognized selector (only `data` and `failures` are valid).

**Result**: Parse error -- `"invalid usage of :: separator, [path] is not a recognized selector"`.

### What About `FROM my_s3::s3://bucket/file.parquet`?

Same problem. The lexer would tokenize `s3` as unquoted source, then `:` would be a `COLON` token, then `//bucket/file.parquet` would hit the UNQUOTED_SOURCE rule but `/` followed by `/` would match the "allow single `/` but not followed by `*`" rule, so `//bucket/file.parquet` would be tokenized. But the parser grammar for `indexPattern` is:

```antlr
(clusterString COLON)? unquotedIndexString (CAST_OP selectorString)?
```

So the parser would try to interpret it as `cluster("my_s3") : index("s3") :: selector("//bucket/file.parquet")` which would fail selector validation.

### Additional Validation: MetadataCreateIndexService.validateIndexOrAliasName

**File**: `/Users/oleglvovitch/github/root/elasticsearch/server/src/main/java/org/elasticsearch/cluster/metadata/MetadataCreateIndexService.java`

Line 328-351. Invalid characters include: `\`, `/`, `*`, `?`, `"`, `<`, `>`, `|`, ` ` (space), `,`. Additionally, `:` is explicitly forbidden (line 338-340):

```java
if (index.contains(":")) {
    throw exceptionCtor.apply(index, "must not contain ':'");
}
```

So even if `::` were not interpreted as a selector, the `:` characters in `s3://` would fail index name validation.

---

## 8. What Exactly Needs to Change for `FROM datasource::expression`

### Design Requirements

The target syntax is `FROM datasource::expression` where:
- `datasource` is a registered datasource name (e.g., `my_s3`, `lakehouse`)
- `expression` is the datasource-specific path/identifier (e.g., `bucket/data/*.parquet`, `catalog.db.table`)

### Conflict with Existing `::` Semantics

The `::` token (`CAST_OP`) is already used in FROM context for index component selectors (`::data`, `::failures`). The new `datasource::expression` syntax reuses the same delimiter but with reversed semantics:
- **Existing**: `index_name::component_selector` (suffix)
- **Proposed**: `datasource_name::source_expression` (prefix)

**Disambiguation**: The grammar can distinguish these because:
- Selector values are from a closed set (`data`, `failures`)
- Datasource names would come from registered datasources (and could be validated at resolution time)
- More practically, we can parse them differently: if `::` appears, the suffix is checked against known selectors first; if it's not a known selector, treat the prefix as a datasource name

However, this approach has fragility issues. A cleaner approach is to define a new grammar alternative.

### Specific Code Changes Required

#### A. Lexer Grammar: No Changes Required

The `FROM_SELECTOR : CAST_OP -> type(CAST_OP)` rule already produces a `CAST_OP` token for `::` in FROM_MODE. The `UNQUOTED_SOURCE_PART` fragment already excludes `:`, so `datasource` and `expression` would be separate `UNQUOTED_SOURCE` tokens naturally.

#### B. Parser Grammar: New Alternative in indexPattern

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/antlr/EsqlBaseParser.g4`

Current (line 127-131):
```antlr
indexPattern
    : (clusterString COLON)? unquotedIndexString (CAST_OP selectorString)?
    | indexString
    ;
```

Proposed (adding a dev-gated datasource alternative):
```antlr
indexPattern
    : (clusterString COLON)? unquotedIndexString (CAST_OP selectorString)?
    | indexString
    | {this.isDevVersion()}? datasourcePattern
    ;

datasourcePattern
    : datasourceName CAST_OP datasourceExpression (commandNamedParameters)?
    ;

datasourceName
    : UNQUOTED_SOURCE
    ;

datasourceExpression
    : UNQUOTED_SOURCE
    | QUOTED_STRING
    ;
```

**Issue**: The `UNQUOTED_SOURCE_PART` fragment excludes `:`, `/`, `"`, spaces, etc. For datasource expressions like `s3://bucket/path/*.parquet`, the `/` characters would be consumed by the existing `'/' ~[*/]` alternative in `UNQUOTED_SOURCE_PART`, but `://` would fail (two slashes after `:` -- the `:` is excluded, and `//` at the start would have the first `/` consumed then the second would match `'/' ~[*/]` only if followed by a non-`*`/`/` char, but the second `/` IS followed by a non-`*`/`/` char in `//bucket`).

Wait -- let me re-read the fragment more carefully:

```antlr
fragment UNQUOTED_SOURCE_PART
    : ~[:"=|,[\]/() \t\r\n]          // any char except : " = | , [ ] / ( ) space tab \r \n
    | '/' ~[*/]                       // or / not followed by * or /
    ;
```

So `s3://bucket` would be tokenized as:
- `s3` is matched by `~[:"=|,[\]/() \t\r\n]+`
- `:` is NOT in UNQUOTED_SOURCE_PART -- it triggers `FROM_COLON`
- The second `:` triggers another `FROM_COLON` -- but wait, `::` would trigger `FROM_SELECTOR` (CAST_OP) since it's defined before FROM_COLON.

Actually, looking more carefully at the lexer rules in FROM_MODE:
```
FROM_COLON : COLON -> type(COLON);         // matches single ':'
FROM_SELECTOR : CAST_OP -> type(CAST_OP);  // matches '::'
```

ANTLR's maximal munch means `::` matches `FROM_SELECTOR` before `FROM_COLON`. For a single `:`, `FROM_COLON` fires.

So for `my_s3::s3://bucket/file.parquet`:
1. `my_s3` -> `UNQUOTED_SOURCE`
2. `::` -> `CAST_OP`
3. `s3` -> `UNQUOTED_SOURCE`
4. `:` -> `COLON`
5. `//` -> This is problematic. `/` followed by `/` fails the `'/' ~[*/]` alternative, and `/` is not in `~[:"=|,[\]/() \t\r\n]` either. So the first `/` alone doesn't match UNQUOTED_SOURCE_PART.

**This means paths containing `://` or `//` cannot be unquoted source tokens.** They must be quoted strings.

For a quoted approach, the syntax would be:
```
FROM my_s3::"s3://bucket/file.parquet"
```

For glob patterns without `://`:
```
FROM my_s3::bucket/data/*.parquet
```

Where `bucket/data/*.parquet` would tokenize as a single `UNQUOTED_SOURCE` because:
- `bucket` -> matches `~[:"=|,[\]/() \t\r\n]+`
- `/` -> matches `'/' ~[*/]` (if followed by `d`)
- `data` -> matches
- `/` -> matches `'/' ~[*/]` (if followed by `*`)

Wait -- `'/' ~[*/]` means `/` followed by a character that is NOT `*` or `/`. But `/*.` has `/` followed by `*`, which **fails** this alternative. So `bucket/data/*.parquet` would actually be tokenized as:
- `bucket/data/` -> fails at `/*` because `'/' ~[*/]` doesn't match `/` followed by `*`

Actually, re-reading: `'/' ~[*/]` is a fragment alternative inside `UNQUOTED_SOURCE_PART`, which means it matches exactly two characters: `/` followed by not-`*`-not-`/`. The first `UNQUOTED_SOURCE_PART` in `bucket` matches, then `UNQUOTED_SOURCE` = `UNQUOTED_SOURCE_PART+` keeps consuming. Let me trace more carefully:

`bucket/data/*.parquet`:
- `b` matches `~[:"=|,[\]/() \t\r\n]`
- `u`, `c`, `k`, `e`, `t` same
- `/d` matches `'/' ~[*/]` (/ followed by d, which is not * or /)
- `a`, `t`, `a` match first alternative
- `/` followed by `*` -- the `'/' ~[*/]` alternative requires the char after `/` to NOT be `*`, but `*` fails this. And `/` alone doesn't match the first alternative (it's excluded). So the UNQUOTED_SOURCE token ends at `bucket/data`, and then `/` starts a new token attempt.

But `/` alone doesn't match any rule in FROM_MODE. This would be a lexer error.

**Conclusion for the expression part**: Datasource expressions containing `/*/`, `/*`, or `//` (which are common in S3 paths and globs) CANNOT be represented as unquoted tokens in FROM_MODE. They must be quoted strings.

**Practical syntax would be**:
```
FROM my_s3::"s3://bucket/data/*.parquet"
FROM my_s3::"bucket/data/*.parquet"
FROM iceberg::"s3://bucket/warehouse/db/table"
```

Or alternatively, the datasource expression could live entirely in a quoted string after the `::`:
```antlr
datasourcePattern
    : datasourceName CAST_OP (UNQUOTED_SOURCE | QUOTED_STRING) commandNamedParameters?
    ;
```

#### C. IdentifierBuilder: Bypass Selector Validation

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/parser/IdentifierBuilder.java`

Currently, `validateSingleIndexPattern()` (line 246-309) passes the `::` suffix through `IndexNameExpressionResolver.SelectorResolver.validateIndexSelectorString()` which only accepts `data` or `failures`.

If we add a `datasourcePattern` alternative to the grammar, this validation would be bypassed entirely because the `datasourcePattern` would be parsed by a different visitor method, not through `visitIndexPattern()`.

#### D. LogicalPlanBuilder: New Visitor for Datasource Patterns

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/parser/LogicalPlanBuilder.java`

Option 1 -- Handle in `visitRelation()` by detecting `datasourcePattern` in the parse tree:

```java
// In visitRelation(), after extracting index patterns:
if (somePatternIsDatasource) {
    // Extract datasource name and expression
    // Create UnresolvedExternalRelation instead of UnresolvedRelation
}
```

Option 2 -- Intercept at the `indexPatternOrSubquery` level, returning a different plan node when a datasource pattern is detected.

The cleanest approach: add `visitDatasourcePattern()` that creates an `UnresolvedExternalRelation`, and modify `visitRelation()` to check for datasource patterns among the index patterns.

#### E. UnresolvedExternalRelation: Add Datasource Name

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/logical/UnresolvedExternalRelation.java`

Currently stores only `tablePath` and `params`. Would need to also store:
- `datasourceName`: The registered datasource name (e.g., `my_s3`)
- Or alternatively, prepend it to the path: `my_s3::s3://bucket/table`

#### F. ExternalSourceResolver: Route by Datasource Name

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/ExternalSourceResolver.java`

Currently iterates all factories with `canHandle(path)`. With datasource names, it could look up the factory by name directly:

```java
// Instead of iterating all factories:
ExternalSourceFactory factory = dataSourceModule.sourceFactories().get(datasourceName);
if (factory != null) {
    return factory.resolveMetadata(expression, config);
}
```

#### G. PreAnalyzer: Detect External Relations from FROM

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/analysis/PreAnalyzer.java`

Currently only collects paths from `UnresolvedExternalRelation` (line 73-82). If `FROM datasource::expr` creates `UnresolvedExternalRelation` nodes, this code already handles them.

### Summary of Changes Required

| Component | File | Change |
|-----------|------|--------|
| **Parser grammar** | `EsqlBaseParser.g4` | Add `datasourcePattern` alternative to `indexPattern` rule |
| **Lexer grammar** | `lexer/From.g4` | No changes needed (CAST_OP already tokenized) |
| **LogicalPlanBuilder** | `LogicalPlanBuilder.java` | Modify `visitRelation()` to detect datasource patterns and create `UnresolvedExternalRelation` |
| **IdentifierBuilder** | `IdentifierBuilder.java` | No changes if using a new grammar alternative (bypasses existing validation) |
| **UnresolvedExternalRelation** | `UnresolvedExternalRelation.java` | Add optional `datasourceName` field |
| **PreAnalyzer** | `PreAnalyzer.java` | Already handles `UnresolvedExternalRelation` -- no changes needed |
| **Analyzer** | `Analyzer.java` | `ResolveExternalRelations` already handles resolution -- may need datasource-name-aware dispatch |
| **ExternalSourceResolver** | `ExternalSourceResolver.java` | Add datasource-name-based factory lookup |
| **EsqlCapabilities** | `EsqlCapabilities.java` | Add new capability for FROM datasource syntax |
| **EXTERNAL command** | Multiple files | Consider deprecating/removing once FROM syntax works |

### Key Design Decision: Quoting

Given the lexer constraints, there are two practical syntax approaches:

**Option A -- Expression always quoted**:
```
FROM my_s3::"s3://bucket/data/*.parquet"
FROM iceberg::"glue://warehouse/db/table"
```
Pros: Simple grammar change, works with any path. Cons: Always requires quotes.

**Option B -- Expression can be unquoted for simple patterns**:
```
FROM my_s3::bucket/data/file.parquet       -- works (no glob, no //)
FROM my_s3::"s3://bucket/data/*.parquet"   -- must quote (has :// and /*)
```
Pros: Simpler for simple cases. Cons: Inconsistent quoting rules, confusing for users.

**Recommendation**: Option A (always quoted) is simpler and more consistent. The EXTERNAL command already requires quoting, so users are accustomed to it.

### Alternative: Use Different Separator

Instead of `::` (which conflicts with selectors), use a different separator:
- `FROM my_s3 >> "s3://bucket/path"` (ugly)
- `FROM my_s3 : "s3://bucket/path"` (single colon -- conflicts with remote cluster syntax)
- `FROM @my_s3("s3://bucket/path")` (function-like syntax)

The `::` separator is preferred despite the selector conflict because:
1. It's already the established ES|QL convention for type qualification (`field::type`)
2. The grammar can disambiguate (selectors are always suffixes with known values)
3. It reads naturally: `datasource::expression`

---

## Appendix: File Path Reference

All files referenced in this document:

| Description | Absolute Path |
|-------------|---------------|
| Parser grammar | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/antlr/EsqlBaseParser.g4` |
| Lexer grammar (main) | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/antlr/EsqlBaseLexer.g4` |
| Lexer grammar (From) | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/antlr/lexer/From.g4` |
| Lexer grammar (Expression) | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/antlr/lexer/Expression.g4` |
| LogicalPlanBuilder | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/parser/LogicalPlanBuilder.java` |
| IdentifierBuilder | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/parser/IdentifierBuilder.java` |
| ExpressionBuilder | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/parser/ExpressionBuilder.java` |
| EsqlConfig | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/parser/EsqlConfig.java` |
| UnresolvedExternalRelation | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/logical/UnresolvedExternalRelation.java` |
| ExternalRelation | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/logical/ExternalRelation.java` |
| UnresolvedRelation | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plan/logical/UnresolvedRelation.java` |
| PreAnalyzer | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/analysis/PreAnalyzer.java` |
| Analyzer | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/analysis/Analyzer.java` |
| EsqlSession | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/session/EsqlSession.java` |
| ExternalSourceResolver | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/ExternalSourceResolver.java` |
| Mapper | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/planner/mapper/Mapper.java` |
| LocalMapper | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/planner/mapper/LocalMapper.java` |
| MapperUtils | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/planner/mapper/MapperUtils.java` |
| EsqlCapabilities | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/action/EsqlCapabilities.java` |
| FeatureMetric | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/telemetry/FeatureMetric.java` |
| IndexNameExpressionResolver | `/Users/oleglvovitch/github/root/elasticsearch/server/src/main/java/org/elasticsearch/cluster/metadata/IndexNameExpressionResolver.java` |
| IndexComponentSelector | `/Users/oleglvovitch/github/root/elasticsearch/server/src/main/java/org/elasticsearch/action/support/IndexComponentSelector.java` |
| MetadataCreateIndexService | `/Users/oleglvovitch/github/root/elasticsearch/server/src/main/java/org/elasticsearch/cluster/metadata/MetadataCreateIndexService.java` |
| Strings | `/Users/oleglvovitch/github/root/elasticsearch/server/src/main/java/org/elasticsearch/common/Strings.java` |
| IcebergParsingTests | `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/test/java/org/elasticsearch/xpack/esql/parser/IcebergParsingTests.java` |
