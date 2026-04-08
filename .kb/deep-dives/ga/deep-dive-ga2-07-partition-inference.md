# Deep Dive: Automatic Partition Inference for Non-Hive Layouts

## Executive Summary

The codebase already has a **three-tier partition detection system**: Hive (automatic), Template (user-configured), and Auto (Hive-first with Template fallback). The "automatic inference for non-Hive layouts" gap is that bare directory structures like `2024/03/15/` are **only** detected when the user explicitly provides a `partition_path` template via the WITH clause. True automatic detection of non-Hive patterns requires a new heuristic layer in the `AutoPartitionDetector` that analyzes path structure across the file set without user hints.

---

## 1. HivePartitionDetector

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/HivePartitionDetector.java`

### Detection Algorithm (lines 46-100)

1. **Extract partitions from each file** (line 54-55): For each `StorageEntry`, calls `extractPartitions(entry.path())`.

2. **extractPartitions** (lines 102-135): Splits the path on `/`, then for each segment:
   - Skips empty segments (line 113)
   - Looks for `=` sign: requires `eqIdx > 0` AND `eqIdx < segment.length() - 1` (line 116) -- both key and value must be non-empty
   - Rejects segments with multiple `=` (line 120-122): `afterEq.indexOf('=') >= 0`
   - Rejects segments containing `.` (line 123-125) -- this filters out filenames like `data=file.parquet`
   - URL-decodes the value (line 127)
   - Skips duplicate keys (line 128-130)

3. **Consistency validation** (lines 56-65): If ANY file has zero Hive-style partitions, returns `EMPTY`. If any file's partition key set differs from the first file's, returns `EMPTY`. This is an **all-or-nothing** approach.

4. **Collect values per column** (lines 74-82): Transposes per-file maps into per-column value lists.

5. **Type inference** (lines 84-87): For each column, calls `inferType(values)`.

6. **inferType** (lines 145-157): Spark-style cascade:
   - Try `Integer` / `Long` via `StringUtils.parseIntegral` (lines 159-171)
   - Try `Double` via `StringUtils.parseDouble` (lines 174-180)
   - Try `Boolean` -- only exact `true`/`false` case-insensitive (lines 185-189)
   - Fallback to `KEYWORD` (line 156)

7. **Cast values** (lines 194-208): Convert string values to typed Java objects based on inferred type.

8. **Returns**: `PartitionMetadata` containing:
   - `partitionColumns`: `LinkedHashMap<String, DataType>` -- column names to types, insertion-ordered
   - `filePartitionValues`: `LinkedHashMap<StoragePath, Map<String, Object>>` -- per-file typed values

### Key Design Properties
- Singleton: `static final HivePartitionDetector INSTANCE` (line 32)
- Implements `PartitionDetector` interface (line 30)
- `detect(files, config)` ignores config entirely (line 43) -- config parameter exists only for the interface

---

## 2. VirtualColumnInjector

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/VirtualColumnInjector.java`

### Mechanism (lines 27-113)

**Construction** (lines 35-59):
- Takes `fullOutput` (complete schema including partition columns), `partitionColumnNames`, `partitionValues`, and `blockFactory`
- Splits the full output into two index arrays:
  - `dataColumnIndices` -- positions of real data columns in the output schema
  - `partitionColumnIndices` -- positions of virtual partition columns

**Injection** (`inject` method, lines 73-93):
1. If no partition columns, returns the data page unchanged (line 74-76)
2. Creates a new `Block[]` array sized to `fullOutput.size()` (line 79)
3. Copies data blocks from the input page into their correct positions using `dataColumnIndices` (lines 81-84)
4. For each partition column index, creates a **constant block** with the partition value for all positions (lines 86-90)
5. Returns a new `Page(positions, blocks)` (line 92)

**Constant block creation** (`createConstantBlock`, lines 103-112):
- Pattern-matches on the value type: `Integer`, `Long`, `Double`, `Boolean`, `null` (empty BytesRef), or default (String as BytesRef)
- All partition values are constant across every row in a page -- this is the key insight: partition values are per-file, not per-row

### Usage Pattern
The `VirtualColumnInjector` is created per-operator in `AsyncExternalSourceOperatorFactory`:

- **Multi-file mode** (line 176): One injector for the whole file set (uses the same partition values -- this seems like a bug or the multi-file mode doesn't use per-file values)
- **Slice queue mode** (lines 224-231): Creates a NEW injector per `FileSplit`, using `fileSplit.partitionValues()` -- this is the correct distributed behavior

---

## 3. PartitionMetadata and Related Records

### PartitionMetadata
**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/PartitionMetadata.java`

A record (line 25) carrying:
- `Map<String, DataType> partitionColumns` -- column names to inferred types, insertion-ordered
- `Map<StoragePath, Map<String, Object>> filePartitionValues` -- per-file partition key-value pairs with typed values
- Has `EMPTY` sentinel (line 27)
- Both maps are deep-copied to unmodifiable in compact constructor (lines 29-38)

### PartitionDetector Interface
**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/PartitionDetector.java`

Simple interface (lines 18-23):
```java
public interface PartitionDetector {
    PartitionMetadata detect(List<StorageEntry> files, Map<String, Object> config);
    String name();
}
```

### StorageEntry
**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/StorageEntry.java`

Record (line 17): `(StoragePath path, long length, Instant lastModified)`

### FileSplit
**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/FileSplit.java`

Carries partition values per-split (line 41): `Map<String, Object> partitionValues`
- Serializable via `StreamInput`/`StreamOutput` (lines 69-88) -- partition values are sent over the wire via `writeGenericMap`
- This means partition values survive distributed execution

### FileSet
**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/FileSet.java`

Carries `PartitionMetadata` optionally (line 30). Three states: `UNRESOLVED`, `EMPTY`, or resolved with files + partition metadata.

---

## 4. Where Partition Detection Happens in the Pipeline

### Phase 1: Resolution Time (pre-analysis)

**Entry point**: `EsqlSession.preAnalyzeExternalSources()` at line 851.
**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/session/EsqlSession.java`

1. **Filter hint extraction** (line 864): `PartitionFilterHintExtractor.extract(plan)` walks the unresolved logical plan to find filter predicates above `UnresolvedExternalRelation` nodes.

2. **Resolution** (line 866): Calls `externalSourceResolver.resolve(paths, pathParams, filterHints, listener)`.

3. **In ExternalSourceResolver.resolveMultiFileSource()** (line 144):
   **File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/ExternalSourceResolver.java`

   - Calls `GlobExpander.expandGlob()` or `expandCommaSeparated()` (lines 161-165)
   - Inside `GlobExpander.doExpandGlob()` at line 139: `detectPartitions(matched, hivePartitioning, partitionConfig, config)` is called **after** files are listed and matched
   - `detectPartitions()` (lines 144-171) resolves the detector via `resolveDetector(partitionConfig)` and calls `detector.detect(files, config)`
   - The resulting `PartitionMetadata` is stored in the `FileSet`

4. **Schema enrichment** (lines 176-179): If partition metadata is found, `enrichSchemaWithPartitionColumns()` adds partition columns to the schema as `ReferenceAttribute` with `Nullability.TRUE` (line 253).

### Phase 2: Split Discovery Time

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/SplitDiscoveryPhase.java`

1. `resolveExternalSplits()` (line 39) walks the physical plan.
2. For each `ExternalSourceExec`, extracts `PartitionMetadata` from the `FileSet` (line 95).
3. Creates `SplitDiscoveryContext` with partition info and ancestor filter expressions (lines 97-103).
4. Calls `splitProvider.discoverSplits(context)` (line 107).

**In `FileSplitProvider.discoverSplits()`** (line 54):
**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/FileSplitProvider.java`

- For each file, looks up partition values from `partitionInfo.filePartitionValues()` (lines 69-74)
- **L1 partition pruning** (lines 76-79): If the file has partition values and there are filter hints, evaluates `matchesPartitionFilters()` and skips non-matching files
- Creates `FileSplit` with partition values (lines 96, 100) -- these values travel with the split

### Phase 3: Page Reading Time (Operator Execution)

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/AsyncExternalSourceOperatorFactory.java`

1. In `get()` (line 169), the slice queue path creates a `VirtualColumnInjector` per `FileSplit` (lines 224-231), using `fileSplit.partitionValues()`
2. The injector wraps the page iterator via `InjectingIterator` (line 339)
3. Each page returned by the `FormatReader` passes through `injector.inject(page)` which appends constant partition blocks

### Summary of Pipeline Stages

| Stage | Where | What happens |
|-------|-------|-------------|
| Pre-analysis | `EsqlSession` line 864 | Extract filter hints from unresolved plan |
| Resolution | `ExternalSourceResolver` line 144 | List files, detect partitions, enrich schema |
| Split discovery | `SplitDiscoveryPhase` line 86 | L1 pruning, partition values attached to splits |
| Execution | `AsyncExternalSourceOperatorFactory` line 211 | VirtualColumnInjector injects constant blocks per split |

---

## 5. File Path Listing

### How Files Are Enumerated

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/GlobExpander.java`

1. `doExpandGlob()` (line 83): Parses the pattern into `StoragePath`, extracts `patternPrefix()` and `globPart()` (lines 109-111).
2. Calls `provider.listObjects(prefix, recursive)` (line 117) which returns a `StorageIterator`.
3. Iterates results, computing relative paths and applying `GlobMatcher` (lines 118-131).
4. Sorts matched entries by path (line 137).
5. Runs `detectPartitions()` on the matched entries (line 139).

### StorageProvider.listObjects
**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/StorageProvider.java`

Interface method (line 40): `StorageIterator listObjects(StoragePath prefix, boolean recursive)`
- S3/GCS/Azure/HTTP implementations each provide their own listing
- Returns lazy `StorageIterator` for large directories

### Where Path Pattern Analysis Would Happen

For automatic non-Hive partition detection, the analysis point is in `GlobExpander.detectPartitions()` (line 144), which calls `detector.detect(files, config)`. The `AutoPartitionDetector.detect()` method at line 38 is where the fallback logic lives. Currently it falls back to template-based detection only if a template is configured.

---

## 6. Current partition_path Config

### PartitionConfig Record
**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/PartitionConfig.java`

**Config keys** (lines 27-29):
```java
public static final String CONFIG_PARTITIONING_DETECTION = "partition_detection";  // strategy
public static final String CONFIG_PARTITIONING_PATH = "partition_path";             // template
public static final String CONFIG_PARTITIONING_HIVE = "hive_partitioning";         // boolean
```

**Strategies** (lines 22-25): `AUTO`, `HIVE`, `TEMPLATE`, `NONE`

**Parsing** (`fromConfig`, lines 39-55):
- Reads `partition_detection` from config map, defaults to `AUTO` (line 45)
- Reads `partition_path` for template string (line 47-48)
- If template is provided AND strategy is AUTO, automatically switches to `TEMPLATE` strategy (lines 50-52)
- Default: `new PartitionConfig(AUTO, null)` (line 31)

### How partition_path Is Used

The `partition_path` value becomes the template string passed to `TemplatePartitionDetector`. Example WITH clause usage:

```
EXTERNAL "s3://bucket/data/*/*/*.parquet"
WITH partition_path = "{year}/{month}/{day}"
```

This tells the system to interpret the last 3 directory segments before the filename as `year`, `month`, `day`.

### GlobExpander.resolveDetector
**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/GlobExpander.java` (lines 32-49)

Dispatches based on strategy:
- `NONE` -> null (no detection)
- `HIVE` -> `HivePartitionDetector.INSTANCE`
- `TEMPLATE` -> `new TemplatePartitionDetector(template)`
- `AUTO` -> `AutoPartitionDetector.fromConfig(config)`

---

## 7. What Exactly Needs to Change for Automatic Non-Hive Detection

### The Problem

For paths like `s3://bucket/data/2024/03/15/*.parquet`, Hive detection fails (no `key=value` segments), and without a user-provided `partition_path` template, the `AutoPartitionDetector` returns `EMPTY`. The user must manually specify `partition_path = "{year}/{month}/{day}"`.

### Required Changes

#### 7a. New Heuristic in AutoPartitionDetector

**File to modify**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/AutoPartitionDetector.java`

Currently (lines 38-53):
```java
public PartitionMetadata detect(List<StorageEntry> files, Map<String, Object> config) {
    // Try Hive first
    PartitionMetadata hiveResult = HivePartitionDetector.INSTANCE.detect(files, config);
    if (hiveResult.isEmpty() == false) {
        return hiveResult;
    }
    // Fall back to template if configured
    String template = partitionConfig != null ? partitionConfig.pathTemplate() : null;
    if (template != null && template.isEmpty() == false) {
        TemplatePartitionDetector templateDetector = new TemplatePartitionDetector(template);
        return templateDetector.detect(files, config);
    }
    return PartitionMetadata.EMPTY;
}
```

Needs a third fallback step: **structural inference** that analyzes the directory segments across all files to identify common partition-like patterns.

#### 7b. What the Heuristic Must Do

1. **Find the common prefix**: Identify the longest common directory path across all files. Everything between the common prefix and the filename is the "variable segment zone."

2. **Count variable segments**: Each file should have the same number of variable directory segments between the prefix and the filename. If segment counts differ, return EMPTY.

3. **Assign synthetic column names**: Since there are no `key=value` labels, the heuristic must either:
   - Use positional names: `_partition_0`, `_partition_1`, ...
   - Use type-based heuristic names: if all values in a segment are 4-digit numbers, call it `year`; if all are 1-2 digit numbers in range 1-12, call it `month`; etc.
   - The safer approach is positional names, leaving semantic naming for the user via `partition_path`.

4. **Type inference**: Reuse `HivePartitionDetector.inferType()` (already package-visible at line 145).

5. **Minimum file threshold**: Require at least 2 files to infer patterns (single-file partitioning is meaningless).

#### 7c. Where It Goes in the Pipeline

The heuristic runs at the same point as existing detection -- inside `AutoPartitionDetector.detect()`, as a third fallback after Hive and Template detection. No pipeline changes needed.

**Modified flow**:
```
AutoPartitionDetector.detect():
  1. Try Hive -> if success, return
  2. Try Template (if configured) -> if success, return
  3. Try StructuralInference (NEW) -> if success, return
  4. Return EMPTY
```

#### 7d. Potential New Class: StructuralPartitionDetector

A new `PartitionDetector` implementation that:

1. Splits each file path into segments
2. For each position from the end (right before the filename), checks if the segment varies across files
3. Identifies the "partition zone" -- contiguous variable segments above the filename
4. If the partition zone is empty (all files in the same directory), returns EMPTY
5. Otherwise, creates columns named `_p0`, `_p1`, ... (or applies date heuristics)
6. Runs type inference on collected values

#### 7e. Date Pattern Heuristics (Optional Enhancement)

For common patterns like Kinesis Firehose (`YYYY/MM/DD/HH`):

| Pattern | Heuristic | Column Name |
|---------|-----------|-------------|
| 4-digit, range 1970-2099 | Year | `year` |
| 1-2 digit, range 01-12 | Month | `month` |
| 1-2 digit, range 01-31 | Day | `day` |
| 1-2 digit, range 00-23 | Hour | `hour` |
| ISO date `YYYY-MM-DD` | Date | `date` |

This is risky because:
- False positives (e.g., partition IDs that happen to be in range 1-12)
- Ambiguity (is `03/15` month/day or day/month?)
- Better to leave semantic naming to the user and just assign positional names

#### 7f. Glob Rewriting Implications

For non-Hive partition pruning via glob rewriting, the `GlobExpander.rewriteGlobWithHints()` method already handles template-based rewriting (lines 263-319). If structural inference assigns column names, the same glob rewriting path works -- the template column names map to wildcard positions.

However, without a user-provided template, the system would need to infer which wildcard segments correspond to which detected partition columns. This is already handled by `rewriteGlobWithTemplate()` (line 263) which maps template columns to wildcard positions right-to-left.

#### 7g. Risk Assessment

| Risk | Severity | Mitigation |
|------|----------|------------|
| False positive partition detection | Medium | Require minimum 2 files, require consistent segment structure |
| Incorrect date inference | High | Use positional names by default, date heuristics opt-in |
| Performance overhead from analyzing all paths | Low | Linear scan, already iterating for Hive detection |
| Breaking change for existing queries | None | New behavior only when AUTO detects nothing else |
| Column name instability across runs | Medium | Positional names are stable; date heuristics could change if file set changes |

---

## 8. Complete File Index

### Core Implementation
| File | Purpose |
|------|---------|
| `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/PartitionDetector.java` | Interface: `detect(files, config) -> PartitionMetadata` |
| `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/HivePartitionDetector.java` | Hive-style `key=value` detection |
| `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/TemplatePartitionDetector.java` | User-template `{name}` detection |
| `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/AutoPartitionDetector.java` | Hive-first, template-fallback orchestrator |
| `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/PartitionConfig.java` | WITH clause config: `partition_detection`, `partition_path`, `hive_partitioning` |
| `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/PartitionMetadata.java` | Record: partition columns + per-file values |
| `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/VirtualColumnInjector.java` | Injects constant partition blocks into pages |
| `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/GlobExpander.java` | Glob expansion + partition detection + glob rewriting |
| `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/FileSplit.java` | Split carrying `partitionValues` map (serializable) |
| `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/FileSplitProvider.java` | L1 partition pruning during split discovery |
| `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/SplitDiscoveryPhase.java` | Physical plan walker, split enrichment |
| `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/ExternalSourceResolver.java` | Resolution: file listing, partition detection, schema enrichment |
| `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/AsyncExternalSourceOperatorFactory.java` | Operator creation, VirtualColumnInjector wiring |
| `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/PartitionFilterHintExtractor.java` | Extracts filter hints from unresolved plan for glob rewriting |
| `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/session/EsqlSession.java` | Pre-analysis entry point (line 851) |
| `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/planner/LocalExecutionPlanner.java` | Operator factory creation with partition column names (line 1252) |

### SPI
| File | Purpose |
|------|---------|
| `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/StoragePath.java` | URI-like path with glob support |
| `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/StorageProvider.java` | Interface: `listObjects()`, `newObject()`, `exists()` |
| `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/SplitDiscoveryContext.java` | Context for split discovery with partition info |
| `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/SourceOperatorContext.java` | Context for operator creation with partition column names |

### Tests
| File | Purpose |
|------|---------|
| `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/test/java/org/elasticsearch/xpack/esql/datasources/HivePartitionDetectorTests.java` | 15 tests covering Hive detection, type inference, edge cases |
| `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/test/java/org/elasticsearch/xpack/esql/datasources/TemplatePartitionDetectorTests.java` | 12 tests covering template detection, Kinesis layout, Hudi layout |
| `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/test/java/org/elasticsearch/xpack/esql/datasources/AutoPartitionDetectorTests.java` | 7 tests covering auto detection, Hive-first ordering |
| `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/test/java/org/elasticsearch/xpack/esql/datasources/VirtualColumnInjectorTests.java` | 6 tests covering injection of all types, ordering, empty pages |
| `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/test/java/org/elasticsearch/xpack/esql/datasources/GlobExpanderTests.java` | 18 tests covering glob expansion, Hive detection integration, template rewriting |

---

## 9. Key Architectural Observation

The partition detection system has clean extension points. Adding a new `StructuralPartitionDetector` requires:

1. **One new class** implementing `PartitionDetector`
2. **One modification** to `AutoPartitionDetector.detect()` -- add a third fallback step
3. **Zero changes** to the rest of the pipeline -- `PartitionMetadata`, `VirtualColumnInjector`, `FileSplitProvider`, `SplitDiscoveryPhase`, and `ExternalSourceResolver` all work with the same `PartitionMetadata` output regardless of how it was produced

The existing `TemplatePartitionDetector` already demonstrates the pattern for non-Hive detection -- it extracts values positionally from the last N segments before the filename. The structural detector would just automate the template discovery step.
