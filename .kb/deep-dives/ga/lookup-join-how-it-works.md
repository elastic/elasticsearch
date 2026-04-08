# How LOOKUP JOIN Works in ES|QL

A comprehensive walkthrough of LOOKUP JOIN, from the query text to the final joined result pages. Written as a teaching guide — if you can follow this document end-to-end, you understand one of ESQL's most complex features.

---

## Table of Contents

1. [What is LOOKUP JOIN?](#1-what-is-lookup-join)
2. [The Two Syntax Paths](#2-the-two-syntax-paths)
3. [Parsing: Text to Parse Tree](#3-parsing-text-to-parse-tree)
4. [Logical Plan Construction](#4-logical-plan-construction)
5. [Analysis: Resolution and Validation](#5-analysis-resolution-and-validation)
6. [The Surrogate Pattern](#6-the-surrogate-pattern)
7. [Physical Planning: Mapper](#7-physical-planning-mapper)
8. [Local Execution Planning](#8-local-execution-planning)
9. [The Operator: LookupFromIndexOperator](#9-the-operator-lookupfromindexoperator)
10. [The Lookup Service: Query and Extract](#10-the-lookup-service-query-and-extract)
11. [Joining: RightChunkedLeftJoin](#11-joining-rightchunkedleftjoin)
12. [End-to-End Data Flow](#12-end-to-end-data-flow)
13. [Key Files Reference](#13-key-files-reference)

---

## 1. What is LOOKUP JOIN?

LOOKUP JOIN is a LEFT OUTER JOIN between the main query (left side) and a **lookup index** (right side). A lookup index is an Elasticsearch index created with `index.mode: lookup` — this guarantees a single shard and optimizes the index for point lookups.

The canonical use case: enriching streaming data with reference data.

```
FROM logs
| LOOKUP JOIN users ON user_id
| KEEP timestamp, user_id, user_name, event_type
```

This takes every row from `logs`, looks up the matching row in `users` by `user_id`, and brings back `user_name` (and any other fields from `users`). Rows in `logs` with no match get `null` for the lookup fields (LEFT JOIN semantics).

### Key Constraints

- The right side **must** be an `IndexMode.LOOKUP` index (single shard, no replication)
- The join is always LEFT OUTER (every left row survives, unmatched get nulls)
- No wildcards, no CCS, no index selectors on the right side

---

## 2. The Two Syntax Paths

LOOKUP JOIN has two syntax forms that converge to the same execution:

### Legacy: `LOOKUP` command (preview only)

```
FROM logs | LOOKUP users ON user_id
```

- Grammar: `EsqlBaseParser.g4:371-373`
- Gated by `Build.current().isSnapshot()` — snapshot builds only
- Produces a `Lookup` node (UnaryPlan)

### Modern: `JOIN LOOKUP` command

```
FROM logs | LOOKUP JOIN users ON user_id
```

- Grammar: `Join.g4:9-22`
- Gated by `EsqlCapabilities.Cap.JOIN_LOOKUP_V12`
- Produces a `LookupJoin` node (extends Join, which is a BinaryPlan)
- Supports expression-based conditions: `ON left_a == right_b AND left_a >= right_c`

Both paths ultimately produce a `Join` node through the **surrogate pattern** (explained in section 6).

---

## 3. Parsing: Text to Parse Tree

### The ANTLR Grammar

**LOOKUP command** (`EsqlBaseParser.g4:371`):
```antlr
lookupCommand
    : DEV_LOOKUP tableName=indexPattern ON matchFields=qualifiedNamePatterns
    ;
```

**JOIN LOOKUP command** (`Join.g4:9-22`):
```antlr
joinCommand
    : type=(JOIN_LOOKUP | DEV_JOIN_LEFT | DEV_JOIN_RIGHT) JOIN joinTarget joinCondition
    ;

joinTarget
    : index=indexPattern
    ;

joinCondition
    : ON booleanExpression (COMMA booleanExpression)*
    ;
```

The lexer (`lexer/Join.g4`) defines `JOIN_LOOKUP : 'lookup'` and manages mode transitions — when `ON` is encountered, the lexer switches to `EXPRESSION_MODE` to parse the join condition.

### What the Parser Validates

The `visitJoinCommand()` method in `LogicalPlanBuilder.java:816-867` enforces:

1. **No wildcards** — `users*` is rejected
2. **No CCS** — `remote:users` is rejected
3. **No selectors** — `users::data` is rejected
4. **Only LOOKUP type** — INNER, FULL, CROSS throw `ParsingException`

These are hard grammar-level constraints, not soft analysis warnings.

---

## 4. Logical Plan Construction

### LogicalPlanBuilder Visitors

**LOOKUP command** (`visitLookupCommand`, line 792):
```java
// Returns a PlanFactory — a lambda that takes the child plan and wraps it
return p -> new Lookup(source, p, tableName, matchFields, null);
//                                                          ↑ localRelation is null initially
```

The `Lookup` node is a `UnaryPlan` — it has one child (the left side). The right side is represented as a `tableName` literal (a string), not as a plan node. The `localRelation` field starts null and gets filled during analysis.

**JOIN LOOKUP command** (`visitJoinCommand`, line 816):
```java
// Create right side as an UnresolvedRelation with IndexMode.LOOKUP
UnresolvedRelation right = new UnresolvedRelation(
    source(target),
    new IndexPattern(source(target.index), rightPattern),
    false,                    // not a subquery
    emptyList(),              // no aliases
    IndexMode.LOOKUP,         // CRITICAL: marks this as a lookup index
    null                      // no metadata
);

return p -> new LookupJoin(source, p, right, joinFields, joinOnConditions);
```

The `LookupJoin` node extends `Join` which is a `BinaryPlan` — it has two children (left and right). The right side starts as `UnresolvedRelation` and gets resolved during analysis to `EsRelation`.

### The Plan Node Hierarchy

```
                    LogicalPlan
                    /         \
               UnaryPlan    BinaryPlan
                  |            |
               Lookup         Join
                              |
                          LookupJoin
```

**`Lookup`** (`plan/logical/Lookup.java`):
- Fields: `tableName` (Expression), `matchFields` (List<Attribute>), `localRelation` (LocalRelation, nullable)
- Implements `SurrogateLogicalPlan` — will be converted to `Join`

**`LookupJoin`** (`plan/logical/join/LookupJoin.java`):
- Inherits from `Join`: has `JoinConfig` with type, leftFields, rightFields, joinOnConditions
- Also implements `SurrogateLogicalPlan`

**`Join`** (`plan/logical/join/Join.java`):
- The base join node, a `BinaryPlan` with `JoinConfig`
- This is what both paths converge to after surrogate substitution

**`JoinConfig`** (`plan/logical/join/JoinConfig.java`):
```java
public record JoinConfig(
    JoinType type,                          // LEFT for LOOKUP JOIN
    List<Attribute> leftFields,             // Join keys from left side
    List<Attribute> rightFields,            // Join keys from right side
    @Nullable Expression joinOnConditions   // Optional expression filter
)
```

---

## 5. Analysis: Resolution and Validation

Analysis transforms the unresolved plan nodes into resolved ones. For LOOKUP JOIN, this involves two rules running at different phases.

### Rule 1: ResolveLookupTables (early, LOOKUP command only)

**Location**: `Analyzer.java:577-623`

This rule runs early in analysis (before `ResolveRefs`) and handles the old LOOKUP command path. It resolves the `tableName` against the query configuration's `tables` map:

```java
Map<String, Map<String, Column>> tables = context.configuration().tables();
if (tables.containsKey(tableName)) {
    localRelation = tableMapAsRelation(source, tables.get(tableName));
}
```

The `tables` map comes from the ENRICH policy infrastructure — it contains pre-loaded lookup data that can be shipped to each node. After this rule, the `Lookup` node has its `localRelation` populated.

### Rule 2: ResolveRefs (main resolution)

**Location**: `Analyzer.java:651`

This rule resolves attributes (field references) against the output schemas of child nodes.

**For LOOKUP command** — `resolveLookup()` (lines 797-857):
1. For each match field, resolve against the lookup table's output
2. If found in lookup, also resolve the same field name against the left side
3. Check type compatibility: both sides must have compatible types (numeric↔numeric, KEYWORD↔TEXT, NULL matches anything)

**For JOIN LOOKUP command** — `resolveLookupJoin()` (lines 966-1006):
1. For field-based joins (`ON field1, field2`): resolve `leftFields` against left output, `rightFields` against right output
2. For expression-based joins (`ON left_a == right_b`): parse expressions, extract join keys, resolve both sides
3. Mark as remote if the right side references a remote cluster index

### Type Validation

The join forbids these types on either side: `TEXT`, `VERSION`, `UNSIGNED_LONG`, `GEO_POINT`, `GEO_SHAPE`, `COUNTER_*`, `OBJECT`, `SOURCE`, `DATE_PERIOD`, `TIME_DURATION`, `AGGREGATE_METRIC_DOUBLE`, `DENSE_VECTOR`, and several others. See `Join.java:78-105` for the full list.

### Output Schema

`Join.computeOutputExpressions()` (line 228) computes the output for a LEFT join:
- Take all fields from the left side
- Add fields from the right side that are NOT join keys (join keys come from the left)
- Convert right-side attributes to `ReferenceAttribute` via `makeReference()` — this prevents `SearchStats` from trying to verify these fields exist in the left-side index

---

## 6. The Surrogate Pattern

Both `Lookup` and `LookupJoin` implement `SurrogateLogicalPlan`. This means they exist only during parsing and analysis — before physical planning, they must be converted to their "surrogate" form.

```java
// Lookup.surrogate() — line 101
public LogicalPlan surrogate() {
    return new Join(source(), child(), localRelation, joinConfig());
}

// LookupJoin.surrogate() — line 68
public LogicalPlan surrogate() {
    return new Join(source(), left(), right(), config(), isRemote());
}
```

The optimizer calls `SubstituteSurrogates` to perform this conversion. After substitution, the plan tree contains only `Join` nodes — no `Lookup` or `LookupJoin`.

**Why?** The surrogate pattern separates syntax-specific concerns (two different ways to write a lookup join) from execution-level concerns (there's only one way to execute it). The `Join` node is what gets serialized and sent to data nodes.

---

## 7. Physical Planning: Mapper

**Location**: `Mapper.java:183-233`

The `Mapper` converts logical plans to physical plans. For `Join` nodes, `mapBinary()` handles the conversion:

```
mapBinary(Join)
├─ Validate: type must be LEFT
├─ If isRemote: wrap entire join as FragmentExec (for CCS)
├─ Map left child → physical plan
├─ Map right child → physical plan
│
├─ If right is LocalSourceExec:
│  └─ Create HashJoinExec (in-memory broadcast join)
│
└─ If right is FragmentExec with LOOKUP index mode:
   └─ Create LookupJoinExec
```

### The LOOKUP Index Mode Check

`isIndexModeLookup()` (lines 235-245) checks whether the right-side `FragmentExec` contains an `EsRelation` with `indexMode == LOOKUP`. It handles two shapes:
- Direct: `FragmentExec → EsRelation(LOOKUP)`
- Filtered: `FragmentExec → Filter → EsRelation(LOOKUP)`

Only LOOKUP-mode indices can use `LookupJoinExec`. This is the gatekeeper.

### LookupJoinExec

**Location**: `plan/physical/LookupJoinExec.java`

```java
public class LookupJoinExec extends BinaryExec implements EstimatesRowSize {
    List<Attribute> leftFields;          // Left-side match keys
    List<Attribute> rightFields;         // Right-side match keys
    List<Attribute> addedFields;         // Fields brought back from lookup
    Expression joinOnConditions;         // Optional expression condition
}
```

The `left()` is the streaming source plan. The `right()` (accessed via `lookup()`) is the `FragmentExec` wrapping the lookup index's `EsRelation`.

---

## 8. Local Execution Planning

**Location**: `LocalExecutionPlanner.java:815-909`

`planLookupJoin()` converts `LookupJoinExec` into an actual operator factory:

```
planLookupJoin(LookupJoinExec)
├─ Plan left side → PhysicalOperation (streaming pages)
├─ Extend layout with lookup output fields
├─ Extract EsRelation from right side, validate LOOKUP mode
├─ Resolve index name (handle CCS prefix stripping)
├─ Build MatchConfig list:
│  └─ For each (leftField, rightField) pair:
│     ├─ Get channel position from left-side layout
│     └─ Record field name and data type
├─ Determine streaming vs non-streaming operator variant
└─ Create LookupFromIndexOperator.Factory
```

### Streaming vs Non-Streaming

`shouldUseStreamingOperator()` (lines 912-949) checks:
1. Is this a snapshot build? (streaming is preview-only)
2. Do ALL shard nodes support `ESQL_STREAMING_LOOKUP_JOIN`?

If all conditions met: use `StreamingLookupFromIndexOperator` (continuous bidirectional exchange).
Otherwise: use `LookupFromIndexOperator` (buffer all results before returning).

---

## 9. The Operator: LookupFromIndexOperator

**Location**: `enrich/LookupFromIndexOperator.java`

This is an `AsyncOperator` — it sends requests asynchronously and buffers results.

### The Async Pattern

```
Driver calls needsInput() → true
Driver passes Page → performAsync(page, listener)
  └─ Extract match field blocks from page
  └─ Build LookupFromIndexService.Request
  └─ Call lookupService.lookupAsync(request, listener)
  └─ Callback stores OngoingJoin in buffer

Driver calls getOutput()
  └─ Pull OngoingJoin from buffer
  └─ For each right-hand page from response:
     └─ RightChunkedLeftJoin.join(rightPage) → emit joined page
  └─ When iterator exhausted:
     └─ noMoreRightHandPages() → emit unmatched left rows with nulls
```

### MatchFieldsMapping

For expression-based joins, the same left-side field might appear in multiple conditions (e.g., `ON left_id == right_id AND left_id >= right_min`). `MatchFieldsMapping` deduplicates the blocks extracted from the input page, mapping multiple match conditions to the same physical block channel.

### Key Statistics Tracked

```java
emittedPages   // How many output pages produced
emittedRows    // Total output rows
totalRows      // Total input rows received
```

---

## 10. The Lookup Service: Query and Extract

**Location**: `enrich/LookupFromIndexService.java` (extends `AbstractLookupService`)

The lookup service runs on the **data node hosting the lookup index shard**. It receives a request with match field values and returns pages of matching results.

### Single-Shard Validation

`AbstractLookupService` validates at lines 254-259 that the lookup index has exactly one shard. This is enforced by `IndexMode.LOOKUP` at index creation time via `IndexModeSettingsProvider` (sets `index.number_of_shards: 1`).

### Query Generation

For each row in the input page, the service generates a Lucene query:

**Field-based join** (`ON field1, field2`):
```
Row 0: user_id = "alice"
  → TermQuery("user_id", "alice")

Row 1: user_id = "bob"
  → TermQuery("user_id", "bob")
```

Uses `termQueryList()` to build efficient term queries per field. For multi-field joins, a `BooleanQuery` with FILTER clauses combines them.

**Expression-based join** (`ON left_id == right_id AND left_val >= right_min`):
```
Row 0: left_id = 42, left_val = 100
  → BooleanQuery(
      FILTER: TermQuery("right_id", 42),
      FILTER: RangeQuery("right_min", lte: 100)
    )
```

Uses `ExpressionQueryList` (`enrich/ExpressionQueryList.java`) which decomposes the join condition into:
1. Binary comparisons (Equals → TermQuery, GreaterThan → RangeQuery, etc.)
2. Right-side-only predicates pushed as additional FILTER clauses

### Query Execution

The service uses `EnrichQuerySourceOperator`, which:
1. Creates an `IndexSearcher` directly from the shard's `Engine.Searcher`
2. For each input row, runs `bulkScorer` per segment
3. Collects matching doc IDs
4. Extracts requested fields using field readers (via `BlockDocValuesReader` or `ValuesSourceReaderOperator`)

### Response Format

The response contains pages with:
- **Block 0**: `IntBlock` of positions — maps each result row to an input row index (non-decreasing)
- **Blocks 1+**: field values from the lookup index

Example:
```
Input:  [alice, bob, charlie]  (3 rows)
Lookup: alice→{name:"Alice A"}, bob→{name:"Bob B", name:"Bobby B"}, charlie→(no match)

Response:
  positions: [0, 1, 1]          ← alice matched once, bob matched twice
  name:      ["Alice A", "Bob B", "Bobby B"]
```

---

## 11. Joining: RightChunkedLeftJoin

**Location**: `compute/.../operator/lookup/RightChunkedLeftJoin.java`

This class implements LEFT JOIN semantics using the positional index from the lookup response.

### The Algorithm

The left page is stored at construction time. As right-hand pages arrive (potentially multiple per request):

```
join(rightPage):
1. Read positions block (block 0) — must be non-decreasing
2. For positions that are contiguous with previous page, continue
3. For gaps (e.g., positions jump from 1 to 3), insert null-filled rows for position 2
4. Filter left page by positions → duplicate left rows as needed
5. Combine: [filtered_left_columns..., right_columns_with_null_inserts...]
6. Return joined page
```

### Concrete Example

```
Left page:  row 0=[ts:10am, user:"alice"]
            row 1=[ts:11am, user:"bob"]
            row 2=[ts:12pm, user:"charlie"]

Right page: positions=[0, 1, 1]
            name=["Alice A", "Bob B", "Bobby B"]

Step 1: positions = [0, 1, 1]
Step 2: Gap? No (0→1 is contiguous)
Step 3: Filter left by positions [0, 1, 1]:
        [ts:10am,user:"alice", ts:11am,user:"bob", ts:11am,user:"bob"]
Step 4: Right data stays as-is: ["Alice A", "Bob B", "Bobby B"]
Step 5: Result:
        row 0 = [10am, "alice", "Alice A"]
        row 1 = [11am, "bob",   "Bob B"]
        row 2 = [11am, "bob",   "Bobby B"]

After all right pages consumed:
noMoreRightHandPages():
  position 2 (charlie) was never matched → emit:
        row 3 = [12pm, "charlie", null]
```

### Why "Chunked"?

The lookup service may return multiple pages of results (when results are large). `RightChunkedLeftJoin` processes them one at a time, maintaining state about which left rows have been seen so far. The `next` field tracks the next expected position, and positions must be non-decreasing across all pages.

---

## 12. End-to-End Data Flow

Here's the complete journey of a query:

```
FROM logs | LOOKUP JOIN users ON user_id | KEEP timestamp, user_id, user_name
```

```
1. PARSING
   "LOOKUP JOIN users ON user_id"
   → visitJoinCommand()
   → LookupJoin(left=child, right=UnresolvedRelation("users", LOOKUP), fields=[user_id])

2. ANALYSIS
   ResolveLookupTables: (legacy path only)
   ResolveRefs.resolveLookupJoin():
     - Resolve "users" → EsRelation(indexMode=LOOKUP, fields=[user_id, user_name, ...])
     - Resolve user_id against left output → FieldAttribute
     - Resolve user_id against right output → FieldAttribute
     - Build JoinConfig(LEFT, [left.user_id], [right.user_id], null)
   → LookupJoin(left, EsRelation, config)

3. SURROGATE SUBSTITUTION
   LookupJoin.surrogate()
   → Join(left, right, config)

4. OPTIMIZATION
   Standard optimizer rules run (push filters, prune columns, etc.)

5. PHYSICAL PLANNING (Mapper)
   mapBinary(Join):
     - left → FragmentExec (distributed scan of logs)
     - right → FragmentExec with LOOKUP EsRelation
     - isIndexModeLookup(right) == true
   → LookupJoinExec(left, right, leftFields, rightFields, addedFields=[user_name])

6. LOCAL EXECUTION PLANNING
   planLookupJoin(LookupJoinExec):
     - Plan left → PhysicalOperation (scan pages)
     - Extract EsRelation, validate LOOKUP mode
     - Build MatchConfig: channel=user_id_channel, field="user_id", type=KEYWORD
     - Create LookupFromIndexOperator.Factory
   → PhysicalOperation with lookup operator

7. EXECUTION
   Driver loop:
   a. SourceOperator produces Page from logs: [ts, user_id, event_type]
   b. LookupFromIndexOperator.performAsync(page):
      - Extract user_id block
      - Send to LookupFromIndexService on lookup shard node
   c. Data node:
      - For each user_id value, build TermQuery
      - Search "users" index
      - Extract user_name from matches
      - Build response: [positions, user_name]
   d. Callback received:
      - Create RightChunkedLeftJoin(inputPage, 1 added field)
   e. getOutput():
      - join(responsePage) → merged page [ts, user_id, event_type, user_name]
      - noMoreRightHandPages() → unmatched rows with null user_name
   f. Next operator (ProjectOperator for KEEP) filters to [ts, user_id, user_name]
```

---

## 13. Key Files Reference

| Component | File | Key Lines |
|-----------|------|-----------|
| LOOKUP grammar | `esql/src/main/antlr/EsqlBaseParser.g4` | 371-373 |
| JOIN grammar | `esql/src/main/antlr/parser/Join.g4` | 9-22 |
| JOIN lexer | `esql/src/main/antlr/lexer/Join.g4` | 12 |
| Parser visitors | `esql/src/.../parser/LogicalPlanBuilder.java` | 792-939 |
| Lookup node | `esql/src/.../plan/logical/Lookup.java` | all |
| LookupJoin node | `esql/src/.../plan/logical/join/LookupJoin.java` | all |
| Join node | `esql/src/.../plan/logical/join/Join.java` | all |
| JoinConfig | `esql/src/.../plan/logical/join/JoinConfig.java` | all |
| JoinTypes | `esql/src/.../plan/logical/join/JoinTypes.java` | all |
| Analysis rules | `esql/src/.../analysis/Analyzer.java` | 577-623, 797-857, 966-1006 |
| Mapper | `esql/src/.../planner/mapper/Mapper.java` | 183-245 |
| LookupJoinExec | `esql/src/.../plan/physical/LookupJoinExec.java` | all |
| LocalExecutionPlanner | `esql/src/.../planner/LocalExecutionPlanner.java` | 815-949 |
| LookupFromIndexOperator | `esql/src/.../enrich/LookupFromIndexOperator.java` | all |
| LookupFromIndexService | `esql/src/.../enrich/LookupFromIndexService.java` | all |
| AbstractLookupService | `esql/src/.../enrich/AbstractLookupService.java` | 254-259 |
| EnrichQuerySourceOperator | `esql/src/.../enrich/EnrichQuerySourceOperator.java` | all |
| RightChunkedLeftJoin | `esql/compute/.../operator/lookup/RightChunkedLeftJoin.java` | all |
| ExpressionQueryList | `esql/src/.../enrich/ExpressionQueryList.java` | all |

All paths relative to `x-pack/plugin/`.
