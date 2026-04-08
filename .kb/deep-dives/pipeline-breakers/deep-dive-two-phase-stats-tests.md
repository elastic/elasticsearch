# Deep Dive: Two-Phase STATS Aggregation Tests (ES Indices vs External Sources)

## Summary

There are four distinct layers of testing for STATS/aggregation behavior. ES index tests thoroughly verify the INITIAL/FINAL two-phase pattern. External source tests exist but critically **never verify AggregatorMode** -- they only test distribution mechanics and result correctness, not plan structure.

---

## 1. ES Index Tests: Two-Phase Aggregation (INITIAL on data nodes, FINAL on coordinator)

### PhysicalPlanOptimizerTests (Plan Structure)
**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/test/java/org/elasticsearch/xpack/esql/optimizer/PhysicalPlanOptimizerTests.java`

These tests verify the **plan shape** -- that the optimizer produces a FINAL AggregateExec above an Exchange, with an INITIAL AggregateExec below it. This is the canonical two-phase pattern for ES indices.

Key tests:

- **`testDoExtractGroupingFields`** (line 1066): `from test | stats x = sum(salary) by first_name`
  - Plan comment at line 1058-1062 shows the expected structure:
    ```
    AggregateExec[..],FINAL  (coordinator)
      ExchangeExec
        AggregateExec[..],INITIAL  (data node)
          FieldExtractExec
            EsQueryExec
    ```
  - Asserts two AggregateExec nodes separated by an Exchange, verifying groupings and estimated row size on both.

- **`testQueryWithAggregation`** (line 1151): `from test | stats sum(emp_no)`
  - Plan comment at lines 1145-1149 shows FINAL above Exchange, PARTIAL (=INITIAL) below.
  - Asserts `exchange.child()` is an `AggregateExec`.

- **`testQueryWithAggAfterEval`** (line 1183): `from test | stats agg_emp = sum(emp_no) | eval x = agg_emp + 7`
  - Same two-phase structure verified.

- **`testProjectAwayColumnsDoesNothingForPipelineBreakingAggs`** (line 3544): `from test | stats avg(emp_no)`
  - Explicitly names `finalAgg` and `initialAgg` in assertions (lines 3553, 3556).
  - Shows: `LimitExec -> AggregateExec(FINAL) -> ExchangeExec -> AggregateExec(INITIAL) -> FieldExtractExec -> EsQueryExec`

- **`testExtractorsOverridingFields`** (line 1040): `from test | stats emp_no = sum(emp_no)`
  - Same: AggregateExec above Exchange, AggregateExec below.

- **`testQueryForStatWithMultiAgg`** (line 1208): `from test | stats agg_1 = sum(emp_no), agg_2 = min(salary)`
  - Two-phase pattern with multi-field aggregation.

### Tests with Explicit `getMode()` Assertions

These tests go beyond structural checks and explicitly assert the `AggregatorMode`:

- **`testProjectAwayAllColumnsWhenOnlyTheCountMattersInStats`** (line 3602):
  ```java
  assertThat(agg.getMode(), equalTo(SINGLE));  // line 3611
  ```
  This is SINGLE because there's a LIMIT 10 before STATS -- the optimizer collapses to single-phase.

- **`testAvgSurrogateFunctionAfterRenameAndLimit`** (line 3686):
  ```java
  assertThat(agg.getMode(), equalTo(SINGLE));  // line 3700
  ```
  SINGLE because LIMIT precedes STATS.

- **`testGlobalAggFoldingOutput`** (line 3800):
  ```java
  assertThat(agg.getMode(), equalTo(SINGLE));  // line 3812
  ```
  SINGLE because LIMIT precedes STATS (count on missing field).

- **`testSpatialTypesAndStatsUseDocValuesMultiAggregations`** (line 4820):
  ```java
  assertThat("Aggregation is PARTIAL", agg.getMode(), equalTo(INITIAL));  // line 4847
  ```
  Explicitly checks the data-node side is INITIAL.

- **`testSpatialTypesAndStatsUseDocValuesMultiAggregationsGrouped`** (line ~4858ff):
  ```java
  assertThat("Aggregation is PARTIAL", agg.getMode(), equalTo(INITIAL));  // lines 4910, 4971, 5038
  ```

- **`testSpatialAggCentroidFromRow`** (line ~4775):
  ```java
  assertThat("Aggregation is SINGLE", agg.getMode(), equalTo(SINGLE));  // lines 4782, 4792
  ```
  SINGLE because data source is a ROW (local source), not ES index.

- **Multiple spatial tests with explicit FINAL assertions** (lines 5081, 5086, 5102).

### LocalPhysicalPlanOptimizerTests (Data-Node Optimization)
**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/test/java/org/elasticsearch/xpack/esql/optimizer/LocalPhysicalPlanOptimizerTests.java`

Tests that verify the **locally optimized** plan on data nodes:

- **`testCountOneFieldWithFilter`** (line 263): `from test | where salary > 1000 | stats c = count(salary)`
  ```java
  assertThat(agg.getMode(), is(FINAL));  // line 273
  ```
  After local optimization, this becomes FINAL with `EsStatsQueryExec` child (count pushdown to Lucene).

- **`testAnotherCountAllWithFilter`** (line 373): `from test | where emp_no > 10010 | stats c = count()`
  ```java
  assertThat(agg.getMode(), is(FINAL));  // line 383
  ```

- **`testSingleCountWithStatsFilter`** (line 405): `from test | stats c = count(hire_date) where emp_no < 10042`
  ```java
  assertThat(agg.getMode(), is(FINAL));  // line 429
  ```

- **`testIsNotNull_TextField_Pushdown`** (line 562): `from test | where gender is not null | stats count(gender)`
  - Shows FINAL->Exchange->PARTIAL structure at lines 568-571.

- **`testIsNull_TextField_Pushdown_WithCount`** (line 614): Folded to `LocalSourceExec`.

### SubstituteRoundToTests (RoundTo Aggregation Mode Assertions)
**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/test/java/org/elasticsearch/xpack/esql/optimizer/rules/physical/local/SubstituteRoundToTests.java`

- Line 517: `assertThat(agg.getMode(), is(SINGLE));`
- Line 696: `assertThat(agg.getMode(), is(FINAL));`
- Lines 906, 926, 971: More FINAL assertions for nested aggregations.

---

## 2. Compute Engine Tests: INITIAL/FINAL at Operator Level

### ForkingOperatorTestCase
**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/compute/src/test/java/org/elasticsearch/compute/operator/ForkingOperatorTestCase.java`

This is the **definitive test** that two-phase aggregation works at the compute operator level:

- **`testInitialFinal`** (line 72): Runs INITIAL operator then FINAL operator in sequence, verifies same result as SINGLE.
- **`testManyInitialFinal`** (line 91): Runs INITIAL per-page (simulating multiple data nodes), then collects all partials into one FINAL.
- **`testInitialIntermediateFinal`** (line 111): Three-phase: INITIAL -> INTERMEDIATE -> FINAL.

All concrete aggregator tests extend this: `AggregationOperatorTests`, `HashAggregationOperatorTests`, each `*AggregatorFunctionTests`.

### HashAggregationOperatorTests
**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/compute/src/test/java/org/elasticsearch/compute/operator/HashAggregationOperatorTests.java`

- Line 301-331: Direct test of INITIAL on two "datanodes" feeding into FINAL, verifying multi-group TopN aggregation correctness.
  ```java
  try (var collectingOperator = makeAggWithMode.apply(AggregatorMode.FINAL)) {
      try (var datanodeOperator = makeAggWithMode.apply(AggregatorMode.INITIAL)) { ... }
      try (var datanodeOperator = makeAggWithMode.apply(AggregatorMode.INITIAL)) { ... }
  }
  ```

---

## 3. External Source Tests: STATS Present but Mode Never Verified

### ExternalDistributionTests (Mapper + Exchange Collapsing)
**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/test/java/org/elasticsearch/xpack/esql/plugin/ExternalDistributionTests.java`

**Critical finding**: `testMapperCreatesSingleAggForExternalSource` (line 46):
```java
Mapper mapper = new Mapper();
PhysicalPlan physicalPlan = mapper.map(new Versioned<>(aggregate, TransportVersion.current()));

assertTrue("Expected AggregateExec at top", physicalPlan instanceof AggregateExec);
AggregateExec aggExec = (AggregateExec) physicalPlan;
assertEquals(AggregatorMode.SINGLE, aggExec.getMode());  // <-- KEY ASSERTION
assertTrue("Expected ExternalSourceExec child", aggExec.child() instanceof ExternalSourceExec);
```

This is the **only test** that explicitly checks AggregatorMode for external sources. It confirms the Mapper produces `SINGLE` mode (not INITIAL/FINAL) for external sources. **No two-phase split.** The aggregation runs entirely on one node.

- **`testCollapseExternalSourceExchangePreservesNonExternalExchanges`** (line 130): Constructs an `AggregateExec` with `AggregatorMode.INITIAL` wrapping an `ExternalSourceExec`, but this is a **manually constructed** plan for testing exchange collapsing logic, not produced by the Mapper.

### ExternalSourceDataNodeTests (Distribution + Plan Breaking)
**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/test/java/org/elasticsearch/xpack/esql/plugin/ExternalSourceDataNodeTests.java`

Tests distribution planning, split injection, and plan breaking:

- **`testDistributionResultDistributedWithSplitsAndAggregation`** (line 60): Manually creates `AggregateExec` with `AggregatorMode.INITIAL`, but this tests distribution logic, not verifying the Mapper produces this.
- **`testExternalDistributionWithAggregationPlanBreaks`** (line 398): Same -- manually creates INITIAL mode AggregateExec to verify plan breaking works. Never checks whether the real planner actually produces this.

**Gap**: These tests construct plans by hand with AggregatorMode.INITIAL. The actual Mapper produces SINGLE (as proven by the test above). So these distribution tests test a code path that **cannot currently be reached** through the normal planning pipeline for STATS queries.

### ExternalDistributionPropertyTests (Randomized Property Tests)
**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/test/java/org/elasticsearch/xpack/esql/plugin/ExternalDistributionPropertyTests.java`

- `createAggPlan()` helper (line 249-251) manually creates `AggregateExec` with `AggregatorMode.INITIAL`.
- All tests validate split assignment invariants (completeness, load balance, determinism) but never examine whether the Mapper would actually produce INITIAL mode.

### AdaptiveStrategyTests (Distribution Strategy)
**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/test/java/org/elasticsearch/xpack/esql/plugin/AdaptiveStrategyTests.java`

- `testAggregationWithMultipleSplitsDistributes` (line 62): Uses `AggregatorMode.SINGLE` when constructing the plan, then checks that `AdaptiveStrategy` distributes it. The strategy looks for `AggregateExec.class::isInstance` in the plan, not the mode.
- This means: even with SINGLE mode, the adaptive strategy will distribute. But **the aggregation will run as SINGLE on each data node** without a FINAL merge on the coordinator.

### AnalyzerTests (Analysis Only, No Mode Check)
**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/test/java/org/elasticsearch/xpack/esql/analysis/AnalyzerTests.java`

- **`testResolveExternalRelationWithFileSet`** (line ~6461): Parses `EXTERNAL "s3://bucket/data/*.parquet" | STATS count = COUNT(*)` and verifies the ExternalRelation is in the analyzed plan. No physical plan or AggregatorMode checks.
- **`testResolveExternalRelationUnresolvedFileSet`** (line ~6516): Same query, verifies unresolved file set handling.

---

## 4. Integration Tests: External Source STATS Queries

### CSV Spec Tests (external-basic.csv-spec)
**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/qa/testFixtures/src/main/resources/external-basic.csv-spec`

Tests that STATS queries produce correct results on external sources:

- **`aggregateCount`** (line 88): `EXTERNAL "{{employees}}" | STATS count = COUNT(*)` -- expects 100.
- **`aggregateByGender`** (line 97): `EXTERNAL "{{employees}}" | STATS count = COUNT(*) BY gender | SORT gender` -- grouped aggregation.
- **`aggregateAverageSalary`** (line 109): `EXTERNAL "{{employees}}" | STATS avg_salary = AVG(salary)` -- AVG (surrogate decomposed to SUM+COUNT).
- **`aggregateSalaryStats`** (line 118): `EXTERNAL "{{employees}}" | STATS min_salary = MIN(salary), max_salary = MAX(salary), avg_salary = AVG(salary)` -- multi-aggregation.
- **`aggregateSalaryByGender`** (line 127): `EXTERNAL "{{employees}}" | STATS avg_salary = AVG(salary), count = COUNT(*) BY gender` -- grouped multi-agg.
- **`complexQuery`** (line 169): `WHERE ... | EVAL ... | STATS count = COUNT(*), avg_salary = AVG(salary) BY salary_category | SORT ...` -- full pipeline with STATS.

### CSV Spec Tests (external-multifile.csv-spec)
**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/qa/testFixtures/src/main/resources/external-multifile.csv-spec`

- **All tests are `-Ignore`d** (not yet runnable).
- `readAllEmployeesMultiFile-Ignore`: `STATS count = COUNT(*)`
- `aggregateMultiFileByGender-Ignore`: `STATS count = COUNT(*) BY gender`
- `multiFileSalaryStats-Ignore`: `STATS min, max, avg`

### ExternalDistributedSpecIT (Multi-Node Integration)
**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/qa/server/multi-node/src/javaRestTest/java/org/elasticsearch/xpack/esql/qa/multi_node/ExternalDistributedSpecIT.java`

- Runs ALL `external-basic.csv-spec` tests (including the STATS tests above) on a **3-node cluster**.
- Cross-products each test with three distribution strategies: `coordinator_only`, `round_robin`, `adaptive`.
- The key invariant (line 33-34): "all three modes produce identical results for every query; divergence flags a split assignment, exchange, or aggregation bug."
- Tests correctness of results only, not plan structure.

### AbstractExternalSourceSpecTestCase (Test Infrastructure)
**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/qa/server/src/main/java/org/elasticsearch/xpack/esql/qa/rest/AbstractExternalSourceSpecTestCase.java`

- Base class that provides S3/GCS/Azure/HTTP/LOCAL fixture infrastructure.
- Cross-products each test with storage backends.
- Provides template resolution ({{employees}} -> actual paths).
- No aggregation-specific logic.

---

## 5. Key Findings

### What is well-tested:
1. **ES index two-phase pattern**: Extensive plan structure tests verify INITIAL below Exchange, FINAL above.
2. **Compute operator two-phase**: ForkingOperatorTestCase proves INITIAL->FINAL produces correct results.
3. **External source STATS correctness**: CSV spec tests verify COUNT, AVG, MIN, MAX, grouped aggregations produce correct results.
4. **External source distribution mechanics**: Split assignment, plan breaking, exchange collapsing all tested.

### What is NOT tested:
1. **No test verifies the optimizer/planner produces INITIAL/FINAL for external sources when distributed.** The only mode assertion for external sources (`ExternalDistributionTests:57`) confirms SINGLE mode from the Mapper.
2. **No test for the SINGLE->INITIAL/FINAL conversion** that would be needed when the distribution infrastructure decides to distribute a STATS query.
3. **No plan-level test** that takes an external STATS query through the full optimizer pipeline and checks the resulting plan shape (like PhysicalPlanOptimizerTests does for ES indices).
4. **ExternalDistributedSpecIT tests correctness but not plan shape** -- it can't distinguish whether STATS ran as SINGLE (all computation on coordinator) or as INITIAL+FINAL (distributed computation). Both produce correct results, but SINGLE doesn't scale.
5. **The hand-constructed INITIAL-mode plans in distribution tests are unreachable** -- the Mapper produces SINGLE, so the distribution tests are testing a code path the system can't actually enter.

### The Gap:
The Mapper maps `Aggregate` over `ExternalRelation` to `AggregateExec(SINGLE)`. For ES indices, the Mapper maps to `AggregateExec(INITIAL)` + `AggregateExec(FINAL)` separated by an Exchange. The infrastructure to distribute external source queries exists (split assignment, plan breaking), but the planner never produces the two-phase plan for external sources. This means even when AdaptiveStrategy decides to distribute, the aggregation runs as SINGLE on each data node -- which is correct but means **each data node computes the full aggregation over its splits independently, and the coordinator gets multiple full results that aren't properly merged.** This is the core GA-8 problem.
