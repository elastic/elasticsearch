/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.optimizer.GoldenTestCase;
import org.elasticsearch.xpack.esql.plan.physical.RemoteFetchBoundaryExec;

import java.util.EnumSet;
import java.util.Objects;

public class LateMaterializationPlannerGoldenTests extends GoldenTestCase {

    @ParametersFactory(argumentFormatting = "%1$s")
    public static Iterable<Object[]> parameters() {
        return goldenModes();
    }

    public LateMaterializationPlannerGoldenTests(@Name("mode") String mode) {
        super(mode);
    }

    private static final EnumSet<Stage> STAGES = EnumSet.of(
        Stage.PHYSICAL_OPTIMIZATION,
        Stage.NODE_REDUCE,
        Stage.NODE_REDUCE_LOCAL_PHYSICAL_OPTIMIZATION
    );

    private void checkLimitByLateMaterializationFeatureFlag() {
        assumeTrue(
            "late materialization for LimitBy/TopNBy requires "
                + LateMaterializationPlanner.ESQL_LATE_MATERIALIZATION_LIMIT_BY_FEATURE_FLAG,
            LateMaterializationPlanner.ESQL_LATE_MATERIALIZATION_LIMIT_BY_FEATURE_FLAG.isEnabled()
        );
    }

    private void checkLimitByLateMaterializationFeatureFlagDisabled() {
        assumeFalse(
            "test requires " + LateMaterializationPlanner.ESQL_LATE_MATERIALIZATION_LIMIT_BY_FEATURE_FLAG + " to be disabled",
            LateMaterializationPlanner.ESQL_LATE_MATERIALIZATION_LIMIT_BY_FEATURE_FLAG.isEnabled()
        );
    }

    public void testBasicTopNLateMaterialization() {
        String query = """
            FROM employees
            | keep hire_date, salary, emp_no
            | SORT hire_date
            | LIMIT 20
            """;
        runGoldenTest(query, STAGES, unindexedStats());
    }

    public void testRemoteFetchTopNBoundaryAndRealizedPlans() {
        runRemoteFetchGoldenTest("""
            FROM employees
            | KEEP hire_date, salary, emp_no
            | SORT hire_date
            | LIMIT 20
            """);
    }

    public void testRemoteFetchTopNDoesNotPlanAggregationAfterTopN() {
        runRemoteFetchGoldenTest("""
            FROM employees
            | KEEP hire_date, salary, emp_no
            | SORT hire_date
            | LIMIT 20
            | STATS max_salary = MAX(salary)
            """);
    }

    public void testRemoteFetchTopNDoesNotPlanAggregationBelowTopN() {
        runRemoteFetchGoldenTest("""
            FROM employees
            | STATS max_salary = MAX(salary) BY hire_date
            | SORT max_salary DESC
            | LIMIT 20
            """);
    }

    public void testRemoteFetchTopNDoesNotPlanExpressionBeforeTopN() {
        runRemoteFetchGoldenTest("""
            FROM employees
            | EVAL adjusted_salary = salary + 1
            | SORT hire_date
            | LIMIT 20
            | KEEP hire_date, adjusted_salary, emp_no
            """);
    }

    public void testRemoteFetchTopNDoesNotPlanUserEvalSortKey() {
        runRemoteFetchGoldenTest("""
            FROM employees
            | EVAL adjusted_salary = salary + 1
            | SORT adjusted_salary
            | LIMIT 20
            | KEEP adjusted_salary, emp_no
            """);
    }

    public void testRemoteFetchTopNDoesNotPlanNestedPipelineBreaker() {
        runRemoteFetchGoldenTest("""
            FROM employees
            | SORT salary DESC
            | LIMIT 100
            | SORT hire_date
            | LIMIT 20
            | KEEP hire_date, salary, emp_no
            """);
    }

    public void testMultipleTopN() {
        String query = """
            FROM employees
            | keep hire_date, salary, emp_no
            | SORT hire_date
            | LIMIT 20
            | SORT salary
            | LIMIT 10
            """;
        runGoldenTest(query, STAGES, unindexedStats());
    }

    public void testSomeFieldsNeededBeforeLateMaterialization() {
        String query = """
            FROM employees
            | keep hire_date, salary, emp_no
            | WHERE salary > 10000
            | SORT hire_date
            | LIMIT 20
            """;
        runGoldenTest(query, STAGES, unindexedStats());
    }

    /**
     * No narrowing {@code KEEP}, so {@code ProjectAwayColumns} leaves the top-level {@code Project} covering the whole relation.
     * That is the shape every {@code FORK} branch has (reproduced here without {@code FORK}, which {@code GoldenTestCase} cannot
     * plan because of its multiple {@code ExchangeExec}s), and the case where using that {@code Project} to decide what crosses the
     * exchange prunes nothing. The data driver must still come out as {@code [_doc, hire_date]}.
     */
    public void testNoKeepWithFilter() {
        String query = """
            FROM employees
            | WHERE salary > 10000
            | SORT hire_date
            | LIMIT 20
            """;
        runGoldenTest(query, STAGES, unindexedStats());
    }

    /**
     * {@code _score} is produced by the Lucene source operator and has no block loader, so the node-reduce driver cannot re-read it.
     * It must cross the exchange even though it is neither a sort key nor referenced below the TopN.
     */
    public void testScoreMustCrossTheExchange() {
        String query = """
            FROM books METADATA _score
            | WHERE title:"Tolkien"
            | SORT year
            | LIMIT 5
            """;
        runGoldenTest(query, STAGES, unindexedStats());
    }

    /**
     * The positive counterpart to {@link #testScoreMustCrossTheExchange}. {@code _index} and {@code _id} are on
     * {@code LateMaterializationPlanner}'s allow-list of metadata attributes that have a block loader, so even though the
     * {@code EVAL} below the TopN pulls them into the pipeline breaker's output - which is the only way a metadata attribute
     * reaches the pruning decision at all - they must be dropped from the exchange and re-read on the node-reduce driver.
     * Only the sort key and the {@code EVAL} result should cross.
     */
    public void testReloadableMetadataMustNotCrossTheExchange() {
        String query = """
            FROM employees METADATA _index, _id
            | EVAL id_len = LENGTH(CONCAT(_index, _id))
            | SORT hire_date
            | LIMIT 5
            | KEEP _index, _id, id_len
            """;
        runGoldenTest(query, STAGES, unindexedStats());
    }

    /**
     * For a TSDB index {@code ReplaceSourceAttributes} synthesizes a fresh {@code FieldAttribute} per
     * {@code EsQueryExec#TIME_SERIES_SOURCE_FIELDS}, so {@code _ts_slice_index} and {@code _ts_future_max_timestamp} appear in the
     * pipeline breaker's physical output without being the output of any {@code EsRelation}. They must not reach the data-side
     * {@code Project}: its child is the logical fragment, which cannot produce them - the data node mints its own pair while
     * mapping. Regression test for {@code missing references [_ts_slice_index, _ts_future_max_timestamp]}.
     */
    public void testTimeSeriesSourceAttributesMustNotBeProjected() {
        String query = """
            TS k8s METADATA _tsid
            """;
        runGoldenTest(query, STAGES, unindexedStats());
    }

    public void testMultipleFieldSortTopN() {
        String query = """
            FROM employees
            | keep hire_date, emp_no, height
            | SORT hire_date, height
            | LIMIT 20
            """;
        runGoldenTest(query, STAGES, unindexedStats());
    }

    public void testExpressionSortTopNKeepBeforeSort() {
        String query = """
            FROM employees
            | keep hire_date, height
            | SORT sin(height) * 2
            | LIMIT 20
            """;
        runGoldenTest(query, STAGES, unindexedStats());
    }

    public void testExpressionSortTopNKeepAfterSort() {
        String query = """
            FROM employees
            | SORT sin(height) * 2
            | keep hire_date
            | LIMIT 20
            """;
        runGoldenTest(query, STAGES, unindexedStats());
    }

    public void testPushedDownTopN() {
        String query = """
            FROM employees
            | keep emp_no, height
            | SORT height
            | LIMIT 20
            """;
        runGoldenTest(query, STAGES); // default stats are indexed.
    }

    public void testPushedDownTopNWithFilter() {
        String query = """
            FROM employees
            | WHERE salary > 10000
            | keep emp_no, height
            | SORT height
            | LIMIT 20
            """;
        runGoldenTest(query, STAGES); // default stats are indexed.
    }

    public void testTopNWithMissingSortField() {
        String query = """
            FROM employees
            | keep hire_date, salary, emp_no
            | SORT hire_date
            | LIMIT 20
            """;
        runGoldenTest(query, STAGES, missingFieldStats("hire_date"));
    }

    public void testLookupJoinOnDataNode() {
        String query = """
            FROM employees
            | EVAL language_code = languages
            | LOOKUP JOIN languages_lookup ON language_code
            | WHERE emp_no >= 10091 AND emp_no < 10094
            | SORT emp_no
            | LIMIT 20
            """;
        runGoldenTest(query, STAGES, unindexedStats());
    }

    public void testTopNThenStats() {
        String query = """
            FROM employees
            | keep hire_date, salary, emp_no
            | SORT hire_date
            | LIMIT 20
            | STATS max_salary = MAX(salary), count = COUNT(*)
            """;
        runGoldenTest(query, STAGES, unindexedStats());
    }

    public void testNullifiedFieldWithLateMaterialization() {
        String query = """
            SET unmapped_fields="nullify";
            FROM employees
            | KEEP hire_date, salary, emp_no, does_not_exist
            | SORT hire_date
            | LIMIT 20
            """;
        runGoldenTest(query, STAGES, unindexedStats());
    }

    public void testNullifiedFieldAsSort() {
        String query = """
            SET unmapped_fields="nullify";
            FROM employees
            | KEEP hire_date, salary, does_not_exist
            | SORT does_not_exist
            | LIMIT 20
            """;
        runGoldenTest(query, STAGES, unindexedStats());
    }

    public void testMvExpandBeforeTopN() {
        String query = """
            FROM employees
            | keep emp_no, job_positions, salary
            | MV_EXPAND job_positions
            | SORT salary
            | LIMIT 20
            """;
        runGoldenTest(query, STAGES, unindexedStats());
    }

    public void testBasicTopNByLateMaterialization() {
        checkLimitByLateMaterializationFeatureFlag();
        String query = """
            FROM employees
            | keep hire_date, salary, languages, emp_no
            | SORT hire_date
            | LIMIT 5 BY languages
            """;
        runGoldenTest(query, STAGES, unindexedStats());
    }

    public void testMultipleTopNBy() {
        checkLimitByLateMaterializationFeatureFlag();
        String query = """
            FROM employees
            | keep hire_date, salary, languages, gender, emp_no
            | SORT hire_date
            | LIMIT 5 BY languages
            | SORT salary
            | LIMIT 3 BY gender
            """;
        runGoldenTest(query, STAGES, unindexedStats());
    }

    public void testTopNByWithFilter() {
        checkLimitByLateMaterializationFeatureFlag();
        String query = """
            FROM employees
            | keep hire_date, salary, languages, emp_no
            | WHERE salary > 10000
            | SORT hire_date
            | LIMIT 5 BY languages
            """;
        runGoldenTest(query, STAGES, unindexedStats());
    }

    public void testTopNByWithMissingSortField() {
        checkLimitByLateMaterializationFeatureFlag();
        String query = """
            FROM employees
            | keep hire_date, salary, languages, emp_no
            | SORT hire_date
            | LIMIT 5 BY languages
            """;
        runGoldenTest(query, STAGES, missingFieldStats("hire_date"));
    }

    public void testBasicLimitByLateMaterialization() {
        checkLimitByLateMaterializationFeatureFlag();
        String query = """
            FROM employees
            | keep salary, languages, emp_no
            | LIMIT 5 BY languages
            """;
        runGoldenTest(query, STAGES, unindexedStats());
    }

    public void testLimitByMultipleGroupings() {
        checkLimitByLateMaterializationFeatureFlag();
        String query = """
            FROM employees
            | keep salary, languages, gender, emp_no
            | LIMIT 3 BY languages, gender
            """;
        runGoldenTest(query, STAGES, unindexedStats());
    }

    public void testLimitByWithMissingGroupField() {
        checkLimitByLateMaterializationFeatureFlag();
        String query = """
            FROM employees
            | keep salary, languages, emp_no
            | LIMIT 5 BY languages
            """;
        runGoldenTest(query, STAGES, missingFieldStats("languages"));
    }

    // Late materialization for TOP N BY is disabled in releases
    public void testBasicTopNByNodeReduceWithoutLateMaterialization() {
        checkLimitByLateMaterializationFeatureFlagDisabled();
        String query = """
            FROM employees
            | keep hire_date, salary, languages, emp_no
            | SORT hire_date
            | LIMIT 5 BY languages
            """;
        runGoldenTest(query, STAGES, unindexedStats());
    }

    // Late materialization for LIMIT BY is disabled in releases
    public void testBasicLimitByNodeReduceWithoutLateMaterialization() {
        checkLimitByLateMaterializationFeatureFlagDisabled();
        String query = """
            FROM employees
            | keep salary, languages, emp_no
            | LIMIT 5 BY languages
            """;
        runGoldenTest(query, STAGES, unindexedStats());
    }

    // Prevents TopN pushdown.
    private static EsqlTestUtils.TestSearchStats unindexedStats() {
        return new EsqlTestUtils.TestSearchStats() {
            @Override
            public boolean isIndexed(FieldAttribute.FieldName field) {
                return false;
            }
        };
    }

    private void runRemoteFetchGoldenTest(String query) {
        builder(query).stages(STAGES)
            .searchStats(unindexedStats())
            .flags(EsqlFlags.withRemoteFetchTopN(true))
            .since(RemoteFetchBoundaryExec.ESQL_REMOTE_FETCH_TOPN_REDUCTION)
            .run();
    }

    // Returns false for exists() for the specified field, simulating a missing field on the data node.
    private static EsqlTestUtils.TestSearchStats missingFieldStats(String missingField) {
        return new EsqlTestUtils.TestSearchStats() {
            @Override
            public boolean exists(FieldAttribute.FieldName field) {
                return Objects.equals(field.string(), missingField) == false;
            }

            @Override
            public boolean isIndexed(FieldAttribute.FieldName field) {
                return exists(field);
            }
        };
    }
}
