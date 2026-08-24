/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner.reduction;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.optimizer.GoldenTestCase;
import org.elasticsearch.xpack.esql.plan.physical.FetchBoundaryExec;
import org.elasticsearch.xpack.esql.plugin.LateMaterializationPlanner;

import java.util.EnumSet;
import java.util.Objects;

public class ReductionPlannerGoldenTests extends GoldenTestCase {

    @ParametersFactory(argumentFormatting = "%1$s")
    public static Iterable<Object[]> parameters() {
        return goldenModes();
    }

    public ReductionPlannerGoldenTests(@Name("mode") String mode) {
        super(mode);
    }

    private static final EnumSet<Stage> STAGES = EnumSet.of(
        Stage.PHYSICAL_OPTIMIZATION,
        Stage.NODE_REDUCE,
        Stage.NODE_REDUCE_LOCAL_PHYSICAL_OPTIMIZATION
    );
    private static final EnumSet<Stage> FETCH_STAGES = EnumSet.of(Stage.PHYSICAL_OPTIMIZATION, Stage.DISTRIBUTED_REDUCTION);

    public void testTopNFetchBoundary() throws Exception {
        String query = """
            FROM employees
            | KEEP hire_date, salary, emp_no
            | SORT hire_date
            | LIMIT 20
            """;
        builder(query).stages(FETCH_STAGES).searchStats(unindexedStats()).since(FetchBoundaryExec.ESQL_FETCH_BOUNDARY).run();
    }

    public void testFetchBoundaryDoesNotApplyAfterAggregation() throws Exception {
        String query = """
            FROM employees
            | STATS total = SUM(salary)
            | SORT total DESC
            | LIMIT 5
            """;
        builder(query).stages(FETCH_STAGES).searchStats(unindexedStats()).since(FetchBoundaryExec.ESQL_FETCH_BOUNDARY).run();
    }

    public void testFetchBoundaryDoesNotApplyWithExpressionBeforeTopN() throws Exception {
        String query = """
            FROM employees
            | EVAL x = salary + 1
            | SORT hire_date
            | LIMIT 20
            | KEEP hire_date, x
            """;
        builder(query).stages(FETCH_STAGES).searchStats(unindexedStats()).since(FetchBoundaryExec.ESQL_FETCH_BOUNDARY).run();
    }

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
