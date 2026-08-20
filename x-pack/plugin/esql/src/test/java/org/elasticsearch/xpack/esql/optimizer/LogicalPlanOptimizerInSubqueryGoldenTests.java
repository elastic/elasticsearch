/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.expression.function.aggregate.DimensionValues;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;

import java.util.EnumSet;
import java.util.Map;

/**
 * Captures the analyzed and logically-optimized plans for IN/NOT IN subquery scenarios.
 */
public class LogicalPlanOptimizerInSubqueryGoldenTests extends GoldenTestCase {

    private static final String PACK_DIMS_AGG = "pack_dims_agg";

    @ParametersFactory(argumentFormatting = "%1$s")
    public static Iterable<Object[]> parameters() {
        return goldenModes();
    }

    public LogicalPlanOptimizerInSubqueryGoldenTests(@Name("mode") String mode) {
        super(mode);
    }

    private static final EnumSet<Stage> STAGES = EnumSet.of(Stage.ANALYSIS, Stage.LOGICAL_OPTIMIZATION);

    private static void requireMultiColumnInSubquerySupport() {
        assumeTrue("Requires multi-column IN subquery support", EsqlCapabilities.Cap.WHERE_IN_MULTI_COLUMN_SUBQUERY.isEnabled());
    }

    public void testDisjunctiveInSubqueryAtTopLevel() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no IN (FROM employees | KEEP emp_no) OR salary > 50000
            """, STAGES);
    }

    public void testDisjunctiveInSubqueryInsideFromSubquery() {
        runGoldenTest("""
            FROM employees,
                 (FROM employees | WHERE emp_no IN (FROM employees | KEEP emp_no) OR salary > 50000 | KEEP emp_no)
            """, STAGES);
    }

    public void testDisjunctiveNotInSubqueryInsideFromSubquery() {
        runGoldenTest("""
            FROM employees,
                 (FROM employees | WHERE emp_no NOT IN (FROM employees | KEEP emp_no) OR salary > 50000 | KEEP emp_no)
            """, STAGES);
    }

    public void testNestedDisjunctiveInSubqueries() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no IN (
                FROM employees
                | WHERE salary IN (FROM employees | KEEP salary) OR languages > 2
                | KEEP emp_no
              ) OR salary > 50000
            """, STAGES);
    }

    public void testDisjunctiveInSubqueryWithFork() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no IN (FROM employees | KEEP emp_no) OR salary > 50000
            | FORK (WHERE emp_no > 10000) (WHERE emp_no < 10050)
            """, STAGES);
    }

    public void testSortWithLimitInSubqueryIsAllowed() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no IN (FROM employees | SORT emp_no | LIMIT 5 | KEEP emp_no)
            """, STAGES);
    }

    public void testStatsWithSortLimitInSubquery() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no IN (FROM employees | STATS m = MAX(emp_no) BY languages | SORT m | LIMIT 3 | KEEP m)
            """, STAGES);
    }

    public void testMultipleFiltersInSubqueryCombined() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no IN (FROM employees | WHERE salary > 50000 | WHERE languages > 2 | KEEP emp_no)
            """, STAGES);
    }

    public void testCombineDisjunctionsInsideInSubquery() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no IN (FROM employees | WHERE salary == 50000 or salary == 10000 | KEEP emp_no)
            """, STAGES);
    }

    // The grouping key `cluster` is a time-series dimension, so TranslateTimeSeriesAggregate rewrites it to either
    // DIMENSIONVALUES (when the negotiated cluster version supports `dimension_values`) or VALUES (when it does not).
    // These tests characterize the DIMENSIONVALUES form, so their builder chains declare the corresponding lower bound.
    // At `pack_dims_agg` the PackDims node folds into the TimeSeriesAggregate as PACKDIMSAGG, so that older shape lives in
    // [before_pack_dims_agg].
    public void testTsRateWithInSubquery() {
        builder("""
            TS k8s
            | WHERE cluster IN (TS k8s
                               | STATS m = max(rate(network.total_bytes_in)) BY cluster
                               | KEEP cluster)
            | STATS max_rate = max(rate(network.total_bytes_in)) BY cluster
            """).stages(STAGES).since(DimensionValues.DIMENSION_VALUES_VERSION).expectationChangesAt(PACK_DIMS_AGG).run();
    }

    public void testTsRateWithNotInSubquery() {
        builder("""
            TS k8s
            | WHERE cluster NOT IN (TS k8s
                                   | STATS m = max(rate(network.total_bytes_in)) BY cluster
                                   | KEEP cluster)
            | STATS max_rate = max(rate(network.total_bytes_in)) BY cluster
            """).stages(STAGES).since(DimensionValues.DIMENSION_VALUES_VERSION).expectationChangesAt(PACK_DIMS_AGG).run();
    }

    // The outer TS pipeline groups BY WITHOUT(...), so the WITHOUT-bearing TimeSeriesAggregate sits directly
    // above the SemiJoin produced by the IN subquery. Before the TranslateTimeSeriesWithout fix the rule lowered
    // WITHOUT into a _timeseries metadata attribute by descending into every EsRelation under the aggregate's
    // child, without excluding the right-hand side of the SemiJoin, injecting the lowered _timeseries attribute
    // into the subquery (RHS) relation as well. After the fix only the main (left) relation carries _timeseries;
    // the subquery relation keeps just its own _tsid. The SUM overflow fix is this query's floor; `pack_dims_agg` is its newer boundary.
    public void testTsWithoutAndRateWithInSubquery() {
        assumeTrue("Requires WITHOUT grouping support", EsqlCapabilities.Cap.ESQL_WITHOUT_GROUPING.isEnabled());
        builder("""
            TS k8s
            | WHERE cluster IN (TS k8s
                               | STATS m = max(rate(network.total_bytes_in)) BY cluster
                               | KEEP cluster)
            | STATS total_cost = sum(network.cost) BY WITHOUT(pod, region)
            """).stages(STAGES).since(Sum.ESQL_SUM_LONG_OVERFLOW_FIX).expectationChangesAt(PACK_DIMS_AGG).run();
    }

    public void testTsWithoutAndRateWithNotInSubquery() {
        assumeTrue("Requires WITHOUT grouping support", EsqlCapabilities.Cap.ESQL_WITHOUT_GROUPING.isEnabled());
        builder("""
            TS k8s
            | WHERE cluster NOT IN (TS k8s
                                   | STATS m = max(rate(network.total_bytes_in)) BY cluster
                                   | KEEP cluster)
            | STATS total_cost = sum(network.cost) BY WITHOUT(pod, region)
            """).stages(STAGES).since(Sum.ESQL_SUM_LONG_OVERFLOW_FIX).expectationChangesAt(PACK_DIMS_AGG).run();
    }

    public void testMultipleTsSubqueriesInsideInSubquery() {
        builder("""
            TS k8s
            | WHERE cluster IN (FROM
                                   (TS k8s
                                    | STATS max_bytes = max(to_long(network.total_bytes_in)) BY cluster
                                    | WHERE max_bytes > 10500
                                    | KEEP cluster),
                                   (TS k8s
                                    | STATS max_bytes = max(to_long(network.total_bytes_in)) BY cluster
                                    | WHERE max_bytes < 8000
                                    | KEEP cluster)
                               )
            | STATS max_bytes = max(to_long(network.total_bytes_in)) BY cluster
            | SORT cluster
            """).stages(STAGES).since(DimensionValues.DIMENSION_VALUES_VERSION).expectationChangesAt(PACK_DIMS_AGG).run();
    }

    public void testMultipleTsSubqueriesInsideNotInSubquery() {
        builder("""
            TS k8s
            | WHERE cluster NOT IN (FROM
                                       (TS k8s
                                        | STATS max_bytes = max(to_long(network.total_bytes_in)) BY cluster
                                        | WHERE max_bytes > 10500
                                        | KEEP cluster),
                                       (TS k8s
                                        | STATS max_bytes = max(to_long(network.total_bytes_in)) BY cluster
                                        | WHERE max_bytes < 8000
                                        | KEEP cluster)
                                   )
            | STATS max_bytes = max(to_long(network.total_bytes_in)) BY cluster
            | SORT cluster
            """).stages(STAGES).since(DimensionValues.DIMENSION_VALUES_VERSION).expectationChangesAt(PACK_DIMS_AGG).run();
    }

    // -- PropagateEmptyRelation through SEMI / ANTI / MARK join tests --

    public void testPropagateEmptyRelationThroughSemiJoin() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no IN (FROM employees | KEEP emp_no) AND false
            """, STAGES);
    }

    public void testPropagateEmptyRelationThroughAntiJoin() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no NOT IN (FROM employees | KEEP emp_no) AND false
            """, STAGES);
    }

    public void testPropagateEmptyRelationThroughMarkJoin() {
        runGoldenTest("""
            FROM employees
            | WHERE false
            | WHERE emp_no > 0 OR emp_no IN (FROM employees | KEEP emp_no)
            """, STAGES);
    }

    public void testPropagateEmptyRelationThroughSemiJoinWithEmptySubquery() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no IN (FROM employees | WHERE false | KEEP emp_no)
            """, STAGES);
    }

    public void testPropagateEmptyRelationThroughAntiJoinWithEmptySubquery() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no NOT IN (FROM employees | WHERE false | KEEP emp_no)
            """, STAGES);
    }

    public void testPropagateEmptyRelationThroughMarkJoinWithEmptySubquery() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no > 0 OR emp_no IN (FROM employees | WHERE false | KEEP emp_no)
            """, STAGES);
    }

    // Both sides empty: left becomes LocalRelation(EMPTY) via PruneFilters(WHERE false), right via WHERE false in subquery.

    public void testPropagateEmptyRelationThroughSemiJoinBothSidesEmpty() {
        runGoldenTest("""
            FROM employees
            | WHERE false
            | WHERE emp_no IN (FROM employees | WHERE false | KEEP emp_no)
            """, STAGES);
    }

    public void testPropagateEmptyRelationThroughAntiJoinBothSidesEmpty() {
        runGoldenTest("""
            FROM employees
            | WHERE false
            | WHERE emp_no NOT IN (FROM employees | WHERE false | KEEP emp_no)
            """, STAGES);
    }

    public void testPropagateEmptyRelationThroughMarkJoinBothSidesEmpty() {
        runGoldenTest("""
            FROM employees
            | WHERE false
            | WHERE emp_no > 0 OR emp_no NOT IN (FROM employees | WHERE false | KEEP emp_no)
            """, STAGES);
    }

    // Empty left side cascading through downstream operators

    public void testPropagateEmptyRelationThroughSemiJoinWithKeep() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no IN (FROM employees | KEEP emp_no) AND false
            | KEEP emp_no, first_name
            """, STAGES);
    }

    public void testPropagateEmptyRelationThroughSemiJoinWithStats() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no IN (FROM employees | KEEP emp_no) AND false
            | STATS cnt = COUNT(*)
            """, STAGES);
    }

    public void testPropagateEmptyRelationThroughAntiJoinWithKeep() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no NOT IN (FROM employees | KEEP emp_no) AND false
            | KEEP emp_no, first_name
            """, STAGES);
    }

    public void testPropagateEmptyRelationThroughAntiJoinWithStats() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no NOT IN (FROM employees | KEEP emp_no) AND false
            | STATS cnt = COUNT(*)
            """, STAGES);
    }

    public void testPropagateEmptyRelationThroughMarkJoinWithKeep() {
        runGoldenTest("""
            FROM employees
            | WHERE false
            | WHERE emp_no > 0 OR emp_no IN (FROM employees | KEEP emp_no)
            | KEEP emp_no
            """, STAGES);
    }

    public void testPropagateEmptyRelationThroughMarkJoinWithStats() {
        runGoldenTest("""
            FROM employees
            | WHERE false
            | WHERE emp_no > 0 OR emp_no IN (FROM employees | KEEP emp_no)
            | STATS cnt = COUNT(*)
            """, STAGES);
    }

    // -- IN / NOT IN subqueries referencing views --

    public void testInSubqueryReferencingView() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no IN (FROM emps_view)
            | KEEP emp_no, first_name
            """, STAGES, Map.of("emps_view", "FROM employees | KEEP emp_no"));
    }

    public void testNotInSubqueryReferencingView() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no NOT IN (FROM emps_view)
            | KEEP emp_no
            """, STAGES, Map.of("emps_view", "FROM employees | WHERE salary > 50000 | KEEP emp_no"));
    }

    public void testInSubqueryReferencingViewWithInSubqueryInDefinition() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no IN (FROM filtered_emps)
            | KEEP emp_no
            """, STAGES, Map.of("filtered_emps", "FROM employees | WHERE emp_no IN (FROM employees | KEEP emp_no) | KEEP emp_no"));
    }

    public void testInSubqueryReferencingViewWithSortLimit() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no IN (FROM sorted_emps)
            | KEEP emp_no
            """, STAGES, Map.of("sorted_emps", "FROM employees | SORT emp_no | LIMIT 5 | KEEP emp_no"));
    }

    public void testDisjunctiveInSubqueryReferencingView() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no IN (FROM emps_view) OR salary > 50000
            | KEEP emp_no
            """, STAGES, Map.of("emps_view", "FROM employees | KEEP emp_no"));
    }

    public void testMainFromAndNotInSubqueryEachReferenceMultipleViewSubqueries() {
        runGoldenTest(
            """
                FROM (FROM main_view_a | KEEP emp_no), (FROM main_view_b | KEEP emp_no)
                | WHERE emp_no NOT IN (FROM (FROM in_view_a | KEEP emp_no), (FROM in_view_b | KEEP emp_no) | KEEP emp_no)
                """,
            STAGES,
            Map.of(
                "main_view_a",
                "FROM employees | KEEP emp_no",
                "main_view_b",
                "FROM employees | WHERE salary > 50000 | KEEP emp_no",
                "in_view_a",
                "FROM employees | KEEP emp_no",
                "in_view_b",
                "FROM employees | WHERE salary > 60000 | KEEP emp_no"
            )
        );
    }

    // -- IN subquery inside CASE, COALESCE, IS [NOT] NULL in WHERE --

    public void testInSubqueryInCaseWhen() {
        runGoldenTest("""
            FROM employees
            | WHERE CASE(emp_no IN (FROM employees | KEEP emp_no), true, false)
            """, STAGES);
    }

    public void testNotInSubqueryInCaseWhen() {
        runGoldenTest("""
            FROM employees
            | WHERE CASE(emp_no NOT IN (FROM employees | KEEP emp_no), true, false)
            """, STAGES);
    }

    public void testInSubqueryInCoalesce() {
        runGoldenTest("""
            FROM employees
            | WHERE COALESCE(emp_no IN (FROM employees | KEEP emp_no), false)
            """, STAGES);
    }

    public void testInSubqueryInIsNotNull() {
        runGoldenTest("""
            FROM employees
            | WHERE (emp_no IN (FROM employees | KEEP emp_no)) IS NOT NULL
            """, STAGES);
    }

    public void testInSubqueryInIsNull() {
        runGoldenTest("""
            FROM employees
            | WHERE (emp_no IN (FROM employees | KEEP emp_no)) IS NULL
            """, STAGES);
    }

    public void testCaseWithConjunctiveInSubqueries() {
        runGoldenTest("""
            FROM employees
            | WHERE CASE(emp_no IN (FROM employees | KEEP emp_no) AND languages IN (FROM employees | KEEP languages), true, false)
            """, STAGES);
    }

    public void testCaseWithMixedConjunctiveDisjunctiveInSubqueries() {
        runGoldenTest("""
            FROM employees
            | WHERE CASE(emp_no IN (FROM employees | KEEP emp_no) AND (salary > 50000 OR languages IN (FROM employees | KEEP languages)),
                         true, false)
            """, STAGES);
    }

    public void testIsNotNullWithDisjunctiveInSubqueries() {
        runGoldenTest("""
            FROM employees
            | WHERE (emp_no IN (FROM employees | KEEP emp_no) OR languages IN (FROM employees | KEEP languages)) IS NOT NULL
            """, STAGES);
    }

    public void testIsNullWithConjunctiveInSubqueries() {
        runGoldenTest("""
            FROM employees
            | WHERE (emp_no IN (FROM employees | KEEP emp_no) AND languages IN (FROM employees | KEEP languages)) IS NULL
            """, STAGES);
    }

    public void testCoalesceWithDisjunctiveInSubqueries() {
        runGoldenTest("""
            FROM employees
            | WHERE COALESCE(emp_no IN (FROM employees | KEEP emp_no) OR languages IN (FROM employees | KEEP languages), false)
            """, STAGES);
    }

    public void testCoalesceWithConjunctiveInSubqueries() {
        runGoldenTest("""
            FROM employees
            | WHERE COALESCE(emp_no IN (FROM employees | KEEP emp_no), languages IN (FROM employees | KEEP languages), false)
            """, STAGES);
    }

    public void testCoalesceInSubqueryAsSecondArg() {
        runGoldenTest("""
            FROM employees
            | WHERE COALESCE(null, emp_no IN (FROM employees | KEEP emp_no))
            """, STAGES);
    }

    public void testCaseMixingCoalesceAndIsNotNull() {
        runGoldenTest("""
            FROM employees
            | WHERE CASE(COALESCE(emp_no IN (FROM employees | KEEP emp_no), false)
                         AND (languages IN (FROM employees | KEEP languages)) IS NOT NULL,
                         true, false)
            """, STAGES);
    }

    public void testDisjunctiveIsNotNullAndCoalesceCase() {
        runGoldenTest("""
            FROM employees
            | WHERE (emp_no IN (FROM employees | KEEP emp_no) AND languages IN (FROM employees | KEEP languages)) IS NOT NULL
              OR COALESCE(CASE(salary IN (FROM employees | KEEP salary), true, false), false)
            """, STAGES);
    }

    public void testConjunctiveWithCoalesceDisjunctionAndIsNull() {
        runGoldenTest("""
            FROM employees
            | WHERE salary > 50000
              AND COALESCE(emp_no IN (FROM employees | KEEP emp_no) OR languages IN (FROM employees | KEEP languages), false)
              AND (salary IN (FROM employees | KEEP salary)) IS NULL
            """, STAGES);
    }

    public void testCaseInSubqueryAndBareInSubqueryWithAnd() {
        runGoldenTest("""
            FROM employees
            | WHERE CASE(emp_no IN (FROM employees | KEEP emp_no), true, false)
              AND salary > 50000
              AND languages IN (FROM employees | KEEP languages)
            """, STAGES);
    }

    public void testCaseInSubqueryAndBareInSubqueryWithOr() {
        runGoldenTest("""
            FROM employees
            | WHERE CASE(emp_no IN (FROM employees | KEEP emp_no), true, false)
              OR salary > 50000
              OR languages IN (FROM employees | KEEP languages)
            """, STAGES);
    }

    public void testCaseInSubqueryAndOrWithBareIn() {
        runGoldenTest("""
            FROM employees
            | WHERE CASE(emp_no IN (FROM employees | KEEP emp_no), true, false)
              AND (salary > 50000 OR languages IN (FROM employees | KEEP languages))
            """, STAGES);
    }

    // -- multi-column IN / NOT IN subqueries: WHERE (field1, field2) IN (subquery) --

    public void testMultiColumnInSubqueryAtTopLevel() {
        requireMultiColumnInSubquerySupport();
        runGoldenTest("""
            FROM employees
            | WHERE (emp_no, languages) IN (FROM employees | KEEP emp_no, languages)
            """, STAGES);
    }

    public void testMultiColumnNotInSubqueryAtTopLevel() {
        requireMultiColumnInSubquerySupport();
        runGoldenTest("""
            FROM employees
            | WHERE (emp_no, languages) NOT IN (FROM employees | KEEP emp_no, languages)
            """, STAGES);
    }

    public void testDisjunctiveMultiColumnInSubqueryAtTopLevel() {
        requireMultiColumnInSubquerySupport();
        runGoldenTest("""
            FROM employees
            | WHERE (emp_no, languages) IN (FROM employees | KEEP emp_no, languages) OR salary > 50000
            """, STAGES);
    }

    public void testDisjunctiveMultiColumnInSubqueryInsideFromSubquery() {
        requireMultiColumnInSubquerySupport();
        runGoldenTest("""
            FROM employees,
                 (FROM employees | WHERE (emp_no, languages) IN (FROM employees | KEEP emp_no, languages) OR salary > 50000 | KEEP emp_no)
            """, STAGES);
    }

    public void testDisjunctiveMultiColumnNotInSubqueryInsideFromSubquery() {
        requireMultiColumnInSubquerySupport();
        runGoldenTest("""
            FROM employees,
                 (FROM employees
                  | WHERE (emp_no, languages) NOT IN (FROM employees | KEEP emp_no, languages) OR salary > 50000
                  | KEEP emp_no)
            """, STAGES);
    }

    public void testNestedDisjunctiveMultiColumnInSubqueries() {
        requireMultiColumnInSubquerySupport();
        runGoldenTest("""
            FROM employees
            | WHERE (emp_no, languages) IN (
                FROM employees
                | WHERE (salary, languages) IN (FROM employees | KEEP salary, languages) OR languages > 2
                | KEEP emp_no, languages
              ) OR salary > 50000
            """, STAGES);
    }

    public void testDisjunctiveMultiColumnInSubqueryWithFork() {
        requireMultiColumnInSubquerySupport();
        runGoldenTest("""
            FROM employees
            | WHERE (emp_no, languages) IN (FROM employees | KEEP emp_no, languages) OR salary > 50000
            | FORK (WHERE emp_no > 10000) (WHERE emp_no < 10050)
            """, STAGES);
    }

    public void testSortWithLimitInMultiColumnInSubqueryIsAllowed() {
        requireMultiColumnInSubquerySupport();
        runGoldenTest("""
            FROM employees
            | WHERE (emp_no, languages) IN (FROM employees | SORT emp_no | LIMIT 5 | KEEP emp_no, languages)
            """, STAGES);
    }

    public void testStatsWithSortLimitInMultiColumnInSubquery() {
        requireMultiColumnInSubquerySupport();
        runGoldenTest("""
            FROM employees
            | WHERE (emp_no, languages) IN (FROM employees | STATS m = MAX(emp_no) BY languages | SORT m | LIMIT 3 | KEEP m, languages)
            """, STAGES);
    }

    public void testMultipleFiltersInMultiColumnInSubqueryCombined() {
        requireMultiColumnInSubquerySupport();
        runGoldenTest("""
            FROM employees
            | WHERE (emp_no, languages) IN (FROM employees | WHERE salary > 50000 | WHERE languages > 2 | KEEP emp_no, languages)
            """, STAGES);
    }

    public void testCombineDisjunctionsInsideMultiColumnInSubquery() {
        requireMultiColumnInSubquerySupport();
        runGoldenTest("""
            FROM employees
            | WHERE (emp_no, languages) IN (FROM employees | WHERE salary == 50000 or salary == 10000 | KEEP emp_no, languages)
            """, STAGES);
    }

    // -- IN subquery combined with LOOKUP JOIN --

    public void testInSubqueryWithLimitFollowedByLookupJoin() {
        runGoldenTest("""
            FROM employees
            | WHERE emp_no IN (FROM employees | SORT emp_no | LIMIT 3 | KEEP emp_no)
            | EVAL language_code = languages
            | LOOKUP JOIN languages_lookup ON language_code
            """, STAGES);
    }

    public void testMultiColumnInSubqueryReferencingView() {
        requireMultiColumnInSubquerySupport();
        runGoldenTest("""
            FROM employees
            | WHERE (emp_no, languages) IN (FROM emps_view)
            | KEEP emp_no, first_name
            """, STAGES, Map.of("emps_view", "FROM employees | KEEP emp_no, languages"));
    }

    public void testMultiColumnNotInSubqueryReferencingView() {
        requireMultiColumnInSubquerySupport();
        runGoldenTest("""
            FROM employees
            | WHERE (emp_no, languages) NOT IN (FROM emps_view)
            | KEEP emp_no
            """, STAGES, Map.of("emps_view", "FROM employees | WHERE salary > 50000 | KEEP emp_no, languages"));
    }

    // -- IN subquery inside EVAL --

    public void testInSubqueryInEval() {
        runGoldenTest("""
            FROM employees
            | EVAL m = emp_no IN (FROM employees | KEEP emp_no)
            """, STAGES);
    }

    public void testNotInSubqueryInEval() {
        runGoldenTest("""
            FROM employees
            | EVAL m = emp_no NOT IN (FROM employees | KEEP emp_no)
            """, STAGES);
    }

    public void testConstantInSubqueryInEval() {
        runGoldenTest("""
            FROM employees
            | EVAL m = 10001 IN (FROM employees | KEEP emp_no)
            """, STAGES);
    }

    public void testInSubqueryWithMultipleFieldsInEval() {
        runGoldenTest("""
            FROM employees
            | EVAL a = 1, m = emp_no IN (FROM employees | KEEP emp_no), b = salary
            """, STAGES);
    }

    public void testInSubqueryReferencingAnotherAttributeInEval() {
        runGoldenTest("""
            FROM employees
            | EVAL a = emp_no + 1, m = a IN (FROM employees | KEEP emp_no)
            """, STAGES);
    }

    public void testInSubqueryInEvalAndReferencedInWhere() {
        runGoldenTest("""
            FROM employees
            | EVAL m = emp_no IN (FROM employees | KEEP emp_no)
            | WHERE m
            """, STAGES);
    }

    public void testInSubqueryEvalFieldRenamed() {
        runGoldenTest("""
            FROM employees
            | EVAL matches = emp_no IN (FROM employees | KEEP emp_no)
            | RENAME matches AS renamed_matches
            """, STAGES);
    }

    public void testInSubqueryEvalFieldDropped() {
        // validate pruneUnusedMarkJoin in PruneColumns
        runGoldenTest("""
            FROM employees
            | EVAL matches = emp_no IN (FROM employees | KEEP emp_no)
            | DROP matches
            """, STAGES);
    }

    public void testCaseWithInSubqueryInEval() {
        runGoldenTest("""
            FROM employees
            | EVAL m = CASE(emp_no IN (FROM employees | KEEP emp_no), "yes", "no")
            """, STAGES);
    }

    public void testCoalesceWithInSubqueryInEval() {
        runGoldenTest("""
            FROM employees
            | EVAL m = COALESCE(emp_no IN (FROM employees | KEEP emp_no), false)
            """, STAGES);
    }

    public void testIsNullOfInSubqueryInEval() {
        runGoldenTest("""
            FROM employees
            | EVAL m = (emp_no IN (FROM employees | KEEP emp_no)) IS NULL
            """, STAGES);
    }

    public void testDisjunctiveInSubqueriesInEvalOnOneField() {
        runGoldenTest("""
            FROM employees
            | EVAL m = emp_no IN (FROM employees | KEEP emp_no) OR emp_no IN (FROM employees | WHERE salary > 50000 | KEEP emp_no)
            """, STAGES);
    }

    public void testInSubqueryInEvalReferencingView() {
        runGoldenTest("""
            FROM employees
            | EVAL m = emp_no IN (FROM emps_view)
            """, STAGES, Map.of("emps_view", "FROM employees | KEEP emp_no"));
    }

    public void testInSubqueryInEvalInsideViewDefinition() {
        runGoldenTest("""
            FROM marked_emps
            | KEEP emp_no, m
            """, STAGES, Map.of("marked_emps", "FROM employees | EVAL m = emp_no IN (FROM employees | KEEP emp_no)"));
    }

    public void testInTsSubqueryInEval() {
        builder("""
            TS k8s
            | EVAL m = cluster IN (TS k8s | STATS m = max(rate(network.total_bytes_in)) BY cluster | KEEP cluster)
            | STATS max_bytes = max(to_long(network.total_bytes_in)) BY cluster, m
            | SORT cluster
            """).stages(STAGES).since(DimensionValues.DIMENSION_VALUES_VERSION).expectationChangesAt(PACK_DIMS_AGG).run();
    }

    public void testMultiColumnInTsSubqueryInEval() {
        requireMultiColumnInSubquerySupport();
        builder("""
            TS k8s
            | EVAL m = (pod, cluster) IN (TS k8s | STATS m = max(rate(network.total_bytes_in)) BY pod, cluster | KEEP pod, cluster)
            | STATS max_bytes = max(to_long(network.total_bytes_in)) BY pod, cluster, m
            | SORT pod, cluster
            """).stages(STAGES).since(DimensionValues.DIMENSION_VALUES_VERSION).expectationChangesAt(PACK_DIMS_AGG).run();
    }

    public void testInRowSubqueryInEval() {
        runGoldenTest("""
            FROM employees
            | EVAL m = emp_no IN (ROW emp_no = 1)
            """, STAGES);
    }

    public void testMultiColumnInRowSubqueryInEval() {
        requireMultiColumnInSubquerySupport();
        runGoldenTest("""
            FROM employees
            | EVAL m = (emp_no, languages) IN (ROW emp_no = 1, languages = 2)
            """, STAGES);
    }

    public void testMultiColumnInSubqueryNestedInCaseInEval() {
        requireMultiColumnInSubquerySupport();
        runGoldenTest("""
            FROM employees
            | EVAL m = CASE((emp_no, languages) IN (FROM employees | KEEP emp_no, languages), true, false)
            """, STAGES);
    }

    public void testMultiColumnInSubqueryNestedInCoalesceInEval() {
        requireMultiColumnInSubquerySupport();
        runGoldenTest("""
            FROM employees
            | EVAL m = COALESCE((emp_no, languages) IN (FROM employees | KEEP emp_no, languages), false)
            """, STAGES);
    }

    public void testMultiColumnInSubqueryNestedInIsNullInEval() {
        requireMultiColumnInSubquerySupport();
        runGoldenTest("""
            FROM employees
            | EVAL m = ((emp_no, languages) IN (FROM employees | KEEP emp_no, languages)) IS NULL
            """, STAGES);
    }

    public void testMultiColumnInSubqueryNestedInIsNotNullInEval() {
        requireMultiColumnInSubquerySupport();
        runGoldenTest("""
            FROM employees
            | EVAL m = ((emp_no, languages) IN (FROM employees | KEEP emp_no, languages)) IS NOT NULL
            """, STAGES);
    }

    public void testInSubqueryInEvalInsideNestedSubqueryPlan() {
        // Verifies that the Analyzer correctly resolves a MarkJoin produced by an EVAL IN subquery
        // that sits inside a nested subquery plan (the right side of an outer SemiJoin). Before the
        // EVAL branch, this shape was rejected by verify() with "IN subquery not supported in Eval".
        // After the branch it is resolved, and this golden test confirms the analyzed plan is correct.
        runGoldenTest("""
            FROM employees
            | WHERE emp_no IN (
                FROM employees
                | EVAL m = emp_no IN (FROM employees | KEEP emp_no)
                | WHERE m
                | KEEP emp_no
              )
            """, STAGES);
    }
}
