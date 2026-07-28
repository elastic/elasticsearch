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
import org.junit.Before;

import java.util.EnumSet;
import java.util.Map;

/**
 * Captures the analyzed and logically-optimized plans for IN/NOT IN subquery scenarios.
 */
public class LogicalPlanOptimizerInSubqueryGoldenTests extends GoldenTestCase {

    @ParametersFactory(argumentFormatting = "%1$s")
    public static Iterable<Object[]> parameters() {
        return goldenModes();
    }

    public LogicalPlanOptimizerInSubqueryGoldenTests(@Name("mode") String mode) {
        super(mode);
    }

    private static final EnumSet<Stage> STAGES = EnumSet.of(Stage.ANALYSIS, Stage.LOGICAL_OPTIMIZATION);

    @Before
    public void checkInSubquerySupport() {
        assumeTrue("Requires IN_SUBQUERY support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITHOUT_VIEW.isEnabled());
    }

    private static void requireInSubqueryViewSupport() {
        assumeTrue("Requires IN subquery with view support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITH_VIEW.isEnabled());
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
    public void testTsRateWithInSubquery() {
        assumeTrue("Requires subquery with TS source support", EsqlCapabilities.Cap.SUBQUERY_WITH_TS.isEnabled());
        assumeTrue("Requires TS subquery support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITH_TS.isEnabled());
        builder("""
            TS k8s
            | WHERE cluster IN (TS k8s
                               | STATS m = max(rate(network.total_bytes_in)) BY cluster
                               | KEEP cluster)
            | STATS max_rate = max(rate(network.total_bytes_in)) BY cluster
            """).stages(STAGES).since(DimensionValues.DIMENSION_VALUES_VERSION).run();
    }

    public void testTsRateWithNotInSubquery() {
        assumeTrue("Requires subquery with TS source support", EsqlCapabilities.Cap.SUBQUERY_WITH_TS.isEnabled());
        assumeTrue("Requires TS subquery support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITH_TS.isEnabled());
        builder("""
            TS k8s
            | WHERE cluster NOT IN (TS k8s
                                   | STATS m = max(rate(network.total_bytes_in)) BY cluster
                                   | KEEP cluster)
            | STATS max_rate = max(rate(network.total_bytes_in)) BY cluster
            """).stages(STAGES).since(DimensionValues.DIMENSION_VALUES_VERSION).run();
    }

    // The outer TS pipeline groups BY WITHOUT(...), so the WITHOUT-bearing TimeSeriesAggregate sits directly
    // above the SemiJoin produced by the IN subquery. Before the TranslateTimeSeriesWithout fix the rule lowered
    // WITHOUT into a _timeseries metadata attribute by descending into every EsRelation under the aggregate's
    // child, without excluding the right-hand side of the SemiJoin, injecting the lowered _timeseries attribute
    // into the subquery (RHS) relation as well. After the fix only the main (left) relation carries _timeseries;
    // the subquery relation keeps just its own _tsid. The SUM overflow fix is the newer transport-version-dependent shape in this query.
    public void testTsWithoutAndRateWithInSubquery() {
        assumeTrue("Requires subquery with TS source support", EsqlCapabilities.Cap.SUBQUERY_WITH_TS.isEnabled());
        assumeTrue("Requires WITHOUT grouping support", EsqlCapabilities.Cap.ESQL_WITHOUT_GROUPING.isEnabled());
        assumeTrue("Requires TS subquery support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITH_TS.isEnabled());
        builder("""
            TS k8s
            | WHERE cluster IN (TS k8s
                               | STATS m = max(rate(network.total_bytes_in)) BY cluster
                               | KEEP cluster)
            | STATS total_cost = sum(network.cost) BY WITHOUT(pod, region)
            """).stages(STAGES).since(Sum.ESQL_SUM_LONG_OVERFLOW_FIX).run();
    }

    public void testTsWithoutAndRateWithNotInSubquery() {
        assumeTrue("Requires subquery with TS source support", EsqlCapabilities.Cap.SUBQUERY_WITH_TS.isEnabled());
        assumeTrue("Requires WITHOUT grouping support", EsqlCapabilities.Cap.ESQL_WITHOUT_GROUPING.isEnabled());
        assumeTrue("Requires TS subquery support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITH_TS.isEnabled());
        builder("""
            TS k8s
            | WHERE cluster NOT IN (TS k8s
                                   | STATS m = max(rate(network.total_bytes_in)) BY cluster
                                   | KEEP cluster)
            | STATS total_cost = sum(network.cost) BY WITHOUT(pod, region)
            """).stages(STAGES).since(Sum.ESQL_SUM_LONG_OVERFLOW_FIX).run();
    }

    public void testMultipleTsSubqueriesInsideInSubquery() {
        assumeTrue("Requires TS subquery support", EsqlCapabilities.Cap.SUBQUERY_WITH_TS.isEnabled());
        assumeTrue("Requires TS subquery support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITH_TS.isEnabled());
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
            """).stages(STAGES).since(DimensionValues.DIMENSION_VALUES_VERSION).run();
    }

    public void testMultipleTsSubqueriesInsideNotInSubquery() {
        assumeTrue("Requires TS subquery support", EsqlCapabilities.Cap.SUBQUERY_WITH_TS.isEnabled());
        assumeTrue("Requires TS subquery support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITH_TS.isEnabled());

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
            """).stages(STAGES).since(DimensionValues.DIMENSION_VALUES_VERSION).run();
    }

    // -- IN / NOT IN subqueries referencing views --

    public void testInSubqueryReferencingView() {
        requireInSubqueryViewSupport();
        runGoldenTest("""
            FROM employees
            | WHERE emp_no IN (FROM emps_view)
            | KEEP emp_no, first_name
            """, STAGES, Map.of("emps_view", "FROM employees | KEEP emp_no"));
    }

    public void testNotInSubqueryReferencingView() {
        requireInSubqueryViewSupport();
        runGoldenTest("""
            FROM employees
            | WHERE emp_no NOT IN (FROM emps_view)
            | KEEP emp_no
            """, STAGES, Map.of("emps_view", "FROM employees | WHERE salary > 50000 | KEEP emp_no"));
    }

    public void testInSubqueryReferencingViewWithInSubqueryInDefinition() {
        requireInSubqueryViewSupport();
        runGoldenTest("""
            FROM employees
            | WHERE emp_no IN (FROM filtered_emps)
            | KEEP emp_no
            """, STAGES, Map.of("filtered_emps", "FROM employees | WHERE emp_no IN (FROM employees | KEEP emp_no) | KEEP emp_no"));
    }

    public void testInSubqueryReferencingViewWithSortLimit() {
        requireInSubqueryViewSupport();
        runGoldenTest("""
            FROM employees
            | WHERE emp_no IN (FROM sorted_emps)
            | KEEP emp_no
            """, STAGES, Map.of("sorted_emps", "FROM employees | SORT emp_no | LIMIT 5 | KEEP emp_no"));
    }

    public void testDisjunctiveInSubqueryReferencingView() {
        requireInSubqueryViewSupport();
        runGoldenTest("""
            FROM employees
            | WHERE emp_no IN (FROM emps_view) OR salary > 50000
            | KEEP emp_no
            """, STAGES, Map.of("emps_view", "FROM employees | KEEP emp_no"));
    }

    public void testMainFromAndNotInSubqueryEachReferenceMultipleViewSubqueries() {
        requireInSubqueryViewSupport();
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
}
