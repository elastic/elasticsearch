/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.expression.function.aggregate.DimensionValues;
import org.junit.Before;

import java.util.EnumSet;
import java.util.Map;

/**
 * Captures the analyzed and logically-optimized plans for IN/NOT IN subquery scenarios.
 */
public class LogicalPlanOptimizerInSubqueryGoldenTests extends GoldenTestCase {

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
    // The default golden-test builder randomizes the minimum transport version, which would make the captured plan
    // flap between the two forms. Pin a version that supports `dimension_values` so the snapshot stays deterministic.
    public void testTsRateWithInSubquery() {
        assumeTrue("Requires subquery with TS source support", EsqlCapabilities.Cap.SUBQUERY_WITH_TS.isEnabled());
        assumeTrue("Requires TS subquery support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITH_TS.isEnabled());
        runGoldenTest("""
            TS k8s
            | WHERE cluster IN (TS k8s
                               | STATS m = max(rate(network.total_bytes_in)) BY cluster
                               | KEEP cluster)
            | STATS max_rate = max(rate(network.total_bytes_in)) BY cluster
            """, STAGES, TransportVersionUtils.randomVersionSupporting(DimensionValues.DIMENSION_VALUES_VERSION));
    }

    public void testTsRateWithNotInSubquery() {
        assumeTrue("Requires subquery with TS source support", EsqlCapabilities.Cap.SUBQUERY_WITH_TS.isEnabled());
        assumeTrue("Requires TS subquery support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITH_TS.isEnabled());
        runGoldenTest("""
            TS k8s
            | WHERE cluster NOT IN (TS k8s
                                   | STATS m = max(rate(network.total_bytes_in)) BY cluster
                                   | KEEP cluster)
            | STATS max_rate = max(rate(network.total_bytes_in)) BY cluster
            """, STAGES, TransportVersionUtils.randomVersionSupporting(DimensionValues.DIMENSION_VALUES_VERSION));
    }

    // The outer TS pipeline groups BY WITHOUT(...), so the WITHOUT-bearing TimeSeriesAggregate sits directly
    // above the SemiJoin produced by the IN subquery. Before the TranslateTimeSeriesWithout fix the rule lowered
    // WITHOUT into a _timeseries metadata attribute by descending into every EsRelation under the aggregate's
    // child, without excluding the right-hand side of the SemiJoin, injecting the lowered _timeseries attribute
    // into the subquery (RHS) relation as well. After the fix only the main (left) relation carries _timeseries;
    // the subquery relation keeps just its own _tsid. Pinned to TransportVersion.current() (rather than a random
    // version supporting `dimension_values`) so that the version-gated SUM long-overflow mode stays deterministic.
    public void testTsWithoutAndRateWithInSubquery() {
        assumeTrue("Requires subquery with TS source support", EsqlCapabilities.Cap.SUBQUERY_WITH_TS.isEnabled());
        assumeTrue("Requires WITHOUT grouping support", EsqlCapabilities.Cap.ESQL_WITHOUT_GROUPING.isEnabled());
        assumeTrue("Requires TS subquery support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITH_TS.isEnabled());
        runGoldenTest("""
            TS k8s
            | WHERE cluster IN (TS k8s
                               | STATS m = max(rate(network.total_bytes_in)) BY cluster
                               | KEEP cluster)
            | STATS total_cost = sum(network.cost) BY WITHOUT(pod, region)
            """, STAGES, TransportVersion.current());
    }

    public void testTsWithoutAndRateWithNotInSubquery() {
        assumeTrue("Requires subquery with TS source support", EsqlCapabilities.Cap.SUBQUERY_WITH_TS.isEnabled());
        assumeTrue("Requires WITHOUT grouping support", EsqlCapabilities.Cap.ESQL_WITHOUT_GROUPING.isEnabled());
        assumeTrue("Requires TS subquery support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITH_TS.isEnabled());
        runGoldenTest("""
            TS k8s
            | WHERE cluster NOT IN (TS k8s
                                   | STATS m = max(rate(network.total_bytes_in)) BY cluster
                                   | KEEP cluster)
            | STATS total_cost = sum(network.cost) BY WITHOUT(pod, region)
            """, STAGES, TransportVersion.current());
    }

    public void testMultipleTsSubqueriesInsideInSubquery() {
        assumeTrue("Requires TS subquery support", EsqlCapabilities.Cap.SUBQUERY_WITH_TS.isEnabled());
        assumeTrue("Requires TS subquery support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITH_TS.isEnabled());
        runGoldenTest("""
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
            """, STAGES, TransportVersion.current());
    }

    public void testMultipleTsSubqueriesInsideNotInSubquery() {
        assumeTrue("Requires TS subquery support", EsqlCapabilities.Cap.SUBQUERY_WITH_TS.isEnabled());
        assumeTrue("Requires TS subquery support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITH_TS.isEnabled());

        runGoldenTest("""
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
            """, STAGES, TransportVersion.current());
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
