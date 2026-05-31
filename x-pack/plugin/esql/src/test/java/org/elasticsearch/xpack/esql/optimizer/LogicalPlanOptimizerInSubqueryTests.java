/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.junit.Before;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.hamcrest.Matchers.containsString;

/**
 * These are negative tests for IN and NOT IN subquery, the positive tests are in {@code LogicalPlanOptimizerInSubqueryGoldenTests}.
 * <ul>
 *     <li>If {@code Knn} is referenced inside an IN subquery without an explicit {@code Limit}, {@code VerificationException} is thrown.
 *     <li>If {@code Sort} is referenced inside an IN subquery without an explicit {@code Limit}, {@code VerificationException} is thrown.
 * </ul>
 * These queries work if an explicit {@code Limit} is specified inside the IN subquery.
 */
public class LogicalPlanOptimizerInSubqueryTests extends AbstractLogicalPlanOptimizerTests {

    @Before
    public void checkInSubquerySupport() {
        assumeTrue("Requires IN_SUBQUERY support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITHOUT_VIEW.isEnabled());
    }

    // -- SORT inside IN subquery tests --
    public void testRejectsSortWithoutLimitInSubquery() {
        String operator = randomBoolean() ? "IN" : "NOT IN";
        String inSubquery = "emp_no " + operator + " (FROM test | SORT emp_no | KEEP emp_no)";
        var e = expectThrows(VerificationException.class, () -> planInSubquery("FROM test | WHERE " + inSubquery));
        assertThat(e.getMessage(), containsString("IN subquery [" + inSubquery + "] cannot yet have an unbounded SORT"));
    }

    // -- KNN inside IN subquery tests --
    /**
     * Verifies that a {@code KNN} function inside an IN / NOT IN subquery without a LIMIT in the subquery is rejected: KNN needs
     * a LIMIT after it to set the number of nearest neighbors (k).
     * <p>
     * The trailing {@code STATS} is an optimization breaker: it stops the outer query's implicit {@code LIMIT} from being
     * pushed (by {@code PushLimitToKnn}) through the SemiJoin into the subquery's KNN, so the KNN genuinely has no LIMIT.
     */
    public void testRejectsKnnWithoutLimitInSubquery() {
        String operator = randomBoolean() ? "IN" : "NOT IN";
        var e = expectThrows(
            VerificationException.class,
            () -> optimize(
                typesAnalyzer().query(
                    "FROM types | WHERE integer "
                        + operator
                        + " (FROM types | WHERE knn(dense_vector, [0, 1, 2]) | KEEP integer) | STATS c = COUNT(*)"
                )
            )
        );
        assertThat(
            e.getMessage(),
            containsString("Knn function must be used with a LIMIT clause after it to set the number of nearest neighbors to find")
        );
    }

    // -- view with unbounded SORT referenced from an IN subquery tests --

    /**
     * Verifies that an IN / NOT IN subquery referencing a view whose definition contains a SORT (but the subquery has no LIMIT)
     * is rejected: after the view is expanded into the SemiJoin, its unbounded SORT is not supported.
     */
    public void testRejectsViewWithUnboundedSortInSubquery() {
        assumeTrue("Requires IN subquery with view support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITH_VIEW.isEnabled());
        String operator = randomBoolean() ? "IN" : "NOT IN";
        var e = expectThrows(VerificationException.class, () -> {
            TestAnalyzer viewAnalyzer = analyzer().addEmployees("test").addView("sorted_emps", "FROM test | SORT emp_no | KEEP emp_no");
            optimize(viewAnalyzer.query("FROM test | WHERE emp_no " + operator + " (FROM sorted_emps)"));
        });
        assertThat(e.getMessage(), containsString("cannot yet have an unbounded SORT"));
    }

    private void planInSubquery(String query) {
        TestAnalyzer inSubqueryAnalyzer = analyzer().addEmployees("test");
        optimize(inSubqueryAnalyzer.query(query));
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }
}
