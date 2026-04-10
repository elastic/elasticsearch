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
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.junit.Before;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.hamcrest.Matchers.containsString;

/**
 * Tests for IN/NOT IN subquery behavior that requires the logical plan optimizer,
 * specifically for post-optimization verification checks like nested UnionAll detection.
 */
public class LogicalPlanOptimizerInSubqueryTests extends AbstractLogicalPlanOptimizerTests {

    @Before
    public void checkInSubquerySupport() {
        assumeTrue("Requires IN_SUBQUERY support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY.isEnabled());
        assumeTrue("Requires FROM subquery support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
    }

    /**
     * Verifies that a disjunctive IN subquery inside a FROM subquery (which already creates a UnionAll)
     * is rejected because it would produce nested UnionAll.
     */
    public void testRejectsDisjunctiveInSubqueryInsideFromSubquery() {
        var e = expectThrows(VerificationException.class, () -> planInSubquery("""
            FROM test,
                 (FROM test | WHERE emp_no IN (FROM test | KEEP emp_no) OR salary > 50000 | KEEP emp_no)
            """));
        assertThat(e.getMessage(), containsString("Disjunctive (OR) IN/NOT IN subqueries are not supported inside FROM subqueries"));
    }

    /**
     * Verifies that a disjunctive NOT IN subquery inside a FROM subquery is rejected.
     */
    public void testRejectsDisjunctiveNotInSubqueryInsideFromSubquery() {
        var e = expectThrows(VerificationException.class, () -> planInSubquery("""
            FROM test,
                 (FROM test | WHERE emp_no NOT IN (FROM test | KEEP emp_no) OR salary > 50000 | KEEP emp_no)
            """));
        assertThat(e.getMessage(), containsString("Disjunctive (OR) IN/NOT IN subqueries are not supported inside FROM subqueries"));
    }

    /**
     * Verifies that nested disjunctive IN subqueries (OR inside OR) are rejected.
     */
    public void testRejectsNestedDisjunctiveInSubqueries() {
        var e = expectThrows(VerificationException.class, () -> planInSubquery("""
            FROM test
            | WHERE emp_no IN (
                FROM test
                | WHERE salary IN (FROM test | KEEP salary) OR languages > 2
                | KEEP emp_no
              ) OR salary > 50000
            """));
        assertThat(e.getMessage(), containsString("Nested disjunctive (OR) IN/NOT IN subqueries are not supported"));
    }

    /**
     * Verifies that a disjunctive IN subquery combined with FORK is rejected.
     */
    public void testRejectsDisjunctiveInSubqueryWithFork() {
        var e = expectThrows(VerificationException.class, () -> planInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM test | KEEP emp_no) OR salary > 50000
            | FORK (WHERE emp_no > 10000) (WHERE emp_no < 10050)
            """));
        assertThat(e.getMessage(), containsString("FORK after disjunctive (OR) IN/NOT IN subquery is not supported"));
    }

    /**
     * Verifies that a disjunctive IN subquery at the top level (no FROM subquery nesting) works fine.
     * This should NOT produce nested UnionAll since there's only one level.
     */
    public void testDisjunctiveInSubqueryAtTopLevelIsAllowed() {
        var plan = planInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM test | KEEP emp_no) OR salary > 50000
            """);
        assertNotNull(plan);
    }

    // -- SORT inside IN subquery tests --

    /**
     * Verifies that SORT without LIMIT inside an IN subquery is rejected.
     */
    public void testRejectsSortWithoutLimitInInSubquery() {
        var e = expectThrows(VerificationException.class, () -> planInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM test | SORT emp_no | KEEP emp_no)
            """));
        assertThat(
            e.getMessage(),
            containsString("IN/NOT IN subquery [emp_no IN (FROM test | SORT emp_no | KEEP emp_no)] cannot yet have an unbounded SORT")
        );
    }

    /**
     * Verifies that SORT without LIMIT inside a NOT IN subquery is rejected.
     */
    public void testRejectsSortWithoutLimitInNotInSubquery() {
        var e = expectThrows(VerificationException.class, () -> planInSubquery("""
            FROM test
            | WHERE emp_no NOT IN (FROM test | SORT emp_no DESC | KEEP emp_no)
            """));
        assertThat(
            e.getMessage(),
            containsString(
                "IN/NOT IN subquery [emp_no NOT IN (FROM test | SORT emp_no DESC | KEEP emp_no)] cannot yet have an unbounded SORT"
            )
        );
    }

    /**
     * Verifies that SORT with LIMIT inside an IN subquery is allowed.
     */
    public void testSortWithLimitInSubqueryIsAllowed() {
        var plan = planInSubquery("""
            FROM test
            | WHERE emp_no IN (FROM test | SORT emp_no | LIMIT 5 | KEEP emp_no)
            """);
        assertNotNull(plan);
    }

    private LogicalPlan planInSubquery(String query) {
        TestAnalyzer inSubqueryAnalyzer = analyzer().addEmployees("test");
        return optimize(inSubqueryAnalyzer.query(query));
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }
}
