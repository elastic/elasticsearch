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
 * Tests for IN/NOT IN subquery behavior that requires the logical plan optimizer.
 */
public class LogicalPlanOptimizerInSubqueryTests extends AbstractLogicalPlanOptimizerTests {

    @Before
    public void checkInSubquerySupport() {
        assumeTrue("Requires IN_SUBQUERY support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITHOUT_VIEW.isEnabled());
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
            containsString("IN subquery [emp_no IN (FROM test | SORT emp_no | KEEP emp_no)] cannot yet have an unbounded SORT")
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
            containsString("IN subquery [emp_no NOT IN (FROM test | SORT emp_no DESC | KEEP emp_no)] cannot yet have an unbounded SORT")
        );
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
