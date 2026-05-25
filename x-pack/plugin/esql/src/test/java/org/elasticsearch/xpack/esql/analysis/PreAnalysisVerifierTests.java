/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.parser.AbstractStatementParserTests;
import org.hamcrest.Matcher;
import org.junit.Before;

import static org.hamcrest.Matchers.containsString;

/**
 * Tests for {@link PreAnalysisVerifier}, which runs structural checks between view
 * resolution and pre-analysis. These tests exercise the verifier in isolation: parse
 * the query with the snapshot grammar, then call {@code PreAnalysisVerifier.verify}
 * directly. We don't go through the analyzer, because the whole point of the verifier
 * is that it runs <em>before</em> field-caps / index resolution.
 */
public class PreAnalysisVerifierTests extends AbstractStatementParserTests {

    @Before
    public void checkInSubquerySupport() {
        assumeTrue("Requires IN subquery support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY.isEnabled());
    }

    public void testWhereInSubqueryRejected() {
        expectVerifierError("from test | where emp_no in (from test)", containsString("IN subquery is not yet supported"));
    }

    public void testWhereNotInSubqueryRejected() {
        expectVerifierError("from test | where emp_no not in (from test)", containsString("IN subquery is not yet supported"));
    }

    public void testInSubqueryInEvalRejected() {
        expectVerifierError(
            "from test | eval x = emp_no in (from test)",
            containsString("IN subquery is not supported in [eval x = emp_no in (from test)]")
        );
    }

    public void testInSubqueryInSortRejected() {
        expectVerifierError(
            "from test | sort emp_no in (from test)",
            containsString("IN subquery is not supported in [sort emp_no in (from test)]")
        );
    }

    public void testInSubqueryInStatsFilterRejected() {
        expectVerifierError(
            "from test | stats c = count(*) where emp_no in (from test)",
            containsString("IN subquery is not supported in [stats c = count(*) where emp_no in (from test)]")
        );
    }

    public void testInSubqueryInInlineStatsFilterRejected() {
        expectVerifierError(
            "from test | inline stats c = count(*) where emp_no in (from test)",
            containsString("IN subquery is not supported in [inline stats c = count(*) where emp_no in (from test)]")
        );
    }

    public void testWhereInSubqueryUnderAndRejectedAsNotYetSupported() {
        expectVerifierError("from test | where emp_no > 0 and emp_no in (from test)", containsString("IN subquery is not yet supported"));
    }

    public void testWhereInSubqueryUnderOrRejectedAsNotYetSupported() {
        expectVerifierError("from test | where emp_no in (from test) or emp_no > 0", containsString("IN subquery is not yet supported"));
    }

    public void testWhereInSubqueryUnderNestedLogicalOpsRejectedAsNotYetSupported() {
        expectVerifierError(
            "from test | where not (emp_no > 0 and (emp_no in (from test) or emp_no < 100))",
            containsString("IN subquery is not yet supported")
        );
    }

    public void testInSubqueryAsValueUnderAndRejected() {
        expectVerifierError(
            "from test | where emp_no > 0 and MV_CONTAINS(x IN (from test), [true, false])",
            containsString("IN subquery is not supported within other expressions [MV_CONTAINS(x IN (from test), [true, false])]")
        );
    }

    public void testInSubqueryAsValueUnderOrRejected() {
        expectVerifierError(
            "from test | where MV_CONTAINS(x IN (from test), [true, false]) or emp_no > 0",
            containsString("IN subquery is not supported within other expressions [MV_CONTAINS(x IN (from test), [true, false])]")
        );
    }

    public void testInSubqueryAsValueUnderNotRejected() {
        expectVerifierError(
            "from test | where not MV_CONTAINS(x IN (from test), [true, false])",
            containsString("IN subquery is not supported within other expressions [MV_CONTAINS(x IN (from test), [true, false])]")
        );
    }

    private void expectVerifierError(String query, Matcher<String> messageMatcher) {
        VerificationException e = expectThrows(VerificationException.class, () -> PreAnalysisVerifier.verify(query(query)));
        assertThat(e.getMessage(), messageMatcher);
    }
}
