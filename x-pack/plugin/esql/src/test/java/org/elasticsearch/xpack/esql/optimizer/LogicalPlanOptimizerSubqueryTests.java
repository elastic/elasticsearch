/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Negative tests for subquery-in-{@code FROM} after coordinating-node logical optimization.
 * A multi-source {@code FROM} builds a {@code UnionAll}, so the full-text position check is deferred
 * until after the filter is pushed into each branch. Positive plan-shape tests live in
 * {@code LogicalPlanOptimizerSubqueryGoldenTests}.
 */
public class LogicalPlanOptimizerSubqueryTests extends AbstractLogicalPlanOptimizerTests {

    /**
     * {@code SORT | LIMIT} is rewritten to {@code TopN}; the message must be
     * {@code SORT and LIMIT}, not {@code SORT} (bare SORT is legal).
     * A single-branch {@code FROM (subquery)} has no {@code UnionAll}, so analysis still sees {@code Limit}
     * and reports {@code LIMIT} — covered by {@code VerifierTests}.
     */
    public void testFullTextAfterSubqueryTopNReportsSortAndLimit() {
        List<String> fullTextFunctions = List.of(
            "match(title, \"Meditation\")",
            "match_phrase(title, \"Meditation\")",
            "title : \"Meditation\"",
            "kql(\"title: Meditation\")",
            "qstr(\"title: Meditation\")",
            "knn(vector, [1, 2, 3])"
        );
        for (String ftf : fullTextFunctions) {
            String query = LoggerMessageFormat.format(null, """
                FROM (FROM test | SORT title | LIMIT 10),
                     (FROM test | WHERE id > 0)
                | WHERE {}
                """, ftf);
            String err = error(query);
            assertThat(ftf, err, containsString(" cannot be used after SORT and LIMIT"));
        }
    }

    public void testFullTextAfterSubqueryLimitOnlyReportsLimit() {
        String err = error("""
            FROM (FROM test | LIMIT 10),
                 (FROM test | WHERE id > 0)
            | WHERE match(title, "Meditation")
            """);
        assertThat(err, containsString("[MATCH] function cannot be used after LIMIT"));
    }

    /**
     * Alignment nodes often inherit the whole {@code FROM (subquery), index} clause.
     * The first token is {@code FROM}, which is legal before KQL; name the parenthesized
     * subquery instead of reporting a bare {@code FROM}.
     */
    public void testKqlAfterFromSubqueryReportsParenthesizedSource() {
        TestAnalyzer testAnalyzer = analyzer().addIndex("hash_algorithms", "mapping-hash_algorithms.json")
            .addIndex("k8s-downsampled", "k8s-downsampled-mappings.json", IndexMode.TIME_SERIES);
        String err = error(testAnalyzer, """
            FROM (FROM hash_algorithms), k8s-downsampled
            | WHERE event_log RLIKE ".*b" OR NOT network.total_cost >= 85 AND kql("world")
            """);
        assertThat(err, containsString("[KQL] function cannot be used after FROM (FROM hash_algorithms), k8s-downsampled"));
    }

    private String error(String query) {
        return error(analyzer().addIndex("test", "mapping-full_text_search.json"), query);
    }

    private String error(TestAnalyzer testAnalyzer, String query) {
        LogicalPlan plan = testAnalyzer.query(query);
        Throwable e = expectThrows(
            VerificationException.class,
            "Expected error for plan [" + plan + "] but no error was raised",
            () -> optimize(plan)
        );
        assertThat(e, instanceOf(VerificationException.class));

        String message = e.getMessage();
        assertTrue(message.startsWith("Found "));

        String pattern = "\nline ";
        int index = message.indexOf(pattern);
        return message.substring(index + pattern.length());
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }
}
