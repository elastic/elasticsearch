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
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

/**
 * Tests for subquery-in-{@code FROM} after coordinating-node logical optimization.
 * A multi-source {@code FROM} builds a {@code UnionAll}, so the full-text position check is deferred
 * until after the filter is pushed into each branch. Snapshot plan-shape tests live in
 * {@code LogicalPlanOptimizerSubqueryGoldenTests}.
 */
public class LogicalPlanOptimizerSubqueryTests extends AbstractLogicalPlanOptimizerTests {

    public LogicalPlanOptimizerSubqueryTests(VersionMode versionMode) {
        super(versionMode);
    }

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

    /**
     * Verifies that a type conversion over a grouping key sitting above an {@link org.elasticsearch.xpack.esql.plan.logical.Aggregate}
     * is NOT pushed into the {@code UnionAll} branches. The conversion reads from aggregate output, not
     * from union branch columns; pushing it down would produce a synthetic reference unreachable from the
     * consumer ({@code IllegalStateException} from {@code PlanConsistencyChecker}).
     * Plan-shape assertions for the EVAL consumer live in {@code LogicalPlanOptimizerSubqueryGoldenTests}.
     */
    public void testConvertGroupKeyAfterStatsDoesNotThrow() {
        for (String suffix : List.of(
            "| EVAL g = TO_STRING(gender)",
            "| WHERE TO_STRING(gender) == \"M\"",
            "| STATS c = COUNT(TO_STRING(gender))"
        )) {
            String query = "FROM (FROM test), (FROM test) | STATS max_salary = MAX(salary) BY gender " + suffix;
            LogicalPlan plan = defaultAnalyzer().query(query);
            LogicalPlan optimized = optimize(plan); // must not throw IllegalStateException
            // The UnionAll output must not carry any synthetic $$...$converted_to$... attribute.
            // If the out-of-scope conversion were pushed down, a synthetic attribute would appear here and
            // cause PlanConsistencyChecker to throw on a later validation step.
            optimized.forEachDown(UnionAll.class, ua -> {
                ua.output().forEach(attr -> assertThat(suffix, attr.name(), not(containsString("$converted_to$"))));
            });
        }
    }

    /**
     * Verifies the case where the same type conversion appears <em>both</em> inside the {@code Aggregate}
     * (as an aggregate argument, e.g. {@code COUNT(TO_STRING(gender))}) and above it (e.g.
     * {@code EVAL g = TO_STRING(gender)}). The argument form is legitimately pushed into the
     * {@code UnionAll} branches and its synthetic reference appears in the {@code UnionAll} output; the
     * above-aggregate form must NOT be replaced with that synthetic reference, which would produce an
     * unreachable reference above the {@code Aggregate} and cause {@code PlanConsistencyChecker} to throw.
     * "No exception" is therefore the meaningful assertion here.
     */
    public void testConvertGroupKeyBothInsideAndAboveStats() {
        // TO_STRING(gender) appears inside STATS (aggregate arg) AND above STATS (in EVAL).
        String query = "FROM (FROM test), (FROM test)"
            + " | STATS c = COUNT(TO_STRING(gender)) BY gender"
            + " | EVAL g = TO_STRING(gender)";
        LogicalPlan plan = defaultAnalyzer().query(query);
        optimize(plan); // must not throw IllegalStateException
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
