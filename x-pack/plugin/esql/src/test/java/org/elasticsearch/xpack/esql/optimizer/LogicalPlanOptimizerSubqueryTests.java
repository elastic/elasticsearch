/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.configuration;
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

    public void testTotalBranchCountWithNestedSubqueryAtOrBeyondLimit() {
        assumeTrue("requires nested subquery support", EsqlCapabilities.Cap.NESTED_SUBQUERY_IN_FROM_COMMAND.isEnabled());
        String query = """
            FROM test, (FROM test, (FROM languages))
            | STATS c = COUNT(*)
            """;

        // Three sources; the inner UnionAll is a merge segment, not a leaf.
        planSubquery(query, Settings.builder().put(QueryPragmas.MAX_QUERY_BRANCHES.getKey(), 3).build());
        VerificationException e = expectThrows(
            VerificationException.class,
            () -> planSubquery(query, Settings.builder().put(QueryPragmas.MAX_QUERY_BRANCHES.getKey(), 2).build())
        );
        assertThat(
            e.getMessage(),
            containsString(
                "query resolved to 3 branches in total, exceeding the limit of 2 set by the [max_query_branches] query pragma. "
                    + "Reduce the number of sources"
            )
        );

        String threeDeep = """
            FROM test, (FROM test, (FROM test, (FROM languages)))
            | STATS c = COUNT(*)
            """;
        planSubquery(threeDeep, Settings.builder().put(QueryPragmas.MAX_QUERY_BRANCHES.getKey(), 4).build());
        e = expectThrows(
            VerificationException.class,
            () -> planSubquery(threeDeep, Settings.builder().put(QueryPragmas.MAX_QUERY_BRANCHES.getKey(), 3).build())
        );
        assertThat(e.getMessage(), containsString("query resolved to 4 branches in total, exceeding the limit of 3"));
    }

    public void testTotalBranchCountIgnoresPlansWithoutUnions() {
        assumeTrue("requires nested subquery support", EsqlCapabilities.Cap.NESTED_SUBQUERY_IN_FROM_COMMAND.isEnabled());
        planSubquery("""
            FROM test
            | WHERE emp_no > 10000
            """, Settings.builder().put(QueryPragmas.MAX_QUERY_BRANCHES.getKey(), 1).build());
    }

    public void testTotalBranchCountDoesNotCountLookupJoinAsLeaf() {
        assumeTrue("requires nested subquery support", EsqlCapabilities.Cap.NESTED_SUBQUERY_IN_FROM_COMMAND.isEnabled());
        String query = """
            FROM test, (FROM test)
            | EVAL language_code = languages
            | LOOKUP JOIN languages_lookup ON language_code
            """;

        planSubquery(query, Settings.builder().put(QueryPragmas.MAX_QUERY_BRANCHES.getKey(), 2).build());
        VerificationException e = expectThrows(
            VerificationException.class,
            () -> planSubquery(query, Settings.builder().put(QueryPragmas.MAX_QUERY_BRANCHES.getKey(), 1).build())
        );
        assertThat(e.getMessage(), containsString("query resolved to 2 branches in total, exceeding the limit of 1"));
    }

    public void testTotalBranchCountDoesNotCountEnrichAsLeaf() {
        assumeTrue("requires nested subquery support", EsqlCapabilities.Cap.NESTED_SUBQUERY_IN_FROM_COMMAND.isEnabled());
        String query = """
            FROM test, (FROM test)
            | ENRICH languages_idx ON first_name
            """;

        planSubquery(query, Settings.builder().put(QueryPragmas.MAX_QUERY_BRANCHES.getKey(), 2).build());
        VerificationException e = expectThrows(
            VerificationException.class,
            () -> planSubquery(query, Settings.builder().put(QueryPragmas.MAX_QUERY_BRANCHES.getKey(), 1).build())
        );
        assertThat(e.getMessage(), containsString("query resolved to 2 branches in total, exceeding the limit of 1"));
    }

    public void testTotalBranchCountWithViewAtOrBeyondLimit() {
        assumeTrue("requires nested subquery support", EsqlCapabilities.Cap.NESTED_SUBQUERY_IN_FROM_COMMAND.isEnabled());
        String query = "FROM view_0, view_1, test";

        planSubquery(viewAnalyzer(), query, Settings.builder().put(QueryPragmas.MAX_QUERY_BRANCHES.getKey(), 3).build());
        VerificationException e = expectThrows(
            VerificationException.class,
            () -> planSubquery(viewAnalyzer(), query, Settings.builder().put(QueryPragmas.MAX_QUERY_BRANCHES.getKey(), 2).build())
        );
        assertThat(e.getMessage(), containsString("query resolved to 3 branches in total, exceeding the limit of 2"));
    }

    public void testNestingLevelAtOrBeyondLimit() {
        assumeTrue("requires nested subquery support", EsqlCapabilities.Cap.NESTED_SUBQUERY_IN_FROM_COMMAND.isEnabled());
        String flat = """
            FROM test, (FROM test), (FROM languages)
            | STATS c = COUNT(*)
            """;
        planSubquery(flat, Settings.builder().put(QueryPragmas.MAX_QUERY_BRANCH_LEVELS.getKey(), 1).build());

        String nested = """
            FROM test, (FROM test, (FROM test, (FROM languages)))
            | STATS c = COUNT(*)
            """;

        planSubquery(nested, Settings.builder().put(QueryPragmas.MAX_QUERY_BRANCH_LEVELS.getKey(), 3).build());
        VerificationException e = expectThrows(
            VerificationException.class,
            () -> planSubquery(nested, Settings.builder().put(QueryPragmas.MAX_QUERY_BRANCH_LEVELS.getKey(), 2).build())
        );
        assertThat(e.getMessage(), containsString("query resolved to 3 nested union levels, exceeding the limit of 2"));
        assertThat(e.getMessage(), containsString("[max_query_branch_levels] query pragma"));
        assertThat(e.getMessage(), containsString("Reduce the nesting of sources"));
    }

    public void testNestingLevelIgnoresPlansWithoutUnions() {
        assumeTrue("requires nested subquery support", EsqlCapabilities.Cap.NESTED_SUBQUERY_IN_FROM_COMMAND.isEnabled());
        planSubquery("""
            FROM test
            | WHERE emp_no > 10000
            """, Settings.builder().put(QueryPragmas.MAX_QUERY_BRANCH_LEVELS.getKey(), 1).build());
    }

    public void testBothNestedSubqueryLimitsAtOrBeyondLimit() {
        assumeTrue("requires nested subquery support", EsqlCapabilities.Cap.NESTED_SUBQUERY_IN_FROM_COMMAND.isEnabled());
        String query = """
            FROM test, (FROM test, (FROM test, (FROM languages)))
            | STATS c = COUNT(*)
            """;
        Settings atLimit = Settings.builder()
            .put(QueryPragmas.MAX_QUERY_BRANCHES.getKey(), 4)
            .put(QueryPragmas.MAX_QUERY_BRANCH_LEVELS.getKey(), 3)
            .build();
        Settings beyondBranchLimit = Settings.builder()
            .put(QueryPragmas.MAX_QUERY_BRANCHES.getKey(), 3)
            .put(QueryPragmas.MAX_QUERY_BRANCH_LEVELS.getKey(), 3)
            .build();
        Settings beyondNestingLimit = Settings.builder()
            .put(QueryPragmas.MAX_QUERY_BRANCHES.getKey(), 4)
            .put(QueryPragmas.MAX_QUERY_BRANCH_LEVELS.getKey(), 2)
            .build();
        Settings beyondBothLimits = Settings.builder()
            .put(QueryPragmas.MAX_QUERY_BRANCHES.getKey(), 3)
            .put(QueryPragmas.MAX_QUERY_BRANCH_LEVELS.getKey(), 2)
            .build();

        planSubquery(query, atLimit);
        VerificationException e = expectThrows(VerificationException.class, () -> planSubquery(query, beyondBranchLimit));
        assertThat(e.getMessage(), containsString("query resolved to 4 branches in total, exceeding the limit of 3"));
        e = expectThrows(VerificationException.class, () -> planSubquery(query, beyondNestingLimit));
        assertThat(e.getMessage(), containsString("query resolved to 3 nested union levels, exceeding the limit of 2"));
        e = expectThrows(VerificationException.class, () -> planSubquery(query, beyondBothLimits));
        assertThat(e.getMessage(), containsString("Found 2 problems"));
        assertThat(e.getMessage(), containsString("query resolved to 4 branches in total, exceeding the limit of 3"));
        assertThat(e.getMessage(), containsString("query resolved to 3 nested union levels, exceeding the limit of 2"));
    }

    public void testBothNestedSubqueryLimitsWithViewAtOrBeyondLimit() {
        assumeTrue("Requires IN subquery with view support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITH_VIEW.isEnabled());
        assumeTrue("requires nested subquery support", EsqlCapabilities.Cap.NESTED_SUBQUERY_IN_FROM_COMMAND.isEnabled());
        String query = """
            FROM test, (FROM test, (FROM languages))
            | WHERE emp_no IN (FROM view_0, view_1 | KEEP emp_no)
            """;
        Settings atLimit = Settings.builder()
            .put(QueryPragmas.MAX_QUERY_BRANCHES.getKey(), 3)
            .put(QueryPragmas.MAX_QUERY_BRANCH_LEVELS.getKey(), 2)
            .build();
        Settings beyondBranchLimit = Settings.builder()
            .put(QueryPragmas.MAX_QUERY_BRANCHES.getKey(), 2)
            .put(QueryPragmas.MAX_QUERY_BRANCH_LEVELS.getKey(), 2)
            .build();
        Settings beyondNestingLimit = Settings.builder()
            .put(QueryPragmas.MAX_QUERY_BRANCHES.getKey(), 3)
            .put(QueryPragmas.MAX_QUERY_BRANCH_LEVELS.getKey(), 1)
            .build();
        Settings beyondBothLimits = Settings.builder()
            .put(QueryPragmas.MAX_QUERY_BRANCHES.getKey(), 2)
            .put(QueryPragmas.MAX_QUERY_BRANCH_LEVELS.getKey(), 1)
            .build();

        planSubquery(viewAnalyzer(), query, atLimit);
        VerificationException e = expectThrows(VerificationException.class, () -> planSubquery(viewAnalyzer(), query, beyondBranchLimit));
        assertThat(e.getMessage(), containsString("query resolved to 3 branches in total, exceeding the limit of 2"));
        e = expectThrows(VerificationException.class, () -> planSubquery(viewAnalyzer(), query, beyondNestingLimit));
        assertThat(e.getMessage(), containsString("query resolved to 2 nested union levels, exceeding the limit of 1"));
        e = expectThrows(VerificationException.class, () -> planSubquery(viewAnalyzer(), query, beyondBothLimits));
        assertThat(e.getMessage(), containsString("Found 2 problems"));
        assertThat(e.getMessage(), containsString("query resolved to 3 branches in total, exceeding the limit of 2"));
        assertThat(e.getMessage(), containsString("query resolved to 2 nested union levels, exceeding the limit of 1"));
    }

    public void testNestedSubqueryLimitsWithinInSubquery() {
        assumeTrue("requires nested subquery support", EsqlCapabilities.Cap.NESTED_SUBQUERY_IN_FROM_COMMAND.isEnabled());
        String query = """
            FROM (FROM test
                  | WHERE emp_no IN (
                      FROM test, (FROM test, (FROM test))
                      | KEEP emp_no
                    )
                 ),
                 (FROM test)
            """;
        assertNestedSubqueryLimits(query, subqueryAnalyzer(), 3, 2);
    }

    public void testNestedSubqueryLimitsWithinInSubqueryWithView() {
        assumeTrue("requires nested subquery support", EsqlCapabilities.Cap.NESTED_SUBQUERY_IN_FROM_COMMAND.isEnabled());
        String query = """
            FROM (FROM test
                  | WHERE emp_no IN (
                      FROM view_0, (FROM view_1, (FROM test))
                      | KEEP emp_no
                    )
                 ),
                 (FROM test)
            """;
        assertNestedSubqueryLimits(query, viewAnalyzer(), 3, 2);
    }

    public void testNestedSubqueryLimitsForMultipleInSubqueriesAreIndependent() {
        assumeTrue("requires nested subquery support", EsqlCapabilities.Cap.NESTED_SUBQUERY_IN_FROM_COMMAND.isEnabled());
        String query = """
            FROM test
            | WHERE emp_no IN (
                FROM test, (FROM test, (FROM test))
                | KEEP emp_no
              )
              AND languages IN (
                FROM test, (FROM test), (FROM test, (FROM test | WHERE languages > 0))
                | KEEP languages
              )
            """;
        Settings atLimit = Settings.builder()
            .put(QueryPragmas.MAX_QUERY_BRANCHES.getKey(), 4)
            .put(QueryPragmas.MAX_QUERY_BRANCH_LEVELS.getKey(), 2)
            .build();
        Settings beyondBothLimits = Settings.builder()
            .put(QueryPragmas.MAX_QUERY_BRANCHES.getKey(), 2)
            .put(QueryPragmas.MAX_QUERY_BRANCH_LEVELS.getKey(), 1)
            .build();

        planSubquery(query, atLimit);
        VerificationException e = expectThrows(VerificationException.class, () -> planSubquery(query, beyondBothLimits));
        assertThat(e.getMessage(), containsString("Found 4 problems"));
        assertThat(e.getMessage(), containsString("query resolved to 3 branches in total, exceeding the limit of 2"));
        assertThat(e.getMessage(), containsString("query resolved to 4 branches in total, exceeding the limit of 2"));
        assertThat(e.getMessage(), containsString("query resolved to 2 nested union levels, exceeding the limit of 1"));
    }

    private static void assertNestedSubqueryLimits(String query, TestAnalyzer analyzer, int branches, int levels) {
        Settings atLimit = Settings.builder()
            .put(QueryPragmas.MAX_QUERY_BRANCHES.getKey(), branches)
            .put(QueryPragmas.MAX_QUERY_BRANCH_LEVELS.getKey(), levels)
            .build();
        Settings beyondBranchLimit = Settings.builder()
            .put(QueryPragmas.MAX_QUERY_BRANCHES.getKey(), branches - 1)
            .put(QueryPragmas.MAX_QUERY_BRANCH_LEVELS.getKey(), levels)
            .build();
        Settings beyondNestingLimit = Settings.builder()
            .put(QueryPragmas.MAX_QUERY_BRANCHES.getKey(), branches)
            .put(QueryPragmas.MAX_QUERY_BRANCH_LEVELS.getKey(), levels - 1)
            .build();
        Settings beyondBothLimits = Settings.builder()
            .put(QueryPragmas.MAX_QUERY_BRANCHES.getKey(), branches - 1)
            .put(QueryPragmas.MAX_QUERY_BRANCH_LEVELS.getKey(), levels - 1)
            .build();

        planSubquery(analyzer, query, atLimit);
        VerificationException e = expectThrows(VerificationException.class, () -> planSubquery(analyzer, query, beyondBranchLimit));
        assertThat(
            e.getMessage(),
            containsString("query resolved to " + branches + " branches in total, exceeding the limit of " + (branches - 1))
        );
        e = expectThrows(VerificationException.class, () -> planSubquery(analyzer, query, beyondNestingLimit));
        assertThat(
            e.getMessage(),
            containsString("query resolved to " + levels + " nested union levels, exceeding the limit of " + (levels - 1))
        );
        e = expectThrows(VerificationException.class, () -> planSubquery(analyzer, query, beyondBothLimits));
        assertThat(e.getMessage(), containsString("Found 2 problems"));
        assertThat(
            e.getMessage(),
            containsString("query resolved to " + branches + " branches in total, exceeding the limit of " + (branches - 1))
        );
        assertThat(
            e.getMessage(),
            containsString("query resolved to " + levels + " nested union levels, exceeding the limit of " + (levels - 1))
        );
    }

    private static LogicalPlan planSubquery(String query, Settings pragmaSettings) {
        return planSubquery(subqueryAnalyzer(), query, pragmaSettings);
    }

    private static LogicalPlan planSubquery(TestAnalyzer analyzer, String query, Settings pragmaSettings) {
        var context = new LogicalOptimizerContext(
            configuration(new QueryPragmas(pragmaSettings), query),
            logicalOptimizerCtx.foldCtx(),
            logicalOptimizerCtx.minimumVersion()
        );
        return new LogicalPlanOptimizer(context).optimize(analyzer.query(query));
    }

    private static TestAnalyzer viewAnalyzer() {
        return subqueryAnalyzer().addView("view_0", "FROM test").addView("view_1", "FROM test");
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
