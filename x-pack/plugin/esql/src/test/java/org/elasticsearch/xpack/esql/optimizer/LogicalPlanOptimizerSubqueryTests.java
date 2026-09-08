/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.configuration;
import static org.hamcrest.Matchers.containsString;

/**
 * Negative tests for FROM subqueries at the logical-optimizer stage; the positive coverage is in
 * {@code LogicalPlanOptimizerSubqueryGoldenTests}.
 */
public class LogicalPlanOptimizerSubqueryTests extends AbstractLogicalPlanOptimizerTests {

    public void testUnboundedSortInsideInSubqueryInUnionAllBranchIsRejected() {
        var e = expectThrows(VerificationException.class, () -> planSubquery("""
            FROM (FROM test
                  | WHERE emp_no IN (FROM test | SORT emp_no | KEEP emp_no)),
                 (FROM languages)
            | STATS c = COUNT(*)
            """));
        assertThat(e.getMessage(), containsString("Unbounded SORT not supported yet [SORT emp_no] please add a LIMIT"));
        assertThat(
            e.getMessage(),
            containsString(
                "cannot yet have an unbounded SORT [SORT emp_no] before it: either move the SORT after it, or add a LIMIT after the SORT"
            )
        );
    }

    public void testUnboundedSortInsideInSubqueryInNestedUnionAllBranchIsRejected() {
        var e = expectThrows(VerificationException.class, () -> planSubquery("""
            FROM (FROM (FROM test
                        | WHERE emp_no IN (FROM test | SORT emp_no | KEEP emp_no)),
                       (FROM test)
                 ),
                 (FROM languages)
            | STATS c = COUNT(*)
            """));
        assertThat(e.getMessage(), containsString("Unbounded SORT not supported yet [SORT emp_no] please add a LIMIT"));
        assertThat(
            e.getMessage(),
            containsString(
                "cannot yet have an unbounded SORT [SORT emp_no] before it: either move the SORT after it, or add a LIMIT after the SORT"
            )
        );
    }

    public void testTotalBranchCountAtOrBeyondLimit() {
        String query = """
            FROM test, (FROM test), (FROM languages)
            | STATS c = COUNT(*)
            """;

        planSubquery(query, Settings.builder().put(QueryPragmas.MAX_QUERY_BRANCHES.getKey(), 3).build());
        VerificationException e = expectThrows(
            VerificationException.class,
            () -> planSubquery(query, Settings.builder().put(QueryPragmas.MAX_QUERY_BRANCHES.getKey(), 2).build())
        );
        assertThat(e.getMessage(), containsString("query resolved to 3 branches in total, exceeding the limit of 2"));
    }

    public void testTotalBranchCountWithNestedSubqueryAtOrBeyondLimit() {
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
        planSubquery("""
            FROM test
            | WHERE emp_no > 10000
            """, Settings.builder().put(QueryPragmas.MAX_QUERY_BRANCHES.getKey(), 1).build());
    }

    public void testTotalBranchCountWithViewAtOrBeyondLimit() {
        String query = "FROM view_0, view_1, test";

        planSubquery(viewAnalyzer(), query, Settings.builder().put(QueryPragmas.MAX_QUERY_BRANCHES.getKey(), 3).build());
        VerificationException e = expectThrows(
            VerificationException.class,
            () -> planSubquery(viewAnalyzer(), query, Settings.builder().put(QueryPragmas.MAX_QUERY_BRANCHES.getKey(), 2).build())
        );
        assertThat(e.getMessage(), containsString("query resolved to 3 branches in total, exceeding the limit of 2"));
    }

    public void testNestingLevelAtOrBeyondLimit() {
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
        planSubquery("""
            FROM test
            | WHERE emp_no > 10000
            """, Settings.builder().put(QueryPragmas.MAX_QUERY_BRANCH_LEVELS.getKey(), 1).build());
    }

    public void testBothNestedSubqueryLimitsAtOrBeyondLimit() {
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
}
