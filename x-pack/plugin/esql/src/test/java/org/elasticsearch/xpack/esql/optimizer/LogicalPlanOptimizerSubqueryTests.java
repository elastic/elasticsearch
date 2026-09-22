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
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.configuration;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

/**
 * Negative tests for FROM subqueries at the logical-optimizer stage; the positive coverage is in
 * {@code LogicalPlanOptimizerSubqueryGoldenTests}.
 *
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

    public void testUnboundedSortInsideInSubqueryInUnionAllBranchIsRejected() {
        var e = expectThrows(VerificationException.class, () -> planSubquery("""
            FROM (FROM test | WHERE emp_no IN (FROM test | SORT emp_no | KEEP emp_no)),
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
            FROM (FROM (FROM test | WHERE emp_no IN (FROM test | SORT emp_no | KEEP emp_no)),
                       (FROM test)),
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
        planSubquery(query, Settings.builder().put(QueryPragmas.MAX_BRANCH_COUNT.getKey(), 3).build());
        VerificationException e = expectThrows(
            VerificationException.class,
            () -> planSubquery(query, Settings.builder().put(QueryPragmas.MAX_BRANCH_COUNT.getKey(), 2).build())
        );
        assertThat(e.getMessage(), containsString("query resolved to 3 branches in total, exceeding the limit of 2"));
    }

    public void testTotalBranchCountWithNestedSubqueryAtOrBeyondLimit() {
        String query = """
            FROM test, (FROM test, (FROM languages))
            | STATS c = COUNT(*)
            """;
        // Three sources; the inner UnionAll is a merge segment, not a leaf.
        planSubquery(query, Settings.builder().put(QueryPragmas.MAX_BRANCH_COUNT.getKey(), 3).build());
        VerificationException e = expectThrows(
            VerificationException.class,
            () -> planSubquery(query, Settings.builder().put(QueryPragmas.MAX_BRANCH_COUNT.getKey(), 2).build())
        );
        assertThat(
            e.getMessage(),
            containsString(
                "query resolved to 3 branches in total, exceeding the limit of 2 set by the [max_branch_count] query pragma. "
                    + "Reduce the number of sources"
            )
        );

        String threeDeep = """
            FROM test, (FROM test, (FROM test, (FROM languages)))
            | STATS c = COUNT(*)
            """;
        planSubquery(threeDeep, Settings.builder().put(QueryPragmas.MAX_BRANCH_COUNT.getKey(), 4).build());
        e = expectThrows(
            VerificationException.class,
            () -> planSubquery(threeDeep, Settings.builder().put(QueryPragmas.MAX_BRANCH_COUNT.getKey(), 3).build())
        );
        assertThat(e.getMessage(), containsString("query resolved to 4 branches in total, exceeding the limit of 3"));
    }

    public void testTotalBranchCountIgnoresPlansWithoutUnions() {
        planSubquery("""
            FROM test
            | WHERE emp_no > 10000
            """, Settings.builder().put(QueryPragmas.MAX_BRANCH_COUNT.getKey(), 1).build());
    }

    public void testTotalBranchCountDoesNotCountLookupJoinAsLeaf() {
        String query = """
            FROM test, (FROM test)
            | EVAL language_code = languages
            | LOOKUP JOIN languages_lookup ON language_code
            """;
        planSubquery(query, Settings.builder().put(QueryPragmas.MAX_BRANCH_COUNT.getKey(), 2).build());
        VerificationException e = expectThrows(
            VerificationException.class,
            () -> planSubquery(query, Settings.builder().put(QueryPragmas.MAX_BRANCH_COUNT.getKey(), 1).build())
        );
        assertThat(e.getMessage(), containsString("query resolved to 2 branches in total, exceeding the limit of 1"));
    }

    public void testTotalBranchCountDoesNotCountEnrichAsLeaf() {
        String query = """
            FROM test, (FROM test)
            | ENRICH languages_idx ON first_name
            """;
        planSubquery(query, Settings.builder().put(QueryPragmas.MAX_BRANCH_COUNT.getKey(), 2).build());
        VerificationException e = expectThrows(
            VerificationException.class,
            () -> planSubquery(query, Settings.builder().put(QueryPragmas.MAX_BRANCH_COUNT.getKey(), 1).build())
        );
        assertThat(e.getMessage(), containsString("query resolved to 2 branches in total, exceeding the limit of 1"));
    }

    public void testTotalBranchCountWithViewAtOrBeyondLimit() {
        String query = "FROM view_0, view_1, test";

        planSubquery(viewAnalyzer(), query, Settings.builder().put(QueryPragmas.MAX_BRANCH_COUNT.getKey(), 3).build());
        VerificationException e = expectThrows(
            VerificationException.class,
            () -> planSubquery(viewAnalyzer(), query, Settings.builder().put(QueryPragmas.MAX_BRANCH_COUNT.getKey(), 2).build())
        );
        assertThat(e.getMessage(), containsString("query resolved to 3 branches in total, exceeding the limit of 2"));
    }

    public void testNestingLevelAtOrBeyondLimit() {
        String flat = """
            FROM test, (FROM test), (FROM languages)
            | STATS c = COUNT(*)
            """;
        planSubquery(flat, Settings.builder().put(QueryPragmas.MAX_BRANCH_LEVEL.getKey(), 1).build());

        String nested = """
            FROM test, (FROM test, (FROM test, (FROM languages)))
            | STATS c = COUNT(*)
            """;
        planSubquery(nested, Settings.builder().put(QueryPragmas.MAX_BRANCH_LEVEL.getKey(), 3).build());
        VerificationException e = expectThrows(
            VerificationException.class,
            () -> planSubquery(nested, Settings.builder().put(QueryPragmas.MAX_BRANCH_LEVEL.getKey(), 2).build())
        );
        assertThat(e.getMessage(), containsString("query resolved to 3 nested union levels, exceeding the limit of 2"));
        assertThat(e.getMessage(), containsString("[max_branch_level] query pragma"));
        assertThat(e.getMessage(), containsString("Reduce the nesting of sources"));
    }

    public void testNestingLevelIgnoresPlansWithoutUnions() {
        planSubquery("""
            FROM test
            | WHERE emp_no > 10000
            """, Settings.builder().put(QueryPragmas.MAX_BRANCH_LEVEL.getKey(), 1).build());
    }

    public void testBothNestedSubqueryLimitsAtOrBeyondLimit() {
        String query = """
            FROM test, (FROM test, (FROM test, (FROM languages)))
            | STATS c = COUNT(*)
            """;
        Settings atLimit = Settings.builder()
            .put(QueryPragmas.MAX_BRANCH_COUNT.getKey(), 4)
            .put(QueryPragmas.MAX_BRANCH_LEVEL.getKey(), 3)
            .build();
        Settings beyondBranchLimit = Settings.builder()
            .put(QueryPragmas.MAX_BRANCH_COUNT.getKey(), 3)
            .put(QueryPragmas.MAX_BRANCH_LEVEL.getKey(), 3)
            .build();
        Settings beyondNestingLimit = Settings.builder()
            .put(QueryPragmas.MAX_BRANCH_COUNT.getKey(), 4)
            .put(QueryPragmas.MAX_BRANCH_LEVEL.getKey(), 2)
            .build();
        Settings beyondBothLimits = Settings.builder()
            .put(QueryPragmas.MAX_BRANCH_COUNT.getKey(), 3)
            .put(QueryPragmas.MAX_BRANCH_LEVEL.getKey(), 2)
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
        String query = """
            FROM test, (FROM test, (FROM languages))
            | WHERE emp_no IN (FROM view_0, view_1 | KEEP emp_no)
            """;
        Settings atLimit = Settings.builder()
            .put(QueryPragmas.MAX_BRANCH_COUNT.getKey(), 3)
            .put(QueryPragmas.MAX_BRANCH_LEVEL.getKey(), 2)
            .build();
        Settings beyondBranchLimit = Settings.builder()
            .put(QueryPragmas.MAX_BRANCH_COUNT.getKey(), 2)
            .put(QueryPragmas.MAX_BRANCH_LEVEL.getKey(), 2)
            .build();
        Settings beyondNestingLimit = Settings.builder()
            .put(QueryPragmas.MAX_BRANCH_COUNT.getKey(), 3)
            .put(QueryPragmas.MAX_BRANCH_LEVEL.getKey(), 1)
            .build();
        Settings beyondBothLimits = Settings.builder()
            .put(QueryPragmas.MAX_BRANCH_COUNT.getKey(), 2)
            .put(QueryPragmas.MAX_BRANCH_LEVEL.getKey(), 1)
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
            .put(QueryPragmas.MAX_BRANCH_COUNT.getKey(), 4)
            .put(QueryPragmas.MAX_BRANCH_LEVEL.getKey(), 2)
            .build();
        Settings beyondBothLimits = Settings.builder()
            .put(QueryPragmas.MAX_BRANCH_COUNT.getKey(), 2)
            .put(QueryPragmas.MAX_BRANCH_LEVEL.getKey(), 1)
            .build();

        planSubquery(query, atLimit);
        VerificationException e = expectThrows(VerificationException.class, () -> planSubquery(query, beyondBothLimits));
        assertThat(e.getMessage(), containsString("Found 4 problems"));
        assertThat(e.getMessage(), containsString("query resolved to 3 branches in total, exceeding the limit of 2"));
        assertThat(e.getMessage(), containsString("query resolved to 4 branches in total, exceeding the limit of 2"));
        assertThat(e.getMessage(), containsString("query resolved to 2 nested union levels, exceeding the limit of 1"));
    }

    public void testTotalBranchCountExceedsMaxBranchCountInClusterSettings() {
        VerificationException e = expectThrows(
            VerificationException.class,
            () -> planSubquery(FOUR_LEAF_THREE_LEVEL, Settings.EMPTY, EsqlFlags.withMaxBranchLimits(3, 5))
        );
        assertThat(
            e.getMessage(),
            containsString(
                "query resolved to 4 branches in total, exceeding the limit of 3 set by the [esql.query.max_branch_count] cluster setting"
            )
        );
    }

    public void testMaxBranchCountPragmaOverridesClusterSettings() {
        // pragma raises the limit above the cluster setting
        planSubquery(
            FOUR_LEAF_THREE_LEVEL,
            Settings.builder().put(QueryPragmas.MAX_BRANCH_COUNT.getKey(), 4).build(),
            EsqlFlags.withMaxBranchLimits(3, 5)
        );
        // pragma lowers the limit below the cluster setting
        VerificationException e = expectThrows(
            VerificationException.class,
            () -> planSubquery(
                FOUR_LEAF_THREE_LEVEL,
                Settings.builder().put(QueryPragmas.MAX_BRANCH_COUNT.getKey(), 2).build(),
                EsqlFlags.withMaxBranchLimits(10, 5)
            )
        );
        assertThat(
            e.getMessage(),
            containsString("query resolved to 4 branches in total, exceeding the limit of 2 set by the [max_branch_count] query pragma")
        );
    }

    public void testNestingLevelExceedsMaxBranchLevelInClusterSettings() {
        VerificationException e = expectThrows(
            VerificationException.class,
            () -> planSubquery(FOUR_LEAF_THREE_LEVEL, Settings.EMPTY, EsqlFlags.withMaxBranchLimits(20, 2))
        );
        assertThat(
            e.getMessage(),
            containsString(
                "query resolved to 3 nested union levels, exceeding the limit of 2 set by the [esql.query.max_branch_level] cluster setting"
            )
        );
    }

    public void testMaxBranchLevelOverridesClusterSettings() {
        // pragma raises the limit above the cluster setting
        planSubquery(
            FOUR_LEAF_THREE_LEVEL,
            Settings.builder().put(QueryPragmas.MAX_BRANCH_LEVEL.getKey(), 3).build(),
            EsqlFlags.withMaxBranchLimits(20, 2)
        );
        // pragma lowers the limit below the cluster setting
        VerificationException e = expectThrows(
            VerificationException.class,
            () -> planSubquery(
                FOUR_LEAF_THREE_LEVEL,
                Settings.builder().put(QueryPragmas.MAX_BRANCH_LEVEL.getKey(), 2).build(),
                EsqlFlags.withMaxBranchLimits(20, 10)
            )
        );
        assertThat(
            e.getMessage(),
            containsString("query resolved to 3 nested union levels, exceeding the limit of 2 set by the [max_branch_level] query pragma")
        );
    }

    public void testTotalBranchCountExceedsMaxBranchCountInClusterSettingsWithView() {
        VerificationException e = expectThrows(
            VerificationException.class,
            () -> planSubquery(viewAnalyzer(), FOUR_LEAF_THREE_LEVEL_VIEWS, Settings.EMPTY, EsqlFlags.withMaxBranchLimits(3, 5))
        );
        assertThat(
            e.getMessage(),
            containsString(
                "query resolved to 4 branches in total, exceeding the limit of 3 set by the [esql.query.max_branch_count] cluster setting"
            )
        );
    }

    public void testMaxBranchCountPragmaOverridesClusterSettingsWithView() {
        planSubquery(
            viewAnalyzer(),
            FOUR_LEAF_THREE_LEVEL_VIEWS,
            Settings.builder().put(QueryPragmas.MAX_BRANCH_COUNT.getKey(), 4).build(),
            EsqlFlags.withMaxBranchLimits(3, 5)
        );
        VerificationException e = expectThrows(
            VerificationException.class,
            () -> planSubquery(
                viewAnalyzer(),
                FOUR_LEAF_THREE_LEVEL_VIEWS,
                Settings.builder().put(QueryPragmas.MAX_BRANCH_COUNT.getKey(), 2).build(),
                EsqlFlags.withMaxBranchLimits(10, 5)
            )
        );
        assertThat(
            e.getMessage(),
            containsString("query resolved to 4 branches in total, exceeding the limit of 2 set by the [max_branch_count] query pragma")
        );
    }

    public void testNestingLevelExceedsMaxBranchLevelInClusterSettingsWithView() {
        VerificationException e = expectThrows(
            VerificationException.class,
            () -> planSubquery(viewAnalyzer(), FOUR_LEAF_THREE_LEVEL_VIEWS, Settings.EMPTY, EsqlFlags.withMaxBranchLimits(20, 2))
        );
        assertThat(
            e.getMessage(),
            containsString(
                "query resolved to 3 nested union levels, exceeding the limit of 2 set by the [esql.query.max_branch_level] cluster setting"
            )
        );
    }

    public void testMaxBranchLevelOverridesClusterSettingsWithView() {
        planSubquery(
            viewAnalyzer(),
            FOUR_LEAF_THREE_LEVEL_VIEWS,
            Settings.builder().put(QueryPragmas.MAX_BRANCH_LEVEL.getKey(), 3).build(),
            EsqlFlags.withMaxBranchLimits(20, 2)
        );
        VerificationException e = expectThrows(
            VerificationException.class,
            () -> planSubquery(
                viewAnalyzer(),
                FOUR_LEAF_THREE_LEVEL_VIEWS,
                Settings.builder().put(QueryPragmas.MAX_BRANCH_LEVEL.getKey(), 2).build(),
                EsqlFlags.withMaxBranchLimits(20, 10)
            )
        );
        assertThat(
            e.getMessage(),
            containsString("query resolved to 3 nested union levels, exceeding the limit of 2 set by the [max_branch_level] query pragma")
        );
    }

    private void assertNestedSubqueryLimits(String query, TestAnalyzer analyzer, int branches, int levels) {
        Settings atLimit = Settings.builder()
            .put(QueryPragmas.MAX_BRANCH_COUNT.getKey(), branches)
            .put(QueryPragmas.MAX_BRANCH_LEVEL.getKey(), levels)
            .build();
        Settings beyondBranchLimit = Settings.builder()
            .put(QueryPragmas.MAX_BRANCH_COUNT.getKey(), branches - 1)
            .put(QueryPragmas.MAX_BRANCH_LEVEL.getKey(), levels)
            .build();
        Settings beyondNestingLimit = Settings.builder()
            .put(QueryPragmas.MAX_BRANCH_COUNT.getKey(), branches)
            .put(QueryPragmas.MAX_BRANCH_LEVEL.getKey(), levels - 1)
            .build();
        Settings beyondBothLimits = Settings.builder()
            .put(QueryPragmas.MAX_BRANCH_COUNT.getKey(), branches - 1)
            .put(QueryPragmas.MAX_BRANCH_LEVEL.getKey(), levels - 1)
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

    private static final String FOUR_LEAF_THREE_LEVEL = """
        FROM test, (FROM test, (FROM test, (FROM languages)))
        | STATS c = COUNT(*)
        """;

    private static final String FOUR_LEAF_THREE_LEVEL_VIEWS = """
        FROM view_0, (FROM view_1, (FROM view_0, (FROM view_1)))
        | STATS c = COUNT(*)
        """;

    private LogicalPlan planSubquery(String query, Settings pragmaSettings) {
        return planSubquery(subqueryAnalyzer(), query, pragmaSettings, null);
    }

    private LogicalPlan planSubquery(String query, Settings pragmaSettings, EsqlFlags flags) {
        return planSubquery(subqueryAnalyzer(), query, pragmaSettings, flags);
    }

    private LogicalPlan planSubquery(TestAnalyzer analyzer, String query, Settings pragmaSettings) {
        return planSubquery(analyzer, query, pragmaSettings, null);
    }

    private LogicalPlan planSubquery(TestAnalyzer analyzer, String query, Settings pragmaSettings, EsqlFlags flags) {
        var configuration = configuration(new QueryPragmas(pragmaSettings), query);
        var context = flags == null
            ? new LogicalOptimizerContext(configuration, logicalOptimizerCtx.foldCtx(), logicalOptimizerCtx.minimumVersion())
            : new LogicalOptimizerContext(configuration, logicalOptimizerCtx.foldCtx(), logicalOptimizerCtx.minimumVersion(), flags);
        return new LogicalPlanOptimizer(context).optimize(analyzer.query(query));
    }

    private TestAnalyzer viewAnalyzer() {
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
