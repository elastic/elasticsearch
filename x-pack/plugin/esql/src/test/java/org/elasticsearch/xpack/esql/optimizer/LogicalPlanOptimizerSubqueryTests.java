/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.cluster.metadata.DataSourceReference;
import org.elasticsearch.cluster.metadata.Dataset;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.DatasetRewriter;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceResolution;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSource;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.configuration;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.logicalOptimizerContext;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.referenceAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

/**
 * Negative tests for FROM subqueries at the logical-optimizer stage; the positive coverage is in
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
        // Three sources; the UnionAll is a merge segment, not a leaf.
        assertNestedSubqueryLimits("""
            FROM test, (FROM test), (FROM languages)
            | STATS c = COUNT(*)
            """, 3, 1);
    }

    public void testTotalBranchCountWithNestedSubqueryAtOrBeyondLimit() {
        assertNestedSubqueryLimits("""
            FROM test, (FROM test, (FROM languages))
            | STATS c = COUNT(*)
            """, 3, 2);
        assertNestedSubqueryLimits("""
            FROM test, (FROM test, (FROM test, (FROM languages)))
            | STATS c = COUNT(*)
            """, 4, 3);
    }

    public void testTotalBranchCountIgnoresPlansWithoutUnions() {
        assertNestedSubqueryLimits("""
            FROM test
            | WHERE emp_no > 10000
            """, 1, 1);
    }

    public void testTotalBranchCountWithForkOnlyAtOrBeyondLimit() {
        assertNestedSubqueryLimits("""
            FROM test
            | FORK (WHERE emp_no > 1) (WHERE emp_no > 2) (WHERE emp_no > 3)
            | STATS c = COUNT(*)
            """, 3, 1);
    }

    public void testTotalBranchCountDoesNotCountLookupJoinAsLeaf() {
        assertNestedSubqueryLimits("""
            FROM test, (FROM test)
            | EVAL language_code = languages
            | LOOKUP JOIN languages_lookup ON language_code
            """, 2, 1);
    }

    public void testTotalBranchCountDoesNotCountEnrichAsLeaf() {
        assertNestedSubqueryLimits("""
            FROM test, (FROM test)
            | ENRICH languages_idx ON first_name
            """, 2, 1);
    }

    public void testTotalBranchCountWithViewAtOrBeyondLimit() {
        assertNestedSubqueryWithViewLimits("FROM view_0, view_1, test", 3, 1);
    }

    public void testNestingLevelAtOrBeyondLimit() {
        assertNestedSubqueryLimits("""
            FROM test, (FROM test), (FROM languages)
            | STATS c = COUNT(*)
            """, 3, 1);
        assertNestedSubqueryLimits("""
            FROM test, (FROM test, (FROM test, (FROM languages)))
            | STATS c = COUNT(*)
            """, 4, 3);
    }

    public void testNestingLevelIgnoresPlansWithoutUnions() {
        assertNestedSubqueryLimits("""
            FROM test
            | WHERE emp_no > 10000
            """, 1, 1);
    }

    public void testBothNestedSubqueryLimitsAtOrBeyondLimit() {
        String query = """
            FROM test, (FROM test, (FROM test, (FROM languages)))
            | STATS c = COUNT(*)
            """;
        assertNestedSubqueryLimits(query, 4, 3);
    }

    public void testBothNestedSubqueryWithViewLimitsAtOrBeyondLimit() {
        String query = """
            FROM test, (FROM test, (FROM languages))
            | WHERE emp_no IN (FROM view_0, view_1 | KEEP emp_no)
            """;
        assertNestedSubqueryWithViewLimits(query, 3, 2);
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
        assertNestedSubqueryLimits(query, 3, 2);
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
        assertNestedSubqueryWithViewLimits(query, 3, 2);
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

    public void testNestedLimitsAgainstClusterSettings() {
        assertNestedLimitsWithClusterSettings(4, 3, (pragmas, flags) -> planSubquery(FOUR_LEAF_THREE_LEVEL, pragmas, flags));
        assertNestedLimitsWithClusterSettings(
            4,
            3,
            (pragmas, flags) -> planSubquery(viewAnalyzer(), FOUR_LEAF_THREE_LEVEL_VIEWS, pragmas, flags)
        );
    }

    public void testQueryPragmaOverridesClusterSettings() {
        assertQueryPragmaOverridesClusterSettings(4, 3, (pragmas, flags) -> planSubquery(FOUR_LEAF_THREE_LEVEL, pragmas, flags));
        assertQueryPragmaOverridesClusterSettings(
            4,
            3,
            (pragmas, flags) -> planSubquery(viewAnalyzer(), FOUR_LEAF_THREE_LEVEL_VIEWS, pragmas, flags)
        );
    }

    // with fork and views

    public void testForkInsideSubquery() {
        String query = """
            FROM (FROM test
                  | FORK (WHERE emp_no > 10000) (WHERE emp_no <= 10000)),
                 (FROM languages | KEEP language_code)
            """;
        assertNestedSubqueryLimits(query, 3, 2);
    }

    public void testForkInsideAndAfterSubquery() {
        String query = """
            FROM (FROM test | FORK (WHERE emp_no > 10000) (WHERE emp_no <= 10000)),
                 (FROM languages | EVAL emp_no = language_code | KEEP emp_no)
            | FORK (WHERE emp_no > 0) (WHERE emp_no <= 0)
            """;
        assertNestedSubqueryLimits(query, 6, 3);
    }

    public void testViewAndSubquery() {
        // query view with subquery inside it.
        assertNestedSubqueryWithViewLimits("FROM subquery_view", 2, 1);
        // query view inside a subquery.
        assertNestedSubqueryWithViewLimits("FROM (FROM subquery_view | LIMIT 10), (FROM test | LIMIT 10)", 3, 2);
        // query nested views
        assertNestedSubqueryWithViewLimits("FROM outer_view", 2, 1);
    }

    public void testViewSubqueryAndFork() {
        // FORK inside a branch of a union that also resolves a view.
        assertNestedSubqueryWithViewLimits("FROM (FROM view_0 | FORK (WHERE emp_no > 0) (WHERE emp_no <= 0)), (FROM test)", 3, 2);
        // A view-created union below FORK.
        assertNestedSubqueryWithViewLimits("FROM view_0, view_1 | FORK (WHERE emp_no > 0) (WHERE emp_no <= 0)", 4, 2);
    }

    public void testDatasetAndFork() {
        assumeTrue("Requires external data source FROM support", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());
        assertNestedDatasetLimits("FROM heavy_a, heavy_b | FORK (WHERE emp_no > 10) (WHERE emp_no <= 10)", datasetAnalyzer(), 4, 2);
    }

    public void testDatasetAndSubquery() {
        assumeTrue("Requires external data source FROM support", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());
        assertNestedDatasetLimits("FROM (FROM heavy_a, heavy_b), (FROM heavy_b) | WHERE emp_no > 10", datasetAnalyzer(), 3, 2);
    }

    public void testDatasetAndView() {
        assumeTrue("Requires external data source FROM support", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());
        assertNestedDatasetLimits(
            "FROM view_datasets | WHERE salary > 1000",
            datasetAnalyzer().addView("view_datasets", "FROM heavy_a, heavy_b"),
            2,
            1
        );
    }

    public void testDatasetViewAndFork() {
        assumeTrue("Requires external data source FROM support", EsqlCapabilities.Cap.DATASET_IN_FROM_COMMAND.isEnabled());
        String query = """
            FROM view_datasets,
                 (FROM heavy_a, heavy_b | WHERE salary > 1000)
            | FORK (WHERE emp_no > 10) (WHERE emp_no <= 10)
            """;
        assertNestedDatasetLimits(query, datasetAnalyzer().addView("view_datasets", "FROM heavy_a, heavy_b"), 8, 3);
    }

    private void assertNestedSubqueryLimits(String query, int branches, int levels) {
        assertNestedLimits(branches, levels, settings -> planSubquery(query, settings));
    }

    private void assertNestedSubqueryWithViewLimits(String query, int branches, int levels) {
        assertNestedLimits(branches, levels, settings -> planView(query, settings));
    }

    private void assertNestedDatasetLimits(String query, TestAnalyzer analyzer, int branches, int levels) {
        assertNestedLimits(branches, levels, settings -> planDataset(analyzer, query, settings));
    }

    private void assertNestedLimits(int branches, int levels, Function<Settings, LogicalPlan> planner) {
        Settings atLimit = Settings.builder()
            .put(QueryPragmas.MAX_BRANCH_COUNT.getKey(), branches)
            .put(QueryPragmas.MAX_BRANCH_LEVEL.getKey(), levels)
            .build();
        planner.apply(atLimit);

        if (branches > 1) {
            Settings beyondBranchLimit = Settings.builder()
                .put(QueryPragmas.MAX_BRANCH_COUNT.getKey(), branches - 1)
                .put(QueryPragmas.MAX_BRANCH_LEVEL.getKey(), levels)
                .build();
            VerificationException e = expectThrows(VerificationException.class, () -> planner.apply(beyondBranchLimit));
            assertThat(
                e.getMessage(),
                containsString("query resolved to " + branches + " branches in total, exceeding the limit of " + (branches - 1))
            );
        }

        if (levels > 1) {
            Settings beyondNestingLimit = Settings.builder()
                .put(QueryPragmas.MAX_BRANCH_COUNT.getKey(), branches)
                .put(QueryPragmas.MAX_BRANCH_LEVEL.getKey(), levels - 1)
                .build();
            VerificationException e = expectThrows(VerificationException.class, () -> planner.apply(beyondNestingLimit));
            assertThat(
                e.getMessage(),
                containsString("query resolved to " + levels + " nested union levels, exceeding the limit of " + (levels - 1))
            );
        }

        if (branches > 1 && levels > 1) {
            Settings beyondBothLimits = Settings.builder()
                .put(QueryPragmas.MAX_BRANCH_COUNT.getKey(), branches - 1)
                .put(QueryPragmas.MAX_BRANCH_LEVEL.getKey(), levels - 1)
                .build();
            VerificationException e = expectThrows(VerificationException.class, () -> planner.apply(beyondBothLimits));
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
    }

    private void assertNestedLimitsWithClusterSettings(int branches, int levels, BiFunction<Settings, EsqlFlags, LogicalPlan> planner) {
        planner.apply(Settings.EMPTY, EsqlFlags.withMaxBranchLimits(branches, levels));

        if (branches > 1) {
            VerificationException e = expectThrows(
                VerificationException.class,
                () -> planner.apply(Settings.EMPTY, EsqlFlags.withMaxBranchLimits(branches - 1, levels))
            );
            assertThat(e.getMessage(), containsString(branchCountExceeded(branches, branches - 1, CLUSTER_BRANCH_COUNT_SOURCE)));
        }

        if (levels > 1) {
            VerificationException e = expectThrows(
                VerificationException.class,
                () -> planner.apply(Settings.EMPTY, EsqlFlags.withMaxBranchLimits(branches, levels - 1))
            );
            assertThat(e.getMessage(), containsString(nestingLevelExceeded(levels, levels - 1, CLUSTER_BRANCH_LEVEL_SOURCE)));
        }

        if (branches > 1 && levels > 1) {
            VerificationException e = expectThrows(
                VerificationException.class,
                () -> planner.apply(Settings.EMPTY, EsqlFlags.withMaxBranchLimits(branches - 1, levels - 1))
            );
            assertThat(e.getMessage(), containsString("Found 2 problems"));
            assertThat(e.getMessage(), containsString(branchCountExceeded(branches, branches - 1, CLUSTER_BRANCH_COUNT_SOURCE)));
            assertThat(e.getMessage(), containsString(nestingLevelExceeded(levels, levels - 1, CLUSTER_BRANCH_LEVEL_SOURCE)));
        }
    }

    private void assertQueryPragmaOverridesClusterSettings(int branches, int levels, BiFunction<Settings, EsqlFlags, LogicalPlan> planner) {
        if (branches > 1) {
            planner.apply(pragmaBranchCount(branches), EsqlFlags.withMaxBranchLimits(branches - 1, levels));
            VerificationException e = expectThrows(
                VerificationException.class,
                () -> planner.apply(pragmaBranchCount(branches - 1), EsqlFlags.withMaxBranchLimits(branches, levels))
            );
            assertThat(e.getMessage(), containsString(branchCountExceeded(branches, branches - 1, PRAGMA_BRANCH_COUNT_SOURCE)));
        }

        if (levels > 1) {
            planner.apply(pragmaBranchLevel(levels), EsqlFlags.withMaxBranchLimits(branches, levels - 1));
            VerificationException e = expectThrows(
                VerificationException.class,
                () -> planner.apply(pragmaBranchLevel(levels - 1), EsqlFlags.withMaxBranchLimits(branches, levels))
            );
            assertThat(e.getMessage(), containsString(nestingLevelExceeded(levels, levels - 1, PRAGMA_BRANCH_LEVEL_SOURCE)));
        }
    }

    private static final String CLUSTER_BRANCH_COUNT_SOURCE = "[" + EsqlFlags.ESQL_MAX_BRANCH_COUNT.getKey() + "] cluster setting";
    private static final String CLUSTER_BRANCH_LEVEL_SOURCE = "[" + EsqlFlags.ESQL_MAX_BRANCH_LEVEL.getKey() + "] cluster setting";
    private static final String PRAGMA_BRANCH_COUNT_SOURCE = "[" + QueryPragmas.MAX_BRANCH_COUNT.getKey() + "] query pragma";
    private static final String PRAGMA_BRANCH_LEVEL_SOURCE = "[" + QueryPragmas.MAX_BRANCH_LEVEL.getKey() + "] query pragma";

    private static Settings pragmaBranchCount(int count) {
        return Settings.builder().put(QueryPragmas.MAX_BRANCH_COUNT.getKey(), count).build();
    }

    private static Settings pragmaBranchLevel(int level) {
        return Settings.builder().put(QueryPragmas.MAX_BRANCH_LEVEL.getKey(), level).build();
    }

    private static String branchCountExceeded(int branches, int limit, String source) {
        return "query resolved to " + branches + " branches in total, exceeding the limit of " + limit + " set by the " + source;
    }

    private static String nestingLevelExceeded(int levels, int limit, String source) {
        return "query resolved to " + levels + " nested union levels, exceeding the limit of " + limit + " set by the " + source;
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

    private LogicalPlan planView(String query, Settings pragmaSettings) {
        return planSubquery(viewAnalyzer(), query, pragmaSettings, null);
    }

    private LogicalPlan planDataset(TestAnalyzer analyzer, String query, Settings pragmaSettings) {
        LogicalPlan rewritten = DatasetRewriter.rewriteUnsecured(
            analyzer.resolveViewsAndInSubqueries(TEST_PARSER.parseQuery(query)),
            heavyDatasetMetadata(),
            TestIndexNameExpressionResolver.newInstance(),
            false
        );
        return optimizeWithPragmas(analyzer.buildAnalyzer().analyze(rewritten), query, pragmaSettings, null);
    }

    private LogicalPlan planSubquery(String query, Settings pragmaSettings, EsqlFlags flags) {
        return planSubquery(subqueryAnalyzer(), query, pragmaSettings, flags);
    }

    private LogicalPlan planSubquery(TestAnalyzer analyzer, String query, Settings pragmaSettings, EsqlFlags flags) {
        return optimizeWithPragmas(analyzer.query(query), query, pragmaSettings, flags);
    }

    private LogicalPlan optimizeWithPragmas(LogicalPlan analyzed, String query, Settings pragmaSettings, EsqlFlags flags) {
        var configuration = configuration(new QueryPragmas(pragmaSettings), query);
        var context = flags == null
            ? logicalOptimizerContext(configuration, logicalOptimizerCtx.foldCtx(), logicalOptimizerCtx.minimumVersion())
            : new LogicalOptimizerContext(configuration, logicalOptimizerCtx.foldCtx(), logicalOptimizerCtx.minimumVersion(), flags);
        return new LogicalPlanOptimizer(context).optimize(analyzed);
    }

    private TestAnalyzer viewAnalyzer() {
        return subqueryAnalyzer().addView("view_0", "FROM test")
            .addView("view_1", "FROM test")
            .addView("subquery_view", "FROM (FROM test | LIMIT 10), (FROM test | LIMIT 20)")
            .addView("outer_view", "FROM subquery_view | LIMIT 5");
    }

    private TestAnalyzer datasetAnalyzer() {
        return analyzer().externalSourceResolution(heavyExternalSourceResolution());
    }

    private static final String HEAVY_A_RESOURCE = "s3://bucket/heavy_a.parquet";
    private static final String HEAVY_B_RESOURCE = "s3://bucket/heavy_b.parquet";

    private static ProjectMetadata heavyDatasetMetadata() {
        DataSource dataSource = new DataSource("heavy_ds", "test", null, Map.of());
        Dataset a = new Dataset("heavy_a", new DataSourceReference("heavy_ds"), HEAVY_A_RESOURCE, null, Map.of());
        Dataset b = new Dataset("heavy_b", new DataSourceReference("heavy_ds"), HEAVY_B_RESOURCE, null, Map.of());
        return ProjectMetadata.builder(ProjectId.DEFAULT)
            .putCustom(DataSourceMetadata.TYPE, new DataSourceMetadata(Map.of("heavy_ds", dataSource)))
            .datasets(Map.of("heavy_a", a, "heavy_b", b))
            .build();
    }

    private static ExternalSourceResolution heavyExternalSourceResolution() {
        return new ExternalSourceResolution(
            Map.of(
                HEAVY_A_RESOURCE,
                new ExternalSourceResolution.ResolvedSource(heavySchema(HEAVY_A_RESOURCE), FileList.UNRESOLVED, Map.of()),
                HEAVY_B_RESOURCE,
                new ExternalSourceResolution.ResolvedSource(heavySchema(HEAVY_B_RESOURCE), FileList.UNRESOLVED, Map.of())
            )
        );
    }

    private static ExternalSourceMetadata heavySchema(String resource) {
        List<Attribute> schema = List.of(
            referenceAttribute("emp_no", DataType.INTEGER),
            referenceAttribute("salary", DataType.INTEGER),
            referenceAttribute("dept", DataType.INTEGER)
        );
        return new ExternalSourceMetadata() {
            @Override
            public String location() {
                return resource;
            }

            @Override
            public List<Attribute> schema() {
                return schema;
            }

            @Override
            public String sourceType() {
                return "parquet";
            }
        };
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
