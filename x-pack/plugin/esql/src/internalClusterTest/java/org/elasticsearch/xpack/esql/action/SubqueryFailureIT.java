/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.FailingFieldPlugin;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Negative tests for subqueries in the {@code FROM} command: failures during batched subquery execution are properly propagated,
 * resources are cleaned up, and the correct error is reported.
 */
@ESIntegTestCase.ClusterScope(minNumDataNodes = 2)
public class SubqueryFailureIT extends AbstractEsqlIntegTestCase {

    private static final int FAIL_SHARDS = 1;

    private int okShards;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(FailingFieldPlugin.class);
        plugins.add(InternalExchangePlugin.class);
        return plugins;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(ExchangeService.INACTIVE_SINKS_INTERVAL_SETTING, TimeValue.timeValueMillis(between(3000, 4000)))
            .build();
    }

    @Before
    public void checkPragma() {
        assumeTrue("requires query pragmas", canUseQueryPragmas());
    }

    @Before
    public void setupIndices() throws Exception {
        // Create "fail" index with a runtime field that throws on read
        XContentBuilder mapping = JsonXContent.contentBuilder().startObject();
        mapping.startObject("runtime");
        {
            mapping.startObject("fail_me");
            {
                mapping.field("type", "long");
                mapping.startObject("script").field("source", "").field("lang", "failing_field").endObject();
            }
            mapping.endObject();
        }
        mapping.endObject();
        mapping.startObject("properties");
        {
            mapping.startObject("id").field("type", "integer").endObject();
        }
        mapping.endObject();
        mapping.endObject();
        // One primary so KEEP fail_me can fail every shard of this index. Extra empty shards would count as successful and hide the
        // query-wide all-targets check.
        client().admin()
            .indices()
            .prepareCreate("fail")
            .setSettings(Settings.builder().put("index.number_of_shards", FAIL_SHARDS).put("index.number_of_replicas", 0))
            .setMapping(mapping)
            .get();
        client().prepareBulk()
            .add(new IndexRequest("fail").id("1").source("id", 1))
            .add(new IndexRequest("fail").id("2").source("id", 2))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        // Create "ok" index with normal data
        okShards = randomIntBetween(1, 3);
        client().admin()
            .indices()
            .prepareCreate("ok")
            .setSettings(Settings.builder().put("index.number_of_shards", okShards))
            .setMapping("id", "type=integer", "value", "type=keyword")
            .get();
        client().prepareBulk()
            .add(new IndexRequest("ok").id("1").source("id", 1, "value", "one"))
            .add(new IndexRequest("ok").id("2").source("id", 2, "value", "two"))
            .add(new IndexRequest("ok").id("3").source("id", 3, "value", "three"))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        ensureYellow("fail", "ok");
    }

    /**
     * A single failing subquery — the simplest failure case.
     */
    public void testSingleFailingSubquery() {
        var query = """
            FROM (FROM fail | KEEP fail_me | LIMIT 10)
            | LIMIT 10
            """;
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> run(syncEsqlQueryRequest(query).pragmas(batchPragmas(1))).close()
        );
        assertThat(e.getMessage(), equalTo("Accessing failing field"));
    }

    /**
     * Failure in the first batch (batch 0), subsequent batches should be canceled.
     * batch_size=1: batch 0 = fail, batch 1 = ok, batch 2 = ok.
     */
    public void testFailingSubqueryInFirstBatch() {
        var query = """
            FROM
               (FROM fail | KEEP fail_me | LIMIT 10),
               (FROM ok | WHERE id == 1),
               (FROM ok | WHERE id == 2)
            | LIMIT 100
            """;
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> run(syncEsqlQueryRequest(query).pragmas(batchPragmas(1))).close()
        );
        assertThat(e.getMessage(), equalTo("Accessing failing field"));
    }

    /**
     * First batches succeed, but a later batch fails.
     * batch_size=1: batch 0 = ok, batch 1 = ok, batch 2 = fail.
     * The query should still fail despite earlier batches having produced data.
     */
    public void testFailingSubqueryInLaterBatch() {
        var query = """
            FROM
               (FROM ok | WHERE id == 1),
               (FROM ok | WHERE id == 2),
               (FROM fail | KEEP fail_me | LIMIT 10)
            | LIMIT 100
            """;
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> run(syncEsqlQueryRequest(query).pragmas(batchPragmas(1))).close()
        );
        assertThat(e.getMessage(), equalTo("Accessing failing field"));
    }

    /**
     * Failure concurrent with success within the same batch.
     * batch_size=2: batch 0 = [fail, ok], batch 1 = [ok, ok].
     */
    public void testFailingSubqueryWithinBatch() {
        var query = """
            FROM
               (FROM fail | KEEP fail_me | LIMIT 10),
               (FROM ok | WHERE id == 1),
               (FROM ok | WHERE id == 2),
               (FROM ok | WHERE id == 3)
            | LIMIT 100
            """;
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> run(syncEsqlQueryRequest(query).pragmas(batchPragmas(2))).close()
        );
        assertThat(e.getMessage(), equalTo("Accessing failing field"));
    }

    /**
     * Multiple failures in different batches.
     * batch_size=1: batch 0 = fail, batch 1 = ok, batch 2 = fail, batch 3 = ok.
     * The FailureCollector should prefer the original IllegalStateException over TaskCancelledException.
     */
    public void testMultipleFailuresAcrossBatches() {
        var query = """
            FROM
               (FROM fail | KEEP fail_me | LIMIT 10),
               (FROM ok | WHERE id == 1),
               (FROM fail | KEEP fail_me | LIMIT 10),
               (FROM ok | WHERE id == 2)
            | LIMIT 100
            """;
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> run(syncEsqlQueryRequest(query).pragmas(batchPragmas(1))).close()
        );
        assertThat(e.getMessage(), equalTo("Accessing failing field"));
    }

    /**
     * Every subquery fails.
     * batch_size=1: all three batches fail sequentially.
     */
    public void testAllSubqueriesFail() {
        var query = """
            FROM
               (FROM fail | KEEP fail_me | LIMIT 10),
               (FROM fail | KEEP fail_me | LIMIT 10),
               (FROM fail | KEEP fail_me | LIMIT 10)
            | LIMIT 100
            """;
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> run(syncEsqlQueryRequest(query).pragmas(batchPragmas(1))).close()
        );
        assertThat(e.getMessage(), equalTo("Accessing failing field"));
    }

    /**
     * All subqueries fail with concurrent failures within each batch.
     * batch_size=2: batch 0 = [fail, fail], batch 1 = [fail, fail].
     * Tests batchRemaining counter correctness when both subplans in a batch fail.
     */
    public void testAllSubqueriesFailBatchSizeTwo() {
        var query = """
            FROM
               (FROM fail | KEEP fail_me | LIMIT 10),
               (FROM fail | KEEP fail_me | LIMIT 10),
               (FROM fail | KEEP fail_me | LIMIT 10),
               (FROM fail | KEEP fail_me | LIMIT 10)
            | LIMIT 100
            """;
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> run(syncEsqlQueryRequest(query).pragmas(batchPragmas(2))).close()
        );
        assertThat(e.getMessage(), equalTo("Accessing failing field"));
    }

    /**
     * All subqueries in a single batch (batch_size > subquery count), one fails.
     * Tests the non-recursive single-batch failure path.
     */
    public void testFailingSubqueryAllInOneBatch() {
        var query = """
            FROM
               (FROM ok | WHERE id == 1),
               (FROM ok | WHERE id == 2),
               (FROM fail | KEEP fail_me | LIMIT 10)
            | LIMIT 100
            """;
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> run(syncEsqlQueryRequest(query).pragmas(batchPragmas(10))).close()
        );
        assertThat(e.getMessage(), equalTo("Accessing failing field"));
    }

    /**
     * Randomized batch size with one failing subquery at a random position.
     * Catches edge cases across many runs.
     */
    public void testFailingSubqueryWithRandomBatchSize() {
        int numOkSubqueries = randomIntBetween(2, 5);
        int failPosition = randomIntBetween(0, numOkSubqueries);
        StringBuilder queryBuilder = new StringBuilder("FROM\n");
        for (int i = 0; i <= numOkSubqueries; i++) {
            if (i > 0) {
                queryBuilder.append(",\n");
            }
            if (i == failPosition) {
                queryBuilder.append("   (FROM fail | KEEP fail_me | LIMIT 10)");
            } else {
                queryBuilder.append("   (FROM ok | WHERE id == ").append((i % 3) + 1).append(")");
            }
        }
        queryBuilder.append("\n| LIMIT 100");

        int batchSize = randomIntBetween(1, numOkSubqueries + 1);
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> run(syncEsqlQueryRequest(queryBuilder.toString()).pragmas(batchPragmas(batchSize))).close()
        );
        assertThat(e.getMessage(), equalTo("Accessing failing field"));
    }

    public void testInnerMostFailureWithQueuedNestedSiblings() {
        var query = """
            FROM
               ( FROM ok | WHERE id == 1 ),
               ( FROM
                    ( FROM ok | WHERE id == 2 ),
                    ( FROM
                         ( FROM fail | KEEP fail_me | LIMIT 10 ),
                         ( FROM ok | WHERE id == 3 )
                    ),
                    ( FROM ok | WHERE id == 1 )
               ),
               ( FROM ok | WHERE id == 2 )
            | LIMIT 100
            """;
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> run(syncEsqlQueryRequest(query).pragmas(batchPragmas(1))).close()
        );
        assertThat(e.getMessage(), equalTo("Accessing failing field"));
    }

    /**
     * One subquery reads from both fail and ok indices — the fail shard fails but the ok shard succeeds.
     * With allowPartialResults=true, the overall query succeeds and returns rows from all ok shards
     * across all subqueries.
     */
    public void testPartialResultsWithFailingShardInSubquery() {
        var query = """
            FROM
               (FROM fail,ok | KEEP fail_me | LIMIT 100),
               (FROM ok | WHERE id == 1),
               (FROM ok | WHERE id == 2)
            | LIMIT 100
            """;
        int batchSize = randomIntBetween(1, 3);
        var pragmas = new QueryPragmas(
            Settings.builder()
                .put(QueryPragmas.BRANCH_PARALLEL_DEGREE.getKey(), batchSize)
                .put(QueryPragmas.MAX_CONCURRENT_SHARDS_PER_NODE.getKey(), 1)
                .build()
        );
        var request = executionMetadataRequest(query, pragmas);
        request.allowPartialResults(true);
        try (EsqlQueryResponse resp = run(request)) {
            assertTrue(resp.isPartial());
            List<List<Object>> rows = EsqlTestUtils.getValuesList(resp);
            // subquery 1: ok shard returns 3 docs (fail_me=null), fail shard is swallowed
            // subquery 2: returns 1 doc (id==1)
            // subquery 3: returns 1 doc (id==2)
            // total = 3 + 1 + 1 = 5
            assertThat(rows.size(), equalTo(5));
            // fail,ok once plus two ok-only branches
            assertLocalShardCounts(resp, FAIL_SHARDS + 3 * okShards, 3 * okShards, FAIL_SHARDS);
            assertLocalClusterStatus(resp, EsqlExecutionInfo.Cluster.Status.PARTIAL);
        }
    }

    /**
     * Same scenario as {@link #testPartialResultsWithFailingShardInSubquery}, but the branch with the failing shard sits
     * one level deeper, inside a nested union. The shard failure has to cross two merge levels on its way up: the nested
     * {@code SubPlansExecutor}'s segment, then the outer one. With {@code allowPartialResults} the rows from every ok
     * shard must still arrive, and the response must be marked partial - the flag travels through
     * {@code EsqlExecutionInfo}, not the row stream, so losing it at a merge boundary would silently misreport a
     * partial result as complete.
     */
    public void testPartialResultsWithFailingShardInNestedSubquery() {
        var query = """
            FROM
               (FROM ok | WHERE id == 1),
               (FROM
                    (FROM fail,ok | KEEP fail_me | LIMIT 100),
                    (FROM ok | WHERE id == 2)
               )
            | LIMIT 100
            """;
        var pragmas = new QueryPragmas(
            Settings.builder()
                .put(QueryPragmas.BRANCH_PARALLEL_DEGREE.getKey(), randomIntBetween(1, 3))
                .put(QueryPragmas.MAX_CONCURRENT_SHARDS_PER_NODE.getKey(), 1)
                .build()
        );
        var request = executionMetadataRequest(query, pragmas);
        request.allowPartialResults(true);
        request.acceptedPragmaRisks(true);
        try (EsqlQueryResponse resp = run(request)) {
            assertTrue("a failing shard inside the nested union must mark the response partial", resp.isPartial());
            List<List<Object>> rows = EsqlTestUtils.getValuesList(resp);
            // outer branch: 1 doc (id==1); nested union: 3 docs from the ok shards of fail,ok plus 1 doc (id==2)
            assertThat(rows.size(), equalTo(5));
            assertLocalShardCounts(resp, FAIL_SHARDS + 3 * okShards, 3 * okShards, FAIL_SHARDS);
            assertLocalClusterStatus(resp, EsqlExecutionInfo.Cluster.Status.PARTIAL);
        }
    }

    /**
     * First leaf loses every shard ({@code FROM fail} only). A later sibling still has healthy shards. {@code failIfAllShardsFailed} must
     * not run against the shared {@code EsqlExecutionInfo} at the first leaf — that would cancel the query before {@code ok} executes.
     */
    public void testPartialResultsWhenFirstBranchLosesAllShards() {
        var query = """
            FROM
               (FROM fail | KEEP fail_me | LIMIT 100),
               (FROM ok | WHERE id == 1)
            | LIMIT 100
            """;
        var request = executionMetadataRequest(query, partialBranchPragmas());
        request.allowPartialResults(true);
        try (EsqlQueryResponse resp = run(request)) {
            assertTrue(resp.isPartial());
            List<List<Object>> rows = EsqlTestUtils.getValuesList(resp);
            // fail branch: 0 rows; ok branch: 1 row (id==1)
            assertThat(rows.size(), equalTo(1));
            assertLocalShardCounts(resp, FAIL_SHARDS + okShards, okShards, FAIL_SHARDS);
            assertLocalClusterStatus(resp, EsqlExecutionInfo.Cluster.Status.PARTIAL);
            assertFalse(resp.getExecutionInfo().getCluster(RemoteClusterService.LOCAL_CLUSTER_GROUP_KEY).getFailures().isEmpty());
        }
    }

    /**
     * Healthy branch first (successful shards, zero rows), then a leaf that loses every shard. Shard counts must accumulate so the last
     * writer cannot trip the query-wide all-targets check.
     */
    public void testPartialResultsWhenLaterBranchLosesAllShardsAfterEmptySuccess() {
        var query = """
            FROM
               (FROM ok | WHERE id == 999),
               (FROM fail | KEEP fail_me | LIMIT 100)
            | LIMIT 100
            """;
        var request = executionMetadataRequest(query, partialBranchPragmas());
        request.allowPartialResults(true);
        try (EsqlQueryResponse resp = run(request)) {
            assertTrue(resp.isPartial());
            List<List<Object>> rows = EsqlTestUtils.getValuesList(resp);
            assertThat(rows.size(), equalTo(0));
            assertLocalShardCounts(resp, FAIL_SHARDS + okShards, okShards, FAIL_SHARDS);
            assertLocalClusterStatus(resp, EsqlExecutionInfo.Cluster.Status.PARTIAL);
            assertFalse(resp.getExecutionInfo().getCluster(RemoteClusterService.LOCAL_CLUSTER_GROUP_KEY).getFailures().isEmpty());
        }
    }

    /**
     * Every merge branch lost all of its shards. The root all-targets check must still fail the query even with
     * {@code allowPartialResults}.
     */
    public void testFailsWhenEveryBranchLosesAllShards() {
        var query = """
            FROM
               (FROM fail | KEEP fail_me | LIMIT 100),
               (FROM fail | KEEP fail_me | LIMIT 100)
            | LIMIT 100
            """;
        var request = executionMetadataRequest(query, partialBranchPragmas());
        request.allowPartialResults(true);
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> run(request).close());
        assertThat(e.getMessage(), equalTo("Accessing failing field"));
    }

    /**
     * Two subqueries reference the same index pattern but with different source commands —
     * {@code FROM} (which produces {@link org.elasticsearch.index.IndexMode#STANDARD}) and
     * {@code TS} (which produces {@link org.elasticsearch.index.IndexMode#TIME_SERIES}).
     * {@link org.elasticsearch.xpack.esql.analysis.PreAnalyzer} forbids the same index pattern
     * from appearing twice with different index modes, so the query must be rejected before
     * index resolution / execution.
     */
    public void testFromAndTsSubqueriesOnSameIndexPatternFails() {
        assumeTrue("Requires subquery with TS source support", EsqlCapabilities.Cap.SUBQUERY_WITH_TS.isEnabled());
        var query = """
            FROM
               (FROM ok | KEEP id | LIMIT 10),
               (TS ok | LIMIT 10)
            | LIMIT 100
            """;
        Exception ex = expectThrows(Exception.class, () -> run(syncEsqlQueryRequest(query)).close());
        Throwable cause = ex;
        while (cause != null && (cause.getMessage() == null || cause.getMessage().contains("different index mode") == false)) {
            cause = cause.getCause();
        }
        assertThat(
            "expected PreAnalyzer rejection for conflicting index modes on pattern [ok]",
            cause,
            org.hamcrest.Matchers.notNullValue()
        );
        assertThat(cause.getMessage(), containsString("index pattern 'ok'"));
        assertThat(cause.getMessage(), containsString("time_series"));
        assertThat(cause.getMessage(), containsString("standard"));
    }

    public void testNestedSubqueryExceedsMaxBranchCountPragma() {
        var query = fourLeafThreeLevelQuery();
        var pragmas = new QueryPragmas(Settings.builder().put(QueryPragmas.MAX_BRANCH_COUNT.getKey(), 3).build());
        expectThrows(
            VerificationException.class,
            containsString("query resolved to 4 branches in total, exceeding the limit of 3 set by the [max_branch_count] query pragma"),
            () -> run(syncEsqlQueryRequest(query).pragmas(pragmas)).close()
        );
    }

    /**
     * The branch limit is also applied by the parser at every FORK it builds, before any view, subquery or analysis step walks the
     * plan. The error names the pragma that supplied the limit, like the verifier's does, and reaches the client as a bad request.
     */
    public void testNestedForksExceedMaxBranchCountPragmaWhileParsing() {
        var query = "FROM (FROM ok | FORK (WHERE id > 0) (WHERE id < 0)), ok | FORK (WHERE id > 0) (WHERE id < 0)";
        var pragmas = new QueryPragmas(Settings.builder().put(QueryPragmas.MAX_BRANCH_COUNT.getKey(), 5).build());
        var e = expectThrows(
            VerificationException.class,
            containsString("query resolved to at least 6 branches, exceeding the limit of 5 set by the [max_branch_count] query pragma"),
            () -> run(syncEsqlQueryRequest(query).pragmas(pragmas)).close()
        );
        assertThat(ExceptionsHelper.status(e), equalTo(RestStatus.BAD_REQUEST));
    }

    public void testNestedSubqueryExceedsMaxBranchLevelPragma() {
        var query = fourLeafThreeLevelQuery();
        var pragmas = new QueryPragmas(Settings.builder().put(QueryPragmas.MAX_BRANCH_LEVEL.getKey(), 2).build());
        expectThrows(
            VerificationException.class,
            containsString("query resolved to 3 nested union levels, exceeding the limit of 2 set by the [max_branch_level] query pragma"),
            () -> run(syncEsqlQueryRequest(query).pragmas(pragmas)).close()
        );
    }

    public void testNestedSubqueryExceedsMaxBranchCountClusterSetting() {
        var query = fourLeafThreeLevelQuery();
        try {
            updateClusterSettings(Settings.builder().put(EsqlFlags.ESQL_MAX_BRANCH_COUNT.getKey(), 3));
            expectThrows(
                VerificationException.class,
                containsString(
                    "query resolved to 4 branches in total, "
                        + "exceeding the limit of 3 set by the [esql.query.max_branch_count] cluster setting"
                ),
                () -> run(query).close()
            );
        } finally {
            updateClusterSettings(Settings.builder().putNull(EsqlFlags.ESQL_MAX_BRANCH_COUNT.getKey()));
        }
    }

    public void testNestedSubqueryExceedsMaxBranchLevelClusterSetting() {
        var query = fourLeafThreeLevelQuery();
        try {
            updateClusterSettings(Settings.builder().put(EsqlFlags.ESQL_MAX_BRANCH_LEVEL.getKey(), 2));
            expectThrows(
                VerificationException.class,
                containsString(
                    "query resolved to 3 nested union levels, "
                        + "exceeding the limit of 2 set by the [esql.query.max_branch_level] cluster setting"
                ),
                () -> run(query).close()
            );
        } finally {
            updateClusterSettings(Settings.builder().putNull(EsqlFlags.ESQL_MAX_BRANCH_LEVEL.getKey()));
        }
    }

    public void testPragmaOverridesClusterMaxBranchCount() {
        var query = fourLeafThreeLevelQuery();
        var pragmas = new QueryPragmas(Settings.builder().put(QueryPragmas.MAX_BRANCH_COUNT.getKey(), 4).build());
        try {
            updateClusterSettings(Settings.builder().put(EsqlFlags.ESQL_MAX_BRANCH_COUNT.getKey(), 3));
            try (var resp = run(syncEsqlQueryRequest(query).pragmas(pragmas))) {
                assertThat(resp.values().hasNext(), equalTo(true));
            }
        } finally {
            updateClusterSettings(Settings.builder().putNull(EsqlFlags.ESQL_MAX_BRANCH_COUNT.getKey()));
        }
    }

    /**
     * The parser counts the commands of the main query and its subqueries together; a pragma raising {@code max_command_count}
     * reaches it through the request.
     */
    public void testPragmaRaisesMaxCommandCount() {
        String pipes = " | WHERE id > 0".repeat(300);
        var query = "FROM ok, (FROM ok" + pipes + ")" + pipes;
        expectThrows(
            ParsingException.class,
            containsString("ES|QL statement exceeded the maximum of [500] commands allowed, including the commands in its subqueries"),
            () -> run(query).close()
        );
        var pragmas = new QueryPragmas(Settings.builder().put(QueryPragmas.MAX_COMMAND_COUNT.getKey(), 1000).build());
        try (var resp = run(syncEsqlQueryRequest(query).pragmas(pragmas))) {
            assertThat(EsqlTestUtils.getValuesList(resp).size(), equalTo(6));
        }
    }

    /**
     * FORKs nested through UNION ALL subqueries double the leaf branches at every level, so the parser rejects them against
     * {@code max_branch_count} as it builds each FORK, naming the pragma when the request sets one; raised enough, the query runs.
     */
    public void testPragmaRaisesMaxBranchCountForNestedForks() {
        var query = nestedForks(3) + " | STATS c = COUNT(*)";
        expectThrows(
            VerificationException.class,
            containsString(
                "query resolved to at least 30 branches, exceeding the limit of 20 set by the [esql.query.max_branch_count] cluster setting"
            ),
            () -> run(query).close()
        );
        var tooLow = new QueryPragmas(Settings.builder().put(QueryPragmas.MAX_BRANCH_COUNT.getKey(), 29).build());
        expectThrows(
            VerificationException.class,
            containsString("query resolved to at least 30 branches, exceeding the limit of 29 set by the [max_branch_count] query pragma"),
            () -> run(syncEsqlQueryRequest(query).pragmas(tooLow)).close()
        );
        var enough = new QueryPragmas(Settings.builder().put(QueryPragmas.MAX_BRANCH_COUNT.getKey(), 30).build());
        try (var resp = run(syncEsqlQueryRequest(query).pragmas(enough))) {
            // Only the id > 0 branch keeps rows, and every level adds the 3 rows of ok to the 3 that start the nesting.
            assertThat(EsqlTestUtils.getValuesList(resp), equalTo(List.of(List.of(12L))));
        }
    }

    /** {@code depth} FORKs, each over a UNION ALL of the previous one and {@code ok}: 2^(depth + 2) - 2 leaves. */
    private static String nestedForks(int depth) {
        String query = "FROM ok | FORK (WHERE id > 0) (WHERE id < 0)";
        for (int i = 0; i < depth; i++) {
            query = "FROM (" + query + "), ok | FORK (WHERE id > 0) (WHERE id < 0)";
        }
        return query;
    }

    public void testPragmaOverridesClusterMaxBranchLevel() {
        var query = fourLeafThreeLevelQuery();
        var pragmas = new QueryPragmas(Settings.builder().put(QueryPragmas.MAX_BRANCH_LEVEL.getKey(), 3).build());
        try {
            updateClusterSettings(Settings.builder().put(EsqlFlags.ESQL_MAX_BRANCH_LEVEL.getKey(), 2));
            try (var resp = run(syncEsqlQueryRequest(query).pragmas(pragmas))) {
                assertThat(resp.values().hasNext(), equalTo(true));
            }
        } finally {
            updateClusterSettings(Settings.builder().putNull(EsqlFlags.ESQL_MAX_BRANCH_LEVEL.getKey()));
        }
    }

    /**
     * Four leaf branches nested three merge levels deep. Each inner {@code LIMIT} applies across its union's combined
     * output, so compaction cannot splice the inner branches into the outer union; without them the analyzer flattens
     * the filter-only nesting into a single four-branch union (see
     * {@link #testFilterOnlyNestedSubqueryFlattensBelowBranchLevelLimit}) and the plan never nests. The limits are
     * high enough to let every row through, so tests that execute the query see the same results either way.
     */
    private static String fourLeafThreeLevelQuery() {
        return """
            FROM
               ( FROM ok | WHERE id == 1 ),
               ( FROM
                    ( FROM ok | WHERE id == 2 ),
                    ( FROM
                         ( FROM ok | WHERE id == 3 ),
                         ( FROM ok | WHERE id == 4 )
                    | LIMIT 1000
                    )
               | LIMIT 1000
               )
            | KEEP id
            """;
    }

    /**
     * Filter-only nested subqueries compact into a single flat union during analysis, so a query that syntactically
     * nests three levels deep resolves to one union level and passes a level limit it would have exceeded before
     * compaction. {@link #fourLeafThreeLevelQuery()} blocks that compaction with inner limits when the nesting itself
     * is under test.
     */
    public void testFilterOnlyNestedSubqueryFlattensBelowBranchLevelLimit() {
        var query = """
            FROM
               ( FROM ok | WHERE id == 1 ),
               ( FROM
                    ( FROM ok | WHERE id == 2 ),
                    ( FROM
                         ( FROM ok | WHERE id == 3 ),
                         ( FROM ok | WHERE id == 4 )
                    )
               )
            | KEEP id
            """;
        var pragmas = new QueryPragmas(Settings.builder().put(QueryPragmas.MAX_BRANCH_LEVEL.getKey(), 2).build());
        try (var resp = run(syncEsqlQueryRequest(query).pragmas(pragmas))) {
            // The flattened union must also be equivalent: each branch contributes its rows exactly once.
            // The ok index holds ids 1-3, so the id == 4 branch is a legitimately empty branch in the flat union.
            List<Integer> ids = EsqlTestUtils.getValuesList(resp).stream().map(row -> (Integer) row.get(0)).sorted().toList();
            assertThat(ids, equalTo(List.of(1, 2, 3)));
        }
    }

    private static QueryPragmas batchPragmas(int batchSize) {
        return new QueryPragmas(Settings.builder().put(QueryPragmas.BRANCH_PARALLEL_DEGREE.getKey(), batchSize).build());
    }

    private static EsqlQueryRequest executionMetadataRequest(String query, QueryPragmas pragmas) {
        EsqlQueryRequest request = syncEsqlQueryRequest(query).pragmas(pragmas);
        request.includeExecutionMetadata(true);
        request.acceptedPragmaRisks(true);
        return request;
    }

    /**
     * Serial ({@code 1}) and overlapping ({@code >1}) dispatch both share one {@code EsqlExecutionInfo}; the all-targets check must be
     * correct for either.
     */
    private static QueryPragmas partialBranchPragmas() {
        return new QueryPragmas(
            Settings.builder()
                .put(QueryPragmas.BRANCH_PARALLEL_DEGREE.getKey(), randomIntBetween(1, 3))
                .put(QueryPragmas.MAX_CONCURRENT_SHARDS_PER_NODE.getKey(), 1)
                .build()
        );
    }
}
