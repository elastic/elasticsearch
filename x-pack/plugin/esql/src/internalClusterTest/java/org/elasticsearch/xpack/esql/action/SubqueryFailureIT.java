/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.FailingFieldPlugin;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.view.DeleteViewAction;
import org.elasticsearch.xpack.esql.view.PutViewAction;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Negative tests for subqueries in the {@code FROM} command.
 * <ul>
 *     <li>Batch-execution failures: failures during batched subquery execution are properly propagated, resources are cleaned up,
 *         and the correct error is reported.
 *     <li>Analysis-time rejections: a {@code FROM} pattern that matches <b>both a view and a real index</b> (e.g. the wildcard
 *         {@code airports*} matching the view {@code airports_view} and the index {@code airports}) resolves to a {@code ViewUnionAll}
 *         with a view branch and a concrete-index branch. Combined with a sibling subquery this nests a {@code UnionAll} under the
 *         subquery {@code UnionAll}, which the planner rejects with a message that names the offending pattern and its real cause (a
 *         pattern or view that expands to multiple sources) rather than the (misleading) generic "Nested subqueries are not supported".
 * </ul>
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

    /**
     * The main {@code FROM} pattern {@code airports*} matches both the {@code airports_view} view and the {@code airports} index, so it
     * expands to a {@code ViewUnionAll}. Combined with the sibling {@code (FROM employees)} subquery it nests that {@code ViewUnionAll}
     * under the subquery {@code UnionAll}, which is rejected.
     */
    public void testViewAndIndexInMainQueryWithSubquery() {
        assumeViewBranchingSupported();
        setupWildcardMatchingViewAndIndices();
        try {
            expectThrows(
                VerificationException.class,
                containsString(
                    "a pattern that expands to multiple sources, [FROM airports*, (FROM employees)], cannot be combined with subqueries"
                ),
                () -> run("FROM airports*, (FROM employees)").close()
            );
        } finally {
            deleteViews("airports_view");
        }
    }

    /**
     * A subquery whose body pattern {@code airports*} matches both the view and the real index expands to a {@code ViewUnionAll} inside
     * the subquery, nesting it under the top-level {@code UnionAll}, which is rejected.
     */
    public void testViewAndIndexInsideSubquery() {
        assumeViewBranchingSupported();
        setupWildcardMatchingViewAndIndices();
        try {
            expectThrows(
                VerificationException.class,
                containsString("a pattern that expands to multiple sources, [FROM airports*], cannot be combined with subqueries"),
                () -> run("FROM employees, (FROM airports*)").close()
            );
        } finally {
            deleteViews("airports_view");
        }
    }

    /**
     * One of several sibling subqueries uses the pattern {@code airports*} that matches both the view and the real index; the resulting
     * {@code ViewUnionAll} is nested inside that subquery, below the top-level {@code UnionAll}, which is rejected.
     */
    public void testViewAndIndexInOneOfMultipleSubqueries() {
        assumeViewBranchingSupported();
        setupWildcardMatchingViewAndIndices();
        try {
            expectThrows(
                VerificationException.class,
                containsString("a pattern that expands to multiple sources, [FROM airports*], cannot be combined with subqueries"),
                () -> run("FROM (FROM airports*), (FROM employees)").close()
            );
        } finally {
            deleteViews("airports_view");
        }
    }

    private static void assumeViewBranchingSupported() {
        assumeTrue("Requires views in cluster state", EsqlCapabilities.Cap.VIEWS_IN_CLUSTER_STATE.isEnabled());
        assumeTrue("Requires views with branching", EsqlCapabilities.Cap.VIEWS_WITH_BRANCHING.isEnabled());
    }

    /**
     * Creates the {@code airports} index and an {@code airports_view} view (both matched by the wildcard {@code airports*}), plus an
     * {@code employees} index used as the sibling relation. The view body carries a processing command ({@code LIMIT}) so it is kept as
     * a named view branch rather than compacted into the concrete index — this is what makes {@code airports*} expand to a branching
     * {@code ViewUnionAll} of the view and the real index. The two indices share the same mapping so the top-level {@code UnionAll} has
     * no column-type conflicts that would fail verification before the nested-subquery check.
     */
    private void setupWildcardMatchingViewAndIndices() {
        client().admin().indices().prepareCreate("airports").setMapping("id", "type=integer", "name", "type=keyword").get();
        client().prepareBulk()
            .add(new IndexRequest("airports").id("1").source("id", 1, "name", "a"))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        client().admin().indices().prepareCreate("employees").setMapping("id", "type=integer", "name", "type=keyword").get();
        client().prepareBulk()
            .add(new IndexRequest("employees").id("1").source("id", 1, "name", "e"))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        ensureYellow("airports", "employees");
        installView("airports_view", "FROM airports | LIMIT 10");
    }

    private static void installView(String name, String query) {
        assertAcked(
            client().execute(
                PutViewAction.INSTANCE,
                new PutViewAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, new View(name, query))
            )
        );
    }

    private static void deleteViews(String... names) {
        for (String name : names) {
            assertAcked(
                client().execute(
                    DeleteViewAction.INSTANCE,
                    new DeleteViewAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, new String[] { name })
                )
            );
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

    private static void assertLocalShardCounts(EsqlQueryResponse resp, int total, int successful, int failed) {
        EsqlExecutionInfo.Cluster local = resp.getExecutionInfo().getCluster(RemoteClusterService.LOCAL_CLUSTER_GROUP_KEY);
        assertNotNull(local);
        assertThat("total shards", local.getTotalShards(), equalTo(total));
        assertThat("successful shards", local.getSuccessfulShards(), equalTo(successful));
        assertThat("failed shards", local.getFailedShards(), equalTo(failed));
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
