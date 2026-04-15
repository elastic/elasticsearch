/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.FailingFieldPlugin;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;

/**
 * Negative tests for subquery batch execution in ComputeService.
 * Verifies that failures during batched subquery execution are properly propagated,
 * resources are cleaned up, and the correct error is reported.
 */
@ESIntegTestCase.ClusterScope(minNumDataNodes = 2)
public class SubqueryFailureIT extends AbstractEsqlIntegTestCase {

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
    public void checkSubqueryInFromCommandSupport() {
        assumeTrue("Requires subquery in FROM command support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
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
        client().admin().indices().prepareCreate("fail").setMapping(mapping).get();
        client().prepareBulk()
            .add(new IndexRequest("fail").id("1").source("id", 1))
            .add(new IndexRequest("fail").id("2").source("id", 2))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        // Create "ok" index with normal data
        client().admin()
            .indices()
            .prepareCreate("ok")
            .setSettings(Settings.builder().put("index.number_of_shards", randomIntBetween(1, 3)))
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
     *
     * TODO: the PARTIAL status is only set on the main plan, the overall cluster status is SUCCESSFUL
     *  even though one shard failed inside a subquery, this should be addressed in a follow up. The
     *  behavior being tested is that the query does NOT fail.
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
        var request = syncEsqlQueryRequest(query).pragmas(pragmas);
        request.allowPartialResults(true);
        request.acceptedPragmaRisks(true);
        try (EsqlQueryResponse resp = run(request)) {
            List<List<Object>> rows = EsqlTestUtils.getValuesList(resp);
            // subquery 1: ok shard returns 3 docs (fail_me=null), fail shard is swallowed
            // subquery 2: returns 1 doc (id==1)
            // subquery 3: returns 1 doc (id==2)
            // total = 3 + 1 + 1 = 5
            assertThat(rows.size(), equalTo(5));
        }
    }

    private static QueryPragmas batchPragmas(int batchSize) {
        return new QueryPragmas(Settings.builder().put(QueryPragmas.BRANCH_PARALLEL_DEGREE.getKey(), batchSize).build());
    }
}
