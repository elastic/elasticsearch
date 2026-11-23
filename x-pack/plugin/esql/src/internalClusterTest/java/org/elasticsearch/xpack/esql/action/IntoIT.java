/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.bulk.BatchIndexingEnabled;
import org.elasticsearch.action.bulk.TransportShardBulkAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.ingest.IngestPipelineTestUtils;
import org.elasticsearch.ingest.common.IngestCommonPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.FailingFieldPlugin;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.plugin.ComputeService;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.hamcrest.Matcher;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

@ESIntegTestCase.ClusterScope(minNumDataNodes = 2)
public class IntoIT extends AbstractEsqlIntegTestCase {

    private static final String DESTINATION = "dest-into";
    private static final String SOURCE = "src-into";
    private static final String FAILING_SOURCE = "src-into-failing";
    private static final String RETRY_SOURCE = "src-into-retry";
    private static final String MAPPING = """
        {
          "dynamic": "strict",
          "properties": {
            "k":  { "type": "keyword" },
            "l":  { "type": "long" },
            "d":  { "type": "double" },
            "b":  { "type": "boolean" },
            "dt": { "type": "date" },
            "ip": { "type": "ip" }
          }
        }
        """;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(
            CollectionUtils.appendToCopy(
                CollectionUtils.appendToCopy(super.nodePlugins(), IngestCommonPlugin.class),
                MockTransportService.TestPlugin.class
            ),
            FailingFieldPlugin.class
        );
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder settings = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        if (BatchIndexingEnabled.FEATURE_FLAG.isEnabled()) {
            // The setting is rejected while the feature flag is off, which would fail the cluster before the tests can be skipped.
            settings.put(BatchIndexingEnabled.BATCH_INDEXING.getKey(), true);
        }
        return settings.build();
    }

    @Before
    public void createDestination() {
        assumeTrue("INTO is only enabled in snapshot builds", EsqlCapabilities.Cap.INTO_COMMAND.isEnabled());
        assumeTrue("INTO writes through batch indexing", BatchIndexingEnabled.FEATURE_FLAG.isEnabled());
        assertAcked(indicesAdmin().prepareCreate(DESTINATION).setSettings(batchIndexingSettings()).setMapping(MAPPING));
    }

    public void testTypes() {
        assertInto(
            "ROW k = \"hello\", l = 42, d = 1.5, b = true, dt = TO_DATETIME(\"2026-01-15T12:34:56.789Z\"), ip = TO_IP(\"192.168.1.1\")",
            1
        );
        assertReadBack(List.of(List.of(true, 1.5, "2026-01-15T12:34:56.789Z", "192.168.1.1", "hello", 42L)));
    }

    public void testRowsAndMultiValues() {
        assertInto("ROW k = [\"alpha\", \"beta\"], l = [1, 2] | MV_EXPAND k", 2);
        assertReadBack("KEEP k, l | SORT k", List.of(List.of("alpha", List.of(1L, 2L)), List.of("beta", List.of(1L, 2L))));
    }

    public void testSourceOmitsNullsAndHasDatesInUtc() {
        assertInto(
            "SET time_zone = \"Europe/Paris\"; ROW k = \"only_k\", l = null::long, dt = TO_DATETIME(\"2026-01-15T12:34:56.789Z\")",
            1
        );
        assertSources(DESTINATION, List.of(Map.of("k", "only_k", "dt", "2026-01-15T12:34:56.789Z")));
    }

    public void testInlineStats() {
        assertInto("ROW k = [\"a\", \"b\"] | MV_EXPAND k | INLINE STATS l = COUNT(*)", 2);
        assertReadBack("KEEP k, l | SORT k", List.of(List.of("a", 2L), List.of("b", 2L)));
    }

    public void testDateMathDestination() {
        String dateMathDest = "<dest-into-{now/d}>";
        String resolvedDest = IndexNameExpressionResolver.resolveDateMathExpression(dateMathDest);
        assertAcked(
            indicesAdmin().prepareCreate(resolvedDest)
                .setSettings(batchIndexingSettings())
                .setMapping("{\"properties\":{\"k\":{\"type\":\"keyword\"}}}")
        );
        assertIntoTarget("ROW k = \"x\"", dateMathDest, 1);
    }

    public void testDateMathDestinationCannotAlsoBeRead() {
        // A fixed date, as now/d could roll over to the next day between the name and the query
        String dateMathDest = "<dest-into-{2026.01.01||/d}>";
        String resolvedDest = IndexNameExpressionResolver.resolveDateMathExpression(dateMathDest);
        assertAcked(
            indicesAdmin().prepareCreate(resolvedDest)
                .setSettings(batchIndexingSettings())
                .setMapping("{\"properties\":{\"k\":{\"type\":\"keyword\"}}}")
        );

        VerificationException e = expectThrows(
            VerificationException.class,
            () -> run("FROM " + resolvedDest + " | INTO \"" + dateMathDest + "\"").close()
        );
        assertThat(
            e.getMessage(),
            containsString("INTO destination [" + dateMathDest + "] is also read by the query, through [" + resolvedDest + "]")
        );
    }

    public void testZeroRows() {
        assertInto("ROW k = \"x\" | WHERE k == \"y\"", 0);
        assertInto("ROW k = \"x\" | WHERE false", 0);
    }

    public void testEvalColumnsAreWritten() {
        assertInto("ROW k = [\"ab\", \"c\"] | MV_EXPAND k | EVAL l = LENGTH(k)", 2);
        assertReadBack("KEEP k, l | SORT k", List.of(List.of("ab", 2L), List.of("c", 1L)));
    }

    public void testExplainIntoDoesNotIndex() {
        try (EsqlQueryResponse response = run("EXPLAIN (ROW k = \"explained\" | INTO " + DESTINATION + ")")) {
            List<String> plans = getValuesList(response).stream().map(row -> (String) row.getLast()).toList();
            assertThat(plans, hasItem(containsString("IntoExec")));
        }
        assertReadBack(List.of());
    }

    public void testRefusedRowFailsTheQuery() {
        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> run("ROW k = \"ok\", unmapped_col = \"nope\" | INTO " + DESTINATION).close()
        );
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(e.getMessage(), containsString("unmapped_col"));
    }

    public void testFromSource() {
        createSource();
        assertInto("FROM " + SOURCE, 3);
        assertReadBack(
            "SORT k",
            List.of(
                List.of(true, 1.5, "2026-01-15T12:34:56.789Z", "192.168.1.1", "a", 1L),
                List.of(false, 2.5, "2026-01-16T12:34:56.789Z", "192.168.1.2", "b", 2L),
                List.of(true, 3.5, "2026-01-17T12:34:56.789Z", "192.168.1.3", "c", 3L)
            )
        );
    }

    /**
     * A destination with an ingest pipeline cannot be written as a pre-built batch, so the query fails.
     */
    public void testIngestPipelineIsRejected() {
        String pipelineDest = createDestinationWithPipeline();

        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> run("ROW k = \"pipelined\" | INTO " + pipelineDest).close()
        );
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(e.getMessage(), containsString("ingest pipelines"));
    }

    public void testFromIngestPipelineIsRejectedBeforeDispatch() {
        createSource();
        String pipelineDest = createDestinationWithPipeline();

        int dataNodeRequests = countDataNodeRequests(() -> {
            ElasticsearchStatusException e = expectThrows(
                ElasticsearchStatusException.class,
                () -> run("FROM " + SOURCE + " | INTO " + pipelineDest).close()
            );
            assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
            assertThat(e.getMessage(), containsString("ingest pipelines"));
        });
        assertThat(dataNodeRequests, equalTo(0));
    }

    public void testAllowPartialResultsRequestedForIntoIsIgnoredWithAWarning() {
        Matcher<Iterable<? super String>> warned = hasItem(containsString("allow_partial_results"));

        assertThat(warningsOf(syncEsqlQueryRequest("ROW k = \"a\" | INTO " + DESTINATION).allowPartialResults(true)), warned);
        assertThat(warningsOf(syncEsqlQueryRequest("ROW k = \"b\" | INTO " + DESTINATION)), not(warned));
        assertThat(warningsOf(syncEsqlQueryRequest("ROW k = \"c\" | INTO " + DESTINATION).allowPartialResults(false)), not(warned));
    }

    public void testDescribeOutputIsWritten() {
        assumeTrue("DESCRIBE is only enabled in snapshot builds", EsqlCapabilities.Cap.DESCRIBE_COMMAND.isEnabled());
        createSource();
        String describeDestination = "dest-describe";
        assertAcked(indicesAdmin().prepareCreate(describeDestination).setSettings(batchIndexingSettings()).setMapping("""
            {
              "dynamic": "strict",
              "properties": {
                "name": { "type": "keyword" },
                "type": { "type": "keyword" },
                "role": { "type": "keyword" },
                "field": { "type": "keyword" },
                "origin": { "type": "keyword" },
                "metric_type": { "type": "keyword" },
                "unit": { "type": "keyword" },
                "interval": { "type": "keyword" },
                "time_zone": { "type": "keyword" },
                "description": { "type": "keyword" }
              }
            }
            """));

        assertIntoTarget("FROM " + SOURCE + " | DESCRIBE", describeDestination, 6);
        refresh(describeDestination);
        try (EsqlQueryResponse response = run("FROM " + describeDestination + " | KEEP name, type | SORT name")) {
            assertThat(
                getValuesList(response),
                equalTo(
                    List.of(
                        List.of("b", "boolean"),
                        List.of("d", "double"),
                        List.of("dt", "date"),
                        List.of("ip", "ip"),
                        List.of("k", "keyword"),
                        List.of("l", "long")
                    )
                )
            );
        }
    }

    public void testFromWithCommandsBeforeInto() {
        createSource();
        assertInto("FROM " + SOURCE + " | WHERE l > 1 | EVAL d = l * 10.0 | KEEP k, d", 2);
        assertReadBack("KEEP k, d, l | SORT k", List.of(Arrays.asList("b", 20.0, null), Arrays.asList("c", 30.0, null)));
    }

    public void testFromSortLimitInto() {
        createSource();
        assertInto("FROM " + SOURCE + " | SORT k | LIMIT 2 | KEEP k", 2);
        assertReadBack("KEEP k | SORT k", List.of(List.of("a"), List.of("b")));
    }

    public void testUnboundedSortBeforeIntoIsRejected() {
        createSource();
        VerificationException e = expectThrows(
            VerificationException.class,
            () -> run("FROM " + SOURCE + " | SORT k | INTO " + DESTINATION).close()
        );
        assertThat(e.getMessage(), containsString("INTO cannot be preceded by an unbounded SORT [SORT k]"));
        assertThat(e.getMessage(), not(containsString("not supported yet")));
    }

    public void testFromStatsInto() {
        createSource();
        assertInto("FROM " + SOURCE + " | STATS l = SUM(l) BY b | EVAL k = TO_STRING(b)", 2);
        assertReadBack("KEEP k, l | SORT k", List.of(List.of("false", 2L), List.of("true", 4L)));
    }

    public void testManyRowsFromManyShardsAreAllWritten() {
        String wide = "x".repeat(4096);
        assertAcked(indicesAdmin().prepareCreate(SOURCE).setSettings(indexSettings(5, 0)).setMapping(MAPPING));
        indexRandom(
            true,
            IntStream.range(0, 2000).mapToObj(i -> client().prepareIndex(SOURCE).setSource("k", wide + i)).collect(Collectors.toList())
        );
        EsqlQueryRequest request = syncEsqlQueryRequest("FROM " + SOURCE + " | KEEP k | INTO " + DESTINATION);
        request.profile(true);
        // Several slices per shard and several drivers, instead of whatever the randomized pragmas pick
        request.pragmas(
            new QueryPragmas(
                Settings.builder()
                    .put(QueryPragmas.TASK_CONCURRENCY.getKey(), 4)
                    .put(QueryPragmas.DATA_PARTITIONING.getKey(), "doc")
                    .put(QueryPragmas.MIN_DOCS_PER_SLICE.getKey(), 100)
                    .build()
            )
        );
        request.acceptedPragmaRisks(true);
        try (EsqlQueryResponse response = run(request)) {
            assertThat(getValuesList(response), equalTo(List.of(List.of(2000L, 2000L))));
            long writers = response.profile()
                .drivers()
                .stream()
                .filter(driver -> driver.description().equals(ComputeService.DATA_DESCRIPTION))
                .filter(driver -> driver.operators().stream().anyMatch(op -> op.operator().contains("IntoOperator")))
                .count();
            assertThat(writers, greaterThan(1L));
        }
        assertReadBack("STATS c = COUNT(*) BY k | STATS groups = COUNT(*), rows = SUM(c)", List.of(List.of(2000L, 2000L)));
    }

    public void testDestinationCannotAlsoBeRead() {
        assertAcked(indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).addAlias(DESTINATION, "dest-alias"));

        for (String source : List.of(DESTINATION, "dest-alias", "dest-into*")) {
            VerificationException e = expectThrows(
                VerificationException.class,
                () -> run("FROM " + source + " | INTO " + DESTINATION).close()
            );
            assertThat(
                e.getMessage(),
                containsString("INTO destination [" + DESTINATION + "] is also read by the query, through [" + source + "]")
            );
        }
    }

    public void testFromShardFailureFailsEvenWithPartialResults() {
        // The failing index has no @timestamp, so it sorts first on its node and no row may be written after it
        String node = clusterService().state().nodes().getDataNodes().values().iterator().next().getName();
        Settings.Builder onNode = indexSettings(1, 0).put("index.routing.allocation.require._name", node);
        assertAcked(indicesAdmin().prepareCreate(FAILING_SOURCE).setSettings(onNode).setMapping("""
            {
              "runtime": {
                "fail_me": { "type": "long", "script": { "source": "", "lang": "failing_field" } }
              },
              "properties": { "k": { "type": "keyword" } }
            }
            """));
        indexRandom(true, client().prepareIndex(FAILING_SOURCE).setSource("k", "boom"));
        createSource(onNode, MAPPING.replace("\"k\":", "\"@timestamp\": { \"type\": \"date\" }, \"k\":"));
        EsqlQueryRequest request = syncEsqlQueryRequest(
            "FROM " + SOURCE + "," + FAILING_SOURCE + " | EVAL l = COALESCE(fail_me, l) | KEEP k, l | INTO " + DESTINATION
        ).allowPartialResults(true)
            .pragmas(new QueryPragmas(Settings.builder().put(QueryPragmas.MAX_CONCURRENT_SHARDS_PER_NODE.getKey(), 1).build()))
            .acceptedPragmaRisks(true);
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> run(request).close());
        assertThat(e.getMessage(), equalTo("Accessing failing field"));
        assertReadBack(List.of());
    }

    public void testShardFailureAfterBulkIsNotRetried() {
        // The destination's strict mapping refuses "extra", so the bulk writes "a" and fails on "b"
        createRetrySource();
        indexRandom(
            true,
            client().prepareIndex(RETRY_SOURCE).setSource("k", "a"),
            client().prepareIndex(RETRY_SOURCE).setSource("k", "b", "extra", "boom")
        );
        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> run("FROM " + RETRY_SOURCE + " | KEEP k, extra | INTO " + DESTINATION).close()
        );
        assertThat(
            e.getMessage(),
            equalTo("INTO [dest-into] failed: [0:0] mapping set to strict, dynamic introduction of [extra] within [_doc] is not allowed")
        );
        assertReadBack("KEEP k", List.of(List.of("a")));
    }

    /**
     * The node fails after it has written a bulk, and before it has returned its stats page, so the coordinator has fetched nothing
     * and the failure looks like one of a read, which could go to the other copy of the shard.
     */
    public void testFailureDuringTheWriteIsNotRetried() {
        createRetrySource();
        String wide = "x".repeat(2048);
        indexRandom(
            true,
            IntStream.range(0, 600).mapToObj(i -> client().prepareIndex(RETRY_SOURCE).setSource("k", wide + i)).collect(Collectors.toList())
        );
        AtomicInteger dataNodeRequests = new AtomicInteger();
        AtomicInteger bulks = new AtomicInteger();
        for (String node : internalCluster().getNodeNames()) {
            MockTransportService service = MockTransportService.getInstance(node);
            service.addRequestHandlingBehavior(ComputeService.DATA_ACTION_NAME, (handler, request, channel, task) -> {
                dataNodeRequests.incrementAndGet();
                handler.messageReceived(request, channel, task);
            });
            service.addRequestHandlingBehavior(TransportShardBulkAction.ACTION_NAME, (handler, request, channel, task) -> {
                if (bulks.incrementAndGet() == 2) {
                    channel.sendResponse(new NoShardAvailableActionException(new ShardId(DESTINATION, "_na_", 0), "simulated"));
                } else {
                    handler.messageReceived(request, channel, task);
                }
            });
        }
        try {
            expectThrows(Exception.class, () -> run("FROM " + RETRY_SOURCE + " | KEEP k | INTO " + DESTINATION).close());
            assertThat("the second bulk was reached", bulks.get(), greaterThan(1));
            assertThat(dataNodeRequests.get(), equalTo(1));
        } finally {
            for (String node : internalCluster().getNodeNames()) {
                MockTransportService.getInstance(node).clearAllRules();
            }
        }
        assertReadBack("STATS c = COUNT(*) BY k | STATS most = MAX(c)", List.of(List.of(1L)));
    }

    public void testAllowPartialResultsDoesNotBypassRowFailure() {
        ElasticsearchStatusException failed = expectThrows(
            ElasticsearchStatusException.class,
            () -> run(syncEsqlQueryRequest("ROW k = \"ok\", unmapped_col = \"nope\" | INTO " + DESTINATION).allowPartialResults(true))
                .close()
        );
        assertThat(failed.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(failed.getMessage(), containsString("unmapped_col"));
    }

    public void testIntoRunsOnCoordinatorWithoutDataNodeRequests() {
        assertThat(countDataNodeRequests(() -> assertInto("ROW k = [\"r1\", \"r2\"] | MV_EXPAND k", 2)), equalTo(0));
        assertReadBack("KEEP k | SORT k", List.of(List.of("r1"), List.of("r2")));
    }

    private int countDataNodeRequests(Runnable action) {
        AtomicInteger dataNodeRequests = new AtomicInteger();
        for (String node : internalCluster().getNodeNames()) {
            MockTransportService.getInstance(node)
                .addRequestHandlingBehavior(ComputeService.DATA_ACTION_NAME, (handler, request, channel, task) -> {
                    dataNodeRequests.incrementAndGet();
                    handler.messageReceived(request, channel, task);
                });
        }
        try {
            action.run();
        } finally {
            for (String node : internalCluster().getNodeNames()) {
                MockTransportService.getInstance(node).clearAllRules();
            }
        }
        return dataNodeRequests.get();
    }

    private void createSource() {
        createSource(indexSettings(between(2, 5), 0), MAPPING);
    }

    private void createSource(Settings.Builder settings, String mapping) {
        assertAcked(indicesAdmin().prepareCreate(SOURCE).setSettings(settings).setMapping(mapping));
        indexRandom(
            true,
            client().prepareIndex(SOURCE)
                .setSource("k", "a", "l", 1, "d", 1.5, "b", true, "dt", "2026-01-15T12:34:56.789Z", "ip", "192.168.1.1"),
            client().prepareIndex(SOURCE)
                .setSource("k", "b", "l", 2, "d", 2.5, "b", false, "dt", "2026-01-16T12:34:56.789Z", "ip", "192.168.1.2"),
            client().prepareIndex(SOURCE)
                .setSource("k", "c", "l", 3, "d", 3.5, "b", true, "dt", "2026-01-17T12:34:56.789Z", "ip", "192.168.1.3")
        );
    }

    private void assertSources(String index, List<Map<String, Object>> expected) {
        refresh(index);
        assertResponse(
            prepareSearch(index),
            r -> assertThat(Arrays.stream(r.getHits().getHits()).map(SearchHit::getSourceAsMap).toList(), equalTo(expected))
        );
    }

    private void createRetrySource() {
        assertAcked(indicesAdmin().prepareCreate(RETRY_SOURCE).setSettings(indexSettings(1, 1)).setMapping("""
            { "properties": { "k": { "type": "keyword" }, "extra": { "type": "keyword" } } }
            """));
        ensureGreen(RETRY_SOURCE);
    }

    private String createDestinationWithPipeline() {
        String pipeline = "into-add-field-pipeline";
        String pipelineDest = "dest-into-with-pipeline";

        IngestPipelineTestUtils.putJsonPipeline(client(), pipeline, """
            {"processors": [{"set": {"field": "pipeline_ran", "value": true}}]}""");

        assertAcked(
            indicesAdmin().prepareCreate(pipelineDest)
                .setSettings(batchIndexingSettings().put("index.default_pipeline", pipeline))
                .setMapping("""
                    {
                      "dynamic": "strict",
                      "properties": {
                        "k":            { "type": "keyword" },
                        "pipeline_ran": { "type": "boolean" }
                      }
                    }""")
        );
        return pipelineDest;
    }

    private List<String> warningsOf(EsqlQueryRequest request) {
        // One client: each call to client() picks a random node, and the warnings are in the thread context of the node that ran it
        Client client = client();
        PlainActionFuture<List<String>> warnings = new PlainActionFuture<>();
        // The framework releases the response after the listener returns, so it is not closed here
        client.execute(EsqlQueryAction.INSTANCE, request, ActionListener.wrap(response -> {
            warnings.onResponse(
                client.threadPool()
                    .getThreadContext()
                    .getResponseHeaders()
                    .getOrDefault("Warning", List.of())
                    .stream()
                    .map(w -> HeaderWarning.extractWarningValueFromWarningHeader(w, false))
                    .toList()
            );
        }, warnings::onFailure));
        return warnings.actionGet(30, TimeUnit.SECONDS);
    }

    private static Settings.Builder batchIndexingSettings() {
        return Settings.builder().put(IndexSettings.BATCH_INDEXING.getKey(), true);
    }

    private void assertInto(String producer, long rows) {
        assertIntoTarget(producer, DESTINATION, rows);
    }

    private void assertIntoTarget(String producer, String target, long rows) {
        try (EsqlQueryResponse response = run(producer + " | INTO " + target)) {
            assertColumnNames(response.columns(), List.of("total", "created"));
            assertColumnTypes(response.columns(), List.of("long", "long"));
            assertThat(getValuesList(response), equalTo(List.of(List.of(rows, rows))));
        }
    }

    private void assertReadBack(List<List<Object>> expected) {
        assertReadBack("", expected);
    }

    private void assertReadBack(String commands, List<List<Object>> expected) {
        refresh(DESTINATION);
        String query = commands.isEmpty() ? "FROM " + DESTINATION : "FROM " + DESTINATION + " | " + commands;
        try (EsqlQueryResponse response = run(query)) {
            assertThat(getValuesList(response), equalTo(expected));
        }
    }
}
