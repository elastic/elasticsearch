/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BatchIndexingEnabled;
import org.elasticsearch.action.bulk.TransportShardBulkAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexingPressure;
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
import org.elasticsearch.xpack.esql.into.IntoOptions.Collisions;
import org.elasticsearch.xpack.esql.into.IntoStats;
import org.elasticsearch.xpack.esql.plugin.ComputeService;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.hamcrest.Matcher;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.singleValue;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

@ESIntegTestCase.ClusterScope(minNumDataNodes = 2)
public class IntoIT extends AbstractEsqlIntegTestCase {

    private static final String DESTINATION = "dest-into";
    private static final String SOURCE = "src-into";
    private static final String FAILING_SOURCE = "src-into-failing";
    private static final String RETRY_SOURCE = "src-into-retry";
    private static final String DATA_STREAM_PREFIX = "into-ds-";
    private static final String DATA_STREAM = DATA_STREAM_PREFIX + "logs";
    private static final String UNMAPPED_COL_REFUSAL =
        "[400] [0:0] mapping set to strict, dynamic introduction of [unmapped_col] within [_doc] is not allowed";
    private static final String COLLIDING_ROWS = "ROW _id = [\"a\", \"b\"], k = \"new\" | MV_EXPAND _id";
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
    @SuppressWarnings("unchecked")
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopyNoNullElements(
            super.nodePlugins(),
            IngestCommonPlugin.class,
            MockTransportService.TestPlugin.class,
            FailingFieldPlugin.class,
            DataStreamsPlugin.class
        );
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder settings = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        if (BatchIndexingEnabled.FEATURE_FLAG.isEnabled()) {
            // The setting is rejected while the feature flag is off, which would fail the cluster before the tests can be skipped.
            settings.put(BatchIndexingEnabled.BATCH_INDEXING.getKey(), true);
        }
        // The size of the bulks that INTO sends comes from these settings, which the test cluster randomizes: with the defaults, a few
        // hundred rows go in one 4MB bulk, and tests that fail or count the bulks of a write have no second bulk to work on. Pin the
        // small sizes, so that they always get several.
        settings.put(IndexingPressure.SPLIT_BULK_LOW_WATERMARK.getKey(), "512B");
        settings.put(IndexingPressure.SPLIT_BULK_LOW_WATERMARK_SIZE.getKey(), "1KB");
        settings.put(IndexingPressure.SPLIT_BULK_HIGH_WATERMARK.getKey(), "16KB");
        settings.put(IndexingPressure.SPLIT_BULK_HIGH_WATERMARK_SIZE.getKey(), "256B");
        return settings.build();
    }

    @Before
    public void createDestination() {
        assumeTrue("INTO command required", EsqlCapabilities.Cap.INTO_COMMAND.isEnabled());
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
            containsString("INTO destination [" + resolvedDest + "] is also read by the query, through [" + resolvedDest + "]")
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

    /** An EXPLAIN never writes, so a destination that is not there is fine, and the EXPLAIN does not create it. */
    public void testExplainIntoMissingDestinationDoesNotCreateIt() {
        String missing = "dest-into-missing";
        try (EsqlQueryResponse response = run("EXPLAIN (ROW k = \"explained\" | INTO " + missing + ")")) {
            List<String> plans = getValuesList(response).stream().map(row -> (String) row.getLast()).toList();
            assertThat(plans, hasItem(containsString("IntoExec")));
        }
        assertFalse(indexExists(missing));
    }

    public void testRefusedRowFailsTheQuery() {
        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> run("ROW k = \"ok\", unmapped_col = \"nope\" | INTO " + DESTINATION).close()
        );
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(e.getMessage(), containsString("unmapped_col"));
    }

    public void testKeepGoingSkipsRefusedRows() {
        String query = Strings.format("""
            ROW k = ["ok1", "bad", "ok2"]
            | MV_EXPAND k
            | EVAL unmapped_col = CASE(k == "bad", "nope", null)
            | INTO %s WITH {"keep_going": true}
            """, DESTINATION);
        try (EsqlQueryResponse response = run(query)) {
            assertIntoColumns(response);
            List<Object> summary = singleValue(getValuesList(response));
            assertThat(summary, hasSize(6));
            assertThat(summary.subList(0, 5), equalTo(List.of(3L, 2L, 0L, 0L, 1L)));
            assertThat(summary.get(5), equalTo(UNMAPPED_COL_REFUSAL));
        }
        assertReadBack("KEEP k | SORT k", List.of(List.of("ok1"), List.of("ok2")));
    }

    public void testKeepGoingFromSourceCountsEveryRefusal() {
        createSource();
        try (var response = run("FROM " + SOURCE + " | EVAL unmapped_col = k | INTO " + DESTINATION + " WITH {\"keep_going\": true}")) {
            List<Object> summary = getValuesList(response).getFirst();
            assertThat(summary, hasSize(6));
            assertThat(summary.subList(0, 5), equalTo(List.of(3L, 0L, 0L, 0L, 3L)));
            assertThat(summary.get(5), equalTo(Collections.nCopies(3, UNMAPPED_COL_REFUSAL)));
        }
        assertReadBack(List.of());
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
        try (EsqlQueryResponse response = run(requestWithManyWriters("FROM " + SOURCE + " | KEEP k | INTO " + DESTINATION))) {
            assertThat(getValuesList(response), equalTo(List.of(Arrays.asList(2000L, 2000L, 0L, 0L, 0L, null))));
            assertThat(countWriters(response), greaterThan(1L));
        }
        assertReadBack("STATS c = COUNT(*) BY k | STATS groups = COUNT(*), rows = SUM(c)", List.of(List.of(2000L, 2000L)));
    }

    /**
     * Writers on several nodes each create some rows and refuse others, which is where the stats of the writers meet: the counts add up
     * exactly, the details stop at the cap however many each writer reports, and every accepted row is there.
     */
    public void testKeepGoingAddsUpTheStatsOfEveryWriter() {
        int rows = 600;
        // No node holds more than three of the five shards, so the source is spread over at least two nodes
        assertAcked(
            indicesAdmin().prepareCreate(SOURCE)
                .setSettings(indexSettings(5, 0).put("index.routing.allocation.total_shards_per_node", 3))
                .setMapping(MAPPING)
        );
        indexRandom(
            true,
            IntStream.range(0, rows)
                .mapToObj(i -> client().prepareIndex(SOURCE).setSource("k", "k" + i, "l", i))
                .collect(Collectors.toList())
        );
        // The rows with an even l are refused, since the mapping of the destination has no unmapped_col
        String query = "FROM "
            + SOURCE
            + " | KEEP k, l | EVAL unmapped_col = CASE(l % 2 == 0, \"nope\", null) | INTO "
            + DESTINATION
            + " WITH {\"keep_going\": true}";
        try (EsqlQueryResponse response = run(requestWithManyWriters(query))) {
            List<Object> summary = singleValue(getValuesList(response));
            assertThat(summary, hasSize(6));
            assertThat(summary.subList(0, 5), equalTo(List.of(600L, 300L, 0L, 0L, 300L)));
            assertThat(summary.get(5), equalTo(Collections.nCopies(IntoStats.MAX_FAILURE_DETAILS, UNMAPPED_COL_REFUSAL)));
            // Writers on one node share their stats, so the stats of several nodes are only merged if writers ran on several nodes
            assertThat(writerNodes(response), hasSize(greaterThan(1)));
        }
        assertReadBack("STATS c = COUNT(*), s = SUM(l)", List.of(List.of(300L, 90_000L)));
    }

    /** A request with several slices per shard and several drivers, instead of whatever the randomized pragmas pick. */
    private static EsqlQueryRequest requestWithManyWriters(String query) {
        EsqlQueryRequest request = syncEsqlQueryRequest(query);
        request.profile(true);
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
        return request;
    }

    private static Set<String> writerNodes(EsqlQueryResponse response) {
        return response.profile()
            .drivers()
            .stream()
            .filter(driver -> driver.description().equals(ComputeService.DATA_DESCRIPTION))
            .filter(driver -> driver.operators().stream().anyMatch(op -> op.operator().contains("IntoOperator")))
            .map(driver -> driver.nodeName())
            .collect(Collectors.toSet());
    }

    private static long countWriters(EsqlQueryResponse response) {
        return response.profile()
            .drivers()
            .stream()
            .filter(driver -> driver.description().equals(ComputeService.DATA_DESCRIPTION))
            .filter(driver -> driver.operators().stream().anyMatch(op -> op.operator().contains("IntoOperator")))
            .count();
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
            // The writer takes a whole page before it looks at the size of its bulk, so all rows in one page are one bulk, and there
            // would be no second one to fail. The randomized pragmas can pick a page of any size, so pick a small one.
            EsqlQueryRequest request = syncEsqlQueryRequest("FROM " + RETRY_SOURCE + " | KEEP k | INTO " + DESTINATION);
            request.pragmas(new QueryPragmas(Settings.builder().put(QueryPragmas.PAGE_SIZE.getKey(), 10).build()));
            request.acceptedPragmaRisks(true);
            expectThrows(Exception.class, () -> run(request).close());
            assertThat("the second bulk was reached", bulks.get(), greaterThan(1));
            assertThat(dataNodeRequests.get(), equalTo(1));
        } finally {
            for (String node : internalCluster().getNodeNames()) {
                MockTransportService.getInstance(node).clearAllRules();
            }
        }
        assertReadBack("STATS c = COUNT(*) BY k | STATS most = MAX(c)", List.of(List.of(1L)));
    }

    /**
     * The destination passes the coordinator's check and then gets an ingest pipeline, before a data node plans the write. The coordinator
     * is the one that authorizes the caller, so the data node must not tell the caller what is wrong with the destination. A single shard
     * means a single data node request, which is held until the destination has changed.
     */
    public void testDestinationThatChangesBeforeADataNodePlansTheWriteFailsWithoutSayingWhy() throws Exception {
        createSource(indexSettings(1, 0), MAPPING);
        indexRandom(true, client().prepareIndex(SOURCE).setSource("k", "a"));
        CountDownLatch requestArrived = new CountDownLatch(1);
        CountDownLatch destinationChanged = new CountDownLatch(1);
        for (String node : internalCluster().getNodeNames()) {
            MockTransportService.getInstance(node)
                .addRequestHandlingBehavior(ComputeService.DATA_ACTION_NAME, (handler, request, channel, task) -> {
                    requestArrived.countDown();
                    destinationChanged.await();
                    handler.messageReceived(request, channel, task);
                });
        }
        try {
            // The query is expected to fail. The listener only borrows a response, which the action releases afterwards.
            PlainActionFuture<Exception> outcome = new PlainActionFuture<>();
            client().execute(
                EsqlQueryAction.INSTANCE,
                syncEsqlQueryRequest("FROM " + SOURCE + " | KEEP k | INTO " + DESTINATION).pragmas(getPragmas()),
                ActionListener.wrap(response -> outcome.onResponse(null), outcome::onResponse)
            );
            try {
                safeAwait(requestArrived);
                assertAcked(
                    indicesAdmin().prepareUpdateSettings(DESTINATION)
                        .setSettings(Settings.builder().put(IndexSettings.DEFAULT_PIPELINE.getKey(), "some-pipeline"))
                );
            } finally {
                destinationChanged.countDown();
            }
            Exception failure = safeGet(outcome);
            assertNotNull("the query was expected to fail", failure);
            ElasticsearchStatusException e = (ElasticsearchStatusException) ExceptionsHelper.unwrapCause(failure);
            assertThat(e.status(), equalTo(RestStatus.CONFLICT));
            assertThat(e.getMessage(), not(containsString("pipeline")));
        } finally {
            for (String node : internalCluster().getNodeNames()) {
                MockTransportService.getInstance(node).clearAllRules();
            }
        }
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

    public void testRefreshByDefaultMakesRowsVisible() {
        disableDestinationRefresh();
        assertInto("ROW k = [\"a\", \"b\"] | MV_EXPAND k", 2);
        assertThat(countDestinationDocuments(), equalTo(2L));
    }

    public void testRefreshByDefaultMakesRowsFromSourceVisible() {
        createSource();
        disableDestinationRefresh();
        assertInto("FROM " + SOURCE, 3);
        assertThat(countDestinationDocuments(), equalTo(3L));
    }

    public void testRefreshFalseLeavesRowsInvisibleUntilRefreshed() {
        disableDestinationRefresh();
        assertRefreshFalseLeavesThreeRowsInvisibleUntilRefreshed("ROW k = [\"a\", \"b\", \"c\"] | MV_EXPAND k");
    }

    public void testRefreshFalseLeavesRowsFromSourceInvisibleUntilRefreshed() {
        createSource();
        disableDestinationRefresh();
        assertRefreshFalseLeavesThreeRowsInvisibleUntilRefreshed("FROM " + SOURCE);
    }

    public void testRefreshSkipsAClosedNonWriteMemberOfTheDestinationAlias() {
        String older = "dest-into-older";
        String alias = "dest-into-alias";
        assertAcked(indicesAdmin().prepareCreate(older).setSettings(batchIndexingSettings()).setMapping(MAPPING));
        assertAcked(
            indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                .addAliasAction(IndicesAliasesRequest.AliasActions.add().index(older).alias(alias).writeIndex(false))
                .addAliasAction(IndicesAliasesRequest.AliasActions.add().index(DESTINATION).alias(alias).writeIndex(true))
        );
        assertAcked(indicesAdmin().prepareClose(older));
        disableDestinationRefresh();

        assertIntoTarget("ROW k = [\"a\", \"b\"] | MV_EXPAND k", alias, 2);
        assertThat("the write index was refreshed", countDestinationDocuments(), equalTo(2L));
    }

    public void testIntoRunsOnCoordinatorWithoutDataNodeRequests() {
        assertThat(countDataNodeRequests(() -> assertInto("ROW k = [\"r1\", \"r2\"] | MV_EXPAND k", 2)), equalTo(0));
        assertReadBack("KEEP k | SORT k", List.of(List.of("r1"), List.of("r2")));
    }

    public void testCopyingTwiceWithKeepCountsEveryDocumentAsAConflict() {
        createSource();
        String copy = "FROM "
            + SOURCE
            + " METADATA _id | KEEP _id, k | INTO "
            + DESTINATION
            + " WITH {\"collisions\": \"keep\", \"refresh\": true}";
        try (EsqlQueryResponse response = run(copy)) {
            assertStats(response, IntoStats.builder().total(3).created(3).build());
        }
        try (EsqlQueryResponse response = run(copy)) {
            assertStats(response, IntoStats.builder().total(3).versionConflicts(3).build());
        }
        assertReadBack("KEEP k | SORT k", List.of(List.of("a"), List.of("b"), List.of("c")));
    }

    public void testCollisionsAbortFailsAndLeavesTheExistingDocument() {
        indexExistingDocument();
        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> run(COLLIDING_ROWS + " | INTO " + DESTINATION + " WITH {\"collisions\": \"abort\"}").close()
        );
        assertThat(e.status(), equalTo(RestStatus.CONFLICT));
        assertThat(
            e.getMessage(),
            equalTo(
                "INTO ["
                    + DESTINATION
                    + "] failed: [a]: version conflict, document already exists (current version [1]); "
                    + "to handle documents that already exist, use \"collisions\": \"keep\", "
                    + "or \"collisions\": \"replace\" (not supported on a data stream)"
            )
        );
        assertReadBackWithIds("WHERE _id == \"a\" | KEEP k, l", List.of(List.of("old", 1L)));
    }

    public void testCollisionsKeepCountsConflictsAndLeavesTheExistingDocument() {
        indexExistingDocument();
        try (EsqlQueryResponse response = run(COLLIDING_ROWS + " | INTO " + DESTINATION + " WITH {\"collisions\": \"keep\"}")) {
            assertStats(response, IntoStats.builder().total(2).created(1).versionConflicts(1).build());
        }
        assertReadBackWithIds("KEEP _id, k, l | SORT _id", List.of(Arrays.asList("a", "old", 1L), Arrays.asList("b", "new", null)));
    }

    public void testCollisionsReplaceOverwritesTheWholeDocument() {
        indexExistingDocument();
        try (EsqlQueryResponse response = run(COLLIDING_ROWS + " | INTO " + DESTINATION + " WITH {\"collisions\": \"REPLACE\"}")) {
            assertStats(response, IntoStats.builder().total(2).created(1).updated(1).build());
        }
        // l was only on the old document, so a replace drops it
        assertReadBackWithIds("KEEP _id, k, l | SORT _id", List.of(Arrays.asList("a", "new", null), Arrays.asList("b", "new", null)));
    }

    public void testNullIdLeavesTheIdToTheDestinationUnderEveryPolicy() {
        for (Collisions collisions : Collisions.values()) {
            String name = collisions.optionValue();
            try (
                var response = run(
                    "ROW _id = null, k = \"" + name + "\" | INTO " + DESTINATION + " WITH {\"collisions\": \"" + name + "\"}"
                )
            ) {
                assertStats(response, IntoStats.builder().total(1).created(1).build());
            }
        }
        assertReadBack("KEEP k | SORT k", List.of(List.of("abort"), List.of("keep"), List.of("replace")));
    }

    private static final String ROWS_WITH_INVALID_IDS = """
        ROW x = [1, 2, 3, 4]
        | MV_EXPAND x
        | EVAL _id = CASE(x == 2, "", x == 3, REPEAT("x", 513), CONCAT("id", TO_STRING(x))), k = CONCAT("k", TO_STRING(x))
        | KEEP _id, k
        """;

    /** An empty or over-long {@code _id} is refused on its own, and does not take the valid rows around it with it. */
    public void testKeepGoingRefusesOnlyTheRowsWithAnInvalidId() {
        try (var response = run(ROWS_WITH_INVALID_IDS + "| INTO " + DESTINATION + " WITH {\"keep_going\": true}")) {
            IntoStats expected = IntoStats.builder()
                .total(4)
                .created(2)
                .failures(2)
                .failureDetails(
                    List.of(
                        "[400] if _id is specified it must not be empty",
                        "[400] _id is too long, must be no longer than 512 bytes but was: 513"
                    )
                )
                .build();
            assertStats(response, expected);
        }
        assertReadBackWithIds("KEEP _id, k | SORT _id", List.of(List.of("id1", "k1"), List.of("id4", "k4")));
    }

    public void testInvalidIdFailsTheQueryWithoutKeepGoing() {
        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> run(ROWS_WITH_INVALID_IDS + "| INTO " + DESTINATION + " WITH {\"keep_going\": false}").close()
        );
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(e.getMessage(), equalTo("INTO [" + DESTINATION + "] failed: if _id is specified it must not be empty"));
        assertReadBack(List.of());
    }

    public void testReplaceOnAnExistingDataStreamIsRejected() {
        createDataStream();
        VerificationException e = expectThrows(
            VerificationException.class,
            () -> run("ROW k = \"y\" | INTO " + DATA_STREAM + " WITH {\"collisions\": \"replace\"}").close()
        );
        assertThat(e.getMessage(), containsString("INTO collisions [replace] is not supported on data stream [" + DATA_STREAM + "]"));
    }

    public void testKeepOnADataStreamCountsConflicts() {
        createDataStream();
        String query = "ROW @timestamp = TO_DATETIME(\"2026-01-01T00:00:00Z\"), _id = \"a\", k = \"x\" | INTO "
            + DATA_STREAM
            + " WITH {\"collisions\": \"keep\"}";
        try (EsqlQueryResponse response = run(query)) {
            assertStats(response, IntoStats.builder().total(1).created(1).build());
        }
        try (EsqlQueryResponse response = run(query)) {
            assertStats(response, IntoStats.builder().total(1).created(0).versionConflicts(1).build());
        }
    }

    private void disableDestinationRefresh() {
        assertAcked(indicesAdmin().prepareUpdateSettings(DESTINATION).setSettings(Settings.builder().put("index.refresh_interval", -1)));
    }

    private void assertRefreshFalseLeavesThreeRowsInvisibleUntilRefreshed(String producer) {
        try (EsqlQueryResponse response = run(producer + " | INTO " + DESTINATION + " WITH {\"refresh\": false}")) {
            assertStats(response, IntoStats.builder().total(3).created(3).build());
        }
        assertThat(countDestinationDocuments(), equalTo(0L));
        refresh(DESTINATION);
        assertThat(countDestinationDocuments(), equalTo(3L));
    }

    private long countDestinationDocuments() {
        try (EsqlQueryResponse response = run("FROM " + DESTINATION + " | STATS c = COUNT(*)")) {
            return (long) getValuesList(response).getFirst().getFirst();
        }
    }

    private void indexExistingDocument() {
        indexRandom(true, client().prepareIndex(DESTINATION).setId("a").setSource("k", "old", "l", 1));
    }

    private void createDataStream() {
        putDataStreamTemplate();
        client().prepareIndex(DATA_STREAM)
            .setCreate(true)
            .setSource("@timestamp", "2025-12-31T00:00:00Z", "k", "seed")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
    }

    private void putDataStreamTemplate() {
        assertAcked(
            client().execute(
                TransportPutComposableIndexTemplateAction.TYPE,
                new TransportPutComposableIndexTemplateAction.Request("into-ds-template").indexTemplate(
                    ComposableIndexTemplate.builder()
                        .indexPatterns(List.of(DATA_STREAM_PREFIX + "*"))
                        .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                        .build()
                )
            )
        );
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
            assertStats(response, IntoStats.builder().total(rows).created(rows).build());
        }
    }

    private static void assertIntoColumns(EsqlQueryResponse response) {
        assertColumnNames(response.columns(), List.of("total", "created", "updated", "version_conflicts", "failures", "failure_details"));
        assertColumnTypes(response.columns(), List.of("long", "long", "long", "long", "long", "keyword"));
    }

    private void assertStats(EsqlQueryResponse response, IntoStats stats) {
        assertIntoColumns(response);
        List<String> details = stats.failureDetails().isEmpty() ? null : stats.failureDetails();
        var asList = Arrays.asList(stats.total(), stats.created(), stats.updated(), stats.versionConflicts(), stats.failures(), details);
        assertThat(singleValue(getValuesList(response)), equalTo(asList));
    }

    private void assertReadBack(List<List<Object>> expected) {
        assertReadBack("", expected);
    }

    private void assertReadBackWithIds(String commands, List<List<Object>> expected) {
        refresh(DESTINATION);
        try (EsqlQueryResponse response = run("FROM " + DESTINATION + " METADATA _id | " + commands)) {
            assertThat(getValuesList(response), equalTo(expected));
        }
    }

    private void assertReadBack(String commands, List<List<Object>> expected) {
        refresh(DESTINATION);
        String query = commands.isEmpty() ? "FROM " + DESTINATION : "FROM " + DESTINATION + " | " + commands;
        try (EsqlQueryResponse response = run(query)) {
            assertThat(getValuesList(response), equalTo(expected));
        }
    }
}
