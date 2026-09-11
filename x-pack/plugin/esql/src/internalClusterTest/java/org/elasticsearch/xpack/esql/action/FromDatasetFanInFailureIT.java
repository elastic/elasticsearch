/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.operator.AggregationOperator;
import org.elasticsearch.compute.operator.DriverStatus;
import org.elasticsearch.compute.operator.DriverTaskRunner;
import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.compute.operator.exchange.ExchangeSourceOperator;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.FailingFieldPlugin;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.ComputeService;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

/**
 * Execution-time failures in a typed source fan-in. The CSV header supplies the
 * schema so analysis does not need a data row; {@code SUM(v)} forces the malformed
 * token to be parsed while the producer runs.
 */
@ESIntegTestCase.ClusterScope(minNumDataNodes = 2, numClientNodes = 0, supportsDedicatedMasters = false)
public class FromDatasetFanInFailureIT extends AbstractExternalDataSourceIT {

    private static final List<String> STRATEGIES = List.of("coordinator_only", "round_robin");
    private static final String GOOD_CSV = "v:integer,name:keyword\n1,a\n2,b\n";
    private static final String BAD_CSV = "v:integer,name:keyword\nnot-a-number,x\n";
    private static final String EMPTY_CSV = "v:integer,name:keyword\n";
    private static final String COMPLETION_FAILURE = "simulated producer completion failure";

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(FailingFieldPlugin.class);
        plugins.add(MockTransportService.TestPlugin.class);
        return plugins;
    }

    @Before
    public void requirePragmas() {
        assumeTrue("requires query pragmas", canUseQueryPragmas());
    }

    public void testTwoFailingProducersFailTheRequest() throws Exception {
        String left = registerCsv("bad_left", BAD_CSV);
        String right = registerCsv("bad_right", BAD_CSV);
        for (String strategy : STRATEGIES) {
            Exception failure = expectThrows(
                Exception.class,
                () -> run(queryRequest(strategy, "FROM " + left + ", " + right + " | WHERE v IS NOT NULL | KEEP v", true), TIMEOUT).close()
            );
            assertAllSourcesFailed(failure);
        }
    }

    public void testSuccessfulProducerPlusFailureIsPartial() throws Exception {
        String good = registerCsv("good_partial", GOOD_CSV);
        String bad = registerCsv("bad_partial", BAD_CSV);
        for (String strategy : STRATEGIES) {
            try (EsqlQueryResponse response = runStats(strategy, good + ", " + bad, true)) {
                assertPartial(response);
                assertThat(getValuesList(response), equalTo(List.of(List.of(3L, 2L))));
            }
        }
    }

    public void testEmptySuccessfulProducerPlusFailureIsPartial() throws Exception {
        String empty = registerCsv("empty_ok", EMPTY_CSV);
        String bad = registerCsv("bad_with_empty", BAD_CSV);
        for (String strategy : STRATEGIES) {
            try (EsqlQueryResponse response = runStats(strategy, empty + ", " + bad, true)) {
                assertPartial(response);
                List<List<Object>> rows = getValuesList(response);
                assertThat(rows, hasSize(1));
                assertThat(rows.getFirst().get(0), nullValue());
                assertThat(rows.getFirst().get(1), equalTo(0L));
            }
        }
    }

    public void testAllSourcesFailingUnderAggregateHasNoSyntheticRow() throws Exception {
        String left = registerCsv("bad_agg_left", BAD_CSV);
        String right = registerCsv("bad_agg_right", BAD_CSV);
        for (String strategy : STRATEGIES) {
            Exception failure = expectThrows(Exception.class, () -> runStats(strategy, left + ", " + right, true).close());
            assertAllSourcesFailed(failure);
        }
    }

    public void testSuccessfulIndexPlusFailingDatasetKeepsIndexResults() throws Exception {
        createOkIndex("ok_idx", 10);
        String bad = registerCsv("bad_vs_index", BAD_CSV);
        try {
            for (String strategy : STRATEGIES) {
                EsqlQueryRequest request = statsRequest(strategy, "ok_idx, " + bad, true);
                request.includeExecutionMetadata(true);
                try (EsqlQueryResponse response = run(request, TIMEOUT)) {
                    assertPartial(response);
                    assertThat(getValuesList(response), equalTo(List.of(List.of(10L, 1L))));
                    EsqlExecutionInfo.Cluster local = response.getExecutionInfo().getCluster("");
                    int shards = getNumShards("ok_idx").numPrimaries;
                    assertThat(local.getTotalShards(), equalTo(shards));
                    assertThat(local.getSuccessfulShards(), equalTo(shards));
                    assertThat(local.getFailedShards(), equalTo(0));
                }
            }
        } finally {
            wipeTestIndex("ok_idx");
        }
    }

    public void testSuccessfulDatasetPlusFailingIndexKeepsDatasetResults() throws Exception {
        createFailingIndex("fail_idx");
        String good = registerCsv("good_vs_fail_idx", GOOD_CSV);
        try {
            for (String strategy : STRATEGIES) {
                EsqlQueryRequest request = queryRequest(
                    strategy,
                    "FROM " + good + ", fail_idx | EVAL x = COALESCE(fail_me, v) | STATS s = SUM(x), c = COUNT(*)",
                    true
                );
                request.includeExecutionMetadata(true);
                try (EsqlQueryResponse response = run(request, TIMEOUT)) {
                    assertPartial(response);
                    assertThat(getValuesList(response), equalTo(List.of(List.of(3L, 2L))));
                    EsqlExecutionInfo.Cluster local = response.getExecutionInfo().getCluster("");
                    assertThat(local.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.PARTIAL));
                    assertThat(local.getFailedShards(), greaterThanOrEqualTo(1));
                }
            }
        } finally {
            wipeTestIndex("fail_idx");
        }
    }

    public void testPartialResultsDisabledPropagatesFailure() throws Exception {
        String good = registerCsv("good_failfast", GOOD_CSV);
        String bad = registerCsv("bad_failfast", BAD_CSV);
        for (String strategy : STRATEGIES) {
            Exception failure = expectThrows(Exception.class, () -> runStats(strategy, good + ", " + bad, false).close());
            assertThat(ExceptionsHelper.stackTrace(failure), containsString("not-a-number"));
        }
    }

    public void testSecurityRefusalOnDistributedProducerStaysFatal() throws Exception {
        String good = registerCsv("good_security", GOOD_CSV);
        for (String node : internalCluster().getNodeNames()) {
            MockTransportService.getInstance(node)
                .addRequestHandlingBehavior(ComputeService.DATA_ACTION_NAME, (handler, request, channel, task) -> {
                    handler.messageReceived(request, new TransportChannel() {
                        @Override
                        public String getProfileName() {
                            return channel.getProfileName();
                        }

                        @Override
                        public void sendResponse(TransportResponse response) {
                            channel.sendResponse(new ElasticsearchSecurityException("denied"));
                        }

                        @Override
                        public void sendResponse(Exception exception) {
                            channel.sendResponse(new ElasticsearchSecurityException("denied"));
                        }
                    }, task);
                });
        }
        try {
            Exception failure = expectThrows(Exception.class, () -> runStats("round_robin", good, true).close());
            Throwable security = ExceptionsHelper.unwrap(failure, ElasticsearchSecurityException.class);
            assertThat(security, instanceOf(ElasticsearchSecurityException.class));
            assertThat(security.getMessage(), containsString("denied"));

            String peer = registerCsv("good_security_peer", GOOD_CSV);
            Exception fanInFailure = expectThrows(Exception.class, () -> runStats("round_robin", good + ", " + peer, true).close());
            Throwable fanInSecurity = ExceptionsHelper.unwrap(fanInFailure, ElasticsearchSecurityException.class);
            assertThat(fanInSecurity, instanceOf(ElasticsearchSecurityException.class));
            assertThat(fanInSecurity.getMessage(), containsString("denied"));
        } finally {
            for (String node : internalCluster().getNodeNames()) {
                MockTransportService.getInstance(node).clearAllRules();
            }
        }
    }

    public void testDistributedProducerWithSomeSuccessfulSplitsIsPartial() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);
        Path dir = createTempDir();
        Files.writeString(dir.resolve("good.csv"), GOOD_CSV, StandardCharsets.UTF_8);
        Files.writeString(dir.resolve("bad.csv"), BAD_CSV, StandardCharsets.UTF_8);
        String mixed = registerCsvDataset("mixed_splits", StoragePath.fileUri(dir) + "/*.csv");
        try (EsqlQueryResponse response = runStats("round_robin", mixed, true)) {
            assertPartial(response);
            assertThat(getValuesList(response), equalTo(List.of(List.of(3L, 2L))));
        }
        String peer = registerCsv("mixed_splits_peer", GOOD_CSV);
        try (EsqlQueryResponse response = runStats("round_robin", mixed + ", " + peer, true)) {
            assertPartial(response);
            assertThat(getValuesList(response), equalTo(List.of(List.of(6L, 4L))));
        }
    }

    public void testRawRowsFetchedBeforeAllProducerCompletionsFail() throws Exception {
        String left = registerCsv("completion_raw_left", GOOD_CSV);
        String right = registerCsv("completion_raw_right", GOOD_CSV);
        try (CompletionFailureFixture fixture = new CompletionFailureFixture(2)) {
            fixture.start(queryRequest("round_robin", "FROM " + left + ", " + right + " | KEEP v | LIMIT 10", true));
            fixture.assertCoordinatorConsumed("final", 4, false);
            fixture.failCompletions();
            assertAllSourcesFailed(expectThrows(Exception.class, fixture::result));
        }
    }

    public void testSyntheticAggregateStatesFetchedBeforeAllProducerCompletionsFail() throws Exception {
        String left = registerCsv("completion_empty_left", EMPTY_CSV);
        String right = registerCsv("completion_empty_right", EMPTY_CSV);
        try (CompletionFailureFixture fixture = new CompletionFailureFixture(2)) {
            fixture.start(statsRequest("round_robin", left + ", " + right, true));
            fixture.assertCoordinatorConsumed("final", 2, true);
            fixture.failCompletions();
            assertAllSourcesFailed(expectThrows(Exception.class, fixture::result));
        }
    }

    public void testIndexRowsFetchedBeforeCompletionFailureArePartial() throws Exception {
        createOkIndex("completion_idx", 10);
        try {
            try (CompletionFailureFixture fixture = new CompletionFailureFixture(1)) {
                fixture.start(queryRequest("round_robin", "FROM completion_idx | KEEP v | LIMIT 10", true));
                fixture.assertCoordinatorConsumed("final", 1, false);
                fixture.failCompletions();
                EsqlQueryResponse response = fixture.result();
                assertPartial(response);
                assertThat(getValuesList(response), equalTo(List.of(List.of(10))));
                EsqlExecutionInfo.Cluster local = response.getExecutionInfo().getCluster("");
                assertThat(local.getSuccessfulShards(), equalTo(0));
                assertThat(local.getFailedShards(), equalTo(1));
                assertThat(local.getFailures().getFirst().reason(), containsString(COMPLETION_FAILURE));
            }
        } finally {
            wipeTestIndex("completion_idx");
        }
    }

    public void testIndexForkRowsFetchedBeforeAllCompletionsFail() throws Exception {
        createOkIndex("completion_fork_idx", 10);
        try {
            try (CompletionFailureFixture fixture = new CompletionFailureFixture(2)) {
                fixture.start(queryRequest("round_robin", """
                    FROM completion_fork_idx
                    | FORK
                        (KEEP v)
                        (EVAL v = v + 1 | KEEP v)
                    | KEEP _fork, v
                    | LIMIT 10
                    """, true));
                fixture.assertCoordinatorConsumed("main.final", 2, false);
                fixture.failCompletions();
                assertAllSourcesFailed(expectThrows(Exception.class, fixture::result));
            }
        } finally {
            wipeTestIndex("completion_fork_idx");
        }
    }

    public void testDistributedProducerThatFailsOnEverySplitFailsTheRequest() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);
        Path dir = createTempDir();
        Files.writeString(dir.resolve("bad1.csv"), BAD_CSV, StandardCharsets.UTF_8);
        Files.writeString(dir.resolve("bad2.csv"), BAD_CSV, StandardCharsets.UTF_8);
        String allBad = registerCsvDataset("all_bad_splits", StoragePath.fileUri(dir) + "/*.csv");
        Exception failure = expectThrows(Exception.class, () -> runStats("round_robin", allBad, true).close());
        assertAllSourcesFailed(failure);
        String allBadPeer = registerCsv("all_bad_peer", BAD_CSV);
        Exception fanInFailure = expectThrows(Exception.class, () -> runStats("round_robin", allBad + ", " + allBadPeer, true).close());
        assertAllSourcesFailed(fanInFailure);
    }

    private static void assertAllSourcesFailed(Exception failure) {
        assertThat(
            ExceptionsHelper.stackTrace(failure),
            anyOf(
                containsString("not-a-number"),
                containsString("nodes assigned external splits failed"),
                containsString(COMPLETION_FAILURE)
            )
        );
    }

    private EsqlQueryResponse runStats(String strategy, String from, boolean allowPartial) {
        return run(statsRequest(strategy, from, allowPartial), TIMEOUT);
    }

    private static EsqlQueryRequest statsRequest(String strategy, String from, boolean allowPartial) {
        return queryRequest(strategy, "FROM " + from + " | STATS s = SUM(v), c = COUNT(*)", allowPartial);
    }

    private static EsqlQueryRequest queryRequest(String strategy, String query, boolean allowPartial) {
        EsqlQueryRequest request = syncEsqlQueryRequest(query);
        request.pragmas(new QueryPragmas(Settings.builder().put(QueryPragmas.EXTERNAL_DISTRIBUTION.getKey(), strategy).build()));
        request.acceptedPragmaRisks(true);
        request.allowPartialResults(allowPartial);
        return request;
    }

    private String registerCsv(String name, String body) throws Exception {
        Path file = createTempFile(name + "-", ".csv");
        Files.writeString(file, body, StandardCharsets.UTF_8);
        return registerCsvDataset(name, StoragePath.fileUri(file));
    }

    private String registerCsvDataset(String name, String resourceUri) {
        return registerDataset(name, resourceUri, Map.of("format", "csv", "error_mode", "fail_fast"));
    }

    private void createOkIndex(String name, int value) {
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(name)
                .setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                .setMapping("v", "type=integer", "name", "type=keyword")
        );
        client().prepareBulk()
            .add(new IndexRequest(name).id("1").source("v", value, "name", "idx"))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        ensureYellow(name);
    }

    private void createFailingIndex(String name) throws Exception {
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
            mapping.startObject("v").field("type", "integer").endObject();
            mapping.startObject("name").field("type", "keyword").endObject();
        }
        mapping.endObject();
        mapping.endObject();
        assertAcked(client().admin().indices().prepareCreate(name).setMapping(mapping));
        client().prepareBulk()
            .add(new IndexRequest(name).id("1").source("v", 99, "name", "fail"))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        ensureYellow(name);
    }

    private void wipeTestIndex(String name) {
        try {
            client().admin().indices().prepareDelete(name).get();
        } catch (Exception e) {
            logger.warn("index cleanup [{}] failed", name, e);
        }
    }

    /**
     * Holds successful compute responses, not reader input: even an empty source can finish
     * its INITIAL aggregation and deliver a synthetic state before its completion fails.
     */
    private class CompletionFailureFixture implements AutoCloseable {
        private final String coordinator = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        private final NodeClient coordinatorClient = internalCluster().getInstance(NodeClient.class, coordinator);
        private final MockTransportService coordinatorTransport = MockTransportService.getInstance(coordinator);
        private final List<MockTransportService> transports = new ArrayList<>();
        private final String opaqueId = "completion-failure-" + randomAlphaOfLength(10);
        private final PlainActionFuture<EsqlQueryResponse> future = new PlainActionFuture<>();
        private final SubscribableListener<Void> release = new SubscribableListener<>();
        private final AtomicInteger completions = new AtomicInteger();
        private final int expectedCompletions;
        private CancellableTask queryTask;
        private String sessionId;

        CompletionFailureFixture(int expectedCompletions) {
            this.expectedCompletions = expectedCompletions;
            for (String node : internalCluster().getNodeNames()) {
                MockTransportService transport = MockTransportService.getInstance(node);
                transports.add(transport);
                transport.addRequestHandlingBehavior(ComputeService.DATA_ACTION_NAME, (handler, request, channel, task) -> {
                    if (opaqueId.equals(task.getHeader(Task.X_OPAQUE_ID_HTTP_HEADER)) == false) {
                        handler.messageReceived(request, channel, task);
                        return;
                    }
                    handler.messageReceived(request, new TransportChannel() {
                        @Override
                        public String getProfileName() {
                            return channel.getProfileName();
                        }

                        @Override
                        public void sendResponse(TransportResponse response) {
                            release.addListener(
                                ActionListener.wrap(
                                    ignored -> channel.sendResponse(new IOException(COMPLETION_FAILURE)),
                                    channel::sendResponse
                                )
                            );
                            completions.incrementAndGet();
                        }

                        @Override
                        public void sendResponse(Exception exception) {
                            channel.sendResponse(exception);
                        }
                    }, task);
                });
            }
        }

        void start(EsqlQueryRequest request) {
            request.includeExecutionMetadata(true);
            try (ThreadContext.StoredContext ignored = coordinatorTransport.getThreadPool().getThreadContext().stashContext()) {
                coordinatorTransport.getThreadPool().getThreadContext().putHeader(Task.X_OPAQUE_ID_HTTP_HEADER, opaqueId);
                queryTask = asInstanceOf(
                    CancellableTask.class,
                    coordinatorClient.executeAndReturnTask(
                        EsqlQueryAction.INSTANCE,
                        request,
                        future.delegateFailureAndWrap((listener, response) -> {
                            response.mustIncRef();
                            listener.onResponse(response);
                        })
                    )
                );
            }
        }

        void assertCoordinatorConsumed(String description, long expectedRows, boolean aggregate) throws Exception {
            assertBusy(() -> assertEquals(expectedCompletions, completions.get()), 60, TimeUnit.SECONDS);
            assertFalse("query must await the held completion responses", future.isDone());
            assertBusy(() -> {
                var tasks = coordinatorClient.admin()
                    .cluster()
                    .prepareListTasks()
                    .setNodesIds(coordinator)
                    .setActions(DriverTaskRunner.ACTION_NAME)
                    .setDetailed(true)
                    .get();
                tasks.rethrowFailures("list driver tasks");
                List<DriverStatus> drivers = tasks.getTasks()
                    .stream()
                    .filter(task -> opaqueId.equals(task.headers().get(Task.X_OPAQUE_ID_HTTP_HEADER)))
                    .map(TaskInfo::status)
                    .filter(DriverStatus.class::isInstance)
                    .map(DriverStatus.class::cast)
                    .filter(status -> description.equals(status.description()))
                    .toList();
                assertThat(drivers, hasSize(1));
                DriverStatus driver = drivers.getFirst();
                sessionId = driver.sessionId().split("/", 2)[0];
                List<OperatorStatus> operators = new ArrayList<>(driver.completedOperators());
                operators.addAll(driver.activeOperators());
                long consumed = 0;
                long aggregateInput = 0;
                for (OperatorStatus operator : operators) {
                    if (operator.status() instanceof ExchangeSourceOperator.Status source) {
                        consumed += source.rowsEmitted();
                    }
                    if (operator.status() instanceof AggregationOperator.Status aggregation) {
                        aggregateInput += aggregation.rowsReceived();
                    }
                }
                assertThat(consumed, equalTo(expectedRows));
                if (aggregate) {
                    assertThat(aggregateInput, equalTo(expectedRows));
                }
            });
        }

        void failCompletions() {
            if (release.isDone() == false) {
                release.onResponse(null);
            }
        }

        EsqlQueryResponse result() {
            return future.actionGet(60, TimeUnit.SECONDS);
        }

        @Override
        public void close() throws Exception {
            try {
                failCompletions();
                if (queryTask != null && future.isDone() == false) {
                    PlainActionFuture<Void> cancelled = new PlainActionFuture<>();
                    coordinatorTransport.getTaskManager()
                        .cancelTaskAndDescendants(queryTask, "completion-failure fixture cleanup", true, cancelled);
                    cancelled.actionGet(60, TimeUnit.SECONDS);
                }
                if (queryTask != null) {
                    assertBusy(() -> assertTrue(future.isDone()), 60, TimeUnit.SECONDS);
                    try {
                        future.result().close();
                    } catch (ExecutionException expected) {
                        // The scenario asserts the query failure; there is no response to release.
                    }
                }
                assertBusy(() -> {
                    var tasks = coordinatorClient.admin().cluster().prepareListTasks().get();
                    tasks.rethrowFailures("list tasks");
                    assertFalse(
                        tasks.getTasks().stream().anyMatch(task -> opaqueId.equals(task.headers().get(Task.X_OPAQUE_ID_HTTP_HEADER)))
                    );
                    if (sessionId != null) {
                        for (String node : internalCluster().getNodeNames()) {
                            assertFalse(
                                internalCluster().getInstance(ExchangeService.class, node)
                                    .sinkKeys()
                                    .stream()
                                    .anyMatch(key -> key.startsWith(sessionId))
                            );
                        }
                    }
                });
                if (queryTask != null) {
                    assertEquals(expectedCompletions, completions.get());
                }
            } finally {
                transports.forEach(MockTransportService::clearAllRules);
            }
        }
    }
}
