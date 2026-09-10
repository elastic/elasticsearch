/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.Build;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteUtils;
import org.elasticsearch.action.fieldcaps.TransportFieldCapabilitiesAction;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.coordination.FollowersChecker;
import org.elasticsearch.cluster.coordination.LeaderChecker;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.operator.DriverSleeps;
import org.elasticsearch.compute.operator.DriverStatus;
import org.elasticsearch.compute.operator.DriverTaskRunner;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.FailingFieldPlugin;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.disruption.ServiceDisruptionScheme;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.TransportSettings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.EsqlStreamTestUtils;
import org.elasticsearch.xpack.esql.EsqlStreamTestUtils.StreamControl;
import org.elasticsearch.xpack.esql.EsqlStreamTestUtils.StreamGate;
import org.elasticsearch.xpack.esql.EsqlStreamTestUtils.StreamOutcome;
import org.elasticsearch.xpack.esql.EsqlStreamTestUtils.Terminal;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.plugin.ComputeService;
import org.elasticsearch.xpack.esql.plugin.TransportEsqlQueryAction;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.elasticsearch.test.ESIntegTestCase.Scope.TEST;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlStreamTestUtils.assertStreamInvariants;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.in;

/**
 * Integration tests for the ES|QL streaming API under network disruption.
 * Tests the pre-stream fault (non-200 HTTP status + single NDJSON error line) and
 * post-stream fault (error as the last NDJSON line, HTTP 200 already flushed) regimes.
 *
 * {@code numClientNodes = 1} gives a stable shard-free node to pin the REST client to and
 * ensures every compute request is a genuine inter-node transport hop. Real Netty HTTP is
 * enabled ({@code addMockHttpTransport() = false}) alongside {@link MockTransportService} —
 * {@code MockNode} gates transport and HTTP independently so they coexist without conflict.
 * Disruption scaffolding is adapted from {@code EsqlDisruptionIT}.
 */
@ESIntegTestCase.ClusterScope(scope = TEST, minNumDataNodes = 2, maxNumDataNodes = 3, numClientNodes = 1)
public class EsqlStreamDisruptionIT extends AbstractEsqlIntegTestCase {

    @BeforeClass
    public static void disableForReleaseBuilds() {
        assumeTrue("ES|QL streaming is not available in release builds yet", Build.current().isSnapshot());
    }

    private static final TimeValue SINK_INACTIVE_INTERVAL = TimeValue.timeValueMinutes(1);

    private static final TimeValue EXCHANGE_CLEANUP_TIMEOUT = TimeValue.timeValueSeconds(SINK_INACTIVE_INTERVAL.seconds() * 3 / 2 + 30);

    // for hitting simulated network failures quickly
    private static final Settings DEFAULT_SETTINGS = Settings.builder()
        .put(LeaderChecker.LEADER_CHECK_TIMEOUT_SETTING.getKey(), "5s")
        .put(LeaderChecker.LEADER_CHECK_RETRY_COUNT_SETTING.getKey(), 1)
        .put(FollowersChecker.FOLLOWER_CHECK_TIMEOUT_SETTING.getKey(), "5s")
        .put(FollowersChecker.FOLLOWER_CHECK_RETRY_COUNT_SETTING.getKey(), 1)
        .put(Coordinator.PUBLISH_TIMEOUT_SETTING.getKey(), "5s")
        .put(TransportSettings.CONNECT_TIMEOUT.getKey(), "10s")
        .build();

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(MockTransportService.TestPlugin.class);
        plugins.add(InternalExchangePlugin.class);
        plugins.add(FailingFieldPlugin.class);
        return plugins;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings settings = Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(DEFAULT_SETTINGS)
            .put(ExchangeService.INACTIVE_SINKS_INTERVAL_SETTING, SINK_INACTIVE_INTERVAL)
            .build();
        logger.info("settings {}", settings);
        return settings;
    }

    private ServiceDisruptionScheme addRandomDisruptionScheme() throws Exception {
        return installDisruption(switch (randomInt(2)) {
            case 0 -> NetworkDisruption.UNRESPONSIVE;
            case 1 -> NetworkDisruption.DISCONNECT;
            case 2 -> NetworkDisruption.NetworkDelay.random(random(), TimeValue.timeValueMillis(2000), TimeValue.timeValueMillis(5000));
            default -> throw new IllegalArgumentException("unexpected disruption type index");
        });
    }

    private ServiceDisruptionScheme installDisruption(NetworkDisruption.NetworkLinkDisruptionType type) throws Exception {
        ensureClusterStateConsistency();
        ensureClusterSizeConsistency();
        ServiceDisruptionScheme scheme = new NetworkDisruption(
            NetworkDisruption.TwoPartitions.random(random(), internalCluster().getNodeNames()),
            type
        );
        setDisruptionScheme(scheme);
        return scheme;
    }

    private void clearDisruption() throws Exception {
        logger.info("--> clear disruption scheme");
        internalCluster().clearDisruptionScheme(false);
        ensureFullyConnectedCluster();
        assertBusy(() -> ClusterRerouteUtils.rerouteRetryFailed(client()), 1, TimeUnit.MINUTES);
        ensureYellow();
    }

    private static final String STREAM_INDEX = "stream_test";
    private static final String FAIL_INDEX = "stream_fail";

    private String coordinatingNode;
    private int streamDocCount;
    private int failDocCount;
    private Set<String> allStreamRows;

    @Before
    public void setUpFixture() throws Exception {
        coordinatingNode = internalCluster().getNodeNameThat(
            settings -> DiscoveryNode.canContainData(settings) == false && DiscoveryNode.isMasterNode(settings) == false
        );
        assertNotNull("expected exactly one coordinating-only node (no data, no master roles)", coordinatingNode);
        allStreamRows = null;

        assertAcked(
            prepareCreate(STREAM_INDEX).setSettings(
                Settings.builder().put("index.number_of_shards", between(2, 5)).put("index.number_of_replicas", 0)
            ).setMapping("value", "type=integer", "tag", "type=keyword")
        );
        streamDocCount = between(500, 2000);
        List<IndexRequestBuilder> docs = new ArrayList<>(streamDocCount);
        for (int i = 0; i < streamDocCount; i++) {
            docs.add(client().prepareIndex(STREAM_INDEX).setSource(Map.of("value", i, "tag", "t" + (i % 10))));
        }
        indexRandom(true, docs);
        client().admin().indices().prepareForceMerge(STREAM_INDEX).setMaxNumSegments(1).get();
        ensureGreen(STREAM_INDEX);

        // Failure index: a runtime field that always throws, mirroring EsqlNodeFailureIT.populateIndices()
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(FAIL_INDEX)
                .setMapping(
                    Strings.format(
                        "{\"runtime\":{\"fail_me\":{\"type\":\"long\",\"script\":{\"source\":\"\",\"lang\":\"%s\"}}}}",
                        FailingFieldPlugin.FAILING_FIELD_LANG
                    )
                )
                .setSettings(Settings.builder().put("index.number_of_shards", between(1, 3)).put("index.number_of_replicas", 0))
        );
        failDocCount = between(5, 20);
        List<IndexRequestBuilder> failDocs = new ArrayList<>(failDocCount);
        for (int i = 0; i < failDocCount; i++) {
            failDocs.add(client().prepareIndex(FAIL_INDEX).setSource(Map.of("x", i)));
        }
        indexRandom(true, failDocs);
    }

    @After
    public void clearFaults() throws Exception {
        for (String node : internalCluster().getNodeNames()) {
            MockTransportService.getInstance(node).clearAllRules();
        }
        clearDisruption();
    }

    private void assertServerFullyCleanedUp() throws Exception {
        clearFaults();
        assertBusy(() -> {
            ListTasksResponse tasks = clusterAdmin().prepareListTasks()
                .setActions(EsqlStreamQueryAction.NAME, ComputeService.DATA_ACTION_NAME, DriverTaskRunner.ACTION_NAME)
                .setDetailed(true)
                .get();
            assertThat("Leftover ES|QL tasks: " + tasks.getTasks(), tasks.getTasks(), empty());
        }, 60, TimeUnit.SECONDS);
        awaitExchangesReleased();
        ensureBlocksReleased();
    }

    private void awaitExchangesReleased() throws Exception {
        for (String node : internalCluster().getNodeNames()) {
            TransportEsqlQueryAction esqlQueryAction = internalCluster().getInstance(TransportEsqlQueryAction.class, node);
            ExchangeService exchangeService = esqlQueryAction.exchangeService();
            assertBusy(() -> {
                if (exchangeService.lifecycleState() == Lifecycle.State.STARTED) {
                    assertTrue("Leftover exchanges " + exchangeService + " on node " + node, exchangeService.isEmpty());
                }
            }, EXCHANGE_CLEANUP_TIMEOUT.millis(), TimeUnit.MILLISECONDS);
        }
    }

    @After
    public void awaitExchangesReleasedAfterDisruption() throws Exception {
        awaitExchangesReleased();
    }

    private StreamOutcome stream(String body, StreamGate gate, String... queryParams) throws Exception {
        try (RestClient client = createRestClient(coordinatingNode)) {
            return EsqlStreamTestUtils.stream(client, body, gate, queryParams);
        }
    }

    private StreamOutcome streamPausingAt(
        int lineIndex,
        String body,
        boolean gateRequired,
        TestAction whileSuspended,
        String... queryParams
    ) throws Exception {
        ArrayBlockingQueue<StreamControl> controls = new ArrayBlockingQueue<>(1);
        StreamGate gate = (i, line, control) -> {
            if (i == lineIndex) {
                control.suspendInput();
                controls.offer(control);
            }
        };
        try (RestClient client = createRestClient(coordinatingNode); ExecutorService executor = Executors.newSingleThreadExecutor()) {
            Future<StreamOutcome> future = executor.submit(() -> EsqlStreamTestUtils.stream(client, body, gate, queryParams));
            StreamControl control = controls.poll(60, TimeUnit.SECONDS);
            if (control == null && gateRequired) {
                String detail;
                try {
                    StreamOutcome early = future.get(30, TimeUnit.SECONDS);
                    detail = describe(early);
                } catch (TimeoutException e) {
                    detail = "stream is still running";
                }
                fail("stream did not reach line " + lineIndex + " within 60s; " + detail);
            }
            Exception actionFailure = null;
            if (control != null) {
                try {
                    whileSuspended.run();
                } catch (Exception e) {
                    actionFailure = e;
                } finally {
                    control.requestInput();
                }
            }
            // Always wait for the future on both success and failure paths so that executor.close()
            // (ExecutorService.close() awaits termination with no timeout) returns promptly instead of
            // hanging the suite until the JUnit deadline when whileSuspended threw.
            StreamOutcome outcome;
            try {
                outcome = future.get(2, TimeUnit.MINUTES);
            } catch (Exception getException) {
                executor.shutdownNow();
                if (actionFailure != null) {
                    actionFailure.addSuppressed(getException);
                    throw actionFailure;
                }
                throw getException;
            }
            if (actionFailure != null) {
                throw actionFailure;
            }
            return outcome;
        }
    }

    private StreamOutcome streamPausingAt(int lineIndex, String body, TestAction whileSuspended) throws Exception {
        return streamPausingAt(lineIndex, body, true, whileSuspended);
    }

    @FunctionalInterface
    private interface TestAction {
        void run() throws Exception;
    }

    private AtomicLong failActionOnAllNodes(String action, String message) {
        AtomicLong hits = new AtomicLong();
        for (String node : internalCluster().getNodeNames()) {
            MockTransportService.getInstance(node).addRequestHandlingBehavior(action, (handler, request, channel, task) -> {
                hits.incrementAndGet();
                channel.sendResponse(new RuntimeException(message));
            });
        }
        return hits;
    }

    private static String streamBody(String query) {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject().field("query", query);
            return Strings.toString(builder.endObject());
        } catch (IOException e) {
            throw new AssertionError("failed to build stream request body", e);
        }
    }

    private static String describe(StreamOutcome outcome) {
        return Strings.format(
            "httpStatus=%s, lines=%d, terminal=%s, error=%s, transportFailure=%s",
            outcome.httpStatus(),
            outcome.lines().size(),
            outcome.terminal(),
            outcome.error(),
            outcome.transportFailure()
        );
    }

    /**
     * Asserts, within a 60-second {@code assertBusy} budget, that the coordinator's {@code final}
     * driver is currently parked on {@code streaming_page_consumer} backpressure. The task list is
     * fetched only from {@link #coordinatingNode} (via {@code .setNodesIds}) so the observation itself
     * never crosses the inter-node transport links and is not perturbed by any active disruption.
     */
    private void assertFinalDriverParkedOnConsumer() throws Exception {
        assertBusy(() -> {
            ListTasksResponse taskResp = client(coordinatingNode).admin()
                .cluster()
                .prepareListTasks()
                .setActions(DriverTaskRunner.ACTION_NAME)
                .setNodesIds(coordinatingNode)
                .setDetailed(true)
                .get();
            assertThat("task list on coordinating node must have no node failures", taskResp.getNodeFailures(), empty());
            DriverStatus finalDriver = taskResp.getTasks()
                .stream()
                .filter(t -> t.status() instanceof DriverStatus s && s.description().endsWith("final"))
                .map(t -> (DriverStatus) t.status())
                .findFirst()
                .orElseThrow(() -> new AssertionError("no 'final' driver task found; all tasks: " + taskResp.getTasks()));
            assertThat(
                "final driver must be ASYNC; status=" + finalDriver.status() + " sleeps=" + finalDriver.sleeps().counts(),
                finalDriver.status(),
                equalTo(DriverStatus.Status.ASYNC)
            );
            List<DriverSleeps.Sleep> lastSleeps = finalDriver.sleeps().last();
            assertFalse("final driver has no recorded sleeps; sleeps=" + finalDriver.sleeps().counts(), lastSleeps.isEmpty());
            DriverSleeps.Sleep currentSleep = lastSleeps.get(lastSleeps.size() - 1);
            assertTrue("final driver must currently be sleeping; last sleep=" + currentSleep, currentSleep.isStillSleeping());
            assertThat(
                "final driver must be parked on streaming_page_consumer backpressure; reason="
                    + currentSleep.reason()
                    + " sleeps="
                    + finalDriver.sleeps().counts(),
                currentSleep.reason(),
                containsString("streaming_page_consumer")
            );
        }, 60, TimeUnit.SECONDS);
    }

    public void testHappyPathOverRealHttp() throws Exception {
        StreamOutcome outcome = stream(streamBody("FROM " + STREAM_INDEX + " | LIMIT 100"), null, "batch_size=5");
        assertServerFullyCleanedUp();
        assertStreamInvariants(outcome, false, 100);
        assertThat("happy path must produce a footer", outcome.terminal(), equalTo(Terminal.FOOTER));
    }

    public void testFaultBeforeStreamStartYieldsHttpError() throws Exception {
        failActionOnAllNodes(TransportFieldCapabilitiesAction.ACTION_NODE_NAME, "injected field-caps failure for pre-stream test");
        StreamOutcome outcome = stream(streamBody("FROM " + STREAM_INDEX + " | LIMIT 100"), null, "batch_size=5");
        assertServerFullyCleanedUp();
        assertStreamInvariants(outcome, false, 100);
        assertNotEquals("field-caps fault before stream start must produce a non-200 HTTP status", 200, (int) outcome.httpStatus());
        assertThat("terminal must be an error line", outcome.terminal(), equalTo(Terminal.ERROR));
    }

    private void assertPostStreamFaultBecomesErrorLine(String action) throws Exception {
        AtomicLong faultHits = failActionOnAllNodes(action, "injected failure for post-stream test");
        StreamOutcome outcome = stream(
            streamBody("FROM " + STREAM_INDEX + " | LIMIT 1000"),
            null,
            "batch_size=5",
            "allow_partial_results=false"
        );
        assertThat("injected fault on [" + action + "] was never invoked; " + describe(outcome), faultHits.get(), greaterThan(0L));
        assertServerFullyCleanedUp();
        assertStreamInvariants(outcome, false, 1000);
        assertThat("HTTP status must be 200 because it was already flushed", outcome.httpStatus(), equalTo(200));
        assertThat("stream must have started with a columns line", outcome.lines().get(0), hasKey("columns"));
        assertThat("error after stream start must arrive as last NDJSON line", outcome.terminal(), equalTo(Terminal.ERROR));
        assertNotEquals("in-body status must not be 200", 200, ((Number) outcome.terminalLine().get("status")).intValue());
        assertNotEquals("error type must not be remote_transport_exception", "remote_transport_exception", outcome.errorType());
    }

    public void testFaultAfterStreamStartYieldsErrorAsLastLine() throws Exception {
        assertPostStreamFaultBecomesErrorLine(ExchangeService.OPEN_EXCHANGE_ACTION_NAME);
    }

    public void testExchangeFaultAfterStreamStartYieldsErrorAsLastLine() throws Exception {
        assertPostStreamFaultBecomesErrorLine(ExchangeService.EXCHANGE_ACTION_NAME);
    }

    private void assertShardFailureMidStream(boolean allowPartial) throws Exception {
        assumeTrue("requires query pragmas for max_concurrent_shards_per_node", canUseQueryPragmas());
        int limit = streamDocCount + failDocCount + 1;
        String body = Strings.format(
            "{\"query\":\"FROM %s,%s METADATA _id | KEEP _id, fail_me | LIMIT %d\","
                + "\"pragma\":{\"max_concurrent_shards_per_node\":1},\"accept_pragma_risks\":true}",
            FAIL_INDEX,
            STREAM_INDEX,
            limit
        );
        StreamOutcome outcome = stream(body, null, "batch_size=5", "allow_partial_results=" + allowPartial);
        assertServerFullyCleanedUp();
        assertStreamInvariants(outcome, allowPartial, limit);
        if (allowPartial) {
            assertThat("allow_partial_results=true must produce a footer", outcome.terminal(), equalTo(Terminal.FOOTER));
            assertTrue("is_partial must be true when some shards failed", outcome.isPartial());
            assertTrue("at least some rows must arrive for partial results", outcome.rowCount() > 0);
        } else {
            assertThat(
                "allow_partial_results=false must produce an error line when shards fail",
                outcome.terminal(),
                equalTo(Terminal.ERROR)
            );
        }
    }

    public void testShardFailureMidStreamWithPartialResults() throws Exception {
        assertShardFailureMidStream(true);
    }

    public void testShardFailureMidStreamWithoutPartialResults() throws Exception {
        assertShardFailureMidStream(false);
    }

    private String createAllShardsFailingIndex() {
        String indexName = "all_shards_fail";
        assertAcked(
            prepareCreate(indexName).setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                .setMapping(
                    Strings.format(
                        "{\"runtime\":{\"fail_me\":{\"type\":\"long\",\"script\":{\"source\":\"\",\"lang\":\"%s\"}}}}",
                        FailingFieldPlugin.FAILING_FIELD_LANG
                    )
                )
        );
        int docCount = between(5, 20);
        List<IndexRequestBuilder> docs = new ArrayList<>(docCount);
        for (int i = 0; i < docCount; i++) {
            docs.add(client().prepareIndex(indexName).setSource(Map.of("x", i)));
        }
        indexRandom(true, docs);
        ensureGreen(indexName);
        return indexName;
    }

    private void assertAllShardsFailedUngroupedStats(boolean allowPartial) throws Exception {
        String indexName = createAllShardsFailingIndex();
        String query = "FROM " + indexName + " | STATS c = COUNT(fail_me)";

        EsqlQueryRequest request = new EsqlQueryRequest();
        request.query(query);
        request.allowPartialResults(allowPartial);
        if (allowPartial) {
            long refCount;
            try (EsqlQueryResponse resp = run(request)) {
                EsqlExecutionInfo.Cluster localInfo = resp.getExecutionInfo().getCluster(RemoteClusterService.LOCAL_CLUSTER_GROUP_KEY);
                assertThat("test premise: all shards must have failed", localInfo.getSuccessfulShards(), equalTo(0));
                assertThat("test premise: at least one shard must have been targeted", localInfo.getFailedShards(), greaterThan(0));
                assertTrue("non-streaming must report is_partial when every shard fails", resp.isPartial());
                List<List<Object>> rows = EsqlTestUtils.getValuesList(resp);
                assertThat("non-streaming must return exactly one row (the aggregation output)", rows, hasSize(1));
                refCount = ((Number) rows.get(0).get(0)).longValue();
                assertThat("COUNT over all-failing shards must be 0", refCount, equalTo(0L));
            }
            StreamOutcome outcome = stream(streamBody(query), null, "batch_size=5", "allow_partial_results=true");
            assertServerFullyCleanedUp();
            assertStreamInvariants(outcome, true, 1);
            assertThat("HTTP status must be 200", outcome.httpStatus(), equalTo(200));
            assertThat(
                "streaming must produce a success footer when allow_partial_results=true",
                outcome.terminal(),
                equalTo(Terminal.FOOTER)
            );
            assertTrue("streaming must report is_partial when every shard fails", outcome.isPartial());
            List<List<Object>> streamRows = outcome.rows();
            assertThat("streaming must return exactly one row", streamRows, hasSize(1));
            assertThat(
                "streaming row count must match non-streaming row count",
                ((Number) streamRows.get(0).get(0)).longValue(),
                equalTo(refCount)
            );
        } else {
            expectThrows(Exception.class, () -> run(request).close());

            StreamOutcome outcome = stream(streamBody(query), null, "batch_size=5", "allow_partial_results=false");
            assertServerFullyCleanedUp();
            assertStreamInvariants(outcome, false, 1);
            assertThat(
                "allow_partial_results=false must produce an error when all shards fail",
                outcome.terminal(),
                equalTo(Terminal.ERROR)
            );
        }
    }

    public void testAllShardsFailedUngroupedStatsMatchesNonStreaming() throws Exception {
        assertAllShardsFailedUngroupedStats(true);
    }

    public void testAllShardsFailedUngroupedStatsWithoutPartialResults() throws Exception {
        assertAllShardsFailedUngroupedStats(false);
    }

    public void testNodeRestartMidStream() throws Exception {
        StreamOutcome outcome = streamPausingAt(
            1,
            streamBody("FROM " + STREAM_INDEX + " | LIMIT 1000"),
            true,
            () -> internalCluster().restartRandomDataNode(),
            "batch_size=2"
        );
        assertServerFullyCleanedUp();
        assertStreamInvariants(outcome, false, 1000);
    }

    public void testClientAbortsMidStream() throws Exception {
        int abortAfterLines = between(2, 6);
        StreamOutcome outcome = stream(streamBody("FROM " + STREAM_INDEX + " | LIMIT 1000"), (lineIndex, line, control) -> {
            if (lineIndex == abortAfterLines - 1) {
                control.abort();
            }
        }, "batch_size=2");
        assertServerFullyCleanedUp();
        assertThat("status must be 200 — abort is a client decision after stream started", outcome.httpStatus(), equalTo(200));
        assertThat("no terminal line expected after abort", outcome.terminal(), equalTo(Terminal.NONE));
        assertTrue("clientAborted flag must be set", outcome.clientAborted());
        assertThat("lines seen must equal the abort point", outcome.lines().size(), equalTo(abortAfterLines));
    }

    public void testSlowConsumerUnderNetworkDelay() throws Exception {
        ServiceDisruptionScheme delay = installDisruption(
            NetworkDisruption.NetworkDelay.random(random(), TimeValue.timeValueMillis(2000), TimeValue.timeValueMillis(5000))
        );

        StreamOutcome outcome = streamPausingAt(
            1,
            streamBody("FROM " + STREAM_INDEX + " | EVAL pad = REPEAT(\"x\", 2048) | LIMIT 1000"),
            true,
            () -> {
                // Verify that the final driver has parked on streaming_page_consumer *before*
                // starting the disruption. The network is healthy here, so pages can flow from
                // data nodes and fill the publisher buffer until the suspended HTTP consumer
                // provides back-pressure. Establishing this state first avoids the race where
                // the disruption starves the exchange, causing the driver to park on "exchange
                // empty" instead and the assertion to time out.
                assertFinalDriverParkedOnConsumer();
                delay.startDisrupting();
                // Verify the driver remains parked on streaming_page_consumer throughout the
                // delay. The HTTP consumer is still suspended, so back-pressure holds; the
                // exchange has buffered pages that continue to arrive (with the induced delay).
                assertFinalDriverParkedOnConsumer();
            },
            "batch_size=2"
        );

        assertServerFullyCleanedUp();
        assertStreamInvariants(outcome, false, 1000);
        assertThat("delayed (not partitioned) stream must still produce a footer", outcome.terminal(), equalTo(Terminal.FOOTER));
    }

    public void testRandomDisruptionDuringStream() throws Exception {
        boolean allowPartial = randomBoolean();
        int pageSize = between(1, 8);
        int limit = randomBoolean() ? between(100, 1000) : streamDocCount + between(1, 100);
        int faultAtLine = randomFrom(0, 1, 2, between(3, 10));
        logger.info(
            "--> testRandomDisruptionDuringStream allowPartial={} pageSize={} limit={} faultAtLine={}",
            allowPartial,
            pageSize,
            limit,
            faultAtLine
        );
        String body = streamBody("FROM " + STREAM_INDEX + " | LIMIT " + limit);
        String partialParam = "allow_partial_results=" + allowPartial;
        String batchParam = "batch_size=" + pageSize;

        ServiceDisruptionScheme scheme = addRandomDisruptionScheme();

        if (faultAtLine == 0) {
            logger.info("--> pre-stream disruption [{}]", scheme);
            scheme.startDisrupting();
            StreamOutcome outcome = stream(body, null, partialParam, batchParam);
            assertServerFullyCleanedUp();
            assertStreamInvariants(outcome, allowPartial, limit);
            assertStreamDeliveredCompleteRows(outcome, limit);
        } else {
            int gateLineIndex = faultAtLine - 1;
            StreamOutcome outcome = streamPausingAt(gateLineIndex, body, false, () -> {
                logger.info("--> post-stream disruption [{}] at line {}", scheme, faultAtLine);
                scheme.startDisrupting();
            }, partialParam, batchParam);
            assertServerFullyCleanedUp();
            assertStreamInvariants(outcome, allowPartial, limit);
            assertStreamDeliveredCompleteRows(outcome, limit);
        }
    }

    private static String rowKey(List<Object> row) {
        return row.stream().map(String::valueOf).collect(Collectors.joining(" "));
    }

    private Set<String> allStreamRows() {
        if (allStreamRows == null) {
            try (EsqlQueryResponse resp = run("FROM " + STREAM_INDEX + " | SORT value | LIMIT " + (streamDocCount + 1))) {
                List<List<Object>> rows = EsqlTestUtils.getValuesList(resp);
                assertThat("reference query must return every indexed doc", rows, hasSize(streamDocCount));
                allStreamRows = rows.stream().map(EsqlStreamDisruptionIT::rowKey).collect(Collectors.toSet());
            }
        }
        return allStreamRows;
    }

    private void assertStreamDeliveredCompleteRows(StreamOutcome outcome, int limit) {
        if (outcome.terminal() != Terminal.FOOTER || outcome.isPartial()) {
            return;
        }
        List<List<Object>> rows = outcome.rows();
        assertThat("clean non-partial footer must deliver exactly the requested row count", rows, hasSize(Math.min(limit, streamDocCount)));
        Set<String> seen = new HashSet<>();
        Set<String> expected = allStreamRows();
        for (List<Object> row : rows) {
            String key = rowKey(row);
            assertThat("stream delivered a row that is not in the index: " + row, key, in(expected));
            assertTrue("stream delivered a duplicate row — a page was double-delivered: " + row, seen.add(key));
        }
    }

    public void testDropNullColumnsPartialFieldCapsFailureRetainsColumn() throws Exception {
        // Ask the cluster state which nodes actually hold data. internalCluster() may include dedicated
        // master-only nodes, and pinning index.routing.allocation.require._name to one of those makes the
        // shard unallocatable, causing create-index to block until the ack timeout.
        List<String> dataNodeNames = clusterService().state().nodes().getDataNodes().values().stream().map(DiscoveryNode::getName).toList();
        assumeTrue("test requires at least two data nodes", dataNodeNames.size() >= 2);
        String nodeA = dataNodeNames.get(0);
        String nodeB = dataNodeNames.get(1);

        String idxA = "drop_null_a";
        String idxB = "drop_null_b";
        assertAcked(
            prepareCreate(idxA).setSettings(
                Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .put("index.routing.allocation.require._name", nodeA)
            ).setMapping("value", "type=keyword", "sparse", "type=keyword")
        );
        assertAcked(
            prepareCreate(idxB).setSettings(
                Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .put("index.routing.allocation.require._name", nodeB)
            ).setMapping("value", "type=keyword", "sparse", "type=keyword")
        );
        indexRandom(
            true,
            client().prepareIndex(idxA).setSource("value", "v1"),
            client().prepareIndex(idxA).setSource("value", "v2"),
            client().prepareIndex(idxB).setSource("value", "v3", "sparse", "s1"),
            client().prepareIndex(idxB).setSource("value", "v4", "sparse", "s2")
        );
        ensureGreen(idxA, idxB);

        String query = "FROM " + idxA + "," + idxB + " | KEEP value, sparse";

        boolean[] control = streamDropNullColumns(query);
        assertEquals("control probe must see two output columns", 2, control.length);
        assertFalse("control: 'sparse' is populated in " + idxB + " and must not be dropped", control[1]);

        MockTransportService.getInstance(nodeB)
            .addRequestHandlingBehavior(TransportFieldCapabilitiesAction.ACTION_NODE_NAME, (handler, request, channel, task) -> {
                if (request.getDescription().contains("includeEmptyFields[false]")) {
                    channel.sendResponse(new IllegalStateException("injected probe failure for drop_null_columns test"));
                } else {
                    handler.messageReceived(request, channel, task);
                }
            });
        try {
            boolean[] withFailure = streamDropNullColumns(query);
            assertFalse("partial field-caps failure must not drop 'sparse' — it may be populated in the failed index", withFailure[1]);
        } finally {
            MockTransportService.getInstance(nodeB).clearAllRules();
        }
    }

    private boolean[] streamDropNullColumns(String query) throws Exception {
        AtomicReference<boolean[]> nullColumnsRef = new AtomicReference<>();
        StreamQueryTestUtils.CountingStreamSubscriber subscriber = new StreamQueryTestUtils.CountingStreamSubscriber();
        EsqlQueryRequest source = EsqlQueryRequest.syncEsqlQueryRequest(query);
        ActionFuture<ActionResponse.Empty> future = client(coordinatingNode).execute(
            EsqlStreamQueryAction.INSTANCE,
            EsqlStreamQueryRequest.from(source, ActionListener.wrap(start -> {
                nullColumnsRef.set(start.nullColumns());
                start.publisher().subscribe(subscriber);
            }, subscriber.failure::set), true, randomIntBetween(1, 10))
        );
        future.actionGet(TimeValue.timeValueSeconds(60));
        subscriber.rethrowIfFailed();
        return nullColumnsRef.get();
    }
}
