/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.TransportCancelTasksAction;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockFactoryBuilder;
import org.elasticsearch.compute.data.BlockFactoryProvider;
import org.elasticsearch.compute.operator.DriverTaskRunner;
import org.elasticsearch.compute.test.MockBlockFactory;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlPluginWithEnterpriseOrTrialLicense;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.junit.After;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Native failure paths with a real snapshot and retained candidate pages. A transport gate targets only SEQUENCE's
 * second-stage search, after stage-one compute has transferred its candidates into the source operator.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class SequenceFailureIT extends AbstractEsqlIntegTestCase {
    private static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(30);
    private static final Instant BASE = Instant.parse("2026-09-25T21:14:51.389Z");
    private static final int ANCHORS = 128;
    private static final String SOURCE_FAILURE = "injected sequence source search failure";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        assertTrue(plugins.removeIf(EsqlPlugin.class::isAssignableFrom));
        plugins.add(SequenceBlockFactoryPlugin.class);
        plugins.add(InternalExchangePlugin.class);
        plugins.add(MockTransportService.TestPlugin.class);
        return plugins;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "128mb")
            .put(
                HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_TYPE_SETTING.getKey(),
                HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_TYPE_SETTING.getDefault(Settings.EMPTY)
            )
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_OVERHEAD_SETTING.getKey(), 1.0)
            .put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), false)
            .put(BlockFactory.LOCAL_BREAKER_OVER_RESERVED_SIZE_SETTING, "0b")
            .put(BlockFactory.LOCAL_BREAKER_OVER_RESERVED_MAX_SIZE_SETTING, "0b")
            .build();
    }

    /** Tracks real compute blocks so failures cannot hide native page leaks behind successful breaker refunds. */
    public static class SequenceBlockFactoryPlugin extends EsqlPluginWithEnterpriseOrTrialLicense {
        @Override
        protected BlockFactoryProvider blockFactoryProvider(BlockFactoryBuilder builder) {
            return new BlockFactoryProvider(new MockBlockFactory(builder));
        }
    }

    public void testParentCancellationWithOpenSnapshotAndRetainedCandidates() throws Exception {
        populateIndices();
        String coordinator = coordinator();
        var gate = new SearchGate(false);
        ActionFuture<EsqlQueryResponse> future = null;
        try {
            future = client(coordinator).execute(EsqlQueryAction.INSTANCE, request());
            gate.awaitStarted();
            assertSnapshotAndCandidatesLive(coordinator);
            List<TaskId> rootTasks = client(coordinator).admin()
                .cluster()
                .prepareListTasks()
                .setActions(EsqlQueryAction.NAME)
                .setDetailed(true)
                .get()
                .getTasks()
                .stream()
                .filter(task -> task.description() != null && task.description().contains("failure-a"))
                .map(task -> task.taskId())
                .toList();
            assertThat(rootTasks, hasSize(1));
            var cancellation = new CancelTasksRequest().setTargetTaskId(rootTasks.getFirst()).setReason("sequence failure test");
            cancellation.setWaitForCompletion(false);
            client(coordinator).execute(TransportCancelTasksAction.TYPE, cancellation).actionGet(TIMEOUT);
            gate.release();
            Exception failure = expectFailure(future);
            assertThat(ExceptionsHelper.unwrap(failure, TaskCancelledException.class), notNullValue());
        } finally {
            finish(gate, future);
        }
        assertReleased();
        assertRecovery(coordinator);
    }

    public void testSourceSearchFailureCannotReturnPartialSequence() throws Exception {
        populateIndices();
        String coordinator = coordinator();
        var gate = new SearchGate(true);
        ActionFuture<EsqlQueryResponse> future = null;
        try {
            // Ordinary ES|QL may allow partial results. Missing sequence stages cannot be treated as a complete match.
            future = client(coordinator).execute(EsqlQueryAction.INSTANCE, request().allowPartialResults(true));
            gate.awaitStarted();
            assertSnapshotAndCandidatesLive(coordinator);
            gate.release();
            Exception failure = expectFailure(future);
            var searchFailure = (SearchPhaseExecutionException) ExceptionsHelper.unwrap(failure, SearchPhaseExecutionException.class);
            assertThat(searchFailure, notNullValue());
            assertTrue(
                "the injected shard failure must be retained",
                java.util.Arrays.stream(searchFailure.shardFailures())
                    .anyMatch(shardFailure -> shardFailure.reason().contains(SOURCE_FAILURE))
            );
        } finally {
            finish(gate, future);
        }
        assertReleased();
        assertRecovery(coordinator);
    }

    public void testSelectedPayloadsRemainInSnapshotAfterUpdateAndDelete() throws Exception {
        assertSelectedPayloadsRemainInSnapshotAfterUpdateAndDelete(false);
    }

    /** Full original sources must come from the same PIT as selection, even when live documents have changed or disappeared. */
    public void testStageSourcesRemainInSnapshotAfterUpdateAndDelete() throws Exception {
        assertSelectedPayloadsRemainInSnapshotAfterUpdateAndDelete(true);
    }

    private void assertSelectedPayloadsRemainInSnapshotAfterUpdateAndDelete(boolean withSource) throws Exception {
        populateIndices();
        Map<String, Object> originalAnchor = client().prepareGet("failure-a", "A" + (ANCHORS - 1)).get().getSource();
        Map<String, Object> originalCompletion = client().prepareGet("failure-b", "B").get().getSource();
        var gate = new SearchGate(false);
        ActionFuture<EsqlQueryResponse> future = null;
        try {
            var query = withSource ? sourceRequest() : request();
            future = client(coordinator()).execute(EsqlQueryAction.INSTANCE, query);
            gate.awaitStarted();
            prepareIndex("failure-a").setId("A" + (ANCHORS - 1))
                .setSource("@timestamp", BASE.plusMillis(ANCHORS - 1).toString(), "key", "shared", "payload", "updated-anchor")
                .get();
            client().prepareDelete("failure-b", "B").get();
            prepareIndex("failure-b").setId("new-B")
                .setSource(
                    "@timestamp",
                    BASE.plusMillis(ANCHORS + 1).toString(),
                    "key",
                    "shared",
                    "payload",
                    "new-completion",
                    "phase",
                    "target"
                )
                .get();
            refresh("failure-a", "failure-b");
            gate.release();
            try (var response = future.actionGet(TIMEOUT)) {
                // AbstractClient's future retains one response reference. Once this scope owns it, cleanup must
                // not release a producer reference that may still be live while its callback returns.
                future = null;
                if (withSource) {
                    List<List<Object>> rows = getValuesList(response);
                    assertThat(rows, hasSize(1));
                    assertEquals(List.of("shared", "anchor-" + (ANCHORS - 1), "completion"), rows.getFirst().subList(0, 3));
                    assertEquals(
                        List.of("failure-a", "A" + (ANCHORS - 1), originalAnchor, "failure-b", "B", originalCompletion),
                        rows.getFirst().subList(3, 9)
                    );
                } else {
                    assertValues(response.values(), List.of(List.of("shared", "anchor-" + (ANCHORS - 1), "completion")));
                }
            }
            assertEquals("updated-anchor", client().prepareGet("failure-a", "A" + (ANCHORS - 1)).get().getSource().get("payload"));
            assertFalse(client().prepareGet("failure-b", "B").get().isExists());
        } finally {
            finish(gate, future);
        }
        assertReleased();
    }

    /** Cancelling while the second stage source fetch is parked must release the first member's already retained source. */
    public void testCancellationDuringStageSourceReplayReleasesSnapshotAndPayloads() throws Exception {
        populateIndices();
        String coordinator = coordinator();
        var gate = new SearchGate(false, true);
        ActionFuture<EsqlQueryResponse> future = null;
        try {
            future = client(coordinator).execute(EsqlQueryAction.INSTANCE, sourceRequest());
            gate.awaitStarted();
            assertTrue("payload or match allocations must be live before cancellation", requestBreaker(coordinator).getUsed() > 0);
            List<TaskId> rootTasks = client(coordinator).admin()
                .cluster()
                .prepareListTasks()
                .setActions(EsqlQueryAction.NAME)
                .setDetailed(true)
                .get()
                .getTasks()
                .stream()
                .filter(task -> task.description() != null && task.description().contains("failure-a"))
                .map(task -> task.taskId())
                .toList();
            assertThat(rootTasks, hasSize(1));
            var cancellation = new CancelTasksRequest().setTargetTaskId(rootTasks.getFirst())
                .setReason("sequence source cancellation test");
            cancellation.setWaitForCompletion(false);
            client(coordinator).execute(TransportCancelTasksAction.TYPE, cancellation).actionGet(TIMEOUT);
            gate.release();
            Exception failure = expectFailure(future);
            assertThat(ExceptionsHelper.unwrap(failure, TaskCancelledException.class), notNullValue());
        } finally {
            finish(gate, future);
        }
        assertReleased();
        assertRecovery(coordinator);
    }

    public void testStageSourceSearchFailureCannotReturnPartialChain() throws Exception {
        populateIndices();
        String coordinator = coordinator();
        var gate = new SearchGate(true, true);
        ActionFuture<EsqlQueryResponse> future = null;
        try {
            future = client(coordinator).execute(EsqlQueryAction.INSTANCE, sourceRequest().allowPartialResults(true));
            gate.awaitStarted();
            assertTrue(requestBreaker(coordinator).getUsed() > 0);
            gate.release();
            Exception failure = expectFailure(future);
            assertThat(ExceptionsHelper.unwrap(failure, SearchPhaseExecutionException.class), notNullValue());
        } finally {
            finish(gate, future);
        }
        assertReleased();
        assertRecovery(coordinator);
    }

    public void testDateMathClockIsFixedAcrossDelayedSearchAndReplay() throws Exception {
        populateIndices();
        Instant window = Instant.now();
        for (String index : List.of("failure-a", "failure-b")) {
            indicesAdmin().preparePutMapping(index).setSource("window", "type=date").get();
        }
        prepareIndex("failure-a").setId("clock-A")
            .setSource(
                "@timestamp",
                BASE.plusMillis(ANCHORS).toString(),
                "key",
                "clock",
                "payload",
                "clock-anchor",
                "window",
                window.toString()
            )
            .get();
        prepareIndex("failure-b").setId("clock-B")
            .setSource(
                "@timestamp",
                BASE.plusMillis(ANCHORS + 1).toString(),
                "key",
                "clock",
                "payload",
                "clock-completion",
                "phase",
                "target",
                "window",
                window.toString()
            )
            .get();
        refresh("failure-a", "failure-b");
        var gate = new SearchGate(false);
        ActionFuture<EsqlQueryResponse> future = null;
        try {
            var query = syncEsqlQueryRequest("""
                sequence by key with {"maxspan":"30s"}
                (from failure-a | eval request_started=NOW() | keep key, payload, request_started),
                (from failure-b | where phase=="target" | eval request_started=NOW() | keep key, payload, request_started)
                | keep key, stage1.payload, stage2.payload, stage1.request_started, stage2.request_started
                | limit 1
                """).filter(org.elasticsearch.index.query.QueryBuilders.rangeQuery("window").gte("now-1m").lte("now+1m"));
            future = client(coordinator()).execute(EsqlQueryAction.INSTANCE, query);
            gate.awaitStarted();
            // An actual delay separates candidate admission from payload transport. The assertion pins the clock
            // crossing the wire rather than depending on an event expiring at a wall-clock boundary.
            Thread.sleep(25);
            gate.release();
            try (var response = future.actionGet(TIMEOUT)) {
                future = null;
                var rows = getValuesList(response);
                assertThat(rows, hasSize(1));
                assertEquals(List.of("clock", "clock-anchor", "clock-completion"), rows.getFirst().subList(0, 3));
                Object started = rows.getFirst().get(3);
                assertEquals(started, rows.getFirst().get(4));
                long startMillis = Instant.parse(started.toString()).toEpochMilli();
                assertTrue("candidate and payload shard searches must execute", gate.clocks.size() >= 4);
                for (long clock : gate.clocks) {
                    assertEquals("Search date math must share the ES|QL NOW() clock", startMillis, clock);
                }
            }
        } finally {
            finish(gate, future);
        }
        assertReleased();
    }

    public void testRequestBreakerAfterSnapshotAndCandidateAdmission() throws Exception {
        populateIndices();
        String coordinator = coordinator();
        CircuitBreaker breaker = requestBreaker(coordinator);
        long previousLimit = breaker.getLimit();
        double previousOverhead = breaker.getOverhead();
        long previousTrips = breaker.getTrippedCount();
        var gate = new SearchGate(false);
        ActionFuture<EsqlQueryResponse> future = null;
        try {
            future = client(coordinator).execute(EsqlQueryAction.INSTANCE, request());
            gate.awaitStarted();
            assertSnapshotAndCandidatesLive(coordinator);
            // Limit only the coordinator after field resolution, snapshot opening and stage-one candidate admission.
            // Data nodes retain normal limits; the pending Search response or its native conversion must fail admission.
            breaker.setLimitAndOverhead(1, 1.0);
            gate.release();
            Exception failure = expectFailure(future);
            assertThat(ExceptionsHelper.unwrap(failure, CircuitBreakingException.class), notNullValue());
            assertTrue("the real request breaker was not tripped", breaker.getTrippedCount() > previousTrips);
        } finally {
            breaker.setLimitAndOverhead(previousLimit, previousOverhead);
            finish(gate, future);
        }
        assertReleased();
        assertRecovery(coordinator);
    }

    private void populateIndices() {
        for (String index : List.of("failure-a", "failure-b")) {
            assertAcked(
                indicesAdmin().prepareCreate(index)
                    .setSettings(indexSettings(1, 0))
                    .setMapping("@timestamp", "type=date_nanos", "key", "type=keyword", "payload", "type=keyword", "phase", "type=keyword")
            );
        }
        ensureGreen("failure-a", "failure-b");
        var bulk = client().prepareBulk();
        for (int event = 0; event < ANCHORS; event++) {
            bulk.add(
                prepareIndex("failure-a").setId("A" + event)
                    .setSource("@timestamp", BASE.plusMillis(event).toString(), "key", "shared", "payload", "anchor-" + event)
            );
        }
        bulk.add(
            prepareIndex("failure-b").setId("B")
                .setSource("@timestamp", BASE.plusMillis(ANCHORS).toString(), "key", "shared", "payload", "completion", "phase", "target")
        );
        assertFalse(bulk.get().hasFailures());
        refresh("failure-a", "failure-b");
    }

    private String coordinator() {
        for (String node : internalCluster().getNodeNames()) {
            var discoveryNode = internalCluster().getInstance(TransportService.class, node).getLocalNode();
            if (discoveryNode.canContainData() == false && discoveryNode.isMasterNode() == false) {
                return node;
            }
        }
        return internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
    }

    private static EsqlQueryRequest request() {
        return syncEsqlQueryRequest("""
            sequence by key with {"maxspan":"30s"}
            (from failure-a | keep key, payload), (from failure-b | where phase=="target" | keep key, payload)
            | keep key, stage1.payload, stage2.payload
            | limit 1
            """);
    }

    private static EsqlQueryRequest sourceRequest() {
        return syncEsqlQueryRequest("""
            SEQUENCE BY key WITH {"maxspan":"30s"}
            (FROM failure-a METADATA _id, _index, _source | KEEP key, payload, _id, _index, _source),
            (FROM failure-b METADATA _id, _index, _source | WHERE phase=="target" | KEEP key, payload, _id, _index, _source)
            | KEEP key, stage1.payload, stage2.payload,
                   stage1._index, stage1._id, stage1._source, stage2._index, stage2._id, stage2._source
            | LIMIT 1
            """);
    }

    private void assertSnapshotAndCandidatesLive(String coordinator) {
        int contexts = 0;
        for (SearchService service : internalCluster().getInstances(SearchService.class)) {
            contexts += service.getActiveContexts();
        }
        assertEquals("both indexed sources must have live PIT readers", 2, contexts);
        assertTrue("stage-one candidate reservations must be retained", requestBreaker(coordinator).getUsed() > 32_768);
    }

    private CircuitBreaker requestBreaker(String node) {
        return internalCluster().getInstance(CircuitBreakerService.class, node).getBreaker(CircuitBreaker.REQUEST);
    }

    private static Exception expectFailure(ActionFuture<EsqlQueryResponse> future) {
        // If the query unexpectedly succeeds, finish() still owns and releases the future's response reference.
        return expectThrows(Exception.class, () -> future.actionGet(TIMEOUT));
    }

    private static void drain(ActionFuture<EsqlQueryResponse> future) {
        if (future == null) {
            return;
        }
        try {
            // Only unconsumed futures reach this method. hasReferences() cannot distinguish our reference from
            // a producer's reference while its response callback is still returning.
            future.actionGet(TIMEOUT).close();
        } catch (Exception e) {
            assertTrue("query did not finish after its transport gate was released: " + e, future.isDone());
        }
    }

    private static void finish(SearchGate gate, ActionFuture<EsqlQueryResponse> future) {
        try {
            gate.close();
        } finally {
            drain(future);
        }
    }

    private void assertRecovery(String coordinator) throws Exception {
        try (var response = client(coordinator).execute(EsqlQueryAction.INSTANCE, request()).actionGet(TIMEOUT)) {
            assertValues(response.values(), List.of(List.of("shared", "anchor-" + (ANCHORS - 1), "completion")));
        }
        assertReleased();
    }

    @After
    public void assertReleased() throws Exception {
        assertBusy(() -> {
            for (SearchService service : internalCluster().getInstances(SearchService.class)) {
                assertEquals("PIT contexts retained after sequence failure", 0, service.getActiveContexts());
            }
            assertThat(
                client().admin()
                    .cluster()
                    .prepareListTasks()
                    .setActions(EsqlQueryAction.NAME, DriverTaskRunner.ACTION_NAME, TransportSearchAction.TYPE.name() + "*")
                    .get()
                    .getTasks(),
                empty()
            );
        });
        ensureBlocksReleased();
    }

    /** Parks one second-stage shard query off the transport thread while preserving the real handler and task. */
    private final class SearchGate implements AutoCloseable {
        private final List<Long> clocks = new CopyOnWriteArrayList<>();
        private final CountDownLatch started = new CountDownLatch(1);
        private final CountDownLatch proceed = new CountDownLatch(1);
        private final CountDownLatch handled = new CountDownLatch(1);
        private final AtomicBoolean claimed = new AtomicBoolean();
        private volatile boolean closed;

        private SearchGate(boolean failSearch) {
            this(failSearch, false);
        }

        private SearchGate(boolean failSearch, boolean payloadOnly) {
            for (String node : internalCluster().getNodeNames()) {
                MockTransportService transport = MockTransportService.getInstance(node);
                transport.<ShardSearchRequest>addRequestHandlingBehavior(
                    SearchTransportService.QUERY_ACTION_NAME,
                    (handler, request, channel, task) -> {
                        clocks.add(request.nowInMillis());
                        boolean payloadSearch = request.source() != null
                            && request.source().fetchSource() != null
                            && request.source().fetchSource().fetchSource();
                        if (closed
                            || isSecondStageSearch(request) == false
                            || (payloadOnly && payloadSearch == false)
                            || claimed.compareAndSet(false, true) == false) {
                            handler.messageReceived(request, channel, task);
                            return;
                        }
                        request.incRef();
                        try {
                            transport.getThreadPool().executor(ThreadPool.Names.GENERIC).execute(new AbstractRunnable() {
                                @Override
                                protected void doRun() throws Exception {
                                    started.countDown();
                                    if (proceed.await(TIMEOUT.millis(), TimeUnit.MILLISECONDS) == false) {
                                        throw new IllegalStateException("sequence shard search gate was not released");
                                    }
                                    if (failSearch) {
                                        channel.sendResponse(new IllegalStateException(SOURCE_FAILURE));
                                    } else {
                                        handler.messageReceived(request, channel, task);
                                    }
                                }

                                @Override
                                public void onFailure(Exception e) {
                                    if (e instanceof InterruptedException) {
                                        Thread.currentThread().interrupt();
                                    }
                                    channel.sendResponse(e);
                                }

                                @Override
                                public void onAfter() {
                                    request.decRef();
                                    handled.countDown();
                                }
                            });
                        } catch (Exception e) {
                            request.decRef();
                            handled.countDown();
                            throw e;
                        }
                    }
                );
            }
        }

        private void awaitStarted() {
            safeAwait(started, TIMEOUT);
        }

        private void release() {
            proceed.countDown();
        }

        @Override
        public void close() {
            closed = true;
            release();
            for (String node : internalCluster().getNodeNames()) {
                MockTransportService.getInstance(node).clearAllRules();
            }
            if (claimed.get()) {
                safeAwait(handled, TIMEOUT);
            }
        }

        private static boolean isSecondStageSearch(ShardSearchRequest request) {
            if (request.source() == null) {
                return false;
            }
            // Only the second stage needs phase for its compute recheck. Unlike its _index query clauses, the physical
            // doc-value projection survives coordinator/shard query rewriting, so the gate cannot miss an optimized query.
            return request.source()
                .ext()
                .stream()
                .anyMatch(
                    extension -> extension instanceof SequenceDocValuesSearchExt projection
                        && projection.fields().stream().anyMatch(field -> field.field.equals("phase"))
                );
        }
    }
}
