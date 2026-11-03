/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.TransportCancelTasksAction;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.test.transport.StubbableTransport;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.plugin.ComputeService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notNullValue;

/**
 * The coordinator charges its request breaker for the sub-plan result it serializes into each request to a remote data node,
 * until that request completes. A dedicated coordinator sends every request remotely, and the trip tests fill the breaker
 * after the first reservation so only the second one trips.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 2, numClientNodes = 0, supportsDedicatedMasters = false)
public class SerializedPlanBreakerIT extends AbstractEsqlIntegTestCase {

    static final int NUM_DOCS = 15_000;
    static final String KEY_PADDING = "x".repeat(200);
    static final String QUERY_TAIL = " | INLINE STATS c = COUNT(*) BY k | STATS n = COUNT(*), s = SUM(c)";
    /** Well below the sub-plan result of {@link #NUM_DOCS} padded keys, and above what the rest of the query allocates. */
    private static final ByteSizeValue HEADROOM = ByteSizeValue.ofMb(1);

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), MockTransportService.TestPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings)).put(memoryRequestBreaker()).build();
    }

    /** The test cluster sometimes picks the {@code noop} request breaker, which never trips. */
    static Settings memoryRequestBreaker() {
        return Settings.builder()
            .put(
                HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_TYPE_SETTING.getKey(),
                HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_TYPE_SETTING.getDefault(Settings.EMPTY)
            )
            .put(
                HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_OVERHEAD_SETTING.getKey(),
                HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_OVERHEAD_SETTING.getDefault(Settings.EMPTY)
            )
            .build();
    }

    /**
     * Fills {@code breaker} when the {@code n}th exchange is opened, leaving less headroom than a reservation of the sub-plan
     * result. INLINE STATS opens one exchange per target for its sub-plan, then one per target for the main plan, each right
     * after that target's reservation, so filling on the first main-plan open trips the reservation of the next target.
     */
    static Releasable fillBreakerOnOpenExchange(MockTransportService transportService, CircuitBreaker breaker, int n) {
        AtomicInteger opens = new AtomicInteger();
        AtomicLong filled = new AtomicLong();
        transportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (ExchangeService.OPEN_EXCHANGE_ACTION_NAME.equals(action) && opens.incrementAndGet() == n) {
                long fill = breaker.getLimit() - breaker.getUsed() - HEADROOM.getBytes();
                breaker.addWithoutBreaking(fill);
                filled.set(fill);
            }
            connection.sendRequest(requestId, action, request, options);
        });
        return () -> {
            transportService.clearAllRules();
            breaker.addWithoutBreaking(-filled.getAndSet(0));
        };
    }

    static void assertTrippedOn(Exception e, String label) {
        assertThat(ExceptionsHelper.status(e), equalTo(RestStatus.TOO_MANY_REQUESTS));
        Throwable cause = ExceptionsHelper.unwrap(e, CircuitBreakingException.class);
        assertThat(e.toString(), cause, notNullValue());
        assertThat(cause.getMessage(), containsString("data for [" + label + "]"));
    }

    /**
     * Holds the {@code n}th {@code action} request any of {@code transportServices} receives until {@code resume} opens, then
     * hands it to {@code then}. A held request has been received in full, so the coordinator has reserved for it and written
     * it, and holds that reservation until the request completes.
     */
    static Releasable holdNthRequest(
        List<MockTransportService> transportServices,
        String action,
        int n,
        CountDownLatch received,
        CountDownLatch resume,
        StubbableTransport.RequestHandlingBehavior<TransportRequest> then
    ) {
        AtomicInteger arrivals = new AtomicInteger();
        for (MockTransportService transportService : transportServices) {
            transportService.addRequestHandlingBehavior(action, (handler, request, channel, task) -> {
                if (arrivals.incrementAndGet() == n) {
                    received.countDown();
                    safeAwait(resume);
                    then.messageReceived(handler, request, channel, task);
                } else {
                    handler.messageReceived(request, channel, task);
                }
            });
        }
        return () -> transportServices.forEach(MockTransportService::clearAllRules);
    }

    static void cancelQuery(Client client) {
        List<TaskInfo> tasks = client.admin().cluster().prepareListTasks().setActions(EsqlQueryAction.NAME).get().getTasks();
        assertThat(tasks, hasSize(1));
        client.execute(
            TransportCancelTasksAction.TYPE,
            new CancelTasksRequest().setTargetTaskId(tasks.getFirst().taskId()).setReason("test cancel")
        ).actionGet();
    }

    static void assertReleased(CircuitBreaker breaker, long before) throws Exception {
        assertBusy(() -> assertThat(breaker.getUsed(), equalTo(before)));
    }

    public void testTripFailsQueryWithoutPartialResults() {
        String coordinator = setupCoordinatorAndIndex();
        try (Releasable filled = fillCoordinatorBreaker(coordinator)) {
            Exception e = expectThrows(Exception.class, () -> run(coordinator, false).close());
            assertTrippedOn(e, "serialize data node request");
        }
    }

    public void testTripDropsShardsWithPartialResults() {
        String coordinator = setupCoordinatorAndIndex();
        try (Releasable filled = fillCoordinatorBreaker(coordinator); EsqlQueryResponse resp = run(coordinator, true)) {
            assertTrue(resp.isPartial());
            List<List<Object>> rows = EsqlTestUtils.getValuesList(resp);
            assertThat(((Number) rows.get(0).get(0)).longValue(), lessThan((long) NUM_DOCS));
            EsqlExecutionInfo.Cluster local = resp.getExecutionInfo().getCluster(RemoteClusterService.LOCAL_CLUSTER_GROUP_KEY);
            assertThat(local.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.PARTIAL));
            assertThat(local.getFailures().toString(), containsString("data for [serialize data node request]"));
        }
    }

    public void testCancellationReleasesReservations() throws Exception {
        String coordinator = setupCoordinatorAndIndex();
        CircuitBreaker breaker = coordinatorBreaker(coordinator);
        long before = breaker.getUsed();
        CountDownLatch received = new CountDownLatch(2);
        CountDownLatch resume = new CountDownLatch(1);
        try (Releasable held = holdMainPlanRequests(received, resume, (h, r, channel, task) -> h.messageReceived(r, channel, task))) {
            ActionFuture<EsqlQueryResponse> future = start(coordinator, false);
            try {
                safeAwait(received);
                cancelQuery(client(coordinator));
            } finally {
                resume.countDown();
            }
            Exception e = expectThrows(Exception.class, () -> future.actionGet(DEFAULT_REQUEST_TIMEOUT).close());
            assertThat(e.toString(), ExceptionsHelper.unwrap(e, TaskCancelledException.class), notNullValue());
        }
        assertReleased(breaker, before);
        assertCountsAllDocs(coordinator);
    }

    public void testDataNodeFailureReleasesReservations() throws Exception {
        String coordinator = setupCoordinatorAndIndex();
        CircuitBreaker breaker = coordinatorBreaker(coordinator);
        long before = breaker.getUsed();
        CountDownLatch received = new CountDownLatch(2);
        CountDownLatch resume = new CountDownLatch(1);
        try (
            Releasable held = holdMainPlanRequests(
                received,
                resume,
                (h, r, channel, task) -> channel.sendResponse(new RuntimeException("simulated data node failure"))
            )
        ) {
            ActionFuture<EsqlQueryResponse> future = start(coordinator, false);
            safeAwait(received);
            resume.countDown();
            Exception e = expectThrows(Exception.class, () -> future.actionGet(DEFAULT_REQUEST_TIMEOUT).close());
            assertThat(e.getMessage(), containsString("simulated data node failure"));
        }
        assertReleased(breaker, before);
        assertCountsAllDocs(coordinator);
    }

    private Releasable holdMainPlanRequests(
        CountDownLatch received,
        CountDownLatch resume,
        StubbableTransport.RequestHandlingBehavior<TransportRequest> then
    ) {
        List<Releasable> held = new ArrayList<>();
        for (String dataNode : internalCluster().nodesInclude("idx")) {
            MockTransportService transportService = MockTransportService.getInstance(dataNode);
            held.add(holdNthRequest(List.of(transportService), ComputeService.DATA_ACTION_NAME, 2, received, resume, then));
        }
        return Releasables.wrap(held);
    }

    private void assertCountsAllDocs(String coordinator) {
        try (EsqlQueryResponse resp = run(coordinator, false)) {
            assertThat(((Number) EsqlTestUtils.getValuesList(resp).get(0).get(0)).longValue(), equalTo((long) NUM_DOCS));
        }
    }

    private String setupCoordinatorAndIndex() {
        String coordinator = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        assertAcked(
            indicesAdmin().prepareCreate("idx")
                .setMapping("k", "type=keyword")
                .setSettings(indexSettings(2, 0).put("index.routing.allocation.total_shards_per_node", 1))
        );
        ensureGreen("idx");
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < NUM_DOCS; i++) {
            bulk.add(prepareIndex("idx").setSource("k", i + KEY_PADDING));
        }
        assertFalse(bulk.get().hasFailures());
        return coordinator;
    }

    private Releasable fillCoordinatorBreaker(String coordinator) {
        return fillBreakerOnOpenExchange(
            (MockTransportService) internalCluster().getInstance(TransportService.class, coordinator),
            coordinatorBreaker(coordinator),
            3
        );
    }

    private CircuitBreaker coordinatorBreaker(String coordinator) {
        return internalCluster().getInstance(CircuitBreakerService.class, coordinator).getBreaker(CircuitBreaker.REQUEST);
    }

    private ActionFuture<EsqlQueryResponse> start(String coordinator, boolean allowPartialResults) {
        var request = syncEsqlQueryRequest("FROM idx" + QUERY_TAIL).allowPartialResults(allowPartialResults);
        return client(coordinator).execute(EsqlQueryAction.INSTANCE, request);
    }

    private EsqlQueryResponse run(String coordinator, boolean allowPartialResults) {
        return start(coordinator, allowPartialResults).actionGet(DEFAULT_REQUEST_TIMEOUT);
    }
}
