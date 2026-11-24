/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.TransportMessageListener;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
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
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notNullValue;

/**
 * The coordinator charges its request breaker for each serialized request that carries the sub-plan result to a remote data
 * node, until that request has been written. A dedicated coordinator sends every request remotely, and the trip tests fill
 * the breaker once the first main-plan request has been written, so only the second one trips.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 2, numClientNodes = 0, supportsDedicatedMasters = false)
public class SerializedPlanBreakerIT extends AbstractEsqlIntegTestCase {

    static final int NUM_DOCS = 15_000;
    static final String QUERY_TAIL = " | INLINE STATS c = COUNT(*) BY k | STATS n = COUNT(*), s = SUM(c)";
    /** The breaker label of the transport's serialized outbound messages. */
    static final String SERIALIZED_REQUEST_LABEL = "RecyclerBytesStreamOutput";
    /** Well below the sub-plan result of {@link #NUM_DOCS} {@link #key keys}, and above what the rest of the query allocates. */
    private static final ByteSizeValue HEADROOM = ByteSizeValue.ofMb(1);

    /**
     * A unique key with a random suffix. The test cluster sometimes compresses transport messages, and only the compressed bytes
     * are charged, so the keys must not compress below {@link #HEADROOM}.
     */
    static String key(int i) {
        return i + randomAlphaOfLength(200);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(MockTransportService.TestPlugin.class);
        return plugins;
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
     * Fills {@code breaker} once the {@code n}th {@code action} request has been written, leaving less headroom than the
     * sub-plan result. INLINE STATS sends one request per target for its sub-plan, which carries no local data, then one per
     * target for the main plan. The requests are serialized one at a time, and the fill waits until the {@code n}th is
     * written, when its own charge is gone, so the next request is the one that trips.
     */
    static Releasable fillBreakerAfterNthSend(MockTransportService transportService, CircuitBreaker breaker, String action, int n) {
        AtomicInteger sends = new AtomicInteger();
        AtomicLong nthRequestId = new AtomicLong(-1);
        CountDownLatch nthWritten = new CountDownLatch(1);
        AtomicLong filled = new AtomicLong();
        TransportMessageListener writtenListener = new TransportMessageListener() {
            @Override
            public void onRequestSent(
                DiscoveryNode node,
                long requestId,
                String sentAction,
                TransportRequest request,
                TransportRequestOptions options,
                int networkMessageSize
            ) {
                if (requestId == nthRequestId.get()) {
                    nthWritten.countDown();
                }
            }
        };
        transportService.addMessageListener(writtenListener);
        Object serializeOneAtATime = new Object();
        transportService.addSendBehavior((connection, requestId, sentAction, request, options) -> {
            if (action.equals(sentAction) == false) {
                connection.sendRequest(requestId, sentAction, request, options);
                return;
            }
            synchronized (serializeOneAtATime) {
                if (sends.incrementAndGet() != n) {
                    connection.sendRequest(requestId, sentAction, request, options);
                    return;
                }
                nthRequestId.set(requestId);
                connection.sendRequest(requestId, sentAction, request, options);
                safeAwait(nthWritten);
                long fill = breaker.getLimit() - breaker.getUsed() - HEADROOM.getBytes();
                breaker.addWithoutBreaking(fill);
                filled.set(fill);
            }
        });
        return () -> {
            transportService.clearAllRules();
            transportService.removeMessageListener(writtenListener);
            breaker.addWithoutBreaking(-filled.getAndSet(0));
        };
    }

    static void assertTrippedOn(Exception e, String label) {
        assertThat(ExceptionsHelper.status(e), equalTo(RestStatus.TOO_MANY_REQUESTS));
        Throwable cause = ExceptionsHelper.unwrap(e, CircuitBreakingException.class);
        assertThat(e.toString(), cause, notNullValue());
        assertThat(cause.getMessage(), containsString("data for [" + label + "]"));
    }

    public void testTripFailsQueryWithoutPartialResults() {
        String coordinator = setupCoordinatorAndIndex();
        try (Releasable filled = fillCoordinatorBreaker(coordinator)) {
            Exception e = expectThrows(Exception.class, () -> run(coordinator, false).close());
            assertTrippedOn(e, SERIALIZED_REQUEST_LABEL);
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
            assertThat(local.getFailures().toString(), containsString("data for [" + SERIALIZED_REQUEST_LABEL + "]"));
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
            bulk.add(prepareIndex("idx").setSource("k", key(i)));
        }
        assertFalse(bulk.get().hasFailures());
        return coordinator;
    }

    private Releasable fillCoordinatorBreaker(String coordinator) {
        return fillBreakerAfterNthSend(
            coordinatorTransport(coordinator),
            coordinatorBreaker(coordinator),
            ComputeService.DATA_ACTION_NAME,
            3
        );
    }

    private MockTransportService coordinatorTransport(String coordinator) {
        return (MockTransportService) internalCluster().getInstance(TransportService.class, coordinator);
    }

    private CircuitBreaker coordinatorBreaker(String coordinator) {
        return internalCluster().getInstance(CircuitBreakerService.class, coordinator).getBreaker(CircuitBreaker.REQUEST);
    }

    private EsqlQueryResponse run(String coordinator, boolean allowPartialResults) {
        var request = syncEsqlQueryRequest("FROM idx" + QUERY_TAIL).allowPartialResults(allowPartialResults);
        return client(coordinator).execute(EsqlQueryAction.INSTANCE, request).actionGet(DEFAULT_REQUEST_TIMEOUT);
    }
}
