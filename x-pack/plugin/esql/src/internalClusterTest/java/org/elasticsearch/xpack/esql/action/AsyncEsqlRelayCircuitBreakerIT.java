/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.xpack.core.async.AsyncStopRequest;
import org.elasticsearch.xpack.core.async.GetAsyncResultRequest;
import org.elasticsearch.xpack.esql.plugin.TransportEsqlAsyncGetResultsAction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.asyncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;

/**
 * A get or stop async ES|QL query request that reaches a node other than the one that ran the query is relayed to that node. The
 * relaying node charges the response to its request breaker while it arrives, so a response it has no room for fails the request with a
 * 429. Once read, its pages stay charged until it is released. The node that ran the query charges the reply it serializes for a relayed
 * request until it is written. A stop of a query that has completed returns its stored result, like a get.
 * <p>
 * The query runs on a data node. The requests go through a coordinating only node with a {@value #RELAY_REQUEST_LIMIT} request
 * breaker, which a response with {@value #DOCS} rows doesn't fit in, and one with {@value #HELD_DOCS} rows does, both while it arrives
 * and as the pages read from it.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class AsyncEsqlRelayCircuitBreakerIT extends AbstractEsqlIntegTestCase {

    private static final String INDEX = "test";
    private static final int DOCS = 200;
    private static final int HELD_DOCS = 8;
    private static final int PAYLOAD_CHARS = 4000;
    private static final String RELAY_REQUEST_LIMIT = "200kb";

    private String owner;
    private String relay;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(MockTransportService.TestPlugin.class);
        return plugins;
    }

    private void startCluster() {
        // The test cluster can make a node's request breaker a noop one, which charges nothing.
        owner = internalCluster().startNode(Settings.builder().put("indices.breaker.request.type", "memory"));
        relay = internalCluster().startCoordinatingOnlyNode(
            Settings.builder()
                .put("indices.breaker.request.type", "memory")
                .put("indices.breaker.request.limit", RELAY_REQUEST_LIMIT)
                .build()
        );
        assertAcked(indicesAdmin().prepareCreate(INDEX).setSettings(indexSettings(1, 0)).setMapping("payload", "type=keyword"));
        BulkRequestBuilder bulk = client(owner).prepareBulk();
        for (int i = 0; i < DOCS; i++) {
            bulk.add(prepareIndex(INDEX).setSource("payload", randomAlphaOfLength(PAYLOAD_CHARS)));
        }
        assertNoFailures(bulk.get());
        refresh(INDEX);
    }

    /**
     * A response that doesn't fit is rejected by the relaying node with a 429 while it arrives, and the stored result is still there.
     */
    public void testRelayedResponseTripsWhileArriving() throws Exception {
        assertRelayedResponseTripsWhileArriving(false);
    }

    public void testRelayedStopResponseTripsWhileArriving() throws Exception {
        assertRelayedResponseTripsWhileArriving(true);
    }

    private void assertRelayedResponseTripsWhileArriving(boolean stop) throws Exception {
        startCluster();
        CircuitBreaker relayBreaker = relayBreaker();
        long tripsBefore = relayBreaker.getTrippedCount();
        String id = submit(DOCS);

        // The trip itself, not wrapped in a remote transport exception, since it is the relaying node that has no room.
        CircuitBreakingException e = expectThrows(CircuitBreakingException.class, () -> rowsThrough(relay, id, stop));
        assertThat(e.status(), equalTo(RestStatus.TOO_MANY_REQUESTS));
        assertThat(e.getMessage(), containsString("[" + TransportEsqlAsyncGetResultsAction.RELAYED_RESPONSE_BREAKER_LABEL + "]"));
        // A piece trips on arrival, which wants less than half of the response.
        assertThat(e.getBytesWanted(), lessThan((long) DOCS * PAYLOAD_CHARS / 2));
        assertThat(relayBreaker.getTrippedCount(), greaterThan(tripsBefore));
        assertBusy(() -> assertThat(relayBreaker.getUsed(), equalTo(0L)));

        assertThat(rowsThrough(owner, id, false), equalTo(DOCS));
    }

    /**
     * The pages of a relayed response that fits stay charged until it is released, and are charged once.
     */
    public void testRelayedResponseIsChargedUntilReleased() throws Exception {
        assertRelayedResponseIsChargedUntilReleased(false);
    }

    public void testRelayedStopResponseIsChargedUntilReleased() throws Exception {
        assertRelayedResponseIsChargedUntilReleased(true);
    }

    private void assertRelayedResponseIsChargedUntilReleased(boolean stop) throws Exception {
        startCluster();
        CircuitBreaker relayBreaker = relayBreaker();
        String id = submit(HELD_DOCS);

        EsqlQueryResponse response = execute(relay, id, stop);
        try {
            assertThat(rows(response), equalTo(HELD_DOCS));
            // About 1.04x the payload, so a second charge of the pages would go past the upper bound.
            assertThat(relayBreaker.getUsed(), greaterThanOrEqualTo((long) HELD_DOCS * PAYLOAD_CHARS));
            assertThat(relayBreaker.getUsed(), lessThan(2L * HELD_DOCS * PAYLOAD_CHARS));
        } finally {
            response.decRef();
        }
        assertBusy(() -> assertThat(relayBreaker.getUsed(), equalTo(0L)));
    }

    /**
     * The node that ran the query passes its request breaker for the reply to a relayed request, which charges it until it is written.
     */
    public void testOwnerPassesItsRequestBreakerForTheReply() throws Exception {
        assertOwnerPassesItsRequestBreakerForTheReply(false);
    }

    public void testOwnerPassesItsRequestBreakerForTheStopReply() throws Exception {
        assertOwnerPassesItsRequestBreakerForTheReply(true);
    }

    private void assertOwnerPassesItsRequestBreakerForTheReply(boolean stop) throws Exception {
        startCluster();
        String id = submit(HELD_DOCS);
        CircuitBreaker ownerBreaker = internalCluster().getInstance(CircuitBreakerService.class, owner).getBreaker(CircuitBreaker.REQUEST);

        AtomicReference<CircuitBreaker> replyBreaker = new AtomicReference<>();
        MockTransportService ownerTransport = MockTransportService.getInstance(owner);
        ownerTransport.addRequestHandlingBehavior(
            stop ? EsqlAsyncStopAction.NAME : EsqlAsyncGetResultAction.NAME,
            (handler, request, channel, task) -> handler.messageReceived(request, capturingBreaker(channel, replyBreaker), task)
        );
        try {
            assertThat(rowsThrough(relay, id, stop), equalTo(HELD_DOCS));
        } finally {
            ownerTransport.clearAllRules();
        }
        assertSame(ownerBreaker, replyBreaker.get());
        assertBusy(() -> assertThat(ownerBreaker.getUsed(), equalTo(0L)));
    }

    private CircuitBreaker relayBreaker() {
        return internalCluster().getInstance(CircuitBreakerService.class, relay).getBreaker(CircuitBreaker.REQUEST);
    }

    /** Gets, or stops, the query through the given node. */
    private static EsqlQueryResponse execute(String node, String id, boolean stop) {
        return (stop
            ? client(node).execute(EsqlAsyncStopAction.INSTANCE, new AsyncStopRequest(id))
            : client(node).execute(EsqlAsyncGetResultAction.INSTANCE, new GetAsyncResultRequest(id))).actionGet(SAFE_AWAIT_TIMEOUT);
    }

    /** Gets, or stops, the query through the given node, and returns the number of rows of its result. */
    private static int rowsThrough(String node, String id, boolean stop) {
        EsqlQueryResponse response = execute(node, id, stop);
        try {
            return rows(response);
        } finally {
            response.decRef();
        }
    }

    private static int rows(EsqlQueryResponse response) {
        return response.pages().stream().mapToInt(Page::getPositionCount).sum();
    }

    /**
     * Runs a query on the node that runs it and stores the result.
     */
    private String submit(int size) {
        EsqlQueryRequest request = asyncEsqlQueryRequest("FROM " + INDEX + " | KEEP payload | LIMIT " + size).keepOnCompletion(true)
            .waitForCompletionTimeout(TimeValue.timeValueSeconds(30));
        EsqlQueryResponse response = client(owner).execute(EsqlQueryAction.INSTANCE, request).actionGet(SAFE_AWAIT_TIMEOUT);
        try {
            assertFalse(response.isRunning());
            assertThat(rows(response), equalTo(size));
            return response.asyncExecutionId().orElseThrow();
        } finally {
            response.decRef();
        }
    }

    /** Records the breaker the handler passes for its reply. */
    private static TransportChannel capturingBreaker(TransportChannel channel, AtomicReference<CircuitBreaker> breaker) {
        return new TransportChannel() {
            @Override
            public String getProfileName() {
                return channel.getProfileName();
            }

            @Override
            public void sendResponse(TransportResponse response) {
                channel.sendResponse(response);
            }

            @Override
            public void sendResponse(TransportResponse response, CircuitBreaker circuitBreaker) {
                breaker.set(circuitBreaker);
                channel.sendResponse(response, circuitBreaker);
            }

            @Override
            public void sendResponse(Exception exception) {
                channel.sendResponse(exception);
            }

            @Override
            public TransportVersion getVersion() {
                return channel.getVersion();
            }
        };
    }
}
