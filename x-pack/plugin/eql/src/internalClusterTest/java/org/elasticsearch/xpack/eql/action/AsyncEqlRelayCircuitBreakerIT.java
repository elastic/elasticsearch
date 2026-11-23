/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.xpack.core.async.GetAsyncResultRequest;
import org.elasticsearch.xpack.core.eql.EqlAsyncActionNames;
import org.elasticsearch.xpack.eql.plugin.EqlAsyncGetResultAction;
import org.elasticsearch.xpack.eql.plugin.TransportEqlAsyncGetResultsAction;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;

/**
 * A get async EQL search request that reaches a node other than the one that ran the search is relayed to that node. The relaying node
 * charges the response to its request breaker while it arrives and again while its listener runs, so a response it has no room for
 * fails the request with a 429. The node that ran the search charges the reply it serializes for a relayed request until it is written.
 * <p>
 * The search runs on a data node. The get requests go through a coordinating only node with a {@value #RELAY_REQUEST_LIMIT} request
 * breaker, which a response with {@value #DOCS} events doesn't fit in, and one with {@value #HELD_DOCS} events does.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class AsyncEqlRelayCircuitBreakerIT extends AbstractEqlIntegTestCase {

    private static final String INDEX = "test";
    private static final int DOCS = 200;
    private static final int HELD_DOCS = 8;
    private static final int PAYLOAD_CHARS = 4000;
    private static final String RELAY_REQUEST_LIMIT = "50kb";

    private String owner;
    private String relay;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateEQLXPackPlugin.class, MockTransportService.TestPlugin.class);
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
        assertAcked(
            indicesAdmin().prepareCreate(INDEX)
                .setSettings(indexSettings(1, 0))
                .setMapping("event_type", "type=keyword", "@timestamp", "type=date", "payload", "type=keyword,index=false,doc_values=false")
        );
        BulkRequestBuilder bulk = client(owner).prepareBulk();
        for (int i = 0; i < DOCS; i++) {
            bulk.add(
                prepareIndex(INDEX).setSource("event_type", "my_event", "@timestamp", i, "payload", randomAlphaOfLength(PAYLOAD_CHARS))
            );
        }
        assertNoFailures(bulk.get());
        refresh(INDEX);
    }

    /**
     * A response that doesn't fit is rejected by the relaying node with a 429 while it arrives, and the stored result is still there.
     */
    public void testRelayedResponseTripsWhileArriving() throws Exception {
        startCluster();
        CircuitBreaker relayBreaker = relayBreaker();
        long tripsBefore = relayBreaker.getTrippedCount();
        String id = submit(DOCS);

        // The trip itself, not wrapped in a remote transport exception, since it is the relaying node that has no room.
        CircuitBreakingException e = expectThrows(CircuitBreakingException.class, () -> eventsThrough(relay, id));
        assertThat(e.status(), equalTo(RestStatus.TOO_MANY_REQUESTS));
        assertThat(e.getMessage(), containsString("[" + TransportEqlAsyncGetResultsAction.RELAYED_RESPONSE_BREAKER_LABEL + "]"));
        // A piece trips on arrival, which wants less than half of the response.
        assertThat(e.getBytesWanted(), lessThan((long) DOCS * PAYLOAD_CHARS / 2));
        assertThat(relayBreaker.getTrippedCount(), greaterThan(tripsBefore));
        assertBusy(() -> assertThat(relayBreaker.getUsed(), equalTo(0L)));

        assertThat(eventsThrough(owner, id), equalTo(DOCS));
    }

    /**
     * A relayed response that fits stays charged while the listener runs, and is released once it returns.
     */
    public void testRelayedResponseIsChargedWhileTheListenerRuns() throws Exception {
        startCluster();
        CircuitBreaker relayBreaker = relayBreaker();
        String id = submit(HELD_DOCS);

        AtomicLong usedInListener = new AtomicLong(-1);
        EqlSearchResponse response = safeAwait(
            l -> client(relay).execute(EqlAsyncGetResultAction.INSTANCE, new GetAsyncResultRequest(id), l.map(r -> {
                usedInListener.set(relayBreaker.getUsed());
                // The transport releases the response once the listener returns.
                r.mustIncRef();
                return r;
            }))
        );
        try {
            assertThat(response.hits().events().size(), equalTo(HELD_DOCS));
        } finally {
            response.decRef();
        }
        // The size the response arrived with, which includes the source of every event.
        assertThat(usedInListener.get(), greaterThanOrEqualTo((long) HELD_DOCS * PAYLOAD_CHARS));
        assertBusy(() -> assertThat(relayBreaker.getUsed(), equalTo(0L)));
    }

    /**
     * The node that ran the search passes its request breaker for the reply to a relayed request, which charges it until it is written.
     */
    public void testOwnerPassesItsRequestBreakerForTheReply() throws Exception {
        startCluster();
        String id = submit(HELD_DOCS);
        CircuitBreaker ownerBreaker = internalCluster().getInstance(CircuitBreakerService.class, owner).getBreaker(CircuitBreaker.REQUEST);

        AtomicReference<CircuitBreaker> replyBreaker = new AtomicReference<>();
        MockTransportService ownerTransport = MockTransportService.getInstance(owner);
        ownerTransport.addRequestHandlingBehavior(
            EqlAsyncActionNames.EQL_ASYNC_GET_RESULT_ACTION_NAME,
            (handler, request, channel, task) -> handler.messageReceived(request, capturingBreaker(channel, replyBreaker), task)
        );
        try {
            assertThat(eventsThrough(relay, id), equalTo(HELD_DOCS));
        } finally {
            ownerTransport.clearAllRules();
        }
        assertSame(ownerBreaker, replyBreaker.get());
        assertBusy(() -> assertThat(ownerBreaker.getUsed(), equalTo(0L)));
    }

    private CircuitBreaker relayBreaker() {
        return internalCluster().getInstance(CircuitBreakerService.class, relay).getBreaker(CircuitBreaker.REQUEST);
    }

    /** Gets the stored result through the given node, and returns the number of its events. */
    private static int eventsThrough(String node, String id) {
        EqlSearchResponse response = client(node).execute(EqlAsyncGetResultAction.INSTANCE, new GetAsyncResultRequest(id))
            .actionGet(SAFE_AWAIT_TIMEOUT);
        try {
            return response.hits().events().size();
        } finally {
            response.decRef();
        }
    }

    /** Runs a search on the node that runs it and stores the result. */
    private String submit(int size) {
        EqlSearchRequest request = new EqlSearchRequest().indices(INDEX)
            .query("my_event where true")
            .eventCategoryField("event_type")
            .size(size)
            .keepOnCompletion(true)
            .waitForCompletionTimeout(TimeValue.timeValueSeconds(30));
        EqlSearchResponse response = client(owner).execute(EqlSearchAction.INSTANCE, request).actionGet(SAFE_AWAIT_TIMEOUT);
        try {
            assertFalse(response.isRunning());
            assertThat(response.hits().events().size(), equalTo(size));
            return response.id();
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
