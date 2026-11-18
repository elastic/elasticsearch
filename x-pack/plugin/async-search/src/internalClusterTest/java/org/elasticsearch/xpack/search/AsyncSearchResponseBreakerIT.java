/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.search;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.xpack.core.async.GetAsyncResultRequest;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.elasticsearch.xpack.core.search.action.GetAsyncSearchAction;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchAction;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchRequest;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Tests the request breaker for a get async search forwarded to the node that runs the search. The search runs on its own coordinating
 * node, so a get sent to the other one is forwarded. The search node passes its breaker for the response it sends, and the other node
 * reserves the response until it is read.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1, numClientNodes = 0)
public class AsyncSearchResponseBreakerIT extends AsyncSearchIntegTestCase {

    private static final String INDEX = "test";
    private static final int NUM_DOCS = 20;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(MockTransportService.TestPlugin.class);
        return plugins;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            // The test cluster randomly makes the request breaker a noop one.
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_TYPE_SETTING.getKey(), "memory")
            .build();
    }

    public void testForwardedGetPassesRequestBreakerAndReleasesIt() throws Exception {
        String searchNode = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        String getNode = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        indexDocs();
        String id = submitAndStore(searchNode, NUM_DOCS);

        AtomicReference<CircuitBreaker> responseBreaker = captureResponseBreaker(searchNode, GetAsyncSearchAction.NAME);
        AsyncSearchResponse response = client(getNode).execute(GetAsyncSearchAction.INSTANCE, new GetAsyncResultRequest(id)).get();
        try {
            assertThat(response.getSearchResponse().getHits().getHits().length, equalTo(NUM_DOCS));
        } finally {
            response.decRef();
        }
        assertSame(requestBreaker(searchNode), responseBreaker.get());
        assertBusy(() -> {
            for (String node : internalCluster().getNodeNames()) {
                assertThat(node, requestBreaker(node).getUsed(), equalTo(0L));
            }
        });
        deleteAsyncSearch(id);
    }

    public void testForwardedGetFailsWhenResponseDoesNotFitOnReceivingNode() throws Exception {
        String searchNode = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        // 20 hits of 4KB are well over the limit.
        String getNode = internalCluster().startCoordinatingOnlyNode(
            Settings.builder().put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "10kb").build()
        );
        indexDocs();

        // Control: a response without hits fits, so the rejection below comes from the size.
        String smallId = submitAndStore(searchNode, 0);
        AsyncSearchResponse small = client(getNode).execute(GetAsyncSearchAction.INSTANCE, new GetAsyncResultRequest(smallId)).get();
        small.decRef();
        deleteAsyncSearch(smallId);

        String id = submitAndStore(searchNode, NUM_DOCS);
        CircuitBreakingException e = expectThrows(
            CircuitBreakingException.class,
            () -> client(getNode).execute(GetAsyncSearchAction.INSTANCE, new GetAsyncResultRequest(id)).actionGet()
        );
        assertThat(e.status(), equalTo(RestStatus.TOO_MANY_REQUESTS));
        // The search node still releases its charge when the write completes.
        assertBusy(() -> {
            assertThat(requestBreaker(getNode).getUsed(), equalTo(0L));
            assertThat(requestBreaker(searchNode).getUsed(), equalTo(0L));
        });
        deleteAsyncSearch(id);
    }

    public void testForwardedGetReturnsFailureOfSearchNode() throws Exception {
        String searchNode = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        String getNode = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        indexDocs();
        String id = submitAndStore(searchNode, NUM_DOCS);
        // A deleted search makes the search node answer with a ResourceNotFoundException.
        deleteAsyncSearch(id);

        AtomicBoolean forwarded = new AtomicBoolean();
        MockTransportService.getInstance(searchNode)
            .addRequestHandlingBehavior(GetAsyncSearchAction.NAME, (handler, request, channel, task) -> {
                forwarded.set(true);
                handler.messageReceived(request, channel, task);
            });
        Exception e = expectThrows(
            Exception.class,
            () -> client(getNode).execute(GetAsyncSearchAction.INSTANCE, new GetAsyncResultRequest(id)).actionGet()
        );
        assertTrue(forwarded.get());
        assertThat(ExceptionsHelper.unwrapCause(e), instanceOf(ResourceNotFoundException.class));
    }

    private void indexDocs() {
        createIndex(INDEX, indexSettings(1, 0).build());
        List<IndexRequestBuilder> docs = new ArrayList<>();
        for (int i = 0; i < NUM_DOCS; i++) {
            docs.add(prepareIndex(INDEX).setSource("f", randomAlphaOfLength(4096)));
        }
        indexRandom(true, docs);
    }

    /**
     * Runs a search with {@code size} hits on {@code node} and stores its response, so a get on another node is forwarded to it.
     */
    private String submitAndStore(String node, int size) throws Exception {
        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(new SearchSourceBuilder().size(size), INDEX)
            .setWaitForCompletionTimeout(TimeValue.timeValueSeconds(30))
            .setKeepOnCompletion(true);
        AsyncSearchResponse response = client(node).execute(SubmitAsyncSearchAction.INSTANCE, request).get();
        try {
            assertFalse(response.isRunning());
            assertNotNull(response.getId());
            return response.getId();
        } finally {
            response.decRef();
        }
    }

    /**
     * Records the breaker {@code node} passes when it sends a response for {@code action}.
     */
    private static AtomicReference<CircuitBreaker> captureResponseBreaker(String node, String action) {
        AtomicReference<CircuitBreaker> responseBreaker = new AtomicReference<>();
        MockTransportService.getInstance(node)
            .addRequestHandlingBehavior(
                action,
                (handler, request, channel, task) -> handler.messageReceived(request, new TransportChannel() {
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
                        responseBreaker.set(circuitBreaker);
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
                }, task)
            );
        return responseBreaker;
    }

    private static CircuitBreaker requestBreaker(String node) {
        return internalCluster().getInstance(CircuitBreakerService.class, node).getBreaker(CircuitBreaker.REQUEST);
    }
}
