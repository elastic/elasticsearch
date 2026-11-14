/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.NetworkPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.junit.After;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * The coordinating node drops a query or DFS response from a data node, as when its breaker trips while it reads the response. It never
 * learns the ids of the contexts the response names, so the data node must free them itself once the coordinating node cancels the
 * request. The keep-alive stays at its 5m default, so the reaper can't be what frees them.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class DroppedShardResponseIT extends ESIntegTestCase {

    private static final int DROPPING_SHARDS = 3;

    private static volatile String droppedAction;
    private static volatile String droppingNode;
    private static final AtomicInteger droppedResponses = new AtomicInteger();

    /**
     * Makes the coordinating node fail to read responses to {@link #droppedAction} from {@link #droppingNode}. It releases what it read,
     * so that only the contexts on the data node can be left behind.
     */
    public static class DropResponsesPlugin extends Plugin implements NetworkPlugin {
        @Override
        public List<TransportInterceptor> getTransportInterceptors(NamedWriteableRegistry registry, ThreadContext threadContext) {
            return List.of(new TransportInterceptor() {
                @Override
                public AsyncSender interceptSender(AsyncSender sender) {
                    return new AsyncSender() {
                        @Override
                        public <T extends TransportResponse> void sendRequest(
                            Transport.Connection connection,
                            String action,
                            TransportRequest request,
                            TransportRequestOptions options,
                            TransportResponseHandler<T> handler
                        ) {
                            if (action.equals(droppedAction) && connection.getNode().getName().equals(droppingNode)) {
                                sender.sendRequest(connection, action, request, options, droppingHandler(handler));
                            } else {
                                sender.sendRequest(connection, action, request, options, handler);
                            }
                        }
                    };
                }
            });
        }
    }

    private static <T extends TransportResponse> TransportResponseHandler<T> droppingHandler(TransportResponseHandler<T> handler) {
        return new TransportResponseHandler<>() {
            @Override
            public T read(StreamInput in) throws IOException {
                handler.read(in).decRef();
                droppedResponses.incrementAndGet();
                throw new CircuitBreakingException("test drops the response", CircuitBreaker.Durability.TRANSIENT);
            }

            @Override
            public Executor executor() {
                return handler.executor();
            }

            @Override
            public Releasable reserveResponseBytes(int bytes) {
                return handler.reserveResponseBytes(bytes);
            }

            @Override
            public void handleResponse(T response) {
                handler.handleResponse(response);
            }

            @Override
            public void handleException(TransportException exp) {
                handler.handleException(exp);
            }
        };
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), DropResponsesPlugin.class);
    }

    @After
    public void resetDropping() {
        droppedAction = null;
        droppingNode = null;
        droppedResponses.set(0);
    }

    public void testPerShardQueryResponseDropped() throws Exception {
        assertDroppedResponseFreesContexts(SearchTransportService.QUERY_ACTION_NAME, SearchType.QUERY_THEN_FETCH, false, false);
    }

    public void testBatchedQueryResponseDropped() throws Exception {
        assertDroppedResponseFreesContexts(
            SearchQueryThenFetchAsyncAction.NODE_SEARCH_ACTION_NAME,
            SearchType.QUERY_THEN_FETCH,
            true,
            false
        );
    }

    public void testDfsResponseDropped() throws Exception {
        assertDroppedResponseFreesContexts(SearchTransportService.DFS_ACTION_NAME, SearchType.DFS_QUERY_THEN_FETCH, randomBoolean(), false);
    }

    public void testScrollQueryResponseDropped() throws Exception {
        assertDroppedResponseFreesContexts(SearchTransportService.QUERY_ACTION_NAME, SearchType.QUERY_THEN_FETCH, false, true);
    }

    private void assertDroppedResponseFreesContexts(String action, SearchType searchType, boolean batched, boolean scroll)
        throws Exception {
        internalCluster().startMasterOnlyNode();
        final String coordinator = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        final String droppingDataNode = internalCluster().startDataOnlyNode();
        final String otherDataNode = internalCluster().startDataOnlyNode();
        updateClusterSettings(Settings.builder().put(SearchService.BATCHED_QUERY_PHASE.getKey(), batched));

        assertAcked(
            prepareCreate("dropped").setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, DROPPING_SHARDS)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put("index.routing.allocation.require._name", droppingDataNode)
            )
        );
        assertAcked(
            prepareCreate("kept").setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put("index.routing.allocation.require._name", otherDataNode)
            )
        );
        ensureGreen("dropped", "kept");
        // Enough docs that every shard has top docs, so it keeps its context for the fetch phase.
        for (int i = 0; i < 10 * DROPPING_SHARDS; i++) {
            prepareIndex("dropped").setSource("f", "v").get();
        }
        prepareIndex("kept").setSource("f", "v").get();
        refresh("dropped", "kept");

        final SearchService droppingSearchService = internalCluster().getInstance(SearchService.class, droppingDataNode);
        final SearchService otherSearchService = internalCluster().getInstance(SearchService.class, otherDataNode);
        droppingNode = droppingDataNode;
        droppedAction = action;

        final SearchRequestBuilder search = client(coordinator).prepareSearch("dropped", "kept")
            .setSearchType(searchType)
            .setSize(100)
            .setAllowPartialSearchResults(true);
        if (scroll) {
            search.setScroll(TimeValue.timeValueMinutes(1));
        }
        assertResponse(search, response -> {
            assertThat(response.getTotalShards(), equalTo(DROPPING_SHARDS + 1));
            assertThat(response.getSuccessfulShards(), equalTo(1));
            assertThat(response.getFailedShards(), equalTo(DROPPING_SHARDS));
            for (ShardSearchFailure failure : response.getShardFailures()) {
                assertThat(failure.reason(), containsString("test drops the response"));
            }
            if (scroll) {
                client(coordinator).prepareClearScroll().addScrollId(response.getScrollId()).get();
            }
        });
        assertThat(droppedResponses.get(), greaterThan(0));

        assertBusy(() -> assertThat(droppingSearchService.getActiveContexts(), equalTo(0)));
        assertBusy(() -> assertThat(otherSearchService.getActiveContexts(), equalTo(0)));
    }
}
