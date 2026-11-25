/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz;

import org.apache.logging.log4j.Level;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.plugins.NetworkPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.fetch.ShardFetchSearchRequest;
import org.elasticsearch.tasks.TaskCancellationService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executor;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;

/**
 * Verifies that the {@code internal:admin/tasks/cancel_child} action triggered by a failed shard child request during a search run
 * under a non-operator user is sent as the system user, and so authorized, rather than denied against that user.
 *
 * <p>The trigger is the chunked fetch coordination path ({@code search.fetch_phase_chunked_enabled}): {@code
 * TransportFetchPhaseCoordinationAction} stashes the thread context and propagates only request headers before sending each shard
 * fetch, so the authentication header survives but the {@code _originating_action_name} transient does not. When the fetch fails,
 * {@code TaskCancellationService#cancelChildRemote} sends {@code cancel_child} in that context. With authentication present but no
 * originating action, the action would be authorized against the calling user and denied as ungrantable ("not an index or cluster
 * action"); {@code cancelChildRemote} avoids this by stashing to a user-less context so it runs as the system user.
 *
 * <p>The coordinating node holds no shards, so every fetch - and every {@code cancel_child} - is a cross-node action seen by the
 * security layer on the data node, where a transport interceptor fails the incoming fetch to surface a transport exception.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 2, numClientNodes = 0, supportsDedicatedMasters = false)
public class ChunkedFetchCancelChildAuthzTests extends SecurityIntegTestCase {

    private static final String INDEX = "test";

    /** The {@link org.elasticsearch.transport.TransportService} transport-tracer logger (named {@code <class>.tracer}). */
    private static final String TRACER_LOGGER_NAME = TransportService.class.getName() + ".tracer";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(FailingFetchInterceptorPlugin.class);
        return List.copyOf(plugins);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            // Chunked fetch (enabled in serverless) propagates headers only, dropping the originating-action transient that
            // normally protects cancel_child.
            .put(SearchService.FETCH_PHASE_CHUNKED_ENABLED.getKey(), true)
            .build();
    }

    /**
     * When the fetch fails under a non-operator user, the resulting cross-node {@code cancel_child} must be authorized as the system
     * user. The transport-tracer "sent response" log is only emitted for a successful (authorized) response - a denial logs "sent
     * error response" - so asserting it proves the cancellation completed end-to-end and keeps the denial check below non-vacuous.
     */
    @TestLogging(
        reason = "verifying cancel_child gets a successful transport response (tracer TRACE) and is not denied (WARN)",
        value = "org.elasticsearch.xpack.security.authz.AuthorizationService:WARN,"
            + "org.elasticsearch.transport.TransportService.tracer:TRACE"
    )
    public void testCancelChildNotDeniedForUserOnFetchFailure() throws Exception {
        final Client userClient = client(createIndexAwayFromCoordinatingNode());

        try (var mockLog = MockLog.capture(TRACER_LOGGER_NAME, AuthorizationService.class.getName())) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "cancel_child got a successful transport response",
                    TRACER_LOGGER_NAME,
                    Level.TRACE,
                    "[*][" + TaskCancellationService.CANCEL_CHILD_ACTION_NAME + "] sent response"
                )
            );
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "cancel_child denied for user",
                    AuthorizationService.class.getName(),
                    Level.WARN,
                    "denying access for * as action ["
                        + TaskCancellationService.CANCEL_CHILD_ACTION_NAME
                        + "] is not an index or cluster action"
                )
            );

            final ActionFuture<SearchResponse> searchFuture = userClient.prepareSearch(INDEX).setAllowPartialSearchResults(false).execute();

            // The search fails because every shard fetch failed and partial results are disallowed.
            expectThrows(Exception.class, searchFuture::actionGet);

            // Asserts the successful cancel_child response was seen and no denial was logged.
            mockLog.awaitAllExpectationsMatched();
        }
    }

    /**
     * Creates the test index with all shards off the coordinating node, so every shard fetch - and every cancel_child - crosses the
     * network to the data node where the security layer sees it.
     *
     * @return the coordinating node name for the user client to target
     */
    private String createIndexAwayFromCoordinatingNode() {
        final String coordinatingNode = internalCluster().getNodeNames()[0];
        final int numShards = 4;
        assertAcked(
            indicesAdmin().prepareCreate(INDEX)
                .setSettings(indexSettings(numShards, 0).put("index.routing.allocation.exclude._name", coordinatingNode))
                .setMapping("field", "type=keyword")
        );
        ensureGreen(INDEX);
        indexTestData();
        return coordinatingNode;
    }

    private void indexTestData() {
        // A few documents suffice: there must be hits for the fetch phase (and thus the failing FETCH_ID request) to run at all.
        final BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < 5; i++) {
            bulk.add(prepareIndex(INDEX).setId(Integer.toString(i)).setSource("field", "value"));
        }
        assertNoFailures(bulk.get());
    }

    /**
     * Fails the incoming shard fetch for the test index, surfacing a transport exception to the coordinator that triggers the
     * cross-node cancel_child. A transport interceptor is used because security disables {@code MockTransportService} and a thrown
     * {@link org.elasticsearch.index.shard.SearchOperationListener} exception is swallowed by the composite listener.
     */
    public static class FailingFetchInterceptorPlugin extends Plugin implements NetworkPlugin {

        @Override
        public List<TransportInterceptor> getTransportInterceptors(
            NamedWriteableRegistry namedWriteableRegistry,
            ThreadContext threadContext
        ) {
            return List.of(new TransportInterceptor() {
                @Override
                public <T extends TransportRequest> TransportRequestHandler<T> interceptHandler(
                    String action,
                    Executor executor,
                    boolean forceExecution,
                    TransportRequestHandler<T> actualHandler
                ) {
                    if (SearchTransportService.FETCH_ID_ACTION_NAME.equals(action) == false) {
                        return actualHandler;
                    }
                    return (request, channel, task) -> {
                        if (request instanceof ShardFetchSearchRequest fetchRequest
                            && Arrays.asList(fetchRequest.indices()).contains(INDEX)) {
                            channel.sendResponse(new IllegalStateException("simulated fetch failure"));
                        } else {
                            actualHandler.messageReceived(request, channel, task);
                        }
                    };
                }
            });
        }
    }
}
