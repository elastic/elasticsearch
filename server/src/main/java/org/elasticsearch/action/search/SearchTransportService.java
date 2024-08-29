/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.get.TransportGetTaskAction;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThrottledTaskRunner;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.QueryFetchSearchResult;
import org.elasticsearch.search.fetch.ScrollQueryFetchSearchResult;
import org.elasticsearch.search.fetch.ShardFetchRequest;
import org.elasticsearch.search.fetch.ShardFetchSearchRequest;
import org.elasticsearch.search.internal.InternalScrollSearchRequest;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.query.QuerySearchRequest;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.query.ScrollQuerySearchResult;
import org.elasticsearch.search.rank.feature.RankFeatureResult;
import org.elasticsearch.search.rank.feature.RankFeatureShardRequest;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportActionProxy;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;

import static org.elasticsearch.action.search.SearchTransportAPMMetrics.ACTION_ATTRIBUTE_NAME;
import static org.elasticsearch.action.search.SearchTransportAPMMetrics.CLEAR_SCROLL_CONTEXTS_ACTION_METRIC;
import static org.elasticsearch.action.search.SearchTransportAPMMetrics.DFS_ACTION_METRIC;
import static org.elasticsearch.action.search.SearchTransportAPMMetrics.FETCH_ID_ACTION_METRIC;
import static org.elasticsearch.action.search.SearchTransportAPMMetrics.FETCH_ID_SCROLL_ACTION_METRIC;
import static org.elasticsearch.action.search.SearchTransportAPMMetrics.FREE_CONTEXT_ACTION_METRIC;
import static org.elasticsearch.action.search.SearchTransportAPMMetrics.FREE_CONTEXT_SCROLL_ACTION_METRIC;
import static org.elasticsearch.action.search.SearchTransportAPMMetrics.QUERY_ACTION_METRIC;
import static org.elasticsearch.action.search.SearchTransportAPMMetrics.QUERY_CAN_MATCH_NODE_METRIC;
import static org.elasticsearch.action.search.SearchTransportAPMMetrics.QUERY_FETCH_SCROLL_ACTION_METRIC;
import static org.elasticsearch.action.search.SearchTransportAPMMetrics.QUERY_ID_ACTION_METRIC;
import static org.elasticsearch.action.search.SearchTransportAPMMetrics.QUERY_SCROLL_ACTION_METRIC;
import static org.elasticsearch.action.search.SearchTransportAPMMetrics.RANK_SHARD_FEATURE_ACTION_METRIC;

/**
 * An encapsulation of {@link org.elasticsearch.search.SearchService} operations exposed through
 * transport.
 */
public class SearchTransportService {

    public static final String FREE_CONTEXT_SCROLL_ACTION_NAME = "indices:data/read/search[free_context/scroll]";
    public static final String FREE_CONTEXT_ACTION_NAME = "indices:data/read/search[free_context]";
    public static final String CLEAR_SCROLL_CONTEXTS_ACTION_NAME = "indices:data/read/search[clear_scroll_contexts]";

    /**
     * Part of DFS_QUERY_THEN_FETCH, which fetches distributed term frequencies and executes KNN.
     */
    public static final String DFS_ACTION_NAME = "indices:data/read/search[phase/dfs]";
    public static final String QUERY_ACTION_NAME = "indices:data/read/search[phase/query]";

    /**
     * Part of DFS_QUERY_THEN_FETCH, which fetches distributed term frequencies and executes KNN.
     */
    public static final String QUERY_ID_ACTION_NAME = "indices:data/read/search[phase/query/id]";
    public static final String QUERY_SCROLL_ACTION_NAME = "indices:data/read/search[phase/query/scroll]";
    public static final String QUERY_FETCH_SCROLL_ACTION_NAME = "indices:data/read/search[phase/query+fetch/scroll]";
    public static final String FETCH_ID_SCROLL_ACTION_NAME = "indices:data/read/search[phase/fetch/id/scroll]";
    public static final String FETCH_ID_ACTION_NAME = "indices:data/read/search[phase/fetch/id]";

    public static final String RANK_FEATURE_SHARD_ACTION_NAME = "indices:data/read/search[phase/rank/feature]";

    /**
     * The Can-Match phase. It is executed to pre-filter shards that a search request hits. It rewrites the query on
     * the shard and checks whether the result of the rewrite matches no documents, in which case the shard can be
     * filtered out.
     */
    public static final String QUERY_CAN_MATCH_NODE_NAME = "indices:data/read/search[can_match][n]";

    private static final Logger logger = LogManager.getLogger(SearchTransportService.class);

    private final TransportService transportService;
    private final NodeClient client;
    private final BiFunction<
        Transport.Connection,
        ActionListener<? super SearchPhaseResult>,
        ActionListener<? super SearchPhaseResult>> responseWrapper;
    private final Map<String, Long> clientConnections = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();

    public SearchTransportService(
        TransportService transportService,
        NodeClient client,
        BiFunction<
            Transport.Connection,
            ActionListener<? super SearchPhaseResult>,
            ActionListener<? super SearchPhaseResult>> responseWrapper
    ) {
        this.transportService = transportService;
        this.client = client;
        this.responseWrapper = responseWrapper;
    }

    private static final ActionListenerResponseHandler<SearchFreeContextResponse> SEND_FREE_CONTEXT_LISTENER =
        new ActionListenerResponseHandler<>(
            ActionListener.noop(),
            SearchFreeContextResponse::readFrom,
            TransportResponseHandler.TRANSPORT_WORKER
        );

    public void sendFreeContext(Transport.Connection connection, final ShardSearchContextId contextId, OriginalIndices originalIndices) {
        transportService.sendRequest(
            connection,
            FREE_CONTEXT_ACTION_NAME,
            new SearchFreeContextRequest(originalIndices, contextId),
            TransportRequestOptions.EMPTY,
            // no need to respond if it was freed or not
            SEND_FREE_CONTEXT_LISTENER
        );
    }

    public void sendFreeContext(
        Transport.Connection connection,
        ShardSearchContextId contextId,
        ActionListener<SearchFreeContextResponse> listener
    ) {
        transportService.sendRequest(
            connection,
            FREE_CONTEXT_SCROLL_ACTION_NAME,
            new ScrollFreeContextRequest(contextId),
            TransportRequestOptions.EMPTY,
            new ActionListenerResponseHandler<>(listener, SearchFreeContextResponse::readFrom, TransportResponseHandler.TRANSPORT_WORKER)
        );
    }

    public void sendCanMatch(
        Transport.Connection connection,
        final CanMatchNodeRequest request,
        SearchTask task,
        final ActionListener<CanMatchNodeResponse> listener
    ) {
        transportService.sendChildRequest(
            connection,
            QUERY_CAN_MATCH_NODE_NAME,
            request,
            task,
            new ActionListenerResponseHandler<>(listener, CanMatchNodeResponse::new, TransportResponseHandler.TRANSPORT_WORKER)
        );
    }

    public void sendClearAllScrollContexts(Transport.Connection connection, final ActionListener<TransportResponse> listener) {
        transportService.sendRequest(
            connection,
            CLEAR_SCROLL_CONTEXTS_ACTION_NAME,
            new ClearScrollContextsRequest(),
            TransportRequestOptions.EMPTY,
            new ActionListenerResponseHandler<>(listener, in -> TransportResponse.Empty.INSTANCE, TransportResponseHandler.TRANSPORT_WORKER)
        );
    }

    public void sendExecuteDfs(
        Transport.Connection connection,
        final ShardSearchRequest request,
        SearchTask task,
        final ActionListener<DfsSearchResult> listener
    ) {
        transportService.sendChildRequest(
            connection,
            DFS_ACTION_NAME,
            request,
            task,
            new ConnectionCountingHandler<>(listener, DfsSearchResult::new, connection)
        );
    }

    public void sendExecuteQuery(
        Transport.Connection connection,
        final ShardSearchRequest request,
        SearchTask task,
        final ActionListener<SearchPhaseResult> listener
    ) {
        // we optimize this and expect a QueryFetchSearchResult if we only have a single shard in the search request
        // this used to be the QUERY_AND_FETCH which doesn't exist anymore.
        final boolean fetchDocuments = request.numberOfShards() == 1
            && (request.source() == null || request.source().rankBuilder() == null);
        Writeable.Reader<SearchPhaseResult> reader = fetchDocuments ? QueryFetchSearchResult::new : in -> new QuerySearchResult(in, true);

        final ActionListener<? super SearchPhaseResult> handler = responseWrapper.apply(connection, listener);
        transportService.sendChildRequest(
            connection,
            QUERY_ACTION_NAME,
            request,
            task,
            new ConnectionCountingHandler<>(handler, reader, connection)
        );
    }

    public void sendExecuteQuery(
        Transport.Connection connection,
        final QuerySearchRequest request,
        SearchTask task,
        final ActionListener<QuerySearchResult> listener
    ) {
        transportService.sendChildRequest(
            connection,
            QUERY_ID_ACTION_NAME,
            request,
            task,
            new ConnectionCountingHandler<>(listener, QuerySearchResult::new, connection)
        );
    }

    public void sendExecuteScrollQuery(
        Transport.Connection connection,
        final InternalScrollSearchRequest request,
        SearchTask task,
        final ActionListener<ScrollQuerySearchResult> listener
    ) {
        transportService.sendChildRequest(
            connection,
            QUERY_SCROLL_ACTION_NAME,
            request,
            task,
            new ConnectionCountingHandler<>(listener, ScrollQuerySearchResult::new, connection)
        );
    }

    public void sendExecuteRankFeature(
        Transport.Connection connection,
        final RankFeatureShardRequest request,
        SearchTask task,
        final ActionListener<RankFeatureResult> listener
    ) {
        transportService.sendChildRequest(
            connection,
            RANK_FEATURE_SHARD_ACTION_NAME,
            request,
            task,
            new ConnectionCountingHandler<>(listener, RankFeatureResult::new, connection)
        );
    }

    public void sendExecuteScrollFetch(
        Transport.Connection connection,
        final InternalScrollSearchRequest request,
        SearchTask task,
        final ActionListener<ScrollQueryFetchSearchResult> listener
    ) {
        transportService.sendChildRequest(
            connection,
            QUERY_FETCH_SCROLL_ACTION_NAME,
            request,
            task,
            new ConnectionCountingHandler<>(listener, ScrollQueryFetchSearchResult::new, connection)
        );
    }

    public void sendExecuteFetch(
        Transport.Connection connection,
        final ShardFetchSearchRequest request,
        SearchTask task,
        final ActionListener<FetchSearchResult> listener
    ) {
        sendExecuteFetch(connection, FETCH_ID_ACTION_NAME, request, task, listener);
    }

    public void sendExecuteFetchScroll(
        Transport.Connection connection,
        final ShardFetchRequest request,
        SearchTask task,
        final ActionListener<FetchSearchResult> listener
    ) {
        sendExecuteFetch(connection, FETCH_ID_SCROLL_ACTION_NAME, request, task, listener);
    }

    private void sendExecuteFetch(
        Transport.Connection connection,
        String action,
        final ShardFetchRequest request,
        SearchTask task,
        final ActionListener<FetchSearchResult> listener
    ) {
        transportService.sendChildRequest(
            connection,
            action,
            request,
            task,
            new ConnectionCountingHandler<>(listener, FetchSearchResult::new, connection)
        );
    }

    /**
     * Used by {@link TransportSearchAction} to send the expand queries (field collapsing).
     */
    void sendExecuteMultiSearch(final MultiSearchRequest request, SearchTask task, final ActionListener<MultiSearchResponse> listener) {
        final Transport.Connection connection = transportService.getConnection(transportService.getLocalNode());
        transportService.sendChildRequest(
            connection,
            TransportMultiSearchAction.TYPE.name(),
            request,
            task,
            new ConnectionCountingHandler<>(listener, MultiSearchResponse::new, connection)
        );
    }

    public RemoteClusterService getRemoteClusterService() {
        return transportService.getRemoteClusterService();
    }

    /**
     * Return a map of nodeId to pending number of search requests.
     * This is a snapshot of the current pending search and not a live map.
     */
    public Map<String, Long> getPendingSearchRequests() {
        return new HashMap<>(clientConnections);
    }

    static class ScrollFreeContextRequest extends TransportRequest {
        private final ShardSearchContextId contextId;

        ScrollFreeContextRequest(ShardSearchContextId contextId) {
            this.contextId = Objects.requireNonNull(contextId);
        }

        ScrollFreeContextRequest(StreamInput in) throws IOException {
            super(in);
            contextId = new ShardSearchContextId(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            contextId.writeTo(out);
        }

        public ShardSearchContextId id() {
            return this.contextId;
        }

    }

    private static class ClearScrollContextsRequest extends TransportRequest {
        ClearScrollContextsRequest() {}

        ClearScrollContextsRequest(StreamInput in) throws IOException {
            super(in);
        }
    }

    static class SearchFreeContextRequest extends ScrollFreeContextRequest implements IndicesRequest {
        private final OriginalIndices originalIndices;

        SearchFreeContextRequest(OriginalIndices originalIndices, ShardSearchContextId id) {
            super(id);
            this.originalIndices = originalIndices;
        }

        SearchFreeContextRequest(StreamInput in) throws IOException {
            super(in);
            originalIndices = OriginalIndices.readOriginalIndices(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            OriginalIndices.writeOriginalIndices(originalIndices, out);
        }

        @Override
        public String[] indices() {
            if (originalIndices == null) {
                return null;
            }
            return originalIndices.indices();
        }

        @Override
        public IndicesOptions indicesOptions() {
            if (originalIndices == null) {
                return null;
            }
            return originalIndices.indicesOptions();
        }

    }

    public static class SearchFreeContextResponse extends TransportResponse {

        private static final SearchFreeContextResponse FREED = new SearchFreeContextResponse(true);
        private static final SearchFreeContextResponse NOT_FREED = new SearchFreeContextResponse(false);

        private final boolean freed;

        static SearchFreeContextResponse readFrom(StreamInput in) throws IOException {
            return of(in.readBoolean());
        }

        static SearchFreeContextResponse of(boolean freed) {
            return freed ? FREED : NOT_FREED;
        }

        private SearchFreeContextResponse(boolean freed) {
            this.freed = freed;
        }

        public boolean isFreed() {
            return freed;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(freed);
        }
    }

    public static void registerRequestHandler(
        TransportService transportService,
        SearchService searchService,
        SearchTransportAPMMetrics searchTransportMetrics
    ) {
        final TransportRequestHandler<ScrollFreeContextRequest> freeContextHandler = (request, channel, task) -> {
            logger.trace("releasing search context [{}]", request.id());
            boolean freed = searchService.freeReaderContext(request.id());
            channel.sendResponse(SearchFreeContextResponse.of(freed));
        };
        final Executor freeContextExecutor = buildFreeContextExecutor(transportService);
        transportService.registerRequestHandler(
            FREE_CONTEXT_SCROLL_ACTION_NAME,
            freeContextExecutor,
            ScrollFreeContextRequest::new,
            instrumentedHandler(FREE_CONTEXT_SCROLL_ACTION_METRIC, transportService, searchTransportMetrics, freeContextHandler)
        );
        TransportActionProxy.registerProxyAction(
            transportService,
            FREE_CONTEXT_SCROLL_ACTION_NAME,
            false,
            SearchFreeContextResponse::readFrom
        );

        transportService.registerRequestHandler(
            FREE_CONTEXT_ACTION_NAME,
            freeContextExecutor,
            SearchFreeContextRequest::new,
            instrumentedHandler(FREE_CONTEXT_ACTION_METRIC, transportService, searchTransportMetrics, freeContextHandler)
        );
        TransportActionProxy.registerProxyAction(transportService, FREE_CONTEXT_ACTION_NAME, false, SearchFreeContextResponse::readFrom);

        transportService.registerRequestHandler(
            CLEAR_SCROLL_CONTEXTS_ACTION_NAME,
            freeContextExecutor,
            ClearScrollContextsRequest::new,
            instrumentedHandler(CLEAR_SCROLL_CONTEXTS_ACTION_METRIC, transportService, searchTransportMetrics, (request, channel, task) -> {
                searchService.freeAllScrollContexts();
                channel.sendResponse(TransportResponse.Empty.INSTANCE);
            })
        );
        TransportActionProxy.registerProxyAction(
            transportService,
            CLEAR_SCROLL_CONTEXTS_ACTION_NAME,
            false,
            (in) -> TransportResponse.Empty.INSTANCE
        );

        transportService.registerRequestHandler(
            DFS_ACTION_NAME,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            ShardSearchRequest::new,
            instrumentedHandler(
                DFS_ACTION_METRIC,
                transportService,
                searchTransportMetrics,
                (request, channel, task) -> searchService.executeDfsPhase(
                    request,
                    (SearchShardTask) task,
                    new ChannelActionListener<>(channel)
                )
            )
        );
        TransportActionProxy.registerProxyAction(transportService, DFS_ACTION_NAME, true, DfsSearchResult::new);

        transportService.registerRequestHandler(
            QUERY_ACTION_NAME,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            ShardSearchRequest::new,
            instrumentedHandler(
                QUERY_ACTION_METRIC,
                transportService,
                searchTransportMetrics,
                (request, channel, task) -> searchService.executeQueryPhase(
                    request,
                    (SearchShardTask) task,
                    new ChannelActionListener<>(channel)
                )
            )
        );
        TransportActionProxy.registerProxyActionWithDynamicResponseType(
            transportService,
            QUERY_ACTION_NAME,
            true,
            (request) -> ((ShardSearchRequest) request).numberOfShards() == 1 ? QueryFetchSearchResult::new : QuerySearchResult::new
        );

        transportService.registerRequestHandler(
            QUERY_ID_ACTION_NAME,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            QuerySearchRequest::new,
            instrumentedHandler(
                QUERY_ID_ACTION_METRIC,
                transportService,
                searchTransportMetrics,
                (request, channel, task) -> searchService.executeQueryPhase(
                    request,
                    (SearchShardTask) task,
                    new ChannelActionListener<>(channel)
                )
            )
        );
        TransportActionProxy.registerProxyAction(transportService, QUERY_ID_ACTION_NAME, true, QuerySearchResult::new);

        transportService.registerRequestHandler(
            QUERY_SCROLL_ACTION_NAME,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            InternalScrollSearchRequest::new,
            instrumentedHandler(
                QUERY_SCROLL_ACTION_METRIC,
                transportService,
                searchTransportMetrics,
                (request, channel, task) -> searchService.executeQueryPhase(
                    request,
                    (SearchShardTask) task,
                    new ChannelActionListener<>(channel)
                )
            )
        );
        TransportActionProxy.registerProxyAction(transportService, QUERY_SCROLL_ACTION_NAME, true, ScrollQuerySearchResult::new);

        transportService.registerRequestHandler(
            QUERY_FETCH_SCROLL_ACTION_NAME,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            InternalScrollSearchRequest::new,
            instrumentedHandler(
                QUERY_FETCH_SCROLL_ACTION_METRIC,
                transportService,
                searchTransportMetrics,
                (request, channel, task) -> searchService.executeFetchPhase(
                    request,
                    (SearchShardTask) task,
                    new ChannelActionListener<>(channel)
                )
            )
        );
        TransportActionProxy.registerProxyAction(transportService, QUERY_FETCH_SCROLL_ACTION_NAME, true, ScrollQueryFetchSearchResult::new);

        final TransportRequestHandler<RankFeatureShardRequest> rankShardFeatureRequest = (request, channel, task) -> searchService
            .executeRankFeaturePhase(request, (SearchShardTask) task, new ChannelActionListener<>(channel));
        transportService.registerRequestHandler(
            RANK_FEATURE_SHARD_ACTION_NAME,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            RankFeatureShardRequest::new,
            instrumentedHandler(RANK_SHARD_FEATURE_ACTION_METRIC, transportService, searchTransportMetrics, rankShardFeatureRequest)
        );
        TransportActionProxy.registerProxyAction(transportService, RANK_FEATURE_SHARD_ACTION_NAME, true, RankFeatureResult::new);

        final TransportRequestHandler<ShardFetchRequest> shardFetchRequestHandler = (request, channel, task) -> searchService
            .executeFetchPhase(request, (SearchShardTask) task, new ChannelActionListener<>(channel));
        transportService.registerRequestHandler(
            FETCH_ID_SCROLL_ACTION_NAME,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            ShardFetchRequest::new,
            instrumentedHandler(FETCH_ID_SCROLL_ACTION_METRIC, transportService, searchTransportMetrics, shardFetchRequestHandler)
        );
        TransportActionProxy.registerProxyAction(transportService, FETCH_ID_SCROLL_ACTION_NAME, true, FetchSearchResult::new);

        transportService.registerRequestHandler(
            FETCH_ID_ACTION_NAME,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            true,
            true,
            ShardFetchSearchRequest::new,
            instrumentedHandler(FETCH_ID_ACTION_METRIC, transportService, searchTransportMetrics, shardFetchRequestHandler)
        );
        TransportActionProxy.registerProxyAction(transportService, FETCH_ID_ACTION_NAME, true, FetchSearchResult::new);

        transportService.registerRequestHandler(
            QUERY_CAN_MATCH_NODE_NAME,
            transportService.getThreadPool().executor(ThreadPool.Names.SEARCH_COORDINATION),
            CanMatchNodeRequest::new,
            instrumentedHandler(
                QUERY_CAN_MATCH_NODE_METRIC,
                transportService,
                searchTransportMetrics,
                (request, channel, task) -> searchService.canMatch(request, new ChannelActionListener<>(channel))
            )
        );
        TransportActionProxy.registerProxyAction(transportService, QUERY_CAN_MATCH_NODE_NAME, true, CanMatchNodeResponse::new);
    }

    private static Executor buildFreeContextExecutor(TransportService transportService) {
        final ThrottledTaskRunner throttledTaskRunner = new ThrottledTaskRunner(
            "free_context",
            1,
            transportService.getThreadPool().generic()
        );
        return r -> throttledTaskRunner.enqueueTask(new ActionListener<>() {
            @Override
            public void onResponse(Releasable releasable) {
                try (releasable) {
                    r.run();
                }
            }

            @Override
            public void onFailure(Exception e) {
                if (r instanceof AbstractRunnable abstractRunnable) {
                    abstractRunnable.onFailure(e);
                }
                // should be impossible, GENERIC pool doesn't reject anything
                logger.error("unexpected failure running " + r, e);
                assert false : new AssertionError("unexpected failure running " + r, e);
            }
        });
    }

    private static <Request extends TransportRequest> TransportRequestHandler<Request> instrumentedHandler(
        String actionQualifier,
        TransportService transportService,
        SearchTransportAPMMetrics searchTransportMetrics,
        TransportRequestHandler<Request> transportRequestHandler
    ) {
        var threadPool = transportService.getThreadPool();
        var latencies = searchTransportMetrics.getActionLatencies();
        Map<String, Object> attributes = Map.of(ACTION_ATTRIBUTE_NAME, actionQualifier);
        return (request, channel, task) -> {
            var startTime = threadPool.relativeTimeInMillis();
            try {
                transportRequestHandler.messageReceived(request, channel, task);
            } finally {
                var elapsedTime = threadPool.relativeTimeInMillis() - startTime;
                latencies.record(elapsedTime, attributes);
            }
        };
    }

    /**
     * Returns a connection to the given node on the provided cluster. If the cluster alias is <code>null</code> the node will be resolved
     * against the local cluster.
     *
     * @param clusterAlias the cluster alias the node should be resolved against
     * @param node         the node to resolve
     * @return a connection to the given node belonging to the cluster with the provided alias.
     */
    public Transport.Connection getConnection(@Nullable String clusterAlias, DiscoveryNode node) {
        if (clusterAlias == null) {
            return transportService.getConnection(node);
        } else {
            return transportService.getRemoteClusterService().getConnection(node, clusterAlias);
        }
    }

    private final class ConnectionCountingHandler<Response extends TransportResponse> extends ActionListenerResponseHandler<Response> {
        private final String nodeId;

        ConnectionCountingHandler(
            final ActionListener<? super Response> listener,
            final Writeable.Reader<Response> responseReader,
            final Transport.Connection connection
        ) {
            super(listener, responseReader, TransportResponseHandler.TRANSPORT_WORKER);
            this.nodeId = connection.getNode().getId();
            // Increment the number of connections for this node by one
            clientConnections.compute(nodeId, (id, conns) -> conns == null ? 1 : conns + 1);
        }

        @Override
        public void handleResponse(Response response) {
            super.handleResponse(response);
            decConnectionCount();
        }

        @Override
        public void handleException(TransportException e) {
            super.handleException(e);
            decConnectionCount();
        }

        // Decrement the number of connections or remove it entirely if there are no more connections
        // We need to remove the entry here so we don't leak when nodes go away forever
        private void decConnectionCount() {
            assert assertNodePresent();
            clientConnections.computeIfPresent(nodeId, (id, conns) -> conns == 1 ? null : conns - 1);
        }

        private boolean assertNodePresent() {
            var conns = clientConnections.get(nodeId);
            assert conns != null : "number of connections for " + nodeId + " is null, but should be an integer";
            assert conns >= 1 : "number of connections for " + nodeId + " should be >= 1 but was " + conns;
            // Always return true, there is additional asserting here, the boolean is just so this
            // can be skipped when assertions are not enabled
            return true;
        }
    }

    public void cancelSearchTask(SearchTask task, String reason) {
        CancelTasksRequest req = new CancelTasksRequest().setTargetTaskId(new TaskId(client.getLocalNodeId(), task.getId()))
            .setReason("Fatal failure during search: " + reason);
        // force the origin to execute the cancellation as a system user
        new OriginSettingClient(client, TransportGetTaskAction.TASKS_ORIGIN).admin().cluster().cancelTasks(req, ActionListener.noop());
    }
}
