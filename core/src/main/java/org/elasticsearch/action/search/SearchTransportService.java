/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.QueryFetchSearchResult;
import org.elasticsearch.search.fetch.ScrollQueryFetchSearchResult;
import org.elasticsearch.search.fetch.ShardFetchRequest;
import org.elasticsearch.search.fetch.ShardFetchSearchRequest;
import org.elasticsearch.search.internal.InternalScrollSearchRequest;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.internal.ShardSearchTransportRequest;
import org.elasticsearch.search.query.QuerySearchRequest;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.query.ScrollQuerySearchResult;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.TaskAwareTransportRequestHandler;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportActionProxy;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * An encapsulation of {@link org.elasticsearch.search.SearchService} operations exposed through
 * transport.
 */
public class SearchTransportService extends AbstractComponent {

    public static final String FREE_CONTEXT_SCROLL_ACTION_NAME = "indices:data/read/search[free_context/scroll]";
    public static final String FREE_CONTEXT_ACTION_NAME = "indices:data/read/search[free_context]";
    public static final String CLEAR_SCROLL_CONTEXTS_ACTION_NAME = "indices:data/read/search[clear_scroll_contexts]";
    public static final String DFS_ACTION_NAME = "indices:data/read/search[phase/dfs]";
    public static final String QUERY_ACTION_NAME = "indices:data/read/search[phase/query]";
    public static final String QUERY_ID_ACTION_NAME = "indices:data/read/search[phase/query/id]";
    public static final String QUERY_SCROLL_ACTION_NAME = "indices:data/read/search[phase/query/scroll]";
    public static final String QUERY_FETCH_SCROLL_ACTION_NAME = "indices:data/read/search[phase/query+fetch/scroll]";
    public static final String FETCH_ID_SCROLL_ACTION_NAME = "indices:data/read/search[phase/fetch/id/scroll]";
    public static final String FETCH_ID_ACTION_NAME = "indices:data/read/search[phase/fetch/id]";
    public static final String QUERY_CAN_MATCH_NAME = "indices:data/read/search[can_match]";

    private final TransportService transportService;
    private final BiFunction<Transport.Connection, SearchActionListener, ActionListener> responseWrapper;
    private final Map<String, Long> clientConnections = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();

    public SearchTransportService(Settings settings, TransportService transportService,
                                  BiFunction<Transport.Connection, SearchActionListener, ActionListener> responseWrapper) {
        super(settings);
        this.transportService = transportService;
        this.responseWrapper = responseWrapper;
    }

    public Map<String, Long> getClientConnections() {
        return Collections.unmodifiableMap(clientConnections);
    }

    public void sendFreeContext(Transport.Connection connection, final long contextId, OriginalIndices originalIndices) {
        transportService.sendRequest(connection, FREE_CONTEXT_ACTION_NAME, new SearchFreeContextRequest(originalIndices, contextId),
            TransportRequestOptions.EMPTY, new ActionListenerResponseHandler<>(new ActionListener<SearchFreeContextResponse>() {
                @Override
                public void onResponse(SearchFreeContextResponse response) {
                    // no need to respond if it was freed or not
                }

                @Override
                public void onFailure(Exception e) {

                }
            }, SearchFreeContextResponse::new));
    }

    public void sendFreeContext(Transport.Connection connection, long contextId, final ActionListener<SearchFreeContextResponse> listener) {
        transportService.sendRequest(connection, FREE_CONTEXT_SCROLL_ACTION_NAME, new ScrollFreeContextRequest(contextId),
            TransportRequestOptions.EMPTY, new ActionListenerResponseHandler<>(listener, SearchFreeContextResponse::new));
    }

    public void sendCanMatch(Transport.Connection connection, final ShardSearchTransportRequest request, SearchTask task, final
                            ActionListener<CanMatchResponse> listener) {
        if (connection.getNode().getVersion().onOrAfter(Version.V_5_6_0)) {
            transportService.sendChildRequest(connection, QUERY_CAN_MATCH_NAME, request, task,
                TransportRequestOptions.EMPTY, new ActionListenerResponseHandler<>(listener, CanMatchResponse::new));
        } else {
            // this might look weird but if we are in a CrossClusterSearch environment we can get a connection
            // to a pre 5.latest node which is proxied by a 5.latest node under the hood since we are only compatible with 5.latest
            // instead of sending the request we shortcut it here and let the caller deal with this -- see #25704
            // also failing the request instead of returning a fake answer might trigger a retry on a replica which might be on a
            // compatible node
            throw new IllegalArgumentException("can_match is not supported on pre 5.6 nodes");
        }
    }

    public void sendClearAllScrollContexts(Transport.Connection connection, final ActionListener<TransportResponse> listener) {
        transportService.sendRequest(connection, CLEAR_SCROLL_CONTEXTS_ACTION_NAME, TransportRequest.Empty.INSTANCE,
            TransportRequestOptions.EMPTY, new ActionListenerResponseHandler<>(listener, () -> TransportResponse.Empty.INSTANCE));
    }

    public void sendExecuteDfs(Transport.Connection connection, final ShardSearchTransportRequest request, SearchTask task,
                               final SearchActionListener<DfsSearchResult> listener) {
        transportService.sendChildRequest(connection, DFS_ACTION_NAME, request, task,
                new ConnectionCountingHandler<>(listener, DfsSearchResult::new, clientConnections, connection.getNode().getId()));
    }

    public void sendExecuteQuery(Transport.Connection connection, final ShardSearchTransportRequest request, SearchTask task,
                                 final SearchActionListener<SearchPhaseResult> listener) {
        // we optimize this and expect a QueryFetchSearchResult if we only have a single shard in the search request
        // this used to be the QUERY_AND_FETCH which doesn't exist anymore.
        final boolean fetchDocuments = request.numberOfShards() == 1;
        Supplier<SearchPhaseResult> supplier = fetchDocuments ? QueryFetchSearchResult::new : QuerySearchResult::new;

        final ActionListener handler = responseWrapper.apply(connection, listener);
        transportService.sendChildRequest(connection, QUERY_ACTION_NAME, request, task,
                new ConnectionCountingHandler<>(handler, supplier, clientConnections, connection.getNode().getId()));
    }

    public void sendExecuteQuery(Transport.Connection connection, final QuerySearchRequest request, SearchTask task,
                                 final SearchActionListener<QuerySearchResult> listener) {
        transportService.sendChildRequest(connection, QUERY_ID_ACTION_NAME, request, task,
                new ConnectionCountingHandler<>(listener, QuerySearchResult::new, clientConnections, connection.getNode().getId()));
    }

    public void sendExecuteScrollQuery(Transport.Connection connection, final InternalScrollSearchRequest request, SearchTask task,
                                       final SearchActionListener<ScrollQuerySearchResult> listener) {
        transportService.sendChildRequest(connection, QUERY_SCROLL_ACTION_NAME, request, task,
                new ConnectionCountingHandler<>(listener, ScrollQuerySearchResult::new, clientConnections, connection.getNode().getId()));
    }

    public void sendExecuteScrollFetch(Transport.Connection connection, final InternalScrollSearchRequest request, SearchTask task,
                                       final SearchActionListener<ScrollQueryFetchSearchResult> listener) {
        transportService.sendChildRequest(connection, QUERY_FETCH_SCROLL_ACTION_NAME, request, task,
                new ConnectionCountingHandler<>(listener, ScrollQueryFetchSearchResult::new,
                        clientConnections, connection.getNode().getId()));
    }

    public void sendExecuteFetch(Transport.Connection connection, final ShardFetchSearchRequest request, SearchTask task,
                                 final SearchActionListener<FetchSearchResult> listener) {
        sendExecuteFetch(connection, FETCH_ID_ACTION_NAME, request, task, listener);
    }

    public void sendExecuteFetchScroll(Transport.Connection connection, final ShardFetchRequest request, SearchTask task,
                                       final SearchActionListener<FetchSearchResult> listener) {
        sendExecuteFetch(connection, FETCH_ID_SCROLL_ACTION_NAME, request, task, listener);
    }

    private void sendExecuteFetch(Transport.Connection connection, String action, final ShardFetchRequest request, SearchTask task,
                                  final SearchActionListener<FetchSearchResult> listener) {
        transportService.sendChildRequest(connection, action, request, task,
                new ConnectionCountingHandler<>(listener, FetchSearchResult::new, clientConnections, connection.getNode().getId()));
    }

    /**
     * Used by {@link TransportSearchAction} to send the expand queries (field collapsing).
     */
    void sendExecuteMultiSearch(final MultiSearchRequest request, SearchTask task,
                                final ActionListener<MultiSearchResponse> listener) {
        final Transport.Connection connection = transportService.getConnection(transportService.getLocalNode());
        transportService.sendChildRequest(connection, MultiSearchAction.NAME, request, task,
                new ConnectionCountingHandler<>(listener, MultiSearchResponse::new, clientConnections, connection.getNode().getId()));
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
        private long id;

        ScrollFreeContextRequest() {
        }

        ScrollFreeContextRequest(long id) {
            this.id = id;
        }

        public long id() {
            return this.id;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            id = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeLong(id);
        }
    }

    static class SearchFreeContextRequest extends ScrollFreeContextRequest implements IndicesRequest {
        private OriginalIndices originalIndices;

        SearchFreeContextRequest() {
        }

        SearchFreeContextRequest(OriginalIndices originalIndices, long id) {
            super(id);
            this.originalIndices = originalIndices;
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

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            originalIndices = OriginalIndices.readOriginalIndices(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            OriginalIndices.writeOriginalIndices(originalIndices, out);
        }
    }

    public static class SearchFreeContextResponse extends TransportResponse {

        private boolean freed;

        SearchFreeContextResponse() {
        }

        SearchFreeContextResponse(boolean freed) {
            this.freed = freed;
        }

        public boolean isFreed() {
            return freed;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            freed = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBoolean(freed);
        }
    }

    public static void registerRequestHandler(TransportService transportService, SearchService searchService) {
        transportService.registerRequestHandler(FREE_CONTEXT_SCROLL_ACTION_NAME, ScrollFreeContextRequest::new, ThreadPool.Names.SAME,
            new TaskAwareTransportRequestHandler<ScrollFreeContextRequest>() {
                @Override
                public void messageReceived(ScrollFreeContextRequest request, TransportChannel channel, Task task) throws Exception {
                    boolean freed = searchService.freeContext(request.id());
                    channel.sendResponse(new SearchFreeContextResponse(freed));
                }
            });
        TransportActionProxy.registerProxyAction(transportService, FREE_CONTEXT_SCROLL_ACTION_NAME,
                (Supplier<TransportResponse>) SearchFreeContextResponse::new);
        transportService.registerRequestHandler(FREE_CONTEXT_ACTION_NAME,  SearchFreeContextRequest::new, ThreadPool.Names.SAME,
            new TaskAwareTransportRequestHandler<SearchFreeContextRequest>() {
                @Override
                public void messageReceived(SearchFreeContextRequest request, TransportChannel channel, Task task) throws Exception {
                    boolean freed = searchService.freeContext(request.id());
                    channel.sendResponse(new SearchFreeContextResponse(freed));
                }
            });
        TransportActionProxy.registerProxyAction(transportService, FREE_CONTEXT_ACTION_NAME,
                (Supplier<TransportResponse>) SearchFreeContextResponse::new);
        transportService.registerRequestHandler(CLEAR_SCROLL_CONTEXTS_ACTION_NAME, () -> TransportRequest.Empty.INSTANCE,
            ThreadPool.Names.SAME, new TaskAwareTransportRequestHandler<TransportRequest.Empty>() {
                @Override
                public void messageReceived(TransportRequest.Empty request, TransportChannel channel, Task task) throws Exception {
                    searchService.freeAllScrollContexts();
                    channel.sendResponse(TransportResponse.Empty.INSTANCE);
                }
            });
        TransportActionProxy.registerProxyAction(transportService, CLEAR_SCROLL_CONTEXTS_ACTION_NAME,
                () -> TransportResponse.Empty.INSTANCE);

        transportService.registerRequestHandler(DFS_ACTION_NAME, ShardSearchTransportRequest::new, ThreadPool.Names.SAME,
            new TaskAwareTransportRequestHandler<ShardSearchTransportRequest>() {
                @Override
                public void messageReceived(ShardSearchTransportRequest request, TransportChannel channel, Task task) throws Exception {
                    searchService.executeDfsPhase(request, (SearchTask) task, new ActionListener<SearchPhaseResult>() {
                        @Override
                        public void onResponse(SearchPhaseResult searchPhaseResult) {
                            try {
                                channel.sendResponse(searchPhaseResult);
                            } catch (IOException e) {
                                throw new UncheckedIOException(e);
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            try {
                                channel.sendResponse(e);
                            } catch (IOException e1) {
                                throw new UncheckedIOException(e1);
                            }
                        }
                    });

                }
            });
        TransportActionProxy.registerProxyAction(transportService, DFS_ACTION_NAME, DfsSearchResult::new);

        transportService.registerRequestHandler(QUERY_ACTION_NAME, ShardSearchTransportRequest::new, ThreadPool.Names.SAME,
            new TaskAwareTransportRequestHandler<ShardSearchTransportRequest>() {
                @Override
                public void messageReceived(ShardSearchTransportRequest request, TransportChannel channel, Task task) throws Exception {
                    searchService.executeQueryPhase(request, (SearchTask) task, new ActionListener<SearchPhaseResult>() {
                        @Override
                        public void onResponse(SearchPhaseResult searchPhaseResult) {
                            try {
                                channel.sendResponse(searchPhaseResult);
                            } catch (IOException e) {
                                throw new UncheckedIOException(e);
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            try {
                                channel.sendResponse(e);
                            } catch (IOException e1) {
                                throw new UncheckedIOException(e1);
                            }
                        }
                    });
                }
            });
        TransportActionProxy.registerProxyAction(transportService, QUERY_ACTION_NAME,
                (request) -> ((ShardSearchRequest)request).numberOfShards() == 1 ? QueryFetchSearchResult::new : QuerySearchResult::new);

        transportService.registerRequestHandler(QUERY_ID_ACTION_NAME, QuerySearchRequest::new, ThreadPool.Names.SEARCH,
            new TaskAwareTransportRequestHandler<QuerySearchRequest>() {
                @Override
                public void messageReceived(QuerySearchRequest request, TransportChannel channel, Task task) throws Exception {
                    QuerySearchResult result = searchService.executeQueryPhase(request, (SearchTask)task);
                    channel.sendResponse(result);
                }
            });
        TransportActionProxy.registerProxyAction(transportService, QUERY_ID_ACTION_NAME, QuerySearchResult::new);

        transportService.registerRequestHandler(QUERY_SCROLL_ACTION_NAME, InternalScrollSearchRequest::new, ThreadPool.Names.SEARCH,
            new TaskAwareTransportRequestHandler<InternalScrollSearchRequest>() {
                @Override
                public void messageReceived(InternalScrollSearchRequest request, TransportChannel channel, Task task) throws Exception {
                    ScrollQuerySearchResult result = searchService.executeQueryPhase(request, (SearchTask)task);
                    channel.sendResponse(result);
                }
            });
        TransportActionProxy.registerProxyAction(transportService, QUERY_SCROLL_ACTION_NAME, ScrollQuerySearchResult::new);

        transportService.registerRequestHandler(QUERY_FETCH_SCROLL_ACTION_NAME, InternalScrollSearchRequest::new, ThreadPool.Names.SEARCH,
            new TaskAwareTransportRequestHandler<InternalScrollSearchRequest>() {
                @Override
                public void messageReceived(InternalScrollSearchRequest request, TransportChannel channel, Task task) throws Exception {
                    ScrollQueryFetchSearchResult result = searchService.executeFetchPhase(request, (SearchTask)task);
                    channel.sendResponse(result);
                }
            });
        TransportActionProxy.registerProxyAction(transportService, QUERY_FETCH_SCROLL_ACTION_NAME, ScrollQueryFetchSearchResult::new);

        transportService.registerRequestHandler(FETCH_ID_SCROLL_ACTION_NAME, ShardFetchRequest::new, ThreadPool.Names.SEARCH,
            new TaskAwareTransportRequestHandler<ShardFetchRequest>() {
                @Override
                public void messageReceived(ShardFetchRequest request, TransportChannel channel, Task task) throws Exception {
                    FetchSearchResult result = searchService.executeFetchPhase(request, (SearchTask)task);
                    channel.sendResponse(result);
                }
            });
        TransportActionProxy.registerProxyAction(transportService, FETCH_ID_SCROLL_ACTION_NAME, FetchSearchResult::new);

        transportService.registerRequestHandler(FETCH_ID_ACTION_NAME, ShardFetchSearchRequest::new, ThreadPool.Names.SEARCH,
            new TaskAwareTransportRequestHandler<ShardFetchSearchRequest>() {
                @Override
                public void messageReceived(ShardFetchSearchRequest request, TransportChannel channel, Task task) throws Exception {
                    FetchSearchResult result = searchService.executeFetchPhase(request, (SearchTask)task);
                    channel.sendResponse(result);
                }
            });
        TransportActionProxy.registerProxyAction(transportService, FETCH_ID_ACTION_NAME, FetchSearchResult::new);

        // this is cheap, it does not fetch during the rewrite phase, so we can let it quickly execute on a networking thread
        transportService.registerRequestHandler(QUERY_CAN_MATCH_NAME, ShardSearchTransportRequest::new, ThreadPool.Names.SAME,
            new TaskAwareTransportRequestHandler<ShardSearchTransportRequest>() {
                @Override
                public void messageReceived(ShardSearchTransportRequest request, TransportChannel channel, Task task) throws Exception {
                    boolean canMatch = searchService.canMatch(request);
                    channel.sendResponse(new CanMatchResponse(canMatch));
                }
            });
        TransportActionProxy.registerProxyAction(transportService, QUERY_CAN_MATCH_NAME,
                (Supplier<TransportResponse>) CanMatchResponse::new);
    }

    public static final class CanMatchResponse extends SearchPhaseResult {
        private boolean canMatch;

        public CanMatchResponse() {
        }

        public CanMatchResponse(boolean canMatch) {
            this.canMatch = canMatch;
        }


        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            canMatch = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBoolean(canMatch);
        }

        public boolean canMatch() {
            return canMatch;
        }
    }


    /**
     * Returns a connection to the given node on the provided cluster. If the cluster alias is <code>null</code> the node will be resolved
     * against the local cluster.
     * @param clusterAlias the cluster alias the node should be resolve against
     * @param node the node to resolve
     * @return a connection to the given node belonging to the cluster with the provided alias.
     */
    Transport.Connection getConnection(String clusterAlias, DiscoveryNode node) {
        if (clusterAlias == null) {
            return transportService.getConnection(node);
        } else {
            return transportService.getRemoteClusterService().getConnection(node, clusterAlias);
        }
    }

    final class ConnectionCountingHandler<Response extends TransportResponse> extends ActionListenerResponseHandler<Response> {
        private final Map<String, Long> clientConnections;
        private final String nodeId;

        ConnectionCountingHandler(final ActionListener<? super Response> listener, final Supplier<Response> responseSupplier,
                                  final Map<String, Long> clientConnections, final String nodeId) {
            super(listener, responseSupplier);
            this.clientConnections = clientConnections;
            this.nodeId = nodeId;
            // Increment the number of connections for this node by one
            clientConnections.compute(nodeId, (id, conns) -> conns == null ? 1 : conns + 1);
        }

        @Override
        public void handleResponse(Response response) {
            super.handleResponse(response);
            // Decrement the number of connections or remove it entirely if there are no more connections
            // We need to remove the entry here so we don't leak when nodes go away forever
            assert assertNodePresent();
            clientConnections.computeIfPresent(nodeId, (id, conns) -> conns.longValue() == 1 ? null : conns - 1);
        }

        @Override
        public void handleException(TransportException e) {
            super.handleException(e);
            // Decrement the number of connections or remove it entirely if there are no more connections
            // We need to remove the entry here so we don't leak when nodes go away forever
            assert assertNodePresent();
            clientConnections.computeIfPresent(nodeId, (id, conns) -> conns.longValue() == 1 ? null : conns - 1);
        }

        private boolean assertNodePresent() {
            clientConnections.compute(nodeId, (id, conns) -> {
                assert conns != null : "number of connections for " + id + " is null, but should be an integer";
                assert conns >= 1 : "number of connections for " + id + " should be >= 1 but was " + conns;
                return conns;
            });
            // Always return true, there is additional asserting here, the boolean is just so this
            // can be skipped when assertions are not enabled
            return true;
        }
    }
}
