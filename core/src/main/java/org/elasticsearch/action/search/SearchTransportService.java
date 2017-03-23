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
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.QueryFetchSearchResult;
import org.elasticsearch.search.fetch.ScrollQueryFetchSearchResult;
import org.elasticsearch.search.fetch.ShardFetchRequest;
import org.elasticsearch.search.fetch.ShardFetchSearchRequest;
import org.elasticsearch.search.internal.InternalScrollSearchRequest;
import org.elasticsearch.search.internal.ShardSearchTransportRequest;
import org.elasticsearch.search.query.QuerySearchRequest;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.query.QuerySearchResultProvider;
import org.elasticsearch.search.query.ScrollQuerySearchResult;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportActionProxy;
import org.elasticsearch.transport.TaskAwareTransportRequestHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * An encapsulation of {@link org.elasticsearch.search.SearchService} operations exposed through
 * transport.
 */
public class SearchTransportService extends AbstractLifecycleComponent {

    public static final String FREE_CONTEXT_SCROLL_ACTION_NAME = "indices:data/read/search[free_context/scroll]";
    public static final String FREE_CONTEXT_ACTION_NAME = "indices:data/read/search[free_context]";
    public static final String CLEAR_SCROLL_CONTEXTS_ACTION_NAME = "indices:data/read/search[clear_scroll_contexts]";
    public static final String DFS_ACTION_NAME = "indices:data/read/search[phase/dfs]";
    public static final String QUERY_ACTION_NAME = "indices:data/read/search[phase/query]";
    public static final String QUERY_ID_ACTION_NAME = "indices:data/read/search[phase/query/id]";
    public static final String QUERY_SCROLL_ACTION_NAME = "indices:data/read/search[phase/query/scroll]";
    public static final String QUERY_FETCH_ACTION_NAME = "indices:data/read/search[phase/query+fetch]";
    public static final String QUERY_FETCH_SCROLL_ACTION_NAME = "indices:data/read/search[phase/query+fetch/scroll]";
    public static final String FETCH_ID_SCROLL_ACTION_NAME = "indices:data/read/search[phase/fetch/id/scroll]";
    public static final String FETCH_ID_ACTION_NAME = "indices:data/read/search[phase/fetch/id]";

    private final TransportService transportService;
    private final RemoteClusterService remoteClusterService;
    private final boolean connectToRemote;

    public SearchTransportService(Settings settings, ClusterSettings clusterSettings, TransportService transportService) {
        super(settings);
        this.connectToRemote = RemoteClusterService.ENABLE_REMOTE_CLUSTERS.get(settings);
        this.transportService = transportService;
        this.remoteClusterService = new RemoteClusterService(settings, transportService);
        if (connectToRemote) {
            clusterSettings.addAffixUpdateConsumer(RemoteClusterService.REMOTE_CLUSTERS_SEEDS, remoteClusterService::updateRemoteCluster,
                (namespace, value) -> {
                });
        }
    }

    public void sendFreeContext(Transport.Connection connection, final long contextId, SearchRequest request) {
        transportService.sendRequest(connection, FREE_CONTEXT_ACTION_NAME, new SearchFreeContextRequest(request, contextId),
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

    public void sendFreeContext(DiscoveryNode node, long contextId, final ActionListener<SearchFreeContextResponse> listener) {
        transportService.sendRequest(node, FREE_CONTEXT_SCROLL_ACTION_NAME, new ScrollFreeContextRequest(contextId),
            new ActionListenerResponseHandler<>(listener, SearchFreeContextResponse::new));
    }

    public void sendClearAllScrollContexts(DiscoveryNode node, final ActionListener<TransportResponse> listener) {
        transportService.sendRequest(node, CLEAR_SCROLL_CONTEXTS_ACTION_NAME, TransportRequest.Empty.INSTANCE,
            new ActionListenerResponseHandler<>(listener, () -> TransportResponse.Empty.INSTANCE));
    }

    public void sendExecuteDfs(Transport.Connection connection, final ShardSearchTransportRequest request, SearchTask task,
                               final ActionListener<DfsSearchResult> listener) {
        transportService.sendChildRequest(connection, DFS_ACTION_NAME, request, task,
            new ActionListenerResponseHandler<>(listener, DfsSearchResult::new));
    }

    public void sendExecuteQuery(Transport.Connection connection, final ShardSearchTransportRequest request, SearchTask task,
                                 final ActionListener<QuerySearchResultProvider> listener) {
        // we optimize this and expect a QueryFetchSearchResult if we only have a single shard in the search request
        // this used to be the QUERY_AND_FETCH which doesn't exists anymore.
        final boolean fetchDocuments = request.numberOfShards() == 1;
        Supplier<QuerySearchResultProvider> supplier = fetchDocuments ? QueryFetchSearchResult::new : QuerySearchResult::new;
        if (connection.getVersion().onOrBefore(Version.V_5_3_0_UNRELEASED) && fetchDocuments) {
            // TODO this BWC layer can be removed once this is back-ported to 5.3
            transportService.sendChildRequest(connection, QUERY_FETCH_ACTION_NAME, request, task,
                new ActionListenerResponseHandler<>(listener, supplier));
        } else {
            transportService.sendChildRequest(connection, QUERY_ACTION_NAME, request, task,
                new ActionListenerResponseHandler<>(listener, supplier));
        }
    }

    public void sendExecuteQuery(Transport.Connection connection, final QuerySearchRequest request, SearchTask task,
                                 final ActionListener<QuerySearchResult> listener) {
        transportService.sendChildRequest(connection, QUERY_ID_ACTION_NAME, request, task,
            new ActionListenerResponseHandler<>(listener, QuerySearchResult::new));
    }

    public void sendExecuteQuery(DiscoveryNode node, final InternalScrollSearchRequest request, SearchTask task,
                                 final ActionListener<ScrollQuerySearchResult> listener) {
        transportService.sendChildRequest(transportService.getConnection(node), QUERY_SCROLL_ACTION_NAME, request, task,
            new ActionListenerResponseHandler<>(listener, ScrollQuerySearchResult::new));
    }

    public void sendExecuteFetch(DiscoveryNode node, final InternalScrollSearchRequest request, SearchTask task,
                                 final ActionListener<ScrollQueryFetchSearchResult> listener) {
        transportService.sendChildRequest(transportService.getConnection(node), QUERY_FETCH_SCROLL_ACTION_NAME, request, task,
            new ActionListenerResponseHandler<>(listener, ScrollQueryFetchSearchResult::new));
    }

    public void sendExecuteFetch(Transport.Connection connection, final ShardFetchSearchRequest request, SearchTask task,
                                 final ActionListener<FetchSearchResult> listener) {
        sendExecuteFetch(connection, FETCH_ID_ACTION_NAME, request, task, listener);
    }

    public void sendExecuteFetchScroll(DiscoveryNode node, final ShardFetchRequest request, SearchTask task,
                                       final ActionListener<FetchSearchResult> listener) {
        sendExecuteFetch(transportService.getConnection(node), FETCH_ID_SCROLL_ACTION_NAME, request, task, listener);
    }

    private void sendExecuteFetch(Transport.Connection connection, String action, final ShardFetchRequest request, SearchTask task,
                                  final ActionListener<FetchSearchResult> listener) {
        transportService.sendChildRequest(connection, action, request, task,
            new ActionListenerResponseHandler<>(listener, FetchSearchResult::new));
    }

    /**
     * Used by {@link TransportSearchAction} to send the expand queries (field collapsing).
     */
    void sendExecuteMultiSearch(final MultiSearchRequest request, SearchTask task,
                                       final ActionListener<MultiSearchResponse> listener) {
        transportService.sendChildRequest(transportService.getConnection(transportService.getLocalNode()), MultiSearchAction.NAME, request,
            task, new ActionListenerResponseHandler<>(listener, MultiSearchResponse::new));
    }

    public RemoteClusterService getRemoteClusterService() {
        return remoteClusterService;
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

        SearchFreeContextRequest(SearchRequest request, long id) {
            super(id);
            this.originalIndices = new OriginalIndices(request);
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
        TransportActionProxy.registerProxyAction(transportService, FREE_CONTEXT_SCROLL_ACTION_NAME, SearchFreeContextResponse::new);
        transportService.registerRequestHandler(FREE_CONTEXT_ACTION_NAME, SearchFreeContextRequest::new, ThreadPool.Names.SAME,
            new TaskAwareTransportRequestHandler<SearchFreeContextRequest>() {
                @Override
                public void messageReceived(SearchFreeContextRequest request, TransportChannel channel, Task task) throws Exception {
                    boolean freed = searchService.freeContext(request.id());
                    channel.sendResponse(new SearchFreeContextResponse(freed));
                }
            });
        TransportActionProxy.registerProxyAction(transportService, FREE_CONTEXT_ACTION_NAME, SearchFreeContextResponse::new);
        transportService.registerRequestHandler(CLEAR_SCROLL_CONTEXTS_ACTION_NAME, () -> TransportRequest.Empty.INSTANCE,
            ThreadPool.Names.SAME,
            new TaskAwareTransportRequestHandler<TransportRequest.Empty>() {
                @Override
                public void messageReceived(TransportRequest.Empty request, TransportChannel channel, Task task) throws Exception {
                    searchService.freeAllScrollContexts();
                    channel.sendResponse(TransportResponse.Empty.INSTANCE);
                }
            });
        TransportActionProxy.registerProxyAction(transportService, CLEAR_SCROLL_CONTEXTS_ACTION_NAME,
            () -> TransportResponse.Empty.INSTANCE);

        transportService.registerRequestHandler(DFS_ACTION_NAME, ShardSearchTransportRequest::new, ThreadPool.Names.SEARCH,
            new TaskAwareTransportRequestHandler<ShardSearchTransportRequest>() {
                @Override
                public void messageReceived(ShardSearchTransportRequest request, TransportChannel channel, Task task) throws Exception {
                    DfsSearchResult result = searchService.executeDfsPhase(request, (SearchTask)task);
                    channel.sendResponse(result);

                }
            });
        TransportActionProxy.registerProxyAction(transportService, DFS_ACTION_NAME, DfsSearchResult::new);

        transportService.registerRequestHandler(QUERY_ACTION_NAME, ShardSearchTransportRequest::new, ThreadPool.Names.SEARCH,
            new TaskAwareTransportRequestHandler<ShardSearchTransportRequest>() {
                @Override
                public void messageReceived(ShardSearchTransportRequest request, TransportChannel channel, Task task) throws Exception {
                    QuerySearchResultProvider result = searchService.executeQueryPhase(request, (SearchTask)task);
                    channel.sendResponse(result);
                }
            });
        TransportActionProxy.registerProxyAction(transportService, QUERY_ACTION_NAME, QuerySearchResult::new);

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

        // this is for BWC with 5.3 until the QUERY_AND_FETCH removal change has been back-ported to 5.x
        // in 5.3 we will only execute a `indices:data/read/search[phase/query+fetch]` if the node is pre 5.3
        // such that we can remove this after the back-port.
        transportService.registerRequestHandler(QUERY_FETCH_ACTION_NAME, ShardSearchTransportRequest::new, ThreadPool.Names.SEARCH,
            new TaskAwareTransportRequestHandler<ShardSearchTransportRequest>() {
                @Override
                public void messageReceived(ShardSearchTransportRequest request, TransportChannel channel, Task task) throws Exception {
                    assert request.numberOfShards() == 1 : "expected single shard request but got: " + request.numberOfShards();
                    QuerySearchResultProvider result = searchService.executeQueryPhase(request, (SearchTask)task);
                    channel.sendResponse(result);
                }
            });
        TransportActionProxy.registerProxyAction(transportService, QUERY_FETCH_ACTION_NAME, QueryFetchSearchResult::new);

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
    }

    Transport.Connection getConnection(DiscoveryNode node) {
        return transportService.getConnection(node);
    }

    @Override
    protected void doStart() {
        if (connectToRemote) {
            // here we start to connect to the remote clusters
            remoteClusterService.initializeRemoteClusters();
        }
    }

    @Override
    protected void doStop() {}

    @Override
    protected void doClose() throws IOException {
        remoteClusterService.close();
    }
}
