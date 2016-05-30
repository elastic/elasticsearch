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

package org.elasticsearch.search.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
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
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

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
    public static final String QUERY_FETCH_ACTION_NAME = "indices:data/read/search[phase/query+fetch]";
    public static final String QUERY_QUERY_FETCH_ACTION_NAME = "indices:data/read/search[phase/query/query+fetch]";
    public static final String QUERY_FETCH_SCROLL_ACTION_NAME = "indices:data/read/search[phase/query+fetch/scroll]";
    public static final String FETCH_ID_SCROLL_ACTION_NAME = "indices:data/read/search[phase/fetch/id/scroll]";
    public static final String FETCH_ID_ACTION_NAME = "indices:data/read/search[phase/fetch/id]";

    private final TransportService transportService;
    private final SearchService searchService;

    @Inject
    public SearchTransportService(Settings settings, TransportService transportService, SearchService searchService) {
        super(settings);
        this.transportService = transportService;
        this.searchService = searchService;
        transportService.registerRequestHandler(FREE_CONTEXT_SCROLL_ACTION_NAME, ScrollFreeContextRequest::new, ThreadPool.Names.SAME,
                new FreeContextTransportHandler<>());
        transportService.registerRequestHandler(FREE_CONTEXT_ACTION_NAME, SearchFreeContextRequest::new, ThreadPool.Names.SAME,
                new FreeContextTransportHandler<>());
        transportService.registerRequestHandler(CLEAR_SCROLL_CONTEXTS_ACTION_NAME, ClearScrollContextsRequest::new, ThreadPool.Names.SAME,
                new ClearScrollContextsTransportHandler());
        transportService.registerRequestHandler(DFS_ACTION_NAME, ShardSearchTransportRequest::new, ThreadPool.Names.SEARCH,
                new SearchDfsTransportHandler());
        transportService.registerRequestHandler(QUERY_ACTION_NAME, ShardSearchTransportRequest::new, ThreadPool.Names.SEARCH,
                new SearchQueryTransportHandler());
        transportService.registerRequestHandler(QUERY_ID_ACTION_NAME, QuerySearchRequest::new, ThreadPool.Names.SEARCH,
                new SearchQueryByIdTransportHandler());
        transportService.registerRequestHandler(QUERY_SCROLL_ACTION_NAME, InternalScrollSearchRequest::new, ThreadPool.Names.SEARCH,
                new SearchQueryScrollTransportHandler());
        transportService.registerRequestHandler(QUERY_FETCH_ACTION_NAME, ShardSearchTransportRequest::new, ThreadPool.Names.SEARCH,
                new SearchQueryFetchTransportHandler());
        transportService.registerRequestHandler(QUERY_QUERY_FETCH_ACTION_NAME, QuerySearchRequest::new, ThreadPool.Names.SEARCH,
                new SearchQueryQueryFetchTransportHandler());
        transportService.registerRequestHandler(QUERY_FETCH_SCROLL_ACTION_NAME, InternalScrollSearchRequest::new, ThreadPool.Names.SEARCH,
                new SearchQueryFetchScrollTransportHandler());
        transportService.registerRequestHandler(FETCH_ID_SCROLL_ACTION_NAME, ShardFetchRequest::new, ThreadPool.Names.SEARCH,
                new FetchByIdTransportHandler<>());
        transportService.registerRequestHandler(FETCH_ID_ACTION_NAME, ShardFetchSearchRequest::new, ThreadPool.Names.SEARCH,
                new FetchByIdTransportHandler<>());
    }

    public void sendFreeContext(DiscoveryNode node, final long contextId, SearchRequest request) {
        transportService.sendRequest(node, FREE_CONTEXT_ACTION_NAME, new SearchFreeContextRequest(request, contextId),
                new ActionListenerResponseHandler<>(new ActionListener<SearchFreeContextResponse>() {
            @Override
            public void onResponse(SearchFreeContextResponse response) {
                // no need to respond if it was freed or not
            }

            @Override
            public void onFailure(Throwable e) {

            }
        }, SearchFreeContextResponse::new));
    }

    public void sendFreeContext(DiscoveryNode node, long contextId, final ActionListener<SearchFreeContextResponse> listener) {
        transportService.sendRequest(node, FREE_CONTEXT_SCROLL_ACTION_NAME, new ScrollFreeContextRequest(contextId),
                new ActionListenerResponseHandler<>(listener, SearchFreeContextResponse::new));
    }

    public void sendClearAllScrollContexts(DiscoveryNode node, final ActionListener<TransportResponse> listener) {
        transportService.sendRequest(node, CLEAR_SCROLL_CONTEXTS_ACTION_NAME, new ClearScrollContextsRequest(),
                new ActionListenerResponseHandler<>(listener, () -> TransportResponse.Empty.INSTANCE));
    }

    public void sendExecuteDfs(DiscoveryNode node, final ShardSearchTransportRequest request,
                               final ActionListener<DfsSearchResult> listener) {
        transportService.sendRequest(node, DFS_ACTION_NAME, request, new ActionListenerResponseHandler<>(listener, DfsSearchResult::new));
    }

    public void sendExecuteQuery(DiscoveryNode node, final ShardSearchTransportRequest request,
                                 final ActionListener<QuerySearchResultProvider> listener) {
        transportService.sendRequest(node, QUERY_ACTION_NAME, request,
                new ActionListenerResponseHandler<>(listener, QuerySearchResult::new));
    }

    public void sendExecuteQuery(DiscoveryNode node, final QuerySearchRequest request, final ActionListener<QuerySearchResult> listener) {
        transportService.sendRequest(node, QUERY_ID_ACTION_NAME, request,
            new ActionListenerResponseHandler<>(listener, QuerySearchResult::new));
    }

    public void sendExecuteQuery(DiscoveryNode node, final InternalScrollSearchRequest request,
                                 final ActionListener<ScrollQuerySearchResult> listener) {
        transportService.sendRequest(node, QUERY_SCROLL_ACTION_NAME, request,
                new ActionListenerResponseHandler<>(listener, ScrollQuerySearchResult::new));
    }

    public void sendExecuteFetch(DiscoveryNode node, final ShardSearchTransportRequest request,
                                 final ActionListener<QueryFetchSearchResult> listener) {
        transportService.sendRequest(node, QUERY_FETCH_ACTION_NAME, request,
                new ActionListenerResponseHandler<>(listener, QueryFetchSearchResult::new));
    }

    public void sendExecuteFetch(DiscoveryNode node, final QuerySearchRequest request,
                                 final ActionListener<QueryFetchSearchResult> listener) {
        transportService.sendRequest(node, QUERY_QUERY_FETCH_ACTION_NAME, request,
                new ActionListenerResponseHandler<>(listener, QueryFetchSearchResult::new));
    }

    public void sendExecuteFetch(DiscoveryNode node, final InternalScrollSearchRequest request,
                                 final ActionListener<ScrollQueryFetchSearchResult> listener) {
        transportService.sendRequest(node, QUERY_FETCH_SCROLL_ACTION_NAME, request,
                new ActionListenerResponseHandler<>(listener, ScrollQueryFetchSearchResult::new));
    }

    public void sendExecuteFetch(DiscoveryNode node, final ShardFetchSearchRequest request,
                                 final ActionListener<FetchSearchResult> listener) {
        sendExecuteFetch(node, FETCH_ID_ACTION_NAME, request, listener);
    }

    public void sendExecuteFetchScroll(DiscoveryNode node, final ShardFetchRequest request,
                                       final ActionListener<FetchSearchResult> listener) {
        sendExecuteFetch(node, FETCH_ID_SCROLL_ACTION_NAME, request, listener);
    }

    private void sendExecuteFetch(DiscoveryNode node, String action, final ShardFetchRequest request,
                                  final ActionListener<FetchSearchResult> listener) {
        transportService.sendRequest(node, action, request, new ActionListenerResponseHandler<>(listener, FetchSearchResult::new));
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

        public SearchFreeContextRequest() {
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

    class FreeContextTransportHandler<FreeContextRequest extends ScrollFreeContextRequest>
            implements TransportRequestHandler<FreeContextRequest> {
        @Override
        public void messageReceived(FreeContextRequest request, TransportChannel channel) throws Exception {
            boolean freed = searchService.freeContext(request.id());
            channel.sendResponse(new SearchFreeContextResponse(freed));
        }
    }

    static class ClearScrollContextsRequest extends TransportRequest {
    }

    class ClearScrollContextsTransportHandler implements TransportRequestHandler<ClearScrollContextsRequest> {
        @Override
        public void messageReceived(ClearScrollContextsRequest request, TransportChannel channel) throws Exception {
            searchService.freeAllScrollContexts();
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }
    }

    class SearchDfsTransportHandler implements TransportRequestHandler<ShardSearchTransportRequest> {
        @Override
        public void messageReceived(ShardSearchTransportRequest request, TransportChannel channel) throws Exception {
            DfsSearchResult result = searchService.executeDfsPhase(request);
            channel.sendResponse(result);
        }
    }

    class SearchQueryTransportHandler implements TransportRequestHandler<ShardSearchTransportRequest> {
        @Override
        public void messageReceived(ShardSearchTransportRequest request, TransportChannel channel) throws Exception {
            QuerySearchResultProvider result = searchService.executeQueryPhase(request);
            channel.sendResponse(result);
        }
    }

    class SearchQueryByIdTransportHandler implements TransportRequestHandler<QuerySearchRequest> {
        @Override
        public void messageReceived(QuerySearchRequest request, TransportChannel channel) throws Exception {
            QuerySearchResult result = searchService.executeQueryPhase(request);
            channel.sendResponse(result);
        }
    }

    class SearchQueryScrollTransportHandler implements TransportRequestHandler<InternalScrollSearchRequest> {
        @Override
        public void messageReceived(InternalScrollSearchRequest request, TransportChannel channel) throws Exception {
            ScrollQuerySearchResult result = searchService.executeQueryPhase(request);
            channel.sendResponse(result);
        }
    }

    class SearchQueryFetchTransportHandler implements TransportRequestHandler<ShardSearchTransportRequest> {
        @Override
        public void messageReceived(ShardSearchTransportRequest request, TransportChannel channel) throws Exception {
            QueryFetchSearchResult result = searchService.executeFetchPhase(request);
            channel.sendResponse(result);
        }
    }

    class SearchQueryQueryFetchTransportHandler implements TransportRequestHandler<QuerySearchRequest> {
        @Override
        public void messageReceived(QuerySearchRequest request, TransportChannel channel) throws Exception {
            QueryFetchSearchResult result = searchService.executeFetchPhase(request);
            channel.sendResponse(result);
        }
    }

    class FetchByIdTransportHandler<Request extends ShardFetchRequest> implements TransportRequestHandler<Request> {
        @Override
        public void messageReceived(Request request, TransportChannel channel) throws Exception {
            FetchSearchResult result = searchService.executeFetchPhase(request);
            channel.sendResponse(result);
        }
    }

    class SearchQueryFetchScrollTransportHandler implements TransportRequestHandler<InternalScrollSearchRequest> {
        @Override
        public void messageReceived(InternalScrollSearchRequest request, TransportChannel channel) throws Exception {
            ScrollQueryFetchSearchResult result = searchService.executeFetchPhase(request);
            channel.sendResponse(result);
        }
    }
}
