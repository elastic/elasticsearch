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

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.fetch.*;
import org.elasticsearch.search.internal.InternalScrollSearchRequest;
import org.elasticsearch.search.internal.ShardSearchTransportRequest;
import org.elasticsearch.search.query.QuerySearchRequest;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.query.QuerySearchResultProvider;
import org.elasticsearch.search.query.ScrollQuerySearchResult;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.io.IOException;
import java.util.concurrent.Callable;

/**
 * An encapsulation of {@link org.elasticsearch.search.SearchService} operations exposed through
 * transport.
 */
public class SearchServiceTransportAction extends AbstractComponent {

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
    public static final String SCAN_ACTION_NAME = "indices:data/read/search[phase/scan]";
    public static final String SCAN_SCROLL_ACTION_NAME = "indices:data/read/search[phase/scan/scroll]";

    static final class FreeContextResponseHandler implements TransportResponseHandler<SearchFreeContextResponse> {

        private final ActionListener<Boolean> listener;

        FreeContextResponseHandler(final ActionListener<Boolean> listener) {
            this.listener = listener;
        }

        @Override
        public SearchFreeContextResponse newInstance() {
            return new SearchFreeContextResponse();
        }

        @Override
        public void handleResponse(SearchFreeContextResponse response) {
            listener.onResponse(response.freed);
        }

        @Override
        public void handleException(TransportException exp) {
            listener.onFailure(exp);
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }
    }
    //
    private final ThreadPool threadPool;
    private final TransportService transportService;
    private final ClusterService clusterService;
    private final SearchService searchService;
    private final FreeContextResponseHandler freeContextResponseHandler = new FreeContextResponseHandler(new ActionListener<Boolean>() {
        @Override
        public void onResponse(Boolean aBoolean) {}

        @Override
        public void onFailure(Throwable exp) {
            logger.warn("Failed to send release search context", exp);
        }
    });

    @Inject
    public SearchServiceTransportAction(Settings settings, ThreadPool threadPool, TransportService transportService, ClusterService clusterService, SearchService searchService) {
        super(settings);
        this.threadPool = threadPool;
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.searchService = searchService;

        transportService.registerHandler(FREE_CONTEXT_SCROLL_ACTION_NAME, new ScrollFreeContextTransportHandler());
        transportService.registerHandler(FREE_CONTEXT_ACTION_NAME, new SearchFreeContextTransportHandler());
        transportService.registerHandler(CLEAR_SCROLL_CONTEXTS_ACTION_NAME, new ClearScrollContextsTransportHandler());
        transportService.registerHandler(DFS_ACTION_NAME, new SearchDfsTransportHandler());
        transportService.registerHandler(QUERY_ACTION_NAME, new SearchQueryTransportHandler());
        transportService.registerHandler(QUERY_ID_ACTION_NAME, new SearchQueryByIdTransportHandler());
        transportService.registerHandler(QUERY_SCROLL_ACTION_NAME, new SearchQueryScrollTransportHandler());
        transportService.registerHandler(QUERY_FETCH_ACTION_NAME, new SearchQueryFetchTransportHandler());
        transportService.registerHandler(QUERY_QUERY_FETCH_ACTION_NAME, new SearchQueryQueryFetchTransportHandler());
        transportService.registerHandler(QUERY_FETCH_SCROLL_ACTION_NAME, new SearchQueryFetchScrollTransportHandler());
        transportService.registerHandler(FETCH_ID_SCROLL_ACTION_NAME, new ScrollFetchByIdTransportHandler());
        transportService.registerHandler(FETCH_ID_ACTION_NAME, new SearchFetchByIdTransportHandler());
        transportService.registerHandler(SCAN_ACTION_NAME, new SearchScanTransportHandler());
        transportService.registerHandler(SCAN_SCROLL_ACTION_NAME, new SearchScanScrollTransportHandler());
    }

    public void sendFreeContext(DiscoveryNode node, final long contextId, SearchRequest request) {
        if (clusterService.state().nodes().localNodeId().equals(node.id())) {
            searchService.freeContext(contextId);
        } else {
            transportService.sendRequest(node, FREE_CONTEXT_ACTION_NAME, new SearchFreeContextRequest(request, contextId), freeContextResponseHandler);
        }
    }

    public void sendFreeContext(DiscoveryNode node, long contextId, ClearScrollRequest request, final ActionListener<Boolean> actionListener) {
        if (clusterService.state().nodes().localNodeId().equals(node.id())) {
            final boolean freed = searchService.freeContext(contextId);
            actionListener.onResponse(freed);
        } else {
            if (node.getVersion().onOrAfter(Version.V_1_4_0_Beta1)) {
                //use the separate action for scroll when possible
                transportService.sendRequest(node, FREE_CONTEXT_SCROLL_ACTION_NAME, new ScrollFreeContextRequest(request, contextId), new FreeContextResponseHandler(actionListener));
            } else {
                //fallback to the previous action name if the new one is not supported by the node we are talking to.
                //Do use the same request since it has the same binary format as the previous SearchFreeContextRequest (without the OriginalIndices addition).
                transportService.sendRequest(node, FREE_CONTEXT_ACTION_NAME, new ScrollFreeContextRequest(request, contextId), new FreeContextResponseHandler(actionListener));
            }
        }
    }

    public void sendClearAllScrollContexts(DiscoveryNode node, ClearScrollRequest request, final ActionListener<Boolean> actionListener) {
        if (clusterService.state().nodes().localNodeId().equals(node.id())) {
            searchService.freeAllScrollContexts();
            actionListener.onResponse(true);
        } else {
            transportService.sendRequest(node, CLEAR_SCROLL_CONTEXTS_ACTION_NAME, new ClearScrollContextsRequest(request), new TransportResponseHandler<TransportResponse>() {
                @Override
                public TransportResponse newInstance() {
                    return TransportResponse.Empty.INSTANCE;
                }

                @Override
                public void handleResponse(TransportResponse response) {
                    actionListener.onResponse(true);
                }

                @Override
                public void handleException(TransportException exp) {
                    actionListener.onFailure(exp);
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }
            });
        }
    }

    public void sendExecuteDfs(DiscoveryNode node, final ShardSearchTransportRequest request, final SearchServiceListener<DfsSearchResult> listener) {
        if (clusterService.state().nodes().localNodeId().equals(node.id())) {
            execute(new Callable<DfsSearchResult>() {
                @Override
                public DfsSearchResult call() throws Exception {
                    return searchService.executeDfsPhase(request);
                }
            }, listener);
        } else {
            transportService.sendRequest(node, DFS_ACTION_NAME, request, new BaseTransportResponseHandler<DfsSearchResult>() {

                @Override
                public DfsSearchResult newInstance() {
                    return new DfsSearchResult();
                }

                @Override
                public void handleResponse(DfsSearchResult response) {
                    listener.onResult(response);
                }

                @Override
                public void handleException(TransportException exp) {
                    listener.onFailure(exp);
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }
            });
        }
    }

    public void sendExecuteQuery(DiscoveryNode node, final ShardSearchTransportRequest request, final SearchServiceListener<QuerySearchResultProvider> listener) {
        if (clusterService.state().nodes().localNodeId().equals(node.id())) {
            execute(new Callable<QuerySearchResultProvider>() {
                @Override
                public QuerySearchResultProvider call() throws Exception {
                    return searchService.executeQueryPhase(request);
                }
            }, listener);
        } else {
            transportService.sendRequest(node, QUERY_ACTION_NAME, request, new BaseTransportResponseHandler<QuerySearchResultProvider>() {

                @Override
                public QuerySearchResult newInstance() {
                    return new QuerySearchResult();
                }

                @Override
                public void handleResponse(QuerySearchResultProvider response) {
                    listener.onResult(response);
                }

                @Override
                public void handleException(TransportException exp) {
                    listener.onFailure(exp);
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }
            });
        }
    }

    public void sendExecuteQuery(DiscoveryNode node, final QuerySearchRequest request, final SearchServiceListener<QuerySearchResult> listener) {
        if (clusterService.state().nodes().localNodeId().equals(node.id())) {
            execute(new Callable<QuerySearchResult>() {
                @Override
                public QuerySearchResult call() throws Exception {
                    return searchService.executeQueryPhase(request);
                }
            }, listener);
        } else {
            transportService.sendRequest(node, QUERY_ID_ACTION_NAME, request, new BaseTransportResponseHandler<QuerySearchResult>() {

                @Override
                public QuerySearchResult newInstance() {
                    return new QuerySearchResult();
                }

                @Override
                public void handleResponse(QuerySearchResult response) {
                    listener.onResult(response);
                }

                @Override
                public void handleException(TransportException exp) {
                    listener.onFailure(exp);
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }
            });
        }
    }

    public void sendExecuteQuery(DiscoveryNode node, final InternalScrollSearchRequest request, final SearchServiceListener<QuerySearchResult> listener) {
        if (clusterService.state().nodes().localNodeId().equals(node.id())) {
            execute(new Callable<QuerySearchResult>() {
                @Override
                public QuerySearchResult call() throws Exception {
                    return searchService.executeQueryPhase(request).queryResult();
                }
            }, listener);
        } else {
            transportService.sendRequest(node, QUERY_SCROLL_ACTION_NAME, request, new BaseTransportResponseHandler<ScrollQuerySearchResult>() {

                @Override
                public ScrollQuerySearchResult newInstance() {
                    return new ScrollQuerySearchResult();
                }

                @Override
                public void handleResponse(ScrollQuerySearchResult response) {
                    listener.onResult(response.queryResult());
                }

                @Override
                public void handleException(TransportException exp) {
                    listener.onFailure(exp);
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }
            });
        }
    }

    public void sendExecuteFetch(DiscoveryNode node, final ShardSearchTransportRequest request, final SearchServiceListener<QueryFetchSearchResult> listener) {
        if (clusterService.state().nodes().localNodeId().equals(node.id())) {
            execute(new Callable<QueryFetchSearchResult>() {
                @Override
                public QueryFetchSearchResult call() throws Exception {
                    return searchService.executeFetchPhase(request);
                }
            }, listener);
        } else {
            transportService.sendRequest(node, QUERY_FETCH_ACTION_NAME, request, new BaseTransportResponseHandler<QueryFetchSearchResult>() {

                @Override
                public QueryFetchSearchResult newInstance() {
                    return new QueryFetchSearchResult();
                }

                @Override
                public void handleResponse(QueryFetchSearchResult response) {
                    listener.onResult(response);
                }

                @Override
                public void handleException(TransportException exp) {
                    listener.onFailure(exp);
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }
            });
        }
    }

    public void sendExecuteFetch(DiscoveryNode node, final QuerySearchRequest request, final SearchServiceListener<QueryFetchSearchResult> listener) {
        if (clusterService.state().nodes().localNodeId().equals(node.id())) {
            execute(new Callable<QueryFetchSearchResult>() {
                @Override
                public QueryFetchSearchResult call() throws Exception {
                    return searchService.executeFetchPhase(request);
                }
            }, listener);
        } else {
            transportService.sendRequest(node, QUERY_QUERY_FETCH_ACTION_NAME, request, new BaseTransportResponseHandler<QueryFetchSearchResult>() {

                @Override
                public QueryFetchSearchResult newInstance() {
                    return new QueryFetchSearchResult();
                }

                @Override
                public void handleResponse(QueryFetchSearchResult response) {
                    listener.onResult(response);
                }

                @Override
                public void handleException(TransportException exp) {
                    listener.onFailure(exp);
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }
            });
        }
    }

    public void sendExecuteFetch(DiscoveryNode node, final InternalScrollSearchRequest request, final SearchServiceListener<QueryFetchSearchResult> listener) {
        if (clusterService.state().nodes().localNodeId().equals(node.id())) {
            execute(new Callable<QueryFetchSearchResult>() {
                @Override
                public QueryFetchSearchResult call() throws Exception {
                    return searchService.executeFetchPhase(request).result();
                }
            }, listener);
        } else {
            transportService.sendRequest(node, QUERY_FETCH_SCROLL_ACTION_NAME, request, new BaseTransportResponseHandler<ScrollQueryFetchSearchResult>() {

                @Override
                public ScrollQueryFetchSearchResult newInstance() {
                    return new ScrollQueryFetchSearchResult();
                }

                @Override
                public void handleResponse(ScrollQueryFetchSearchResult response) {
                    listener.onResult(response.result());
                }

                @Override
                public void handleException(TransportException exp) {
                    listener.onFailure(exp);
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }
            });
        }
    }

    public void sendExecuteFetch(DiscoveryNode node, final ShardFetchSearchRequest request, final SearchServiceListener<FetchSearchResult> listener) {
        sendExecuteFetch(node, FETCH_ID_ACTION_NAME, request, listener);
    }

    public void sendExecuteFetchScroll(DiscoveryNode node, final ShardFetchRequest request, final SearchServiceListener<FetchSearchResult> listener) {
        String action;
        if (node.getVersion().onOrAfter(Version.V_1_4_0_Beta1)) {
            //use the separate action for scroll when possible
            action = FETCH_ID_SCROLL_ACTION_NAME;
        } else {
            //fallback to the previous action name if the new one is not supported by the node we are talking to.
            //Do use the same request since it has the same binary format as the previous FetchSearchRequest (without the OriginalIndices addition).
            action = FETCH_ID_ACTION_NAME;
        }
        sendExecuteFetch(node, action, request, listener);
    }

    private void sendExecuteFetch(DiscoveryNode node, String action, final ShardFetchRequest request, final SearchServiceListener<FetchSearchResult> listener) {
        if (clusterService.state().nodes().localNodeId().equals(node.id())) {
            execute(new Callable<FetchSearchResult>() {
                @Override
                public FetchSearchResult call() throws Exception {
                    return searchService.executeFetchPhase(request);
                }
            }, listener);
        } else {
            transportService.sendRequest(node, action, request, new BaseTransportResponseHandler<FetchSearchResult>() {

                @Override
                public FetchSearchResult newInstance() {
                    return new FetchSearchResult();
                }

                @Override
                public void handleResponse(FetchSearchResult response) {
                    listener.onResult(response);
                }

                @Override
                public void handleException(TransportException exp) {
                    listener.onFailure(exp);
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }
            });
        }
    }

    public void sendExecuteScan(DiscoveryNode node, final ShardSearchTransportRequest request, final SearchServiceListener<QuerySearchResult> listener) {
        if (clusterService.state().nodes().localNodeId().equals(node.id())) {
            execute(new Callable<QuerySearchResult>() {
                @Override
                public QuerySearchResult call() throws Exception {
                    return searchService.executeScan(request);
                }
            }, listener);
        } else {
            transportService.sendRequest(node, SCAN_ACTION_NAME, request, new BaseTransportResponseHandler<QuerySearchResult>() {

                @Override
                public QuerySearchResult newInstance() {
                    return new QuerySearchResult();
                }

                @Override
                public void handleResponse(QuerySearchResult response) {
                    listener.onResult(response);
                }

                @Override
                public void handleException(TransportException exp) {
                    listener.onFailure(exp);
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }
            });
        }
    }

    public void sendExecuteScan(DiscoveryNode node, final InternalScrollSearchRequest request, final SearchServiceListener<QueryFetchSearchResult> listener) {
        if (clusterService.state().nodes().localNodeId().equals(node.id())) {
            execute(new Callable<QueryFetchSearchResult>() {
                @Override
                public QueryFetchSearchResult call() throws Exception {
                    return searchService.executeScan(request).result();
                }
            }, listener);
        } else {
            transportService.sendRequest(node, SCAN_SCROLL_ACTION_NAME, request, new BaseTransportResponseHandler<ScrollQueryFetchSearchResult>() {

                @Override
                public ScrollQueryFetchSearchResult newInstance() {
                    return new ScrollQueryFetchSearchResult();
                }

                @Override
                public void handleResponse(ScrollQueryFetchSearchResult response) {
                    listener.onResult(response.result());
                }

                @Override
                public void handleException(TransportException exp) {
                    listener.onFailure(exp);
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }
            });
        }
    }

    private <T> void execute(final Callable<? extends T> callable, final SearchServiceListener<T> listener) {
        try {
            threadPool.executor(ThreadPool.Names.SEARCH).execute(new Runnable() {
                @Override
                public void run() {
                    // Listeners typically do counting on errors and successes, and the decision to move to second phase, etc. is based on
                    // these counts so we need to be careful here to never propagate exceptions thrown by onResult to onFailure
                    T result = null;
                    Throwable error = null;
                    try {
                        result = callable.call();
                    } catch (Throwable t) {
                        error = t;
                    } finally {
                        if (result == null) {
                            assert error != null;
                            listener.onFailure(error);
                        } else {
                            assert error == null : error;
                            listener.onResult(result);
                        }
                    }
                }
            });
        } catch (Throwable t) {
            listener.onFailure(t);
        }
    }

    static class ScrollFreeContextRequest extends TransportRequest {
        private long id;

        ScrollFreeContextRequest() {
        }

        ScrollFreeContextRequest(ClearScrollRequest request, long id) {
            this((TransportRequest) request, id);
        }

        private ScrollFreeContextRequest(TransportRequest request, long id) {
            super(request);
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
            super(request, id);
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

    static class SearchFreeContextResponse extends TransportResponse {

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
            if (in.getVersion().onOrAfter(Version.V_1_2_0)) {
                freed = in.readBoolean();
            } else {
                freed = true;
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            if (out.getVersion().onOrAfter(Version.V_1_2_0)) {
                out.writeBoolean(freed);
            }
        }
    }

    private abstract class BaseFreeContextTransportHandler<FreeContextRequest extends ScrollFreeContextRequest> extends BaseTransportRequestHandler<FreeContextRequest> {
        @Override
        public abstract FreeContextRequest newInstance();

        @Override
        public void messageReceived(FreeContextRequest request, TransportChannel channel) throws Exception {
            boolean freed = searchService.freeContext(request.id());
            channel.sendResponse(new SearchFreeContextResponse(freed));
        }

        @Override
        public String executor() {
            // freeing the context is cheap,
            // no need for fork it to another thread
            return ThreadPool.Names.SAME;
        }
    }

    class ScrollFreeContextTransportHandler extends BaseFreeContextTransportHandler<ScrollFreeContextRequest> {
        @Override
        public ScrollFreeContextRequest newInstance() {
            return new ScrollFreeContextRequest();
        }
    }

    class SearchFreeContextTransportHandler extends BaseFreeContextTransportHandler<SearchFreeContextRequest> {
        @Override
        public SearchFreeContextRequest newInstance() {
            return new SearchFreeContextRequest();
        }
    }

    static class ClearScrollContextsRequest extends TransportRequest {

        ClearScrollContextsRequest() {
        }

        ClearScrollContextsRequest(TransportRequest request) {
            super(request);
        }

    }

    class ClearScrollContextsTransportHandler extends BaseTransportRequestHandler<ClearScrollContextsRequest> {

        @Override
        public ClearScrollContextsRequest newInstance() {
            return new ClearScrollContextsRequest();
        }

        @Override
        public void messageReceived(ClearScrollContextsRequest request, TransportChannel channel) throws Exception {
            searchService.freeAllScrollContexts();
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }

        @Override
        public String executor() {
            // freeing the context is cheap,
            // no need for fork it to another thread
            return ThreadPool.Names.SAME;
        }
    }

    private class SearchDfsTransportHandler extends BaseTransportRequestHandler<ShardSearchTransportRequest> {

        @Override
        public ShardSearchTransportRequest newInstance() {
            return new ShardSearchTransportRequest();
        }

        @Override
        public void messageReceived(ShardSearchTransportRequest request, TransportChannel channel) throws Exception {
            DfsSearchResult result = searchService.executeDfsPhase(request);
            channel.sendResponse(result);
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SEARCH;
        }
    }

    private class SearchQueryTransportHandler extends BaseTransportRequestHandler<ShardSearchTransportRequest> {

        @Override
        public ShardSearchTransportRequest newInstance() {
            return new ShardSearchTransportRequest();
        }

        @Override
        public void messageReceived(ShardSearchTransportRequest request, TransportChannel channel) throws Exception {
            QuerySearchResultProvider result = searchService.executeQueryPhase(request);
            channel.sendResponse(result);
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SEARCH;
        }
    }

    private class SearchQueryByIdTransportHandler extends BaseTransportRequestHandler<QuerySearchRequest> {

        @Override
        public QuerySearchRequest newInstance() {
            return new QuerySearchRequest();
        }

        @Override
        public void messageReceived(QuerySearchRequest request, TransportChannel channel) throws Exception {
            QuerySearchResult result = searchService.executeQueryPhase(request);
            channel.sendResponse(result);
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SEARCH;
        }
    }

    private class SearchQueryScrollTransportHandler extends BaseTransportRequestHandler<InternalScrollSearchRequest> {

        @Override
        public InternalScrollSearchRequest newInstance() {
            return new InternalScrollSearchRequest();
        }

        @Override
        public void messageReceived(InternalScrollSearchRequest request, TransportChannel channel) throws Exception {
            ScrollQuerySearchResult result = searchService.executeQueryPhase(request);
            channel.sendResponse(result);
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SEARCH;
        }
    }

    private class SearchQueryFetchTransportHandler extends BaseTransportRequestHandler<ShardSearchTransportRequest> {

        @Override
        public ShardSearchTransportRequest newInstance() {
            return new ShardSearchTransportRequest();
        }

        @Override
        public void messageReceived(ShardSearchTransportRequest request, TransportChannel channel) throws Exception {
            QueryFetchSearchResult result = searchService.executeFetchPhase(request);
            channel.sendResponse(result);
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SEARCH;
        }
    }

    private class SearchQueryQueryFetchTransportHandler extends BaseTransportRequestHandler<QuerySearchRequest> {

        @Override
        public QuerySearchRequest newInstance() {
            return new QuerySearchRequest();
        }

        @Override
        public void messageReceived(QuerySearchRequest request, TransportChannel channel) throws Exception {
            QueryFetchSearchResult result = searchService.executeFetchPhase(request);
            channel.sendResponse(result);
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SEARCH;
        }
    }

    private abstract class FetchByIdTransportHandler<Request extends ShardFetchRequest> extends BaseTransportRequestHandler<Request> {

        public abstract Request newInstance();

        @Override
        public void messageReceived(Request request, TransportChannel channel) throws Exception {
            FetchSearchResult result = searchService.executeFetchPhase(request);
            channel.sendResponse(result);
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SEARCH;
        }
    }

    private class ScrollFetchByIdTransportHandler extends FetchByIdTransportHandler<ShardFetchRequest> {
        @Override
        public ShardFetchRequest newInstance() {
            return new ShardFetchRequest();
        }
    }

    private class SearchFetchByIdTransportHandler extends FetchByIdTransportHandler<ShardFetchSearchRequest> {
        @Override
        public ShardFetchSearchRequest newInstance() {
            return new ShardFetchSearchRequest();
        }
    }

    private class SearchQueryFetchScrollTransportHandler extends BaseTransportRequestHandler<InternalScrollSearchRequest> {

        @Override
        public InternalScrollSearchRequest newInstance() {
            return new InternalScrollSearchRequest();
        }

        @Override
        public void messageReceived(InternalScrollSearchRequest request, TransportChannel channel) throws Exception {
            ScrollQueryFetchSearchResult result = searchService.executeFetchPhase(request);
            channel.sendResponse(result);
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SEARCH;
        }
    }

    private class SearchScanTransportHandler extends BaseTransportRequestHandler<ShardSearchTransportRequest> {

        @Override
        public ShardSearchTransportRequest newInstance() {
            return new ShardSearchTransportRequest();
        }

        @Override
        public void messageReceived(ShardSearchTransportRequest request, TransportChannel channel) throws Exception {
            QuerySearchResult result = searchService.executeScan(request);
            channel.sendResponse(result);
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SEARCH;
        }
    }

    private class SearchScanScrollTransportHandler extends BaseTransportRequestHandler<InternalScrollSearchRequest> {

        @Override
        public InternalScrollSearchRequest newInstance() {
            return new InternalScrollSearchRequest();
        }

        @Override
        public void messageReceived(InternalScrollSearchRequest request, TransportChannel channel) throws Exception {
            ScrollQueryFetchSearchResult result = searchService.executeScan(request);
            channel.sendResponse(result);
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SEARCH;
        }
    }
}
