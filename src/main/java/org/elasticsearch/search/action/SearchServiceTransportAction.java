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
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.fetch.FetchSearchRequest;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.QueryFetchSearchResult;
import org.elasticsearch.search.fetch.ScrollQueryFetchSearchResult;
import org.elasticsearch.search.internal.InternalScrollSearchRequest;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.query.QuerySearchRequest;
import org.elasticsearch.search.query.QuerySearchResult;
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

        transportService.registerHandler(SearchFreeContextTransportHandler.ACTION, new SearchFreeContextTransportHandler());
        transportService.registerHandler(ClearScrollContextsTransportHandler.ACTION, new ClearScrollContextsTransportHandler());
        transportService.registerHandler(SearchDfsTransportHandler.ACTION, new SearchDfsTransportHandler());
        transportService.registerHandler(SearchQueryTransportHandler.ACTION, new SearchQueryTransportHandler());
        transportService.registerHandler(SearchQueryByIdTransportHandler.ACTION, new SearchQueryByIdTransportHandler());
        transportService.registerHandler(SearchQueryScrollTransportHandler.ACTION, new SearchQueryScrollTransportHandler());
        transportService.registerHandler(SearchQueryFetchTransportHandler.ACTION, new SearchQueryFetchTransportHandler());
        transportService.registerHandler(SearchQueryQueryFetchTransportHandler.ACTION, new SearchQueryQueryFetchTransportHandler());
        transportService.registerHandler(SearchQueryFetchScrollTransportHandler.ACTION, new SearchQueryFetchScrollTransportHandler());
        transportService.registerHandler(SearchFetchByIdTransportHandler.ACTION, new SearchFetchByIdTransportHandler());
        transportService.registerHandler(SearchScanTransportHandler.ACTION, new SearchScanTransportHandler());
        transportService.registerHandler(SearchScanScrollTransportHandler.ACTION, new SearchScanScrollTransportHandler());
    }

    public void sendFreeContext(DiscoveryNode node, final long contextId, SearchRequest request) {
        if (clusterService.state().nodes().localNodeId().equals(node.id())) {
            searchService.freeContext(contextId);
        } else {
            transportService.sendRequest(node, SearchFreeContextTransportHandler.ACTION, new SearchFreeContextRequest(request, contextId), freeContextResponseHandler);
        }
    }

    public void sendFreeContext(DiscoveryNode node, long contextId, ClearScrollRequest request, final ActionListener<Boolean> actionListener) {
        if (clusterService.state().nodes().localNodeId().equals(node.id())) {
            boolean freed = searchService.freeContext(contextId);
            actionListener.onResponse(freed);
        } else {
            transportService.sendRequest(node, SearchFreeContextTransportHandler.ACTION, new SearchFreeContextRequest(request, contextId), new FreeContextResponseHandler(actionListener));
        }
    }

    public void sendClearAllScrollContexts(DiscoveryNode node, ClearScrollRequest request, final ActionListener<Boolean> actionListener) {
        if (clusterService.state().nodes().localNodeId().equals(node.id())) {
            searchService.freeAllScrollContexts();
            actionListener.onResponse(true);
        } else {
            transportService.sendRequest(node, ClearScrollContextsTransportHandler.ACTION, new ClearScrollContextsRequest(request), new TransportResponseHandler<TransportResponse>() {
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

    public void sendExecuteDfs(DiscoveryNode node, final ShardSearchRequest request, final SearchServiceListener<DfsSearchResult> listener) {
        if (clusterService.state().nodes().localNodeId().equals(node.id())) {
            execute(new Callable<DfsSearchResult>() {
                @Override
                public DfsSearchResult call() throws Exception {
                    return searchService.executeDfsPhase(request);
                }
            }, listener);
        } else {
            transportService.sendRequest(node, SearchDfsTransportHandler.ACTION, request, new BaseTransportResponseHandler<DfsSearchResult>() {

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

    public void sendExecuteQuery(DiscoveryNode node, final ShardSearchRequest request, final SearchServiceListener<QuerySearchResult> listener) {
        if (clusterService.state().nodes().localNodeId().equals(node.id())) {
            execute(new Callable<QuerySearchResult>() {
                @Override
                public QuerySearchResult call() throws Exception {
                    return searchService.executeQueryPhase(request);
                }
            }, listener);
        } else {
            transportService.sendRequest(node, SearchQueryTransportHandler.ACTION, request, new BaseTransportResponseHandler<QuerySearchResult>() {

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

    public void sendExecuteQuery(DiscoveryNode node, final QuerySearchRequest request, final SearchServiceListener<QuerySearchResult> listener) {
        if (clusterService.state().nodes().localNodeId().equals(node.id())) {
            execute(new Callable<QuerySearchResult>() {
                @Override
                public QuerySearchResult call() throws Exception {
                    return searchService.executeQueryPhase(request);
                }
            }, listener);
        } else {
            transportService.sendRequest(node, SearchQueryByIdTransportHandler.ACTION, request, new BaseTransportResponseHandler<QuerySearchResult>() {

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
            transportService.sendRequest(node, SearchQueryScrollTransportHandler.ACTION, request, new BaseTransportResponseHandler<ScrollQuerySearchResult>() {

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

    public void sendExecuteFetch(DiscoveryNode node, final ShardSearchRequest request, final SearchServiceListener<QueryFetchSearchResult> listener) {
        if (clusterService.state().nodes().localNodeId().equals(node.id())) {
            execute(new Callable<QueryFetchSearchResult>() {
                @Override
                public QueryFetchSearchResult call() throws Exception {
                    return searchService.executeFetchPhase(request);
                }
            }, listener);
        } else {
            transportService.sendRequest(node, SearchQueryFetchTransportHandler.ACTION, request, new BaseTransportResponseHandler<QueryFetchSearchResult>() {

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
            transportService.sendRequest(node, SearchQueryQueryFetchTransportHandler.ACTION, request, new BaseTransportResponseHandler<QueryFetchSearchResult>() {

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
            transportService.sendRequest(node, SearchQueryFetchScrollTransportHandler.ACTION, request, new BaseTransportResponseHandler<ScrollQueryFetchSearchResult>() {

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

    public void sendExecuteFetch(DiscoveryNode node, final FetchSearchRequest request, final SearchServiceListener<FetchSearchResult> listener) {
        if (clusterService.state().nodes().localNodeId().equals(node.id())) {
            execute(new Callable<FetchSearchResult>() {
                @Override
                public FetchSearchResult call() throws Exception {
                    return searchService.executeFetchPhase(request);
                }
            }, listener);
        } else {
            transportService.sendRequest(node, SearchFetchByIdTransportHandler.ACTION, request, new BaseTransportResponseHandler<FetchSearchResult>() {

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

    public void sendExecuteScan(DiscoveryNode node, final ShardSearchRequest request, final SearchServiceListener<QuerySearchResult> listener) {
        if (clusterService.state().nodes().localNodeId().equals(node.id())) {
            execute(new Callable<QuerySearchResult>() {
                @Override
                public QuerySearchResult call() throws Exception {
                    return searchService.executeScan(request);
                }
            }, listener);
        } else {
            transportService.sendRequest(node, SearchScanTransportHandler.ACTION, request, new BaseTransportResponseHandler<QuerySearchResult>() {

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
            transportService.sendRequest(node, SearchScanScrollTransportHandler.ACTION, request, new BaseTransportResponseHandler<ScrollQueryFetchSearchResult>() {

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

    static class SearchFreeContextRequest extends TransportRequest {

        private long id;

        SearchFreeContextRequest() {
        }

        SearchFreeContextRequest(TransportRequest request, long id) {
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

    class SearchFreeContextTransportHandler extends BaseTransportRequestHandler<SearchFreeContextRequest> {

        static final String ACTION = "search/freeContext";

        @Override
        public SearchFreeContextRequest newInstance() {
            return new SearchFreeContextRequest();
        }

        @Override
        public void messageReceived(SearchFreeContextRequest request, TransportChannel channel) throws Exception {
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

    static class ClearScrollContextsRequest extends TransportRequest {

        ClearScrollContextsRequest() {
        }

        ClearScrollContextsRequest(TransportRequest request) {
            super(request);
        }

    }

    class ClearScrollContextsTransportHandler extends BaseTransportRequestHandler<ClearScrollContextsRequest> {

        static final String ACTION = "search/clearScrollContexts";

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

    private class SearchDfsTransportHandler extends BaseTransportRequestHandler<ShardSearchRequest> {

        static final String ACTION = "search/phase/dfs";

        @Override
        public ShardSearchRequest newInstance() {
            return new ShardSearchRequest();
        }

        @Override
        public void messageReceived(ShardSearchRequest request, TransportChannel channel) throws Exception {
            DfsSearchResult result = searchService.executeDfsPhase(request);
            channel.sendResponse(result);
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SEARCH;
        }
    }

    private class SearchQueryTransportHandler extends BaseTransportRequestHandler<ShardSearchRequest> {

        static final String ACTION = "search/phase/query";

        @Override
        public ShardSearchRequest newInstance() {
            return new ShardSearchRequest();
        }

        @Override
        public void messageReceived(ShardSearchRequest request, TransportChannel channel) throws Exception {
            QuerySearchResult result = searchService.executeQueryPhase(request);
            channel.sendResponse(result);
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SEARCH;
        }
    }

    private class SearchQueryByIdTransportHandler extends BaseTransportRequestHandler<QuerySearchRequest> {

        static final String ACTION = "search/phase/query/id";

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

        static final String ACTION = "search/phase/query/scroll";

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

    private class SearchQueryFetchTransportHandler extends BaseTransportRequestHandler<ShardSearchRequest> {

        static final String ACTION = "search/phase/query+fetch";

        @Override
        public ShardSearchRequest newInstance() {
            return new ShardSearchRequest();
        }

        @Override
        public void messageReceived(ShardSearchRequest request, TransportChannel channel) throws Exception {
            QueryFetchSearchResult result = searchService.executeFetchPhase(request);
            channel.sendResponse(result);
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SEARCH;
        }
    }

    private class SearchQueryQueryFetchTransportHandler extends BaseTransportRequestHandler<QuerySearchRequest> {

        static final String ACTION = "search/phase/query/query+fetch";

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

    private class SearchFetchByIdTransportHandler extends BaseTransportRequestHandler<FetchSearchRequest> {

        static final String ACTION = "search/phase/fetch/id";

        @Override
        public FetchSearchRequest newInstance() {
            return new FetchSearchRequest();
        }

        @Override
        public void messageReceived(FetchSearchRequest request, TransportChannel channel) throws Exception {
            FetchSearchResult result = searchService.executeFetchPhase(request);
            channel.sendResponse(result);
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SEARCH;
        }
    }

    private class SearchQueryFetchScrollTransportHandler extends BaseTransportRequestHandler<InternalScrollSearchRequest> {

        static final String ACTION = "search/phase/query+fetch/scroll";

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

    private class SearchScanTransportHandler extends BaseTransportRequestHandler<ShardSearchRequest> {

        static final String ACTION = "search/phase/scan";

        @Override
        public ShardSearchRequest newInstance() {
            return new ShardSearchRequest();
        }

        @Override
        public void messageReceived(ShardSearchRequest request, TransportChannel channel) throws Exception {
            QuerySearchResult result = searchService.executeScan(request);
            channel.sendResponse(result);
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SEARCH;
        }
    }

    private class SearchScanScrollTransportHandler extends BaseTransportRequestHandler<InternalScrollSearchRequest> {

        static final String ACTION = "search/phase/scan/scroll";

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
