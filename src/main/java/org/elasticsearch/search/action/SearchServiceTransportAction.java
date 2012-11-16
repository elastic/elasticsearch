/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.LongStreamable;
import org.elasticsearch.common.io.stream.VoidStreamable;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.fetch.FetchSearchRequest;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.QueryFetchSearchResult;
import org.elasticsearch.search.fetch.ScrollQueryFetchSearchResult;
import org.elasticsearch.search.internal.InternalScrollSearchRequest;
import org.elasticsearch.search.internal.InternalSearchRequest;
import org.elasticsearch.search.query.QuerySearchRequest;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.query.ScrollQuerySearchResult;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

/**
 * An encapsulation of {@link org.elasticsearch.search.SearchService} operations exposed through
 * transport.
 */
public class SearchServiceTransportAction extends AbstractComponent {

    static final class FreeContextResponseHandler extends VoidTransportResponseHandler {

        private final ESLogger logger;

        FreeContextResponseHandler(ESLogger logger) {
            super(ThreadPool.Names.SAME);
            this.logger = logger;
        }

        @Override
        public void handleException(TransportException exp) {
            logger.warn("Failed to send release search context", exp);
        }
    }

    private final TransportService transportService;

    private final ClusterService clusterService;

    private final SearchService searchService;

    private final FreeContextResponseHandler freeContextResponseHandler = new FreeContextResponseHandler(logger);

    @Inject
    public SearchServiceTransportAction(Settings settings, TransportService transportService, ClusterService clusterService, SearchService searchService) {
        super(settings);
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.searchService = searchService;

        transportService.registerHandler(SearchFreeContextTransportHandler.ACTION, new SearchFreeContextTransportHandler());
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

    public void sendFreeContext(DiscoveryNode node, final long contextId) {
        if (clusterService.state().nodes().localNodeId().equals(node.id())) {
            searchService.freeContext(contextId);
        } else {
            transportService.sendRequest(node, SearchFreeContextTransportHandler.ACTION, new LongStreamable(contextId), freeContextResponseHandler);
        }
    }

    public void sendExecuteDfs(DiscoveryNode node, final InternalSearchRequest request, final SearchServiceListener<DfsSearchResult> listener) {
        if (clusterService.state().nodes().localNodeId().equals(node.id())) {
            try {
                DfsSearchResult result = searchService.executeDfsPhase(request);
                listener.onResult(result);
            } catch (Exception e) {
                listener.onFailure(e);
            }
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

    public void sendExecuteQuery(DiscoveryNode node, final InternalSearchRequest request, final SearchServiceListener<QuerySearchResult> listener) {
        if (clusterService.state().nodes().localNodeId().equals(node.id())) {
            try {
                QuerySearchResult result = searchService.executeQueryPhase(request);
                listener.onResult(result);
            } catch (Exception e) {
                listener.onFailure(e);
            }
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
            try {
                QuerySearchResult result = searchService.executeQueryPhase(request);
                listener.onResult(result);
            } catch (Exception e) {
                listener.onFailure(e);
            }
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
            try {
                ScrollQuerySearchResult result = searchService.executeQueryPhase(request);
                listener.onResult(result.queryResult());
            } catch (Exception e) {
                listener.onFailure(e);
            }
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

    public void sendExecuteFetch(DiscoveryNode node, final InternalSearchRequest request, final SearchServiceListener<QueryFetchSearchResult> listener) {
        if (clusterService.state().nodes().localNodeId().equals(node.id())) {
            try {
                QueryFetchSearchResult result = searchService.executeFetchPhase(request);
                listener.onResult(result);
            } catch (Exception e) {
                listener.onFailure(e);
            }
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
            try {
                QueryFetchSearchResult result = searchService.executeFetchPhase(request);
                listener.onResult(result);
            } catch (Exception e) {
                listener.onFailure(e);
            }
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
            try {
                ScrollQueryFetchSearchResult result = searchService.executeFetchPhase(request);
                listener.onResult(result.result());
            } catch (Exception e) {
                listener.onFailure(e);
            }
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
            try {
                FetchSearchResult result = searchService.executeFetchPhase(request);
                listener.onResult(result);
            } catch (Exception e) {
                listener.onFailure(e);
            }
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

    public void sendExecuteScan(DiscoveryNode node, final InternalSearchRequest request, final SearchServiceListener<QuerySearchResult> listener) {
        if (clusterService.state().nodes().localNodeId().equals(node.id())) {
            try {
                QuerySearchResult result = searchService.executeScan(request);
                listener.onResult(result);
            } catch (Exception e) {
                listener.onFailure(e);
            }
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
            try {
                ScrollQueryFetchSearchResult result = searchService.executeScan(request);
                listener.onResult(result.result());
            } catch (Exception e) {
                listener.onFailure(e);
            }
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

    private class SearchFreeContextTransportHandler extends BaseTransportRequestHandler<LongStreamable> {

        static final String ACTION = "search/freeContext";

        @Override
        public LongStreamable newInstance() {
            return new LongStreamable();
        }

        @Override
        public void messageReceived(LongStreamable request, TransportChannel channel) throws Exception {
            searchService.freeContext(request.get());
            channel.sendResponse(VoidStreamable.INSTANCE);
        }

        @Override
        public String executor() {
            // freeing the context is cheap,
            // no need for fork it to another thread
            return ThreadPool.Names.SAME;
        }
    }


    private class SearchDfsTransportHandler extends BaseTransportRequestHandler<InternalSearchRequest> {

        static final String ACTION = "search/phase/dfs";

        @Override
        public InternalSearchRequest newInstance() {
            return new InternalSearchRequest();
        }

        @Override
        public void messageReceived(InternalSearchRequest request, TransportChannel channel) throws Exception {
            DfsSearchResult result = searchService.executeDfsPhase(request);
            channel.sendResponse(result);
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SEARCH;
        }
    }

    private class SearchQueryTransportHandler extends BaseTransportRequestHandler<InternalSearchRequest> {

        static final String ACTION = "search/phase/query";

        @Override
        public InternalSearchRequest newInstance() {
            return new InternalSearchRequest();
        }

        @Override
        public void messageReceived(InternalSearchRequest request, TransportChannel channel) throws Exception {
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

    private class SearchQueryFetchTransportHandler extends BaseTransportRequestHandler<InternalSearchRequest> {

        static final String ACTION = "search/phase/query+fetch";

        @Override
        public InternalSearchRequest newInstance() {
            return new InternalSearchRequest();
        }

        @Override
        public void messageReceived(InternalSearchRequest request, TransportChannel channel) throws Exception {
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

    private class SearchScanTransportHandler extends BaseTransportRequestHandler<InternalSearchRequest> {

        static final String ACTION = "search/phase/scan";

        @Override
        public InternalSearchRequest newInstance() {
            return new InternalSearchRequest();
        }

        @Override
        public void messageReceived(InternalSearchRequest request, TransportChannel channel) throws Exception {
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
