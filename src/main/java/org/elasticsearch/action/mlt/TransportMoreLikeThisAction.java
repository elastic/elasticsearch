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

package org.elasticsearch.action.mlt;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.termvector.TermVectorRequest;
import org.elasticsearch.action.termvector.TermVectorResponse;
import org.elasticsearch.action.termvector.TransportSingleShardTermVectorAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.MutableShardRouting;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.query.MoreLikeThisQueryBuilder;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import static org.elasticsearch.index.query.QueryBuilders.moreLikeThisQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;

/**
 * The more like this action.
 */
public class TransportMoreLikeThisAction extends HandledTransportAction<MoreLikeThisRequest, SearchResponse> {

    private final TransportSearchAction searchAction;

    private final TransportSingleShardTermVectorAction termVectorAction;

    private final IndicesService indicesService;

    private final ClusterService clusterService;

    private final TransportService transportService;

    @Inject
    public TransportMoreLikeThisAction(Settings settings, ThreadPool threadPool, TransportSearchAction searchAction, TransportSingleShardTermVectorAction getAction,
                                       ClusterService clusterService, IndicesService indicesService, TransportService transportService, ActionFilters actionFilters) {
        super(settings, MoreLikeThisAction.NAME, threadPool, transportService, actionFilters);
        this.searchAction = searchAction;
        this.termVectorAction = getAction;
        this.indicesService = indicesService;
        this.clusterService = clusterService;
        this.transportService = transportService;
    }

    @Override
    public MoreLikeThisRequest newRequestInstance(){
        return new MoreLikeThisRequest();
    }

    @Override
    protected void doExecute(final MoreLikeThisRequest request, final ActionListener<SearchResponse> listener) {
        // update to actual index name
        ClusterState clusterState = clusterService.state();
        // update to the concrete index
        final String concreteIndex = clusterState.metaData().concreteSingleIndex(request.index(), request.indicesOptions());

        Iterable<MutableShardRouting> routingNode = clusterState.getRoutingNodes().routingNodeIter(clusterService.localNode().getId());
        if (routingNode == null) {
            redirect(request, concreteIndex, listener, clusterState);
            return;
        }
        boolean hasIndexLocally = false;
        for (MutableShardRouting shardRouting : routingNode) {
            if (concreteIndex.equals(shardRouting.index())) {
                hasIndexLocally = true;
                break;
            }
        }
        if (!hasIndexLocally) {
            redirect(request, concreteIndex, listener, clusterState);
            return;
        }

        String[] selectedFields = (request.fields() == null || request.fields().length == 0) ? new String[]{"*"} : request.fields();
        TermVectorRequest termVectorRequest = new TermVectorRequest()
                .index(request.index())
                .type(request.type())
                .id(request.id())
                .selectedFields(selectedFields)
                .routing(request.routing())
                .listenerThreaded(true)
                .operationThreaded(true);

        request.beforeLocalFork();
        termVectorAction.execute(termVectorRequest, new ActionListener<TermVectorResponse>() {
            @Override
            public void onResponse(TermVectorResponse termVectorResponse) {
                if (!termVectorResponse.isExists()) {
                    listener.onFailure(new DocumentMissingException(null, request.type(), request.id()));
                    return;
                }
                final MoreLikeThisQueryBuilder mltQuery = getMoreLikeThis(request, true);
                try {
                    mltQuery.setTermVectorResponse(termVectorResponse);
                } catch (Throwable e) {
                    listener.onFailure(e);
                    return;
                }

                String[] searchIndices = request.searchIndices();
                if (searchIndices == null) {
                    searchIndices = new String[]{request.index()};
                }
                String[] searchTypes = request.searchTypes();
                if (searchTypes == null) {
                    searchTypes = new String[]{request.type()};
                }

                SearchRequest searchRequest = new SearchRequest(request).indices(searchIndices)
                        .types(searchTypes)
                        .searchType(request.searchType())
                        .scroll(request.searchScroll())
                        .listenerThreaded(request.listenerThreaded());

                SearchSourceBuilder extraSource = searchSource().query(mltQuery);
                if (request.searchFrom() != 0) {
                    extraSource.from(request.searchFrom());
                }
                if (request.searchSize() != 0) {
                    extraSource.size(request.searchSize());
                }
                searchRequest.extraSource(extraSource);

                if (request.searchSource() != null) {
                    searchRequest.source(request.searchSource(), request.searchSourceUnsafe());
                }

                searchAction.execute(searchRequest, new ActionListener<SearchResponse>() {
                    @Override
                    public void onResponse(SearchResponse response) {
                        listener.onResponse(response);
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        listener.onFailure(e);
                    }
                });

            }

            @Override
            public void onFailure(Throwable e) {
                listener.onFailure(e);
            }
        });
    }

    // Redirects the request to a data node, that has the index meta data locally available.
    private void redirect(MoreLikeThisRequest request, String concreteIndex, final ActionListener<SearchResponse> listener, ClusterState clusterState) {
        ShardIterator shardIterator = clusterService.operationRouting().getShards(clusterState, concreteIndex, request.type(), request.id(), request.routing(), null);
        ShardRouting shardRouting = shardIterator.nextOrNull();
        if (shardRouting == null) {
            throw new ElasticsearchException("No shards for index " + request.index());
        }
        String nodeId = shardRouting.currentNodeId();
        DiscoveryNode discoveryNode = clusterState.nodes().get(nodeId);
        transportService.sendRequest(discoveryNode, MoreLikeThisAction.NAME, request, new TransportResponseHandler<SearchResponse>() {

            @Override
            public SearchResponse newInstance() {
                return new SearchResponse();
            }

            @Override
            public void handleResponse(SearchResponse response) {
                listener.onResponse(response);
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

    private MoreLikeThisQueryBuilder getMoreLikeThis(MoreLikeThisRequest request, boolean failOnUnsupportedField) {
        return moreLikeThisQuery(request.fields())
                .minimumShouldMatch(request.minimumShouldMatch())
                .boostTerms(request.boostTerms())
                .minDocFreq(request.minDocFreq())
                .maxDocFreq(request.maxDocFreq())
                .minWordLength(request.minWordLength())
                .maxWordLength(request.maxWordLength())
                .minTermFreq(request.minTermFreq())
                .maxQueryTerms(request.maxQueryTerms())
                .stopWords(request.stopWords())
                .include(request.include())
                .failOnUnsupportedField(failOnUnsupportedField);
    }
}
