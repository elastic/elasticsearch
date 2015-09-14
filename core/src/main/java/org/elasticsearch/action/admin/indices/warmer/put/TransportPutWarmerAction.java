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

package org.elasticsearch.action.admin.indices.warmer.put;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.search.warmer.IndexWarmersMetaData;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Internal Actions executed on the master associating a warmer with a name in the cluster state metadata.
 *
 * Note: this is an internal API and should not be used / called by any client code.
 */
public class TransportPutWarmerAction extends TransportMasterNodeAction<PutWarmerRequest, PutWarmerResponse> {

    private final TransportSearchAction searchAction;

    @Inject
    public TransportPutWarmerAction(Settings settings, TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                    TransportSearchAction searchAction, ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, PutWarmerAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver, PutWarmerRequest::new);
        this.searchAction = searchAction;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected PutWarmerResponse newResponse() {
        return new PutWarmerResponse();
    }

    @Override
    protected ClusterBlockException checkBlock(PutWarmerRequest request, ClusterState state) {
        String[] concreteIndices = indexNameExpressionResolver.concreteIndices(state, request);
        ClusterBlockException status = state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, concreteIndices);
        if (status != null) {
            return status;
        }
        // PutWarmer executes a SearchQuery before adding the new warmer to the cluster state,
        // so we need to check the same block as TransportSearchTypeAction here
        return state.blocks().indicesBlockedException(ClusterBlockLevel.READ, concreteIndices);
    }

    @Override
    protected void masterOperation(final PutWarmerRequest request, final ClusterState state, final ActionListener<PutWarmerResponse> listener) {
        // first execute the search request, see that its ok...
        SearchRequest searchRequest = new SearchRequest(request.searchRequest(), request);
        searchAction.execute(searchRequest, new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse searchResponse) {
                if (searchResponse.getFailedShards() > 0) {
                    listener.onFailure(new ElasticsearchException("search failed with failed shards: " + Arrays.toString(searchResponse.getShardFailures())));
                    return;
                }

                clusterService.submitStateUpdateTask("put_warmer [" + request.name() + "]", new AckedClusterStateUpdateTask<PutWarmerResponse>(request, listener) {

                    @Override
                    protected PutWarmerResponse newResponse(boolean acknowledged) {
                        return new PutWarmerResponse(acknowledged);
                    }

                    @Override
                    public void onFailure(String source, Throwable t) {
                        logger.debug("failed to put warmer [{}] on indices [{}]", t, request.name(), request.searchRequest().indices());
                        super.onFailure(source, t);
                    }

                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        MetaData metaData = currentState.metaData();
                        String[] concreteIndices = indexNameExpressionResolver.concreteIndices(currentState, request.searchRequest().indicesOptions(), request.searchRequest().indices());

                        BytesReference source = null;
                        if (request.searchRequest().source() != null && request.searchRequest().source().length() > 0) {
                            source = request.searchRequest().source();
                        } else if (request.searchRequest().extraSource() != null && request.searchRequest().extraSource().length() > 0) {
                            source = request.searchRequest().extraSource();
                        }

                        // now replace it on the metadata
                        MetaData.Builder mdBuilder = MetaData.builder(currentState.metaData());

                        for (String index : concreteIndices) {
                            IndexMetaData indexMetaData = metaData.index(index);
                            if (indexMetaData == null) {
                                throw new IndexNotFoundException(index);
                            }
                            IndexWarmersMetaData warmers = indexMetaData.custom(IndexWarmersMetaData.TYPE);
                            if (warmers == null) {
                                logger.info("[{}] putting warmer [{}]", index, request.name());
                                warmers = new IndexWarmersMetaData(new IndexWarmersMetaData.Entry(request.name(), request.searchRequest().types(), request.searchRequest().requestCache(), source));
                            } else {
                                boolean found = false;
                                List<IndexWarmersMetaData.Entry> entries = new ArrayList<>(warmers.entries().size() + 1);
                                for (IndexWarmersMetaData.Entry entry : warmers.entries()) {
                                    if (entry.name().equals(request.name())) {
                                        found = true;
                                        entries.add(new IndexWarmersMetaData.Entry(request.name(), request.searchRequest().types(), request.searchRequest().requestCache(), source));
                                    } else {
                                        entries.add(entry);
                                    }
                                }
                                if (!found) {
                                    logger.info("[{}] put warmer [{}]", index, request.name());
                                    entries.add(new IndexWarmersMetaData.Entry(request.name(), request.searchRequest().types(), request.searchRequest().requestCache(), source));
                                } else {
                                    logger.info("[{}] update warmer [{}]", index, request.name());
                                }
                                warmers = new IndexWarmersMetaData(entries.toArray(new IndexWarmersMetaData.Entry[entries.size()]));
                            }
                            IndexMetaData.Builder indexBuilder = IndexMetaData.builder(indexMetaData).putCustom(IndexWarmersMetaData.TYPE, warmers);
                            mdBuilder.put(indexBuilder);
                        }

                        return ClusterState.builder(currentState).metaData(mdBuilder).build();
                    }
                });
            }

            @Override
            public void onFailure(Throwable e) {
                listener.onFailure(e);
            }
        });
    }
}
