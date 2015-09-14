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
package org.elasticsearch.action.admin.indices.warmer.delete;

import org.elasticsearch.action.ActionListener;
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
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.search.warmer.IndexWarmerMissingException;
import org.elasticsearch.search.warmer.IndexWarmersMetaData;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Internal Actions executed on the master deleting the warmer from the cluster state metadata.
 *
 * Note: this is an internal API and should not be used / called by any client code.
 */
public class TransportDeleteWarmerAction extends TransportMasterNodeAction<DeleteWarmerRequest, DeleteWarmerResponse> {

    @Inject
    public TransportDeleteWarmerAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                       ThreadPool threadPool, ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, DeleteWarmerAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver, DeleteWarmerRequest::new);
    }

    @Override
    protected String executor() {
        // we go async right away
        return ThreadPool.Names.SAME;
    }

    @Override
    protected DeleteWarmerResponse newResponse() {
        return new DeleteWarmerResponse();
    }

    @Override
    protected ClusterBlockException checkBlock(DeleteWarmerRequest request, ClusterState state) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, indexNameExpressionResolver.concreteIndices(state, request));
    }

    @Override
    protected void masterOperation(final DeleteWarmerRequest request, final ClusterState state, final ActionListener<DeleteWarmerResponse> listener) {
        final String[] concreteIndices = indexNameExpressionResolver.concreteIndices(state, request);
        clusterService.submitStateUpdateTask("delete_warmer [" + Arrays.toString(request.names()) + "]", new AckedClusterStateUpdateTask<DeleteWarmerResponse>(request, listener) {

            @Override
            protected DeleteWarmerResponse newResponse(boolean acknowledged) {
                return new DeleteWarmerResponse(acknowledged);
            }

            @Override
            public void onFailure(String source, Throwable t) {
                logger.debug("failed to delete warmer [{}] on indices [{}]", t, Arrays.toString(request.names()), concreteIndices);
                super.onFailure(source, t);
            }

            @Override
            public ClusterState execute(ClusterState currentState) {
                MetaData.Builder mdBuilder = MetaData.builder(currentState.metaData());

                boolean globalFoundAtLeastOne = false;
                boolean deleteAll = false;
                for (int i=0; i<request.names().length; i++){
                    if (request.names()[i].equals(MetaData.ALL)) {
                        deleteAll = true;
                        break;
                    }
                }

                for (String index : concreteIndices) {
                    IndexMetaData indexMetaData = currentState.metaData().index(index);
                    if (indexMetaData == null) {
                        throw new IndexNotFoundException(index);
                    }
                    IndexWarmersMetaData warmers = indexMetaData.custom(IndexWarmersMetaData.TYPE);
                    if (warmers != null) {
                        List<IndexWarmersMetaData.Entry> entries = new ArrayList<>();
                        for (IndexWarmersMetaData.Entry entry : warmers.entries()) {
                            boolean keepWarmer = true;
                            for (String warmer : request.names()) {
                                if (Regex.simpleMatch(warmer, entry.name()) || warmer.equals(MetaData.ALL)) {
                                    globalFoundAtLeastOne = true;
                                    keepWarmer =  false;
                                    // don't add it...
                                    break;
                                } 
                            }
                            if (keepWarmer) {
                                entries.add(entry);
                            }
                        }
                        // a change, update it...
                        if (entries.size() != warmers.entries().size()) {
                            warmers = new IndexWarmersMetaData(entries.toArray(new IndexWarmersMetaData.Entry[entries.size()]));
                            IndexMetaData.Builder indexBuilder = IndexMetaData.builder(indexMetaData).putCustom(IndexWarmersMetaData.TYPE, warmers);
                            mdBuilder.put(indexBuilder);
                        }
                    }
                }

                if (globalFoundAtLeastOne == false && deleteAll == false) {
                    throw new IndexWarmerMissingException(request.names());
                }

                if (logger.isInfoEnabled()) {
                    for (String index : concreteIndices) {
                        IndexMetaData indexMetaData = currentState.metaData().index(index);
                        if (indexMetaData == null) {
                            throw new IndexNotFoundException(index);
                        }
                        IndexWarmersMetaData warmers = indexMetaData.custom(IndexWarmersMetaData.TYPE);
                        if (warmers != null) {
                            for (IndexWarmersMetaData.Entry entry : warmers.entries()) {
                                for (String warmer : request.names()) {
                                    if (Regex.simpleMatch(warmer, entry.name()) || warmer.equals(MetaData.ALL)) {
                                        logger.info("[{}] delete warmer [{}]", index, entry.name());
                                    }
                                }
                            }
                        } else if(deleteAll){
                            logger.debug("no warmers to delete on index [{}]", index);
                        }
                    }
                }

                return ClusterState.builder(currentState).metaData(mdBuilder).build();
            }
        });
    }
}
