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

package org.elasticsearch.action.admin.indices.delete;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingResponse;
import org.elasticsearch.action.admin.indices.mapping.delete.TransportDeleteMappingAction;
import org.elasticsearch.action.support.master.TransportMasterNodeOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaDataDeleteIndexService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.percolator.PercolatorService;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Delete index action.
 */
public class TransportDeleteIndexAction extends TransportMasterNodeOperationAction<DeleteIndexRequest, DeleteIndexResponse> {

    private final MetaDataDeleteIndexService deleteIndexService;

    private final TransportDeleteMappingAction deleteMappingAction;

    private final boolean disableDeleteAllIndices;

    @Inject
    public TransportDeleteIndexAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                      ThreadPool threadPool, MetaDataDeleteIndexService deleteIndexService, TransportDeleteMappingAction deleteMappingAction) {
        super(settings, transportService, clusterService, threadPool);
        this.deleteIndexService = deleteIndexService;
        this.deleteMappingAction = deleteMappingAction;

        this.disableDeleteAllIndices = settings.getAsBoolean("action.disable_delete_all_indices", false);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected String transportAction() {
        return DeleteIndexAction.NAME;
    }

    @Override
    protected DeleteIndexRequest newRequest() {
        return new DeleteIndexRequest();
    }

    @Override
    protected DeleteIndexResponse newResponse() {
        return new DeleteIndexResponse();
    }

    @Override
    protected void doExecute(DeleteIndexRequest request, ActionListener<DeleteIndexResponse> listener) {
        ClusterState state = clusterService.state();
        String[] indicesOrAliases = request.indices();
        request.indices(state.metaData().concreteIndices(request.indices()));
        if (disableDeleteAllIndices) {
            // simple check on the original indices with "all" default parameter
            if (indicesOrAliases == null || indicesOrAliases.length == 0 || (indicesOrAliases.length == 1 && indicesOrAliases[0].equals("_all"))) {
                throw new ElasticSearchIllegalArgumentException("deleting all indices is disabled");
            }
            // if we end up matching on all indices, check, if its a wildcard parameter, or a "-something" structure
            if (request.indices().length == state.metaData().concreteAllIndices().length && indicesOrAliases.length > 0) {
                boolean hasRegex = false;
                for (String indexOrAlias : indicesOrAliases) {
                    if (Regex.isSimpleMatchPattern(indexOrAlias)) {
                        hasRegex = true;
                    }
                }
                if (indicesOrAliases.length > 0 && (hasRegex || indicesOrAliases[0].charAt(0) == '-')) {
                    throw new ElasticSearchIllegalArgumentException("deleting all indices is disabled");
                }
            }
        }
        super.doExecute(request, listener);
    }

    @Override
    protected ClusterBlockException checkBlock(DeleteIndexRequest request, ClusterState state) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA, request.indices());
    }

    @Override
    protected void masterOperation(final DeleteIndexRequest request, final ClusterState state, final ActionListener<DeleteIndexResponse> listener) throws ElasticSearchException {
        if (request.indices().length == 0) {
            listener.onResponse(new DeleteIndexResponse(true));
            return;
        }
        // TODO: this API should be improved, currently, if one delete index failed, we send a failure, we should send a response array that includes all the indices that were deleted
        final CountDown count = new CountDown(request.indices().length);
        for (final String index : request.indices()) {
            deleteIndexService.deleteIndex(new MetaDataDeleteIndexService.Request(index).timeout(request.timeout()).masterTimeout(request.masterNodeTimeout()), new MetaDataDeleteIndexService.Listener() {

                private volatile Throwable lastFailure;

                @Override
                public void onResponse(final MetaDataDeleteIndexService.Response response) {
                    // YACK, but here we go: If this index is also percolated, make sure to delete all percolated queries from the _percolator index
                    IndexMetaData percolatorMetaData = state.metaData().index(PercolatorService.INDEX_NAME);
                    if (percolatorMetaData != null && percolatorMetaData.mappings().containsKey(index)) {
                        deleteMappingAction.execute(new DeleteMappingRequest(PercolatorService.INDEX_NAME).type(index), new ActionListener<DeleteMappingResponse>() {
                            @Override
                            public void onResponse(DeleteMappingResponse deleteMappingResponse) {
                                if (count.countDown()) {
                                    if (lastFailure != null) {
                                        listener.onFailure(lastFailure);
                                    } else {
                                        listener.onResponse(new DeleteIndexResponse(response.acknowledged()));
                                    }
                                }
                            }

                            @Override
                            public void onFailure(Throwable e) {
                                if (count.countDown()) {
                                    if (lastFailure != null) {
                                        listener.onFailure(lastFailure);
                                    } else {
                                        listener.onResponse(new DeleteIndexResponse(response.acknowledged()));
                                    }
                                }
                            }
                        });
                    } else {
                        if (count.countDown()) {
                            if (lastFailure != null) {
                                listener.onFailure(lastFailure);
                            } else {
                                listener.onResponse(new DeleteIndexResponse(response.acknowledged()));
                            }
                        }
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    logger.debug("[{}] failed to delete index", t, index);
                    lastFailure = t;
                    if (count.countDown()) {
                        listener.onFailure(t);
                    }
                }
            });
        }
    }
}
