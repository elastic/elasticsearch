/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.TransportActions;
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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.percolator.PercolatorService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Delete index action.
 *
 * @author kimchy (shay.banon)
 */
public class TransportDeleteIndexAction extends TransportMasterNodeOperationAction<DeleteIndexRequest, DeleteIndexResponse> {

    private final MetaDataDeleteIndexService deleteIndexService;

    private final TransportDeleteMappingAction deleteMappingAction;

    @Inject public TransportDeleteIndexAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                              ThreadPool threadPool, MetaDataDeleteIndexService deleteIndexService, TransportDeleteMappingAction deleteMappingAction) {
        super(settings, transportService, clusterService, threadPool);
        this.deleteIndexService = deleteIndexService;
        this.deleteMappingAction = deleteMappingAction;
    }

    @Override protected String executor() {
        return ThreadPool.Names.CACHED;
    }

    @Override protected String transportAction() {
        return TransportActions.Admin.Indices.DELETE;
    }

    @Override protected DeleteIndexRequest newRequest() {
        return new DeleteIndexRequest();
    }

    @Override protected DeleteIndexResponse newResponse() {
        return new DeleteIndexResponse();
    }

    @Override protected void doExecute(DeleteIndexRequest request, ActionListener<DeleteIndexResponse> listener) {
        request.indices(clusterService.state().metaData().concreteIndices(request.indices()));
        super.doExecute(request, listener);
    }

    @Override protected ClusterBlockException checkBlock(DeleteIndexRequest request, ClusterState state) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA, request.indices());
    }

    @Override protected DeleteIndexResponse masterOperation(DeleteIndexRequest request, final ClusterState state) throws ElasticSearchException {
        if (request.indices().length == 0) {
            return new DeleteIndexResponse(true);
        }
        final AtomicReference<DeleteIndexResponse> responseRef = new AtomicReference<DeleteIndexResponse>();
        final AtomicReference<Throwable> failureRef = new AtomicReference<Throwable>();
        final CountDownLatch latch = new CountDownLatch(request.indices().length);
        for (final String index : request.indices()) {
            deleteIndexService.deleteIndex(new MetaDataDeleteIndexService.Request(index).timeout(request.timeout()), new MetaDataDeleteIndexService.Listener() {
                @Override public void onResponse(MetaDataDeleteIndexService.Response response) {
                    responseRef.set(new DeleteIndexResponse(response.acknowledged()));
                    // YACK, but here we go: If this index is also percolated, make sure to delete all percolated queries from the _percolator index
                    IndexMetaData percolatorMetaData = state.metaData().index(PercolatorService.INDEX_NAME);
                    if (percolatorMetaData != null && percolatorMetaData.mappings().containsKey(index)) {
                        deleteMappingAction.execute(new DeleteMappingRequest(PercolatorService.INDEX_NAME).type(index), new ActionListener<DeleteMappingResponse>() {
                            @Override public void onResponse(DeleteMappingResponse deleteMappingResponse) {
                                latch.countDown();
                            }

                            @Override public void onFailure(Throwable e) {
                                latch.countDown();
                            }
                        });
                    } else {
                        latch.countDown();
                    }
                }

                @Override public void onFailure(Throwable t) {
                    failureRef.set(t);
                    latch.countDown();
                }
            });
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            failureRef.set(e);
        }

        if (failureRef.get() != null) {
            if (failureRef.get() instanceof ElasticSearchException) {
                throw (ElasticSearchException) failureRef.get();
            } else {
                throw new ElasticSearchException(failureRef.get().getMessage(), failureRef.get());
            }
        }

        return responseRef.get();
    }
}
