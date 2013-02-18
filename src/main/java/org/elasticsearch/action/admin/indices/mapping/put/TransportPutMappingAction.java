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

package org.elasticsearch.action.admin.indices.mapping.put;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.TransportMasterNodeOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.MetaDataMappingService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Put mapping action.
 */
public class TransportPutMappingAction extends TransportMasterNodeOperationAction<PutMappingRequest, PutMappingResponse> {

    private final MetaDataMappingService metaDataMappingService;

    @Inject
    public TransportPutMappingAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                     ThreadPool threadPool, MetaDataMappingService metaDataMappingService) {
        super(settings, transportService, clusterService, threadPool);
        this.metaDataMappingService = metaDataMappingService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    protected String transportAction() {
        return PutMappingAction.NAME;
    }

    @Override
    protected PutMappingRequest newRequest() {
        return new PutMappingRequest();
    }

    @Override
    protected PutMappingResponse newResponse() {
        return new PutMappingResponse();
    }

    @Override
    protected void doExecute(PutMappingRequest request, ActionListener<PutMappingResponse> listener) {
        request.setIndices(clusterService.state().metaData().concreteIndices(request.getIndices()));
        super.doExecute(request, listener);
    }

    @Override
    protected ClusterBlockException checkBlock(PutMappingRequest request, ClusterState state) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA, request.getIndices());
    }

    @Override
    protected PutMappingResponse masterOperation(PutMappingRequest request, ClusterState state) throws ElasticSearchException {
        ClusterState clusterState = clusterService.state();

        // update to concrete indices
        request.setIndices(clusterState.metaData().concreteIndices(request.getIndices()));

        final AtomicReference<PutMappingResponse> responseRef = new AtomicReference<PutMappingResponse>();
        final AtomicReference<Throwable> failureRef = new AtomicReference<Throwable>();
        final CountDownLatch latch = new CountDownLatch(1);
        metaDataMappingService.putMapping(new MetaDataMappingService.PutRequest(request.getIndices(), request.getType(), request.getSource()).ignoreConflicts(request.isIgnoreConflicts()).timeout(request.getTimeout()), new MetaDataMappingService.Listener() {
            @Override
            public void onResponse(MetaDataMappingService.Response response) {
                responseRef.set(new PutMappingResponse(response.acknowledged()));
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
                failureRef.set(t);
                latch.countDown();
            }
        });

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