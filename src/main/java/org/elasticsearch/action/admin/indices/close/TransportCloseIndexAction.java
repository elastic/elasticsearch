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

package org.elasticsearch.action.admin.indices.close;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.support.master.TransportMasterNodeOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.MetaDataStateIndexService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Delete index action.
 */
public class TransportCloseIndexAction extends TransportMasterNodeOperationAction<CloseIndexRequest, CloseIndexResponse> {

    private final MetaDataStateIndexService stateIndexService;

    @Inject
    public TransportCloseIndexAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                     ThreadPool threadPool, MetaDataStateIndexService stateIndexService) {
        super(settings, transportService, clusterService, threadPool);
        this.stateIndexService = stateIndexService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    protected String transportAction() {
        return CloseIndexAction.NAME;
    }

    @Override
    protected CloseIndexRequest newRequest() {
        return new CloseIndexRequest();
    }

    @Override
    protected CloseIndexResponse newResponse() {
        return new CloseIndexResponse();
    }

    @Override
    protected ClusterBlockException checkBlock(CloseIndexRequest request, ClusterState state) {
        request.setIndex(clusterService.state().metaData().concreteIndex(request.getIndex()));
        return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA, request.getIndex());
    }

    @Override
    protected CloseIndexResponse masterOperation(CloseIndexRequest request, ClusterState state) throws ElasticSearchException {
        final AtomicReference<CloseIndexResponse> responseRef = new AtomicReference<CloseIndexResponse>();
        final AtomicReference<Throwable> failureRef = new AtomicReference<Throwable>();
        final CountDownLatch latch = new CountDownLatch(1);
        stateIndexService.closeIndex(new MetaDataStateIndexService.Request(request.getIndex()).timeout(request.getTimeout()), new MetaDataStateIndexService.Listener() {
            @Override
            public void onResponse(MetaDataStateIndexService.Response response) {
                responseRef.set(new CloseIndexResponse(response.acknowledged()));
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
