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

package org.elasticsearch.action.admin.indices.create;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.TransportActions;
import org.elasticsearch.action.support.master.TransportMasterNodeOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.MetaDataCreateIndexService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Create index action.
 *
 * @author kimchy (shay.banon)
 */
public class TransportCreateIndexAction extends TransportMasterNodeOperationAction<CreateIndexRequest, CreateIndexResponse> {

    private final MetaDataCreateIndexService createIndexService;

    @Inject public TransportCreateIndexAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                              ThreadPool threadPool, MetaDataCreateIndexService createIndexService) {
        super(settings, transportService, clusterService, threadPool);
        this.createIndexService = createIndexService;
    }

    @Override protected String executor() {
        return ThreadPool.Names.CACHED;
    }

    @Override protected String transportAction() {
        return TransportActions.Admin.Indices.CREATE;
    }

    @Override protected CreateIndexRequest newRequest() {
        return new CreateIndexRequest();
    }

    @Override protected CreateIndexResponse newResponse() {
        return new CreateIndexResponse();
    }

    @Override protected ClusterBlockException checkBlock(CreateIndexRequest request, ClusterState state) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA, request.index());
    }

    @Override protected CreateIndexResponse masterOperation(CreateIndexRequest request, ClusterState state) throws ElasticSearchException {
        String cause = request.cause();
        if (cause.length() == 0) {
            cause = "api";
        }

        final AtomicReference<CreateIndexResponse> responseRef = new AtomicReference<CreateIndexResponse>();
        final AtomicReference<Throwable> failureRef = new AtomicReference<Throwable>();
        final CountDownLatch latch = new CountDownLatch(1);
        createIndexService.createIndex(new MetaDataCreateIndexService.Request(cause, request.index()).settings(request.settings()).mappings(request.mappings()).timeout(request.timeout()), new MetaDataCreateIndexService.Listener() {
            @Override public void onResponse(MetaDataCreateIndexService.Response response) {
                responseRef.set(new CreateIndexResponse(response.acknowledged()));
                latch.countDown();
            }

            @Override public void onFailure(Throwable t) {
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
