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

package org.elasticsearch.action.admin.indices.alias;

import com.google.common.collect.Sets;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.support.master.TransportMasterNodeOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.AliasAction;
import org.elasticsearch.cluster.metadata.MetaDataIndexAliasesService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
public class TransportIndicesAliasesAction extends TransportMasterNodeOperationAction<IndicesAliasesRequest, IndicesAliasesResponse> {

    private final MetaDataIndexAliasesService indexAliasesService;

    @Inject
    public TransportIndicesAliasesAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                         ThreadPool threadPool, MetaDataIndexAliasesService indexAliasesService) {
        super(settings, transportService, clusterService, threadPool);
        this.indexAliasesService = indexAliasesService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    protected String transportAction() {
        return IndicesAliasesAction.NAME;
    }

    @Override
    protected IndicesAliasesRequest newRequest() {
        return new IndicesAliasesRequest();
    }

    @Override
    protected IndicesAliasesResponse newResponse() {
        return new IndicesAliasesResponse();
    }

    @Override
    protected ClusterBlockException checkBlock(IndicesAliasesRequest request, ClusterState state) {
        Set<String> indices = Sets.newHashSet();
        for (AliasAction aliasAction : request.getAliasActions()) {
            indices.add(aliasAction.index());
        }
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA, indices.toArray(new String[indices.size()]));
    }

    @Override
    protected IndicesAliasesResponse masterOperation(IndicesAliasesRequest request, ClusterState state) throws ElasticSearchException {
        final AtomicReference<IndicesAliasesResponse> responseRef = new AtomicReference<IndicesAliasesResponse>();
        final AtomicReference<Throwable> failureRef = new AtomicReference<Throwable>();
        final CountDownLatch latch = new CountDownLatch(1);
        indexAliasesService.indicesAliases(new MetaDataIndexAliasesService.Request(request.getAliasActions().toArray(new AliasAction[request.getAliasActions().size()]), request.getTimeout()), new MetaDataIndexAliasesService.Listener() {
            @Override
            public void onResponse(MetaDataIndexAliasesService.Response response) {
                responseRef.set(new IndicesAliasesResponse(response.acknowledged()));
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
