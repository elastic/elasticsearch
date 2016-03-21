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

package org.elasticsearch.action.admin.cluster.repositories.put;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Transport action for register repository operation
 */
public class TransportPutRepositoryAction extends TransportMasterNodeAction<PutRepositoryRequest, PutRepositoryResponse> {

    private final RepositoriesService repositoriesService;

    @Inject
    public TransportPutRepositoryAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                        RepositoriesService repositoriesService, ThreadPool threadPool, ActionFilters actionFilters,
                                        IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, PutRepositoryAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver, PutRepositoryRequest::new);
        this.repositoriesService = repositoriesService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected PutRepositoryResponse newResponse() {
        return new PutRepositoryResponse();
    }

    @Override
    protected ClusterBlockException checkBlock(PutRepositoryRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void masterOperation(final PutRepositoryRequest request, ClusterState state, final ActionListener<PutRepositoryResponse> listener) {

        repositoriesService.registerRepository(
                new RepositoriesService.RegisterRepositoryRequest("put_repository [" + request.name() + "]",
                        request.name(), request.type(), request.verify())
                .settings(request.settings())
                .masterNodeTimeout(request.masterNodeTimeout())
                .ackTimeout(request.timeout()), new ActionListener<ClusterStateUpdateResponse>() {

            @Override
            public void onResponse(ClusterStateUpdateResponse response) {
                listener.onResponse(new PutRepositoryResponse(response.isAcknowledged()));
            }

            @Override
            public void onFailure(Throwable e) {
                listener.onFailure(e);
            }
        });
    }

}
