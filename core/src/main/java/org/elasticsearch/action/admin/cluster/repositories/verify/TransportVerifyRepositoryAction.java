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

package org.elasticsearch.action.admin.cluster.repositories.verify;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.RepositoryVerificationException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Transport action for verifying repository operation
 */
public class TransportVerifyRepositoryAction extends TransportMasterNodeAction<VerifyRepositoryRequest, VerifyRepositoryResponse> {

    private final RepositoriesService repositoriesService;


    @Inject
    public TransportVerifyRepositoryAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                           RepositoriesService repositoriesService, ThreadPool threadPool, ActionFilters actionFilters,
                                           IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, VerifyRepositoryAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver, VerifyRepositoryRequest::new);
        this.repositoriesService = repositoriesService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    protected VerifyRepositoryResponse newResponse() {
        return new VerifyRepositoryResponse();
    }

    @Override
    protected ClusterBlockException checkBlock(VerifyRepositoryRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected void masterOperation(final VerifyRepositoryRequest request, ClusterState state, final ActionListener<VerifyRepositoryResponse> listener) {
        repositoriesService.verifyRepository(request.name(), new  ActionListener<RepositoriesService.VerifyResponse>() {
            @Override
            public void onResponse(RepositoriesService.VerifyResponse verifyResponse) {
                if (verifyResponse.failed()) {
                    listener.onFailure(new RepositoryVerificationException(request.name(), verifyResponse.failureDescription()));
                } else {
                    listener.onResponse(new VerifyRepositoryResponse(clusterService.getClusterName(), verifyResponse.nodes()));
                }
            }

            @Override
            public void onFailure(Throwable e) {
                listener.onFailure(e);
            }
        });
    }
}
