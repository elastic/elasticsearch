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
package org.elasticsearch.action.admin.cluster.repositories.cleanup;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public final class TransportCleanupRepositoryAction extends TransportMasterNodeAction<CleanupRepositoryRequest, CleanupRepositoryResponse> {

    private final RepositoriesService repositoriesService;

    @Override
    protected String executor() {
        return ThreadPool.Names.GENERIC;
    }

    @Inject
    public TransportCleanupRepositoryAction(TransportService transportService, ClusterService clusterService,
        RepositoriesService repositoriesService, ThreadPool threadPool, ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver) {
        super(CleanupRepositoryAction.NAME, transportService, clusterService, threadPool, actionFilters,
            indexNameExpressionResolver, CleanupRepositoryRequest::new);
        this.repositoriesService = repositoriesService;
    }

    @Override
    protected CleanupRepositoryResponse newResponse() {
        return null;
    }

    @Override
    protected void masterOperation(CleanupRepositoryRequest request, ClusterState state,
                                   ActionListener<CleanupRepositoryResponse> listener) {

    }

    @Override
    protected ClusterBlockException checkBlock(CleanupRepositoryRequest request, ClusterState state) {
        return null;
    }
}
