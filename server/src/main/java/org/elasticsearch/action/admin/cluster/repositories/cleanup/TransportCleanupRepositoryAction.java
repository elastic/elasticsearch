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
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

public final class TransportCleanupRepositoryAction extends TransportMasterNodeReadAction<CleanupRepositoryRequest,
                                                                                          CleanupRepositoryResponse> {

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
            CleanupRepositoryRequest::new, indexNameExpressionResolver);
        this.repositoriesService = repositoriesService;
    }

    @Override
    protected CleanupRepositoryResponse newResponse() {
        return new CleanupRepositoryResponse(0L, 0L);
    }

    @Override
    protected CleanupRepositoryResponse read(StreamInput in) throws IOException {
        return new CleanupRepositoryResponse(in);
    }

    @Override
    protected void masterOperation(CleanupRepositoryRequest request, ClusterState state,
                                   ActionListener<CleanupRepositoryResponse> listener) {
        Repository repository = repositoriesService.repository(request.repository());
        if (repository instanceof BlobStoreRepository) {
            ((BlobStoreRepository) repository).cleanup(repository.getRepositoryData().getGenId(), listener);
        } else {
            throw new IllegalArgumentException("Repository [" + request.repository()+ "] is not a blob store repository");
        }
    }

    @Override
    protected ClusterBlockException checkBlock(CleanupRepositoryRequest request, ClusterState state) {
        // Cluster is not affected but we look up repositories in metadata
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
