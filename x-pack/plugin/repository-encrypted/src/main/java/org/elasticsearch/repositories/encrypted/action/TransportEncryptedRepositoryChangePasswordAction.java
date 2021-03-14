/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.encrypted.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public final class TransportEncryptedRepositoryChangePasswordAction extends AcknowledgedTransportMasterNodeAction<EncryptedRepositoryChangePasswordRequest> {

    protected TransportEncryptedRepositoryChangePasswordAction(String actionName, TransportService transportService,
                                                               ClusterService clusterService, ThreadPool threadPool,
                                                               ActionFilters actionFilters, Writeable.Reader<EncryptedRepositoryChangePasswordRequest> request, IndexNameExpressionResolver indexNameExpressionResolver, String executor) {
        super(actionName, transportService, clusterService, threadPool, actionFilters, request, indexNameExpressionResolver, executor);
    }

    @Override
    protected void masterOperation(Task task, EncryptedRepositoryChangePasswordRequest request, ClusterState state,
                                   ActionListener<AcknowledgedResponse> listener) throws Exception {

    }

    @Override
    protected ClusterBlockException checkBlock(EncryptedRepositoryChangePasswordRequest request, ClusterState state) {
        return null;
    }
//    public class TransportPutRepositoryAction extends AcknowledgedTransportMasterNodeAction<PutRepositoryRequest> {
//
//        private final RepositoriesService repositoriesService;
//
//        @Inject
//        public TransportPutRepositoryAction(TransportService transportService, ClusterService clusterService,
//                                            RepositoriesService repositoriesService, ThreadPool threadPool, ActionFilters actionFilters,
//                                            IndexNameExpressionResolver indexNameExpressionResolver) {
//            super(PutRepositoryAction.NAME, transportService, clusterService, threadPool, actionFilters,
//                    PutRepositoryRequest::new, indexNameExpressionResolver, ThreadPool.Names.SAME);
//            this.repositoriesService = repositoriesService;
//        }
//
//        @Override
//        protected ClusterBlockException checkBlock(PutRepositoryRequest request, ClusterState state) {
//            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
//        }
//
//        @Override
//        protected void masterOperation(Task task, final PutRepositoryRequest request, ClusterState state,
//                                       final ActionListener<AcknowledgedResponse> listener) {
//            repositoriesService.registerRepository(request, listener.map(response -> AcknowledgedResponse.of(response.isAcknowledged())));
//        }
//    }
}
