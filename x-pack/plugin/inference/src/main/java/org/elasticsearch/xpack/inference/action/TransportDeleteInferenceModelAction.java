/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;

public class TransportDeleteInferenceModelAction extends AcknowledgedTransportMasterNodeAction<DeleteInferenceModelAction.Request> {

    private final ModelRegistry modelRegistry;

    @Inject
    public TransportDeleteInferenceModelAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ModelRegistry modelRegistry
    ) {
        super(
            DeleteInferenceModelAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            DeleteInferenceModelAction.Request::new,
            indexNameExpressionResolver,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.modelRegistry = modelRegistry;
    }

    @Override
    protected void masterOperation(
        Task task,
        DeleteInferenceModelAction.Request request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) throws Exception {
        modelRegistry.deleteModel(
            request.getModelId(),
            ActionListener.wrap(r -> listener.onResponse(AcknowledgedResponse.TRUE), listener::onFailure)
        );
    }

    @Override
    protected ClusterBlockException checkBlock(DeleteInferenceModelAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.WRITE);
    }
}
