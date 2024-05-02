/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.inference.InferenceServiceRegistry;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.inference.action.DeleteInferenceModelAction;
import org.elasticsearch.xpack.inference.common.InferenceExceptions;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;

public class TransportDeleteInferenceModelAction extends AcknowledgedTransportMasterNodeAction<DeleteInferenceModelAction.Request> {

    private final ModelRegistry modelRegistry;
    private final InferenceServiceRegistry serviceRegistry;

    @Inject
    public TransportDeleteInferenceModelAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ModelRegistry modelRegistry,
        InferenceServiceRegistry serviceRegistry
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
        this.serviceRegistry = serviceRegistry;
    }

    @Override
    protected void masterOperation(
        Task task,
        DeleteInferenceModelAction.Request request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) {
        SubscribableListener.<ModelRegistry.UnparsedModel>newForked(modelConfigListener -> {
            modelRegistry.getModel(request.getInferenceEntityId(), modelConfigListener);
        }).<Boolean>andThen((l1, unparsedModel) -> {

            if (request.getTaskType().isAnyOrSame(unparsedModel.taskType()) == false) {
                // specific task type in request does not match the models
                l1.onFailure(InferenceExceptions.mismatchedTaskTypeException(request.getTaskType(), unparsedModel.taskType()));
                return;
            }
            var service = serviceRegistry.getService(unparsedModel.service());
            if (service.isPresent()) {
                service.get().stop(request.getInferenceEntityId(), l1);
            } else {
                l1.onFailure(
                    new ElasticsearchStatusException("No service found for model " + request.getInferenceEntityId(), RestStatus.NOT_FOUND)
                );
            }
        }).<Boolean>andThen((l2, didStop) -> {
            if (didStop) {
                modelRegistry.deleteModel(request.getInferenceEntityId(), l2);
            } else {
                l2.onFailure(
                    new ElasticsearchStatusException(
                        "Failed to stop model " + request.getInferenceEntityId(),
                        RestStatus.INTERNAL_SERVER_ERROR
                    )
                );
            }
        }).addListener(listener.delegateFailure((l3, didDeleteModel) -> listener.onResponse(AcknowledgedResponse.of(didDeleteModel))));
    }

    @Override
    protected ClusterBlockException checkBlock(DeleteInferenceModelAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.WRITE);
    }

}
