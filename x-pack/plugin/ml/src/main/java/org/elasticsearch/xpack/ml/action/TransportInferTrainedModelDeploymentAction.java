/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction;
import org.elasticsearch.xpack.core.ml.action.InferTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelType;
import org.elasticsearch.xpack.core.ml.inference.allocation.AllocationState;
import org.elasticsearch.xpack.core.ml.inference.allocation.TrainedModelAllocation;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.inference.allocation.TrainedModelAllocationMetadata;
import org.elasticsearch.xpack.ml.inference.deployment.TrainedModelDeploymentTask;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;

import java.util.List;

public class TransportInferTrainedModelDeploymentAction extends TransportTasksAction<
    TrainedModelDeploymentTask,
    InferTrainedModelDeploymentAction.Request,
    InferTrainedModelDeploymentAction.Response,
    InferTrainedModelDeploymentAction.Response> {

    private final TrainedModelProvider provider;

    @Inject
    public TransportInferTrainedModelDeploymentAction(
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        TrainedModelProvider provider
    ) {
        super(
            InferTrainedModelDeploymentAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            InferTrainedModelDeploymentAction.Request::new,
            InferTrainedModelDeploymentAction.Response::new,
            InferTrainedModelDeploymentAction.Response::new,
            ThreadPool.Names.SAME
        );
        this.provider = provider;
    }

    @Override
    protected void doExecute(
        Task task,
        InferTrainedModelDeploymentAction.Request request,
        ActionListener<InferTrainedModelDeploymentAction.Response> listener
    ) {
        final String deploymentId = request.getDeploymentId();
        // We need to check whether there is at least an assigned task here, otherwise we cannot redirect to the
        // node running the job task.
        TrainedModelAllocation allocation = TrainedModelAllocationMetadata.allocationForModelId(clusterService.state(), deploymentId)
            .orElse(null);
        if (allocation == null) {
            // If there is no allocation, verify the model even exists so that we can provide a nicer error message
            provider.getTrainedModel(deploymentId, GetTrainedModelsAction.Includes.empty(), ActionListener.wrap(config -> {
                if (config.getModelType() != TrainedModelType.PYTORCH) {
                    listener.onFailure(
                        ExceptionsHelper.badRequestException(
                            "Only [pytorch] models are supported by _infer, provided model [{}] has type [{}]",
                            config.getModelId(),
                            config.getModelType()
                        )
                    );
                    return;
                }
                String message = "Trained model [" + deploymentId + "] is not deployed";
                listener.onFailure(ExceptionsHelper.conflictStatusException(message));
            }, listener::onFailure));
            return;
        }
        if (allocation.getAllocationState() == AllocationState.STOPPING) {
            String message = "Trained model [" + deploymentId + "] is STOPPING";
            listener.onFailure(ExceptionsHelper.conflictStatusException(message));
            return;
        }
        String[] randomRunningNode = allocation.getStartedNodes();
        if (randomRunningNode.length == 0) {
            String message = "Trained model [" + deploymentId + "] is not allocated to any nodes";
            listener.onFailure(ExceptionsHelper.conflictStatusException(message));
            return;
        }
        // TODO Do better routing for inference calls
        int nodeIndex = Randomness.get().nextInt(randomRunningNode.length);
        request.setNodes(randomRunningNode[nodeIndex]);
        super.doExecute(task, request, listener);
    }

    @Override
    protected InferTrainedModelDeploymentAction.Response newResponse(
        InferTrainedModelDeploymentAction.Request request,
        List<InferTrainedModelDeploymentAction.Response> tasks,
        List<TaskOperationFailure> taskOperationFailures,
        List<FailedNodeException> failedNodeExceptions
    ) {
        if (taskOperationFailures.isEmpty() == false) {
            throw org.elasticsearch.ExceptionsHelper.convertToElastic(taskOperationFailures.get(0).getCause());
        } else if (failedNodeExceptions.isEmpty() == false) {
            throw org.elasticsearch.ExceptionsHelper.convertToElastic(failedNodeExceptions.get(0));
        } else if (tasks.isEmpty()) {
            throw new ElasticsearchStatusException(
                "[{}] unable to find deployment task for inference please stop and start the deployment or try again momentarily",
                RestStatus.NOT_FOUND,
                request.getDeploymentId()
            );
        } else {
            return tasks.get(0);
        }
    }

    @Override
    protected void taskOperation(
        InferTrainedModelDeploymentAction.Request request,
        TrainedModelDeploymentTask task,
        ActionListener<InferTrainedModelDeploymentAction.Response> listener
    ) {
        task.infer(
            request.getDocs().get(0),
            request.getUpdate(),
            request.getInferenceTimeout(),
            ActionListener.wrap(
                pyTorchResult -> listener.onResponse(new InferTrainedModelDeploymentAction.Response(pyTorchResult)),
                listener::onFailure
            )
        );
    }
}
