/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.ClearDeploymentCacheAction;
import org.elasticsearch.xpack.core.ml.action.ClearDeploymentCacheAction.Request;
import org.elasticsearch.xpack.core.ml.action.ClearDeploymentCacheAction.Response;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignment;
import org.elasticsearch.xpack.ml.inference.assignment.TrainedModelAssignmentMetadata;
import org.elasticsearch.xpack.ml.inference.deployment.TrainedModelDeploymentTask;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.ExceptionsHelper.convertToElastic;

public class TransportClearDeploymentCacheAction extends TransportTasksAction<TrainedModelDeploymentTask, Request, Response, Response> {

    @Inject
    public TransportClearDeploymentCacheAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService
    ) {
        super(
            ClearDeploymentCacheAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            Request::new,
            Response::new,
            Response::new,
            ThreadPool.Names.SAME
        );
    }

    @Override
    protected Response newResponse(
        Request request,
        List<Response> taskResponse,
        List<TaskOperationFailure> taskOperationFailures,
        List<FailedNodeException> failedNodeExceptions
    ) {
        if (taskOperationFailures.isEmpty() == false) {
            throw convertToElastic(taskOperationFailures.get(0).getCause());
        } else if (failedNodeExceptions.isEmpty() == false) {
            throw convertToElastic(failedNodeExceptions.get(0));
        }
        return new Response(true);
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        final ClusterState clusterState = clusterService.state();
        final TrainedModelAssignmentMetadata assignment = TrainedModelAssignmentMetadata.fromState(clusterState);
        TrainedModelAssignment trainedModelAssignment = assignment.getModelAssignment(request.getDeploymentId());
        if (trainedModelAssignment == null) {
            listener.onFailure(new ResourceNotFoundException("assignment for model with id [{}] not found", request.getDeploymentId()));
            return;
        }
        String[] nodes = trainedModelAssignment.getNodeRoutingTable()
            .entrySet()
            .stream()
            .filter(entry -> entry.getValue().isRoutable())
            .map(Map.Entry::getKey)
            .toArray(String[]::new);

        if (nodes.length == 0) {
            listener.onResponse(new Response(true));
            return;
        }
        request.setNodes(nodes);
        super.doExecute(task, request, listener);
    }

    @Override
    protected void taskOperation(Task actionTask, Request request, TrainedModelDeploymentTask task, ActionListener<Response> listener) {
        task.clearCache(ActionListener.wrap(r -> listener.onResponse(new Response(true)), listener::onFailure));
    }
}
