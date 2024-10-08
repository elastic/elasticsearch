/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.waitforallocations;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.core.ml.action.InferModelAction;
import org.elasticsearch.xpack.core.ml.inference.assignment.AllocationStatus;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignment;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignmentMetadata;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.inference.assignment.TrainedModelAssignmentService;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

import static org.elasticsearch.core.Strings.format;

public class ScalingInference {

    private static final Logger logger = LogManager.getLogger(ScalingInference.class);

    private final Map<String, LinkedBlockingQueue<WaitingRequest>> queueRequests;

    public record WaitingRequest(
        InferModelAction.Request request,
        InferModelAction.Response.Builder responseBuilder,
        TaskId parentTaskId,
        ActionListener<InferModelAction.Response> listener
    ) {
        public String deploymentId() {
            return request.getId();
        }
    }

    private final TrainedModelAssignmentService assignmentService;
    private final BiConsumer<WaitingRequest, TrainedModelAssignment> queuedConsumer;

    public ScalingInference(
        TrainedModelAssignmentService assignmentService,
        BiConsumer<WaitingRequest, TrainedModelAssignment> queuedConsumer
    ) {
        this.assignmentService = assignmentService;
        this.queuedConsumer = queuedConsumer;
        this.queueRequests = new ConcurrentHashMap<>();
    }

    public synchronized void waitForAssignment(WaitingRequest request) {
        var p = queueRequests.computeIfAbsent(request.deploymentId(), k -> new LinkedBlockingQueue<>());

        if (p.isEmpty()) {
            p.offer(request);
            assignmentService.waitForAssignmentCondition(
                request.deploymentId(),
                new DeploymentHasAtLeastOneAllocation(request.deploymentId()),
                request.request().getInferenceTimeout(),
                new WaitingListener(request.deploymentId())
            );
        } else {
            p.offer(request);
        }

    }

    private static class DeploymentHasAtLeastOneAllocation implements Predicate<ClusterState> {

        private final String deploymentId;

        DeploymentHasAtLeastOneAllocation(String deploymentId) {
            this.deploymentId = ExceptionsHelper.requireNonNull(deploymentId, "deployment_id");
        }

        @Override
        public boolean test(ClusterState clusterState) {
            TrainedModelAssignment trainedModelAssignment = TrainedModelAssignmentMetadata.assignmentForDeploymentId(
                clusterState,
                deploymentId
            ).orElse(null);
            if (trainedModelAssignment == null) {
                logger.info(() -> format("[%s] assignment was null while waiting to scale up", deploymentId));
                return true;
            }

            var allocationStatus = trainedModelAssignment.calculateAllocationStatus().orElse(null);
            if (allocationStatus == null) {
                return true;
            }

            var state = allocationStatus.calculateState();
            return state.isAnyOf(AllocationStatus.State.STARTED, AllocationStatus.State.FULLY_ALLOCATED);
        }
    }

    private class WaitingListener implements TrainedModelAssignmentService.WaitForAssignmentListener {

        private final String deploymentId;

        private WaitingListener(String deploymentId) {
            this.deploymentId = deploymentId;
        }

        @Override
        public void onResponse(TrainedModelAssignment assignment) {
            // assignment is started, do inference
            var queued = queueRequests.remove(deploymentId);
            for (var request : queued) {
                queuedConsumer.accept(request, assignment);
            }
        }

        @Override
        public void onFailure(Exception e) {
            var queued = queueRequests.remove(deploymentId);
            for (var request : queued) {
                request.listener().onFailure(e);
            }
        }
    }
}
