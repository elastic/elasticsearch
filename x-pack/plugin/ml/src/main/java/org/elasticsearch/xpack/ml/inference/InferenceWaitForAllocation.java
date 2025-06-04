/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.core.ml.action.InferModelAction;
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingInfo;
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingState;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignment;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignmentMetadata;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.inference.assignment.ModelDeploymentTimeoutException;
import org.elasticsearch.xpack.ml.inference.assignment.TrainedModelAssignmentService;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

import static org.elasticsearch.core.Strings.format;

/**
 * Class for storing inference requests for ml trained models while
 * scaling is in progress. Once the trained model has at least 1
 * allocation the stored requests are forwarded to a consumer for
 * processing.Requests will timeout while waiting for scale.
 */
public class InferenceWaitForAllocation {

    public static final int MAX_PENDING_REQUEST_COUNT = 100;

    /**
     * Track details of the pending request
     */
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

    private static final Logger logger = LogManager.getLogger(InferenceWaitForAllocation.class);

    private final TrainedModelAssignmentService assignmentService;
    private final BiConsumer<WaitingRequest, TrainedModelAssignment> queuedConsumer;
    private AtomicInteger pendingRequestCount = new AtomicInteger();

    /**
     * Create with consumer of the successful requests
     * @param assignmentService            Trained model assignment service
     * @param onInferenceScaledConsumer    The consumer of the waiting request called once an
     *                                     allocation is available.
     */
    public InferenceWaitForAllocation(
        TrainedModelAssignmentService assignmentService,
        BiConsumer<WaitingRequest, TrainedModelAssignment> onInferenceScaledConsumer
    ) {
        this.assignmentService = assignmentService;
        this.queuedConsumer = onInferenceScaledConsumer;
    }

    /**
     * Wait for at least 1 allocation to be started then process the
     * inference request.
     * If the pending request count is greater than {@link #MAX_PENDING_REQUEST_COUNT}
     * the request listener is failed with a too many requests exception
     * The timeout is the inference request timeout.
     * @param request The inference request details
     */
    public synchronized void waitForAssignment(WaitingRequest request) {
        if (pendingRequestCount.incrementAndGet() >= MAX_PENDING_REQUEST_COUNT) {
            pendingRequestCount.decrementAndGet();
            request.listener.onFailure(
                new ElasticsearchStatusException(
                    "Rejected inference request waiting for an allocation of deployment [{}]. Too many pending requests",
                    RestStatus.TOO_MANY_REQUESTS,
                    request.request.getId()
                )
            );
            return;
        }

        var predicate = new DeploymentHasAtLeastOneAllocation(request.deploymentId());

        assignmentService.waitForAssignmentCondition(
            request.deploymentId(),
            predicate,
            request.request().getInferenceTimeout(),
            new WaitingListener(request, predicate)
        );
    }

    private static class DeploymentHasAtLeastOneAllocation implements Predicate<ClusterState> {

        private final String deploymentId;
        private AtomicReference<Exception> exception = new AtomicReference<>();

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
                exception.set(
                    new ElasticsearchStatusException(
                        "[{}] Error waiting for a model allocation, model assignment has been removed",
                        RestStatus.CONFLICT,
                        deploymentId
                    )
                );
                return true; // don't try again
            }

            Map<String, String> nodeFailuresAndReasons = new HashMap<>();
            for (var nodeIdAndRouting : trainedModelAssignment.getNodeRoutingTable().entrySet()) {
                if (RoutingState.FAILED.equals(nodeIdAndRouting.getValue().getState())) {
                    nodeFailuresAndReasons.put(nodeIdAndRouting.getKey(), nodeIdAndRouting.getValue().getReason());
                }
            }
            if (nodeFailuresAndReasons.isEmpty() == false) {
                if (nodeFailuresAndReasons.size() == trainedModelAssignment.getNodeRoutingTable().size()) {
                    exception.set(
                        new ElasticsearchStatusException(
                            "[{}] Error waiting for a model allocation, all nodes have failed with errors [{}]",
                            RestStatus.INTERNAL_SERVER_ERROR,
                            trainedModelAssignment.getDeploymentId(),
                            nodeFailuresAndReasons
                        )
                    );
                    return true; // don't try again
                } else {
                    logger.warn("Deployment [{}] has failed routes [{}]", trainedModelAssignment.getDeploymentId(), nodeFailuresAndReasons);
                }
            }

            var routable = trainedModelAssignment.getNodeRoutingTable().values().stream().filter(RoutingInfo::isRoutable).findFirst();
            return routable.isPresent();
        }
    }

    private class WaitingListener implements TrainedModelAssignmentService.WaitForAssignmentListener {

        private final WaitingRequest request;
        private final DeploymentHasAtLeastOneAllocation predicate;

        private WaitingListener(WaitingRequest request, DeploymentHasAtLeastOneAllocation predicate) {
            this.request = request;
            this.predicate = predicate;
        }

        @Override
        public void onResponse(TrainedModelAssignment assignment) {
            // assignment is started, do inference
            pendingRequestCount.decrementAndGet();

            if (predicate.exception.get() != null) {
                onFailure(predicate.exception.get());
                return;
            }

            queuedConsumer.accept(request, assignment);
        }

        @Override
        public void onFailure(Exception e) {
            pendingRequestCount.decrementAndGet();
            request.listener().onFailure(e);
        }

        @Override
        public void onTimeout(TimeValue timeout) {
            onFailure(
                new ModelDeploymentTimeoutException(
                    format(
                        "Timed out after [%s] waiting for trained model deployment [%s] to start. "
                            + "Use the trained model stats API to track the state of the deployment and try again once it has started.",
                        timeout,
                        request.deploymentId()
                    )
                )
            );
        }
    }
}
