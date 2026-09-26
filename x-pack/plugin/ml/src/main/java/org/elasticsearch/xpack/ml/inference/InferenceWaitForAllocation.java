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
import org.elasticsearch.xpack.core.ml.inference.ModelDeploymentTimeoutException;
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingInfo;
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingState;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignment;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignmentMetadata;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.inference.assignment.TrainedModelAssignmentService;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;
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
     * Track details of the pending request. The deployment id is resolved by the caller
     * from the trained model assignment; the id on the request cannot be used here as it
     * may be a model id or alias rather than the deployment id.
     */
    public record WaitingRequest(
        String deploymentId,
        InferModelAction.Request request,
        InferModelAction.Response.Builder responseBuilder,
        TaskId parentTaskId,
        ActionListener<InferModelAction.Response> listener
    ) {}

    private static final Logger logger = LogManager.getLogger(InferenceWaitForAllocation.class);

    private final TrainedModelAssignmentService assignmentService;
    private final BiConsumer<WaitingRequest, TrainedModelAssignment> queuedConsumer;
    private final LongSupplier relativeTimeInMillisSupplier;
    private AtomicInteger pendingRequestCount = new AtomicInteger();

    // Visible for testing the MAX_PENDING_REQUEST_COUNT back-pressure accounting.
    int pendingRequestCount() {
        return pendingRequestCount.get();
    }

    /**
     * Create with consumer of the successful requests
     * @param assignmentService            Trained model assignment service
     * @param onInferenceScaledConsumer    The consumer of the waiting request called once an
     *                                     allocation is available.
     * @param relativeTimeInMillisSupplier Monotonic clock used to bound how long a failed
     *                                     deployment is tolerated before giving up; the bound is
     *                                     the caller's inference timeout.
     */
    public InferenceWaitForAllocation(
        TrainedModelAssignmentService assignmentService,
        BiConsumer<WaitingRequest, TrainedModelAssignment> onInferenceScaledConsumer,
        LongSupplier relativeTimeInMillisSupplier
    ) {
        this.assignmentService = assignmentService;
        this.queuedConsumer = onInferenceScaledConsumer;
        this.relativeTimeInMillisSupplier = relativeTimeInMillisSupplier;
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
                    request.deploymentId()
                )
            );
            return;
        }

        TimeValue inferenceTimeout = Objects.requireNonNullElse(
            request.request().getInferenceTimeout(),
            InferModelAction.Request.DEFAULT_TIMEOUT_FOR_API
        );
        var predicate = new DeploymentHasAtLeastOneAllocation(request.deploymentId(), relativeTimeInMillisSupplier, inferenceTimeout);

        assignmentService.waitForAssignmentCondition(
            request.deploymentId(),
            predicate,
            inferenceTimeout,
            new WaitingListener(request, predicate)
        );
    }

    private static class DeploymentHasAtLeastOneAllocation implements Predicate<ClusterState> {

        private final String deploymentId;
        private final LongSupplier relativeTimeInMillisSupplier;
        private final TimeValue failureTimeout;
        private AtomicReference<Exception> exception = new AtomicReference<>();
        private volatile long firstFailureObservedAtMillis = -1;

        DeploymentHasAtLeastOneAllocation(String deploymentId, LongSupplier relativeTimeInMillisSupplier, TimeValue failureTimeout) {
            this.deploymentId = ExceptionsHelper.requireNonNull(deploymentId, "deployment_id");
            this.relativeTimeInMillisSupplier = relativeTimeInMillisSupplier;
            this.failureTimeout = failureTimeout;
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

            var routable = trainedModelAssignment.getNodeRoutingTable().values().stream().filter(RoutingInfo::isRoutable).findFirst();
            if (routable.isPresent()) {
                return true;
            }

            Map<String, String> nodeFailuresAndReasons = new HashMap<>();
            for (var nodeIdAndRouting : trainedModelAssignment.getNodeRoutingTable().entrySet()) {
                if (RoutingState.FAILED.equals(nodeIdAndRouting.getValue().getState())) {
                    nodeFailuresAndReasons.put(nodeIdAndRouting.getKey(), nodeIdAndRouting.getValue().getReason());
                }
            }
            if (nodeFailuresAndReasons.isEmpty()) {
                // no current failures; reset so a later failure gets its own fresh timeout window
                firstFailureObservedAtMillis = -1;
                return false;
            }

            long nowMillis = relativeTimeInMillisSupplier.getAsLong();
            if (firstFailureObservedAtMillis < 0) {
                firstFailureObservedAtMillis = nowMillis;
            }
            if (nowMillis - firstFailureObservedAtMillis < failureTimeout.millis()) {
                // Node failures here are often transient, e.g. caused by a stop/start race during
                // routine node churn while scaling up from zero. Keep waiting for the caller's
                // inference timeout so a fresh allocation can recover; when that elapses, fail
                // with the real per-node reasons (409) rather than a generic timeout.
                logger.debug(
                    "Deployment [{}] has failed routes [{}], within inference timeout, keep waiting",
                    trainedModelAssignment.getDeploymentId(),
                    nodeFailuresAndReasons
                );
                return false;
            }

            exception.set(
                new ElasticsearchStatusException(
                    "[{}] Error waiting for a model allocation, all nodes have failed with errors [{}]",
                    RestStatus.CONFLICT,
                    trainedModelAssignment.getDeploymentId(),
                    nodeFailuresAndReasons
                )
            );
            return true; // don't try again
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
                request.listener().onFailure(predicate.exception.get());
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
