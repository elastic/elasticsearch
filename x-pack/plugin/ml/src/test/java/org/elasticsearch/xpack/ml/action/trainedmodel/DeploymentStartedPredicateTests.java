/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.action.trainedmodel;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.inference.assignment.AllocationStatus;
import org.elasticsearch.xpack.core.ml.inference.assignment.Priority;
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingInfo;
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingState;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignment;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignmentMetadata;
import org.elasticsearch.xpack.ml.action.trainedmodel.TransportStartTrainedModelDeploymentAction.DeploymentStartedPredicate;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

public class DeploymentStartedPredicateTests extends ESTestCase {

    private static final String DEPLOYMENT_ID = "test-deployment";

    public void testFailedNodesWithNoneInitializing_terminatesWithConflictNotServerError() {
        DeploymentStartedPredicate predicate = new DeploymentStartedPredicate(DEPLOYMENT_ID, AllocationStatus.State.STARTED);
        ClusterState state = clusterStateWithRoutes(Map.of("node-1", failedRoute()));

        assertTrue("no nodes are still initializing, so the failure is terminal", predicate.test(state));
        assertThat(predicate.exception, instanceOf(ElasticsearchStatusException.class));
        assertThat(((ElasticsearchStatusException) predicate.exception).status(), equalTo(RestStatus.CONFLICT));
    }

    public void testFailedNodesWithNodeStillInitializing_keepsWaiting() {
        DeploymentStartedPredicate predicate = new DeploymentStartedPredicate(DEPLOYMENT_ID, AllocationStatus.State.STARTED);
        ClusterState state = clusterStateWithRoutes(Map.of("node-1", failedRoute(), "node-2", startingRoute()));

        assertFalse("another node is still initializing, so the deployment should keep waiting rather than fail", predicate.test(state));
        assertThat(predicate.exception, nullValue());
    }

    public void testAssignmentRemoved_stillReturnsBadRequest() {
        DeploymentStartedPredicate predicate = new DeploymentStartedPredicate(DEPLOYMENT_ID, AllocationStatus.State.STARTED);
        ClusterState noAssignment = ClusterState.builder(new ClusterName("test")).metadata(Metadata.builder().build()).build();

        assertTrue(predicate.test(noAssignment));
        assertThat(predicate.exception, instanceOf(ElasticsearchStatusException.class));
        assertThat(((ElasticsearchStatusException) predicate.exception).status(), equalTo(RestStatus.BAD_REQUEST));
    }

    public void testFullyAllocated_succeedsWithoutException() {
        DeploymentStartedPredicate predicate = new DeploymentStartedPredicate(DEPLOYMENT_ID, AllocationStatus.State.STARTED);
        ClusterState state = clusterStateWithRoutes(Map.of("node-1", startedRoute()));

        assertTrue(predicate.test(state));
        assertThat(predicate.exception, nullValue());
    }

    private static RoutingInfo failedRoute() {
        return new RoutingInfo(0, 1, RoutingState.FAILED, "model loaded but process is stopped");
    }

    private static RoutingInfo startingRoute() {
        return new RoutingInfo(0, 1, RoutingState.STARTING, "");
    }

    private static RoutingInfo startedRoute() {
        return new RoutingInfo(2, 2, RoutingState.STARTED, "");
    }

    private static ClusterState clusterStateWithRoutes(Map<String, RoutingInfo> routes) {
        TrainedModelAssignment.Builder assignmentBuilder = TrainedModelAssignment.Builder.empty(taskParams(), null);
        routes.forEach(assignmentBuilder::addRoutingEntry);
        return ClusterState.builder(new ClusterName("test"))
            .metadata(
                Metadata.builder()
                    .putCustom(
                        TrainedModelAssignmentMetadata.NAME,
                        TrainedModelAssignmentMetadata.Builder.empty().addNewAssignment(DEPLOYMENT_ID, assignmentBuilder).build()
                    )
                    .build()
            )
            .build();
    }

    private static StartTrainedModelDeploymentAction.TaskParams taskParams() {
        return new StartTrainedModelDeploymentAction.TaskParams(
            DEPLOYMENT_ID,
            DEPLOYMENT_ID,
            1024L,
            2,
            1,
            1024,
            ByteSizeValue.ofBytes(1024L),
            Priority.NORMAL,
            0L,
            0L
        );
    }
}
