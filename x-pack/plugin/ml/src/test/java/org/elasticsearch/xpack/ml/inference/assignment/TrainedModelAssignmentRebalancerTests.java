/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.assignment;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.TestDiscoveryNode;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.inference.assignment.AssignmentState;
import org.elasticsearch.xpack.core.ml.inference.assignment.Priority;
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingInfo;
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingState;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignment;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.job.NodeLoad;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class TrainedModelAssignmentRebalancerTests extends ESTestCase {

    public void testRebalance_GivenNoAssignments() throws Exception {
        TrainedModelAssignmentMetadata result = new TrainedModelAssignmentRebalancer(
            TrainedModelAssignmentMetadata.Builder.empty().build(),
            Map.of(),
            Map.of(),
            Optional.empty()
        ).rebalance().build();
        assertThat(result.allAssignments().isEmpty(), is(true));
    }

    public void testRebalance_GivenAllAssignmentsAreSatisfied_ShouldMakeNoChanges() throws Exception {
        String modelId1 = "model-1";
        String modelId2 = "model-2";
        String deploymentId1 = "deployment-1";
        String deploymentId2 = "deployment-2";
        StartTrainedModelDeploymentAction.TaskParams taskParams1 = normalPriorityParams(deploymentId1, modelId1, 1024L, 1, 2);
        StartTrainedModelDeploymentAction.TaskParams taskParams2 = normalPriorityParams(deploymentId2, modelId2, 1024L, 4, 1);
        TrainedModelAssignmentMetadata currentMetadata = TrainedModelAssignmentMetadata.Builder.empty()
            .addNewAssignment(
                deploymentId1,
                TrainedModelAssignment.Builder.empty(taskParams1).addRoutingEntry("node-1", new RoutingInfo(1, 1, RoutingState.STARTED, ""))
            )
            .addNewAssignment(
                deploymentId2,
                TrainedModelAssignment.Builder.empty(taskParams2)
                    .addRoutingEntry("node-1", new RoutingInfo(1, 1, RoutingState.STARTED, ""))
                    .addRoutingEntry("node-2", new RoutingInfo(3, 3, RoutingState.STARTED, ""))
            )
            .build();
        Map<DiscoveryNode, NodeLoad> nodeLoads = new HashMap<>();
        long oneGbBytes = ByteSizeValue.ofGb(1).getBytes();
        nodeLoads.put(buildNode("node-1", oneGbBytes, 4), NodeLoad.builder("node-1").setMaxMemory(oneGbBytes).build());
        nodeLoads.put(buildNode("node-2", oneGbBytes, 4), NodeLoad.builder("node-2").setMaxMemory(oneGbBytes).build());

        TrainedModelAssignmentMetadata result = new TrainedModelAssignmentRebalancer(currentMetadata, nodeLoads, Map.of(), Optional.empty())
            .rebalance()
            .build();

        assertThat(currentMetadata, equalTo(result));
    }

    public void testRebalance_GivenAllAssignmentsAreSatisfied_GivenOutdatedRoutingEntry_ShouldRebalance() throws Exception {
        long oneGbBytes = ByteSizeValue.ofGb(1).getBytes();
        DiscoveryNode node1 = buildNode("node-1", oneGbBytes, 4);
        DiscoveryNode node2 = buildNode("node-2", oneGbBytes, 4);

        String modelId1 = "model-1";
        String modelId2 = "model-2";
        String deploymentId1 = "deployment-1";
        String deploymentId2 = "deployment-2";
        StartTrainedModelDeploymentAction.TaskParams taskParams1 = normalPriorityParams(deploymentId1, modelId1, 1024L, 1, 2);
        StartTrainedModelDeploymentAction.TaskParams taskParams2 = normalPriorityParams(deploymentId2, modelId2, 1024L, 4, 1);
        TrainedModelAssignmentMetadata currentMetadata = TrainedModelAssignmentMetadata.Builder.empty()
            .addNewAssignment(
                deploymentId1,
                TrainedModelAssignment.Builder.empty(taskParams1).addRoutingEntry("node-1", new RoutingInfo(0, 0, RoutingState.STARTED, ""))
            )
            .addNewAssignment(
                deploymentId2,
                TrainedModelAssignment.Builder.empty(taskParams2)
                    .addRoutingEntry("node-1", new RoutingInfo(1, 1, RoutingState.STARTED, ""))
                    .addRoutingEntry("node-2", new RoutingInfo(3, 3, RoutingState.STARTED, ""))
            )
            .build();
        Map<DiscoveryNode, NodeLoad> nodeLoads = new HashMap<>();
        nodeLoads.put(node1, NodeLoad.builder("node-1").setMaxMemory(oneGbBytes).build());
        nodeLoads.put(node2, NodeLoad.builder("node-2").setMaxMemory(oneGbBytes).build());

        TrainedModelAssignmentMetadata result = new TrainedModelAssignmentRebalancer(
            currentMetadata,
            nodeLoads,
            Map.of(List.of(), List.of(node1, node2)),
            Optional.empty()
        ).rebalance().build();

        assertThat(result.allAssignments(), is(aMapWithSize(2)));

        for (String deploymentId : List.of(deploymentId1, deploymentId2)) {
            TrainedModelAssignment assignment = result.getDeploymentAssignment(deploymentId);
            assertThat(assignment, is(notNullValue()));
            assertThat(assignment.hasOutdatedRoutingEntries(), is(false));
            assertThat(
                assignment.getNodeRoutingTable().values().stream().mapToInt(RoutingInfo::getTargetAllocations).sum(),
                equalTo(currentMetadata.getDeploymentAssignment(deploymentId).getTaskParams().getNumberOfAllocations())
            );
        }
    }

    public void testRebalance_GivenModelToAddAlreadyExists() {
        String modelId = "model-to-add";
        StartTrainedModelDeploymentAction.TaskParams taskParams = normalPriorityParams(modelId, modelId, 1024L, 1, 1);
        TrainedModelAssignmentMetadata currentMetadata = TrainedModelAssignmentMetadata.Builder.empty()
            .addNewAssignment(modelId, TrainedModelAssignment.Builder.empty(taskParams))
            .build();
        expectThrows(
            ResourceAlreadyExistsException.class,
            () -> new TrainedModelAssignmentRebalancer(currentMetadata, Map.of(), Map.of(), Optional.of(taskParams)).rebalance()
        );
    }

    public void testRebalance_GivenFirstModelToAdd_NoMLNodes() throws Exception {
        String modelId = "model-to-add";
        StartTrainedModelDeploymentAction.TaskParams taskParams = normalPriorityParams(modelId, modelId, 1024L, 1, 1);
        TrainedModelAssignmentMetadata currentMetadata = TrainedModelAssignmentMetadata.Builder.empty().build();

        TrainedModelAssignmentMetadata result = new TrainedModelAssignmentRebalancer(
            currentMetadata,
            Map.of(),
            Map.of(),
            Optional.of(taskParams)
        ).rebalance().build();

        TrainedModelAssignment assignment = result.getDeploymentAssignment(modelId);
        assertThat(assignment, is(notNullValue()));
        assertThat(assignment.getAssignmentState(), equalTo(AssignmentState.STARTING));
        assertThat(assignment.getNodeRoutingTable(), is(anEmptyMap()));
        assertThat(assignment.getReason().isPresent(), is(true));
        assertThat(assignment.getReason().get(), equalTo("No ML nodes exist in the cluster"));
    }

    public void testRebalance_GivenFirstModelToAdd_NotEnoughProcessors() throws Exception {
        long nodeMemoryBytes = ByteSizeValue.ofGb(1).getBytes();
        DiscoveryNode node = buildNode("node-1", nodeMemoryBytes, 3);

        String modelId = "model-to-add";
        StartTrainedModelDeploymentAction.TaskParams taskParams = normalPriorityParams(modelId, modelId, 1024L, 1, 4);
        TrainedModelAssignmentMetadata currentMetadata = TrainedModelAssignmentMetadata.Builder.empty().build();
        Map<DiscoveryNode, NodeLoad> nodeLoads = new HashMap<>();

        nodeLoads.put(node, NodeLoad.builder("node-1").setMaxMemory(nodeMemoryBytes).build());

        TrainedModelAssignmentMetadata result = new TrainedModelAssignmentRebalancer(
            currentMetadata,
            nodeLoads,
            Map.of(List.of(), List.of(node)),
            Optional.of(taskParams)
        ).rebalance().build();

        TrainedModelAssignment assignment = result.getDeploymentAssignment(modelId);
        assertThat(assignment, is(notNullValue()));
        assertThat(assignment.getAssignmentState(), equalTo(AssignmentState.STARTING));
        assertThat(assignment.getNodeRoutingTable(), is(anEmptyMap()));
        assertThat(assignment.getReason().isPresent(), is(true));
        assertThat(
            assignment.getReason().get(),
            equalTo(
                "Could not assign (more) allocations on node [node-1]. Reason: This node has insufficient allocated processors. "
                    + "Available processors [3], free processors [3], processors required for each allocation of this model [4]"
            )
        );
    }

    public void testRebalance_GivenFirstModelToAdd_NotEnoughMemory() throws Exception {
        String modelId = "model-to-add";
        StartTrainedModelDeploymentAction.TaskParams taskParams = normalPriorityParams(
            modelId,
            modelId,
            ByteSizeValue.ofGb(2).getBytes(),
            1,
            1
        );
        TrainedModelAssignmentMetadata currentMetadata = TrainedModelAssignmentMetadata.Builder.empty().build();
        Map<DiscoveryNode, NodeLoad> nodeLoads = new HashMap<>();
        long nodeMemoryBytes = ByteSizeValue.ofGb(1).getBytes();
        nodeLoads.put(buildNode("node-1", nodeMemoryBytes, 3), NodeLoad.builder("node-1").setMaxMemory(nodeMemoryBytes).build());

        TrainedModelAssignmentMetadata result = new TrainedModelAssignmentRebalancer(
            currentMetadata,
            nodeLoads,
            Map.of(),
            Optional.of(taskParams)
        ).rebalance().build();

        TrainedModelAssignment assignment = result.getDeploymentAssignment(modelId);
        assertThat(assignment, is(notNullValue()));
        assertThat(assignment.getAssignmentState(), equalTo(AssignmentState.STARTING));
        assertThat(assignment.getNodeRoutingTable(), is(anEmptyMap()));
        assertThat(assignment.getReason().isPresent(), is(true));
        assertThat(
            assignment.getReason().get(),
            containsString("Could not assign (more) allocations on node [node-1]. Reason: This node has insufficient available memory.")
        );
    }

    public void testRebalance_GivenFirstModelToAdd_ErrorDetectingNodeLoad() throws Exception {
        String modelId = "model-to-add";
        StartTrainedModelDeploymentAction.TaskParams taskParams = normalPriorityParams(
            modelId,
            modelId,
            ByteSizeValue.ofGb(2).getBytes(),
            1,
            1
        );
        TrainedModelAssignmentMetadata currentMetadata = TrainedModelAssignmentMetadata.Builder.empty().build();
        Map<DiscoveryNode, NodeLoad> nodeLoads = new HashMap<>();
        long nodeMemoryBytes = ByteSizeValue.ofGb(1).getBytes();
        nodeLoads.put(
            buildNode("node-1", nodeMemoryBytes, 3),
            NodeLoad.builder("node-1").setMaxMemory(nodeMemoryBytes).setError("error detecting load").build()
        );

        TrainedModelAssignmentMetadata result = new TrainedModelAssignmentRebalancer(
            currentMetadata,
            nodeLoads,
            Map.of(),
            Optional.of(taskParams)
        ).rebalance().build();

        TrainedModelAssignment assignment = result.getDeploymentAssignment(modelId);
        assertThat(assignment, is(notNullValue()));
        assertThat(assignment.getAssignmentState(), equalTo(AssignmentState.STARTING));
        assertThat(assignment.getNodeRoutingTable(), is(anEmptyMap()));
        assertThat(assignment.getReason().isPresent(), is(true));
        assertThat(
            assignment.getReason().get(),
            containsString("Could not assign (more) allocations on node [node-1]. Reason: error detecting load")
        );
    }

    public void testRebalance_GivenProblemsOnMultipleNodes() throws Exception {
        DiscoveryNode node1 = buildNode("node-1", ByteSizeValue.ofGb(1).getBytes(), 8);
        DiscoveryNode node2 = buildNode("node-2", ByteSizeValue.ofGb(10).getBytes(), 3);

        String modelId = "model-to-add";
        StartTrainedModelDeploymentAction.TaskParams taskParams = normalPriorityParams(
            modelId,
            modelId,
            ByteSizeValue.ofGb(2).getBytes(),
            1,
            4
        );
        TrainedModelAssignmentMetadata currentMetadata = TrainedModelAssignmentMetadata.Builder.empty().build();
        Map<DiscoveryNode, NodeLoad> nodeLoads = new HashMap<>();
        nodeLoads.put(node1, NodeLoad.builder("node-1").setMaxMemory(ByteSizeValue.ofGb(1).getBytes()).build());
        nodeLoads.put(node2, NodeLoad.builder("node-2").setMaxMemory(ByteSizeValue.ofGb(10).getBytes()).build());

        TrainedModelAssignmentMetadata result = new TrainedModelAssignmentRebalancer(
            currentMetadata,
            nodeLoads,
            Map.of(List.of(), List.of(node1, node2)),
            Optional.of(taskParams)
        ).rebalance().build();

        TrainedModelAssignment assignment = result.getDeploymentAssignment(modelId);
        assertThat(assignment, is(notNullValue()));
        assertThat(assignment.getAssignmentState(), equalTo(AssignmentState.STARTING));
        assertThat(assignment.getNodeRoutingTable(), is(anEmptyMap()));
        assertThat(assignment.getReason().isPresent(), is(true));
        assertThat(
            assignment.getReason().get(),
            containsString("Could not assign (more) allocations on node [node-1]. Reason: This node has insufficient available memory.")
        );
        assertThat(
            assignment.getReason().get(),
            containsString("Could not assign (more) allocations on node [node-2]. Reason: This node has insufficient allocated processors.")
        );
    }

    public void testRebalance_GivenFirstModelToAdd_FitsFully() throws Exception {
        long nodeMemoryBytes = ByteSizeValue.ofGb(1).getBytes();
        DiscoveryNode node1 = buildNode("node-1", nodeMemoryBytes, 4);

        String modelId = "model-to-add";
        StartTrainedModelDeploymentAction.TaskParams taskParams = normalPriorityParams(modelId, modelId, 1024L, 1, 1);
        TrainedModelAssignmentMetadata currentMetadata = TrainedModelAssignmentMetadata.Builder.empty().build();
        Map<DiscoveryNode, NodeLoad> nodeLoads = new HashMap<>();
        nodeLoads.put(node1, NodeLoad.builder("node-1").setMaxMemory(nodeMemoryBytes).build());

        TrainedModelAssignmentMetadata result = new TrainedModelAssignmentRebalancer(
            currentMetadata,
            nodeLoads,
            Map.of(List.of(), List.of(node1)),
            Optional.of(taskParams)
        ).rebalance().build();

        TrainedModelAssignment assignment = result.getDeploymentAssignment(modelId);
        assertThat(assignment, is(notNullValue()));
        assertThat(assignment.getAssignmentState(), equalTo(AssignmentState.STARTING));
        assertThat(assignment.getNodeRoutingTable(), is(aMapWithSize(1)));
        assertThat(assignment.getNodeRoutingTable(), hasKey("node-1"));
        assertThat(assignment.getNodeRoutingTable().get("node-1").getCurrentAllocations(), equalTo(1));
        assertThat(assignment.getNodeRoutingTable().get("node-1").getTargetAllocations(), equalTo(1));
        assertThat(assignment.getNodeRoutingTable().get("node-1").getState(), equalTo(RoutingState.STARTING));
        assertThat(assignment.getReason().isPresent(), is(false));
    }

    public void testRebalance_GivenModelToAdd_AndPreviousAssignments_AndTwoNodes_AllFit() throws Exception {
        long nodeMemoryBytes = ByteSizeValue.ofGb(1).getBytes();
        DiscoveryNode node1 = buildNode("node-1", nodeMemoryBytes, 4);
        DiscoveryNode node2 = buildNode("node-2", nodeMemoryBytes, 4);

        String deploymentToAddId = "model-to-add";
        String previousDeploymentId = "previous-model";
        StartTrainedModelDeploymentAction.TaskParams taskParams = normalPriorityParams(deploymentToAddId, deploymentToAddId, 1024L, 1, 2);
        TrainedModelAssignmentMetadata currentMetadata = TrainedModelAssignmentMetadata.Builder.empty()
            .addNewAssignment(
                previousDeploymentId,
                TrainedModelAssignment.Builder.empty(normalPriorityParams(previousDeploymentId, previousDeploymentId, 1024L, 3, 2))
                    .addRoutingEntry("node-1", new RoutingInfo(2, 2, RoutingState.STARTED, ""))
                    .addRoutingEntry("node-2", new RoutingInfo(1, 1, RoutingState.STARTED, ""))
            )
            .build();
        Map<DiscoveryNode, NodeLoad> nodeLoads = new HashMap<>();
        nodeLoads.put(node1, NodeLoad.builder("node-1").setMaxMemory(nodeMemoryBytes).build());
        nodeLoads.put(node2, NodeLoad.builder("node-2").setMaxMemory(nodeMemoryBytes).build());

        TrainedModelAssignmentMetadata result = new TrainedModelAssignmentRebalancer(
            currentMetadata,
            nodeLoads,
            Map.of(List.of(), List.of(node1, node2)),
            Optional.of(taskParams)
        ).rebalance().build();

        assertThat(result.allAssignments(), is(aMapWithSize(2)));

        {
            TrainedModelAssignment assignment = result.getDeploymentAssignment(deploymentToAddId);
            assertThat(assignment, is(notNullValue()));
            assertThat(assignment.getAssignmentState(), equalTo(AssignmentState.STARTING));
            assertThat(assignment.getNodeRoutingTable(), is(aMapWithSize(1)));
            assertThat(assignment.getNodeRoutingTable(), hasKey("node-2"));
            assertThat(assignment.getNodeRoutingTable().get("node-2").getCurrentAllocations(), equalTo(1));
            assertThat(assignment.getNodeRoutingTable().get("node-2").getTargetAllocations(), equalTo(1));
            assertThat(assignment.getNodeRoutingTable().get("node-2").getState(), equalTo(RoutingState.STARTING));
            assertThat(assignment.getReason().isPresent(), is(false));
        }
        {
            TrainedModelAssignment assignment = result.getDeploymentAssignment(previousDeploymentId);
            assertThat(assignment, is(notNullValue()));
            assertThat(assignment.getAssignmentState(), equalTo(AssignmentState.STARTED));
            assertThat(assignment.getNodeRoutingTable(), is(aMapWithSize(2)));
            assertThat(assignment.getNodeRoutingTable(), hasKey("node-1"));
            assertThat(assignment.getNodeRoutingTable(), hasKey("node-2"));
            assertThat(assignment.getNodeRoutingTable().get("node-1").getCurrentAllocations(), equalTo(2));
            assertThat(assignment.getNodeRoutingTable().get("node-1").getTargetAllocations(), equalTo(2));
            assertThat(assignment.getNodeRoutingTable().get("node-1").getState(), equalTo(RoutingState.STARTED));
            assertThat(assignment.getNodeRoutingTable().get("node-2").getCurrentAllocations(), equalTo(1));
            assertThat(assignment.getNodeRoutingTable().get("node-2").getTargetAllocations(), equalTo(1));
            assertThat(assignment.getNodeRoutingTable().get("node-2").getState(), equalTo(RoutingState.STARTED));
            assertThat(assignment.getReason().isPresent(), is(false));
        }
    }

    public void testRebalance_GivenPreviousAssignments_AndNewNode() throws Exception {
        long nodeMemoryBytes = ByteSizeValue.ofGb(1).getBytes();
        DiscoveryNode node1 = buildNode("node-1", nodeMemoryBytes, 4);
        DiscoveryNode node2 = buildNode("node-2", nodeMemoryBytes, 4);
        DiscoveryNode node3 = buildNode("node-3", nodeMemoryBytes, 4);

        String previousDeployment1Id = "previous-model-1";
        String previousDeployment2Id = "previous-model-2";
        TrainedModelAssignmentMetadata currentMetadata = TrainedModelAssignmentMetadata.Builder.empty()
            .addNewAssignment(
                previousDeployment1Id,
                TrainedModelAssignment.Builder.empty(normalPriorityParams(previousDeployment1Id, 1024L, 3, 2))
                    .addRoutingEntry("node-1", new RoutingInfo(2, 2, RoutingState.STARTED, ""))
                    .addRoutingEntry("node-2", new RoutingInfo(1, 1, RoutingState.STARTED, ""))
            )
            .addNewAssignment(
                previousDeployment2Id,
                TrainedModelAssignment.Builder.empty(normalPriorityParams(previousDeployment2Id, 1024L, 4, 1))
                    .addRoutingEntry("node-2", new RoutingInfo(1, 1, RoutingState.STARTED, ""))
            )
            .build();
        Map<DiscoveryNode, NodeLoad> nodeLoads = new HashMap<>();
        nodeLoads.put(node1, NodeLoad.builder("node-1").setMaxMemory(nodeMemoryBytes).build());
        nodeLoads.put(node2, NodeLoad.builder("node-2").setMaxMemory(nodeMemoryBytes).build());
        nodeLoads.put(node3, NodeLoad.builder("node-3").setMaxMemory(nodeMemoryBytes).build());

        TrainedModelAssignmentMetadata result = new TrainedModelAssignmentRebalancer(
            currentMetadata,
            nodeLoads,
            Map.of(List.of(), List.of(node1, node2, node3)),
            Optional.empty()
        ).rebalance().build();

        assertThat(result.allAssignments(), is(aMapWithSize(2)));

        {
            TrainedModelAssignment assignment = result.getDeploymentAssignment(previousDeployment1Id);
            assertThat(assignment, is(notNullValue()));
            assertThat(assignment.getAssignmentState(), equalTo(AssignmentState.STARTED));
            assertThat(assignment.getNodeRoutingTable(), is(aMapWithSize(2)));
            assertThat(assignment.getNodeRoutingTable(), hasKey("node-1"));
            assertThat(assignment.getNodeRoutingTable(), hasKey("node-2"));
            assertThat(assignment.getNodeRoutingTable().get("node-1").getCurrentAllocations(), equalTo(2));
            assertThat(assignment.getNodeRoutingTable().get("node-1").getTargetAllocations(), equalTo(2));
            assertThat(assignment.getNodeRoutingTable().get("node-1").getState(), equalTo(RoutingState.STARTED));
            assertThat(assignment.getNodeRoutingTable().get("node-2").getCurrentAllocations(), equalTo(1));
            assertThat(assignment.getNodeRoutingTable().get("node-2").getTargetAllocations(), equalTo(1));
            assertThat(assignment.getNodeRoutingTable().get("node-2").getState(), equalTo(RoutingState.STARTED));
            assertThat(assignment.getReason().isPresent(), is(false));
        }
        {
            TrainedModelAssignment assignment = result.getDeploymentAssignment(previousDeployment2Id);
            assertThat(assignment, is(notNullValue()));
            assertThat(assignment.getAssignmentState(), equalTo(AssignmentState.STARTED));
            assertThat(assignment.getNodeRoutingTable(), is(aMapWithSize(2)));
            assertThat(assignment.getNodeRoutingTable(), hasKey("node-2"));
            assertThat(assignment.getNodeRoutingTable(), hasKey("node-3"));
            assertThat(assignment.getNodeRoutingTable().get("node-2").getCurrentAllocations(), equalTo(1));
            assertThat(assignment.getNodeRoutingTable().get("node-2").getTargetAllocations(), equalTo(2));
            assertThat(assignment.getNodeRoutingTable().get("node-2").getState(), equalTo(RoutingState.STARTED));
            assertThat(assignment.getNodeRoutingTable().get("node-3").getCurrentAllocations(), equalTo(2));
            assertThat(assignment.getNodeRoutingTable().get("node-3").getTargetAllocations(), equalTo(2));
            assertThat(assignment.getNodeRoutingTable().get("node-3").getState(), equalTo(RoutingState.STARTING));
            assertThat(assignment.getReason().isPresent(), is(false));
        }
    }

    public void testRebalance_GivenPreviousAssignments_AndRemovedNode_AndRemainingNodeNotLargeEnough() throws Exception {
        long nodeMemoryBytes = ByteSizeValue.ofGb(1).getBytes();
        DiscoveryNode node1 = buildNode("node-1", nodeMemoryBytes, 4);

        String previousDeployment1Id = "previous-deployment-1";
        String previousDeployment2Id = "previous-deployment-2";
        TrainedModelAssignmentMetadata currentMetadata = TrainedModelAssignmentMetadata.Builder.empty()
            .addNewAssignment(
                previousDeployment1Id,
                TrainedModelAssignment.Builder.empty(normalPriorityParams(previousDeployment1Id, 1024L, 3, 2))
                    .addRoutingEntry("node-1", new RoutingInfo(2, 2, RoutingState.STARTED, ""))
                    .addRoutingEntry("node-2", new RoutingInfo(1, 1, RoutingState.STARTED, ""))
            )
            .addNewAssignment(
                previousDeployment2Id,
                TrainedModelAssignment.Builder.empty(normalPriorityParams(previousDeployment2Id, 1024L, 4, 1))
                    .addRoutingEntry("node-2", new RoutingInfo(1, 1, RoutingState.STARTED, ""))
            )
            .build();
        Map<DiscoveryNode, NodeLoad> nodeLoads = new HashMap<>();
        nodeLoads.put(node1, NodeLoad.builder("node-1").setMaxMemory(nodeMemoryBytes).build());

        TrainedModelAssignmentMetadata result = new TrainedModelAssignmentRebalancer(
            currentMetadata,
            nodeLoads,
            Map.of(List.of(), List.of(node1)),
            Optional.empty()
        ).rebalance().build();

        assertThat(result.allAssignments(), is(aMapWithSize(2)));

        {
            TrainedModelAssignment assignment = result.getDeploymentAssignment(previousDeployment1Id);
            assertThat(assignment, is(notNullValue()));
            assertThat(assignment.getAssignmentState(), equalTo(AssignmentState.STARTED));
            assertThat(assignment.getNodeRoutingTable(), is(aMapWithSize(1)));
            assertThat(assignment.getNodeRoutingTable(), hasKey("node-1"));
            assertThat(assignment.getNodeRoutingTable().get("node-1").getCurrentAllocations(), equalTo(2));
            assertThat(assignment.getNodeRoutingTable().get("node-1").getTargetAllocations(), equalTo(1));
            assertThat(assignment.getNodeRoutingTable().get("node-1").getState(), equalTo(RoutingState.STARTED));
            assertThat(assignment.getReason().isPresent(), is(true));
            assertThat(
                assignment.getReason().get(),
                equalTo(
                    "Could not assign (more) allocations on node [node-1]. Reason: This node has insufficient allocated processors. "
                        + "Available processors [4], free processors [0], processors required for each allocation of this model [2]"
                )
            );
        }
        {
            TrainedModelAssignment assignment = result.getDeploymentAssignment(previousDeployment2Id);
            assertThat(assignment, is(notNullValue()));
            assertThat(assignment.getAssignmentState(), equalTo(AssignmentState.STARTING));
            assertThat(assignment.getNodeRoutingTable(), is(aMapWithSize(1)));
            assertThat(assignment.getNodeRoutingTable(), hasKey("node-1"));
            assertThat(assignment.getNodeRoutingTable().get("node-1").getCurrentAllocations(), equalTo(2));
            assertThat(assignment.getNodeRoutingTable().get("node-1").getTargetAllocations(), equalTo(2));
            assertThat(assignment.getNodeRoutingTable().get("node-1").getState(), equalTo(RoutingState.STARTING));
            assertThat(assignment.getReason().isPresent(), is(true));
            assertThat(
                assignment.getReason().get(),
                equalTo(
                    "Could not assign (more) allocations on node [node-1]. Reason: This node has insufficient allocated processors. "
                        + "Available processors [4], free processors [0], processors required for each allocation of this model [1]"
                )
            );
        }
    }

    public void testRebalance_GivenPreviousAssignments_AndRemovedNode_AndRemainingNodeLargeEnough() throws Exception {
        long nodeMemoryBytes = ByteSizeValue.ofGb(1).getBytes();
        DiscoveryNode node1 = buildNode("node-1", nodeMemoryBytes, 7);

        String previousDeployment1Id = "previous-deployment-1";
        String previousDeployment2Id = "previous-deployment-2";
        TrainedModelAssignmentMetadata currentMetadata = TrainedModelAssignmentMetadata.Builder.empty()
            .addNewAssignment(
                previousDeployment1Id,
                TrainedModelAssignment.Builder.empty(normalPriorityParams(previousDeployment1Id, 1024L, 3, 2))
                    .addRoutingEntry("node-1", new RoutingInfo(2, 2, RoutingState.STARTED, ""))
                    .addRoutingEntry("node-2", new RoutingInfo(1, 1, RoutingState.STARTED, ""))
            )
            .addNewAssignment(
                previousDeployment2Id,
                TrainedModelAssignment.Builder.empty(normalPriorityParams(previousDeployment2Id, 1024L, 1, 1))
                    .addRoutingEntry("node-2", new RoutingInfo(1, 1, RoutingState.STARTED, ""))
            )
            .build();
        Map<DiscoveryNode, NodeLoad> nodeLoads = new HashMap<>();
        nodeLoads.put(node1, NodeLoad.builder("node-1").setMaxMemory(nodeMemoryBytes).build());

        TrainedModelAssignmentMetadata result = new TrainedModelAssignmentRebalancer(
            currentMetadata,
            nodeLoads,
            Map.of(List.of(), List.of(node1)),
            Optional.empty()
        ).rebalance().build();

        assertThat(result.allAssignments(), is(aMapWithSize(2)));

        {
            TrainedModelAssignment assignment = result.getDeploymentAssignment(previousDeployment1Id);
            assertThat(assignment, is(notNullValue()));
            assertThat(assignment.getAssignmentState(), equalTo(AssignmentState.STARTED));
            assertThat(assignment.getNodeRoutingTable(), is(aMapWithSize(1)));
            assertThat(assignment.getNodeRoutingTable(), hasKey("node-1"));
            assertThat(assignment.getNodeRoutingTable().get("node-1").getCurrentAllocations(), equalTo(2));
            assertThat(assignment.getNodeRoutingTable().get("node-1").getTargetAllocations(), equalTo(3));
            assertThat(assignment.getNodeRoutingTable().get("node-1").getState(), equalTo(RoutingState.STARTED));
            assertThat(assignment.getReason().isPresent(), is(false));
        }
        {
            TrainedModelAssignment assignment = result.getDeploymentAssignment(previousDeployment2Id);
            assertThat(assignment, is(notNullValue()));
            assertThat(assignment.getAssignmentState(), equalTo(AssignmentState.STARTING));
            assertThat(assignment.getNodeRoutingTable(), is(aMapWithSize(1)));
            assertThat(assignment.getNodeRoutingTable(), hasKey("node-1"));
            assertThat(assignment.getNodeRoutingTable().get("node-1").getCurrentAllocations(), equalTo(1));
            assertThat(assignment.getNodeRoutingTable().get("node-1").getTargetAllocations(), equalTo(1));
            assertThat(assignment.getNodeRoutingTable().get("node-1").getState(), equalTo(RoutingState.STARTING));
            assertThat(assignment.getReason().isPresent(), is(false));
        }
    }

    public void testRebalance_GivenFailedAssignment_RestartsAssignment() throws Exception {
        long nodeMemoryBytes = ByteSizeValue.ofGb(1).getBytes();
        DiscoveryNode node1 = buildNode("node-1", nodeMemoryBytes, 4);

        String modelId = "model-1";
        TrainedModelAssignmentMetadata currentMetadata = TrainedModelAssignmentMetadata.Builder.empty()
            .addNewAssignment(
                modelId,
                TrainedModelAssignment.Builder.empty(normalPriorityParams(modelId, 1024L, 1, 1))
                    .addRoutingEntry("node-1", new RoutingInfo(1, 1, RoutingState.FAILED, "some error"))
            )
            .build();
        Map<DiscoveryNode, NodeLoad> nodeLoads = new HashMap<>();
        nodeLoads.put(node1, NodeLoad.builder("node-1").setMaxMemory(nodeMemoryBytes).build());

        TrainedModelAssignmentMetadata result = new TrainedModelAssignmentRebalancer(
            currentMetadata,
            nodeLoads,
            Map.of(List.of(), List.of(node1)),
            Optional.empty()
        ).rebalance().build();

        assertThat(result.allAssignments(), is(aMapWithSize(1)));

        TrainedModelAssignment assignment = result.getDeploymentAssignment(modelId);
        assertThat(assignment, is(notNullValue()));
        assertThat(assignment.getAssignmentState(), equalTo(AssignmentState.STARTING));
        assertThat(assignment.getNodeRoutingTable(), is(aMapWithSize(1)));
        assertThat(assignment.getNodeRoutingTable(), hasKey("node-1"));
        assertThat(assignment.getNodeRoutingTable().get("node-1").getCurrentAllocations(), equalTo(1));
        assertThat(assignment.getNodeRoutingTable().get("node-1").getTargetAllocations(), equalTo(1));
        assertThat(assignment.getNodeRoutingTable().get("node-1").getState(), equalTo(RoutingState.STARTING));
        assertThat(assignment.getReason().isPresent(), is(false));
    }

    public void testRebalance_GivenLowPriorityModelToAdd_OnlyModel_NotEnoughMemory() throws Exception {
        String modelId = "model-to-add";
        String deploymentId = "deployment-to-add";
        StartTrainedModelDeploymentAction.TaskParams taskParams = lowPriorityParams(
            deploymentId,
            modelId,
            ByteSizeValue.ofGb(2).getBytes()
        );
        TrainedModelAssignmentMetadata currentMetadata = TrainedModelAssignmentMetadata.Builder.empty().build();
        Map<DiscoveryNode, NodeLoad> nodeLoads = new HashMap<>();
        long nodeMemoryBytes = ByteSizeValue.ofGb(1).getBytes();
        nodeLoads.put(buildNode("node-1", nodeMemoryBytes, 3), NodeLoad.builder("node-1").setMaxMemory(nodeMemoryBytes).build());

        TrainedModelAssignmentMetadata result = new TrainedModelAssignmentRebalancer(
            currentMetadata,
            nodeLoads,
            Map.of(),
            Optional.of(taskParams)
        ).rebalance().build();

        TrainedModelAssignment assignment = result.getDeploymentAssignment(deploymentId);
        assertThat(assignment, is(notNullValue()));
        assertThat(assignment.getAssignmentState(), equalTo(AssignmentState.STARTING));
        assertThat(assignment.getNodeRoutingTable(), is(anEmptyMap()));
        assertThat(assignment.getReason().isPresent(), is(true));
        assertThat(
            assignment.getReason().get(),
            containsString("Could not assign (more) allocations on node [node-1]. Reason: This node has insufficient available memory.")
        );
    }

    public void testRebalance_GivenLowPriorityModelToAdd_NotEnoughMemoryNorProcessors() throws Exception {
        long nodeMemoryBytes = ByteSizeValue.ofGb(1).getBytes();
        DiscoveryNode node1 = buildNode("node-1", nodeMemoryBytes, 1);
        DiscoveryNode node2 = buildNode("node-2", nodeMemoryBytes, 1);

        Map<DiscoveryNode, NodeLoad> nodeLoads = new HashMap<>();
        nodeLoads.put(node1, NodeLoad.builder("node-1").setMaxMemory(nodeMemoryBytes).build());
        nodeLoads.put(node2, NodeLoad.builder("node-2").setMaxMemory(nodeMemoryBytes).build());

        String deployment1 = "deployment-1";
        StartTrainedModelDeploymentAction.TaskParams taskParams1 = lowPriorityParams(deployment1, ByteSizeValue.ofMb(300).getBytes());
        String deployment2 = "deployment-2";
        StartTrainedModelDeploymentAction.TaskParams taskParams2 = normalPriorityParams(
            deployment2,
            ByteSizeValue.ofMb(300).getBytes(),
            2,
            1
        );
        TrainedModelAssignmentMetadata currentMetadata = TrainedModelAssignmentMetadata.Builder.empty()
            .addNewAssignment(
                deployment2,
                TrainedModelAssignment.Builder.empty(taskParams2)
                    .addRoutingEntry("node-1", new RoutingInfo(1, 1, RoutingState.STARTED, ""))
                    .addRoutingEntry("node-2", new RoutingInfo(1, 1, RoutingState.STARTED, ""))
            )
            .build();

        TrainedModelAssignmentMetadata result = new TrainedModelAssignmentRebalancer(
            currentMetadata,
            nodeLoads,
            Map.of(List.of("zone-1"), List.of(node1), List.of("zone-2"), List.of(node2)),
            Optional.of(taskParams1)
        ).rebalance().build();

        TrainedModelAssignment assignment = result.getDeploymentAssignment(deployment1);
        assertThat(assignment, is(notNullValue()));
        assertThat(assignment.getAssignmentState(), equalTo(AssignmentState.STARTING));
        assertThat(assignment.getNodeRoutingTable(), is(anEmptyMap()));
        assertThat(assignment.getReason().isPresent(), is(true));
        assertThat(
            assignment.getReason().get(),
            containsString("Could not assign (more) allocations on node [node-1]. Reason: This node has insufficient available memory.")
        );
        assertThat(
            assignment.getReason().get(),
            containsString("Could not assign (more) allocations on node [node-2]. Reason: This node has insufficient available memory.")
        );
    }

    public void testRebalance_GivenMixedPriorityModels_NotEnoughMemoryForLowPriority() throws Exception {
        long nodeMemoryBytes = ByteSizeValue.ofGb(1).getBytes();
        DiscoveryNode node1 = buildNode("node-1", nodeMemoryBytes, 7);

        Map<DiscoveryNode, NodeLoad> nodeLoads = new HashMap<>();
        nodeLoads.put(node1, NodeLoad.builder("node-1").setMaxMemory(nodeMemoryBytes).build());

        String modelId1 = "model-1";
        StartTrainedModelDeploymentAction.TaskParams taskParams1 = lowPriorityParams(modelId1, ByteSizeValue.ofMb(250).getBytes());
        String modelId2 = "model-2";
        StartTrainedModelDeploymentAction.TaskParams taskParams2 = normalPriorityParams(modelId2, ByteSizeValue.ofMb(300).getBytes(), 1, 1);
        TrainedModelAssignmentMetadata currentMetadata = TrainedModelAssignmentMetadata.Builder.empty()
            .addNewAssignment(modelId1, TrainedModelAssignment.Builder.empty(taskParams1))
            .addNewAssignment(modelId2, TrainedModelAssignment.Builder.empty(taskParams2))
            .build();

        TrainedModelAssignmentMetadata result = new TrainedModelAssignmentRebalancer(
            currentMetadata,
            nodeLoads,
            Map.of(List.of(), List.of(node1)),
            Optional.empty()
        ).rebalance().build();

        {
            TrainedModelAssignment assignment = result.getDeploymentAssignment(modelId1);
            assertThat(assignment, is(notNullValue()));
            assertThat(assignment.getAssignmentState(), equalTo(AssignmentState.STARTING));
            assertThat(assignment.getNodeRoutingTable(), is(anEmptyMap()));
            assertThat(assignment.getReason().isPresent(), is(true));
            assertThat(
                assignment.getReason().get(),
                containsString("Could not assign (more) allocations on node [node-1]. Reason: This node has insufficient available memory.")
            );
        }
        {
            TrainedModelAssignment assignment = result.getDeploymentAssignment(modelId2);
            assertThat(assignment, is(notNullValue()));
            assertThat(assignment.getAssignmentState(), equalTo(AssignmentState.STARTING));
            assertThat(assignment.getNodeRoutingTable(), is(aMapWithSize(1)));
            assertThat(assignment.getNodeRoutingTable(), hasKey("node-1"));
            assertThat(assignment.getNodeRoutingTable().get("node-1").getCurrentAllocations(), equalTo(1));
            assertThat(assignment.getNodeRoutingTable().get("node-1").getTargetAllocations(), equalTo(1));
            assertThat(assignment.getNodeRoutingTable().get("node-1").getState(), equalTo(RoutingState.STARTING));
            assertThat(assignment.getReason().isPresent(), is(false));
        }
    }

    public void testRebalance_GivenMixedPriorityModels_TwoZones_EachNodeCanHoldOneModel() throws Exception {
        long nodeMemoryBytes = ByteSizeValue.ofGb(1).getBytes();
        DiscoveryNode node1 = buildNode("node-1", nodeMemoryBytes, 1);
        DiscoveryNode node2 = buildNode("node-2", nodeMemoryBytes, 1);

        Map<DiscoveryNode, NodeLoad> nodeLoads = new HashMap<>();
        nodeLoads.put(node1, NodeLoad.builder("node-1").setMaxMemory(nodeMemoryBytes).build());
        nodeLoads.put(node2, NodeLoad.builder("node-2").setMaxMemory(nodeMemoryBytes).build());

        String modelId1 = "model-1";
        StartTrainedModelDeploymentAction.TaskParams taskParams1 = lowPriorityParams(modelId1, ByteSizeValue.ofMb(300).getBytes());
        String modelId2 = "model-2";
        StartTrainedModelDeploymentAction.TaskParams taskParams2 = normalPriorityParams(modelId2, ByteSizeValue.ofMb(300).getBytes(), 1, 1);
        TrainedModelAssignmentMetadata currentMetadata = TrainedModelAssignmentMetadata.Builder.empty()
            .addNewAssignment(modelId1, TrainedModelAssignment.Builder.empty(taskParams1))
            .addNewAssignment(
                modelId2,
                TrainedModelAssignment.Builder.empty(taskParams2).addRoutingEntry("node-1", new RoutingInfo(1, 1, RoutingState.STARTED, ""))
            )
            .build();

        TrainedModelAssignmentMetadata result = new TrainedModelAssignmentRebalancer(
            currentMetadata,
            nodeLoads,
            Map.of(List.of("zone-1"), List.of(node1), List.of("zone-2"), List.of(node2)),
            Optional.empty()
        ).rebalance().build();

        List<String> assignedNodes = new ArrayList<>();

        {
            TrainedModelAssignment assignment = result.getDeploymentAssignment(modelId1);
            assertThat(assignment, is(notNullValue()));
            assertThat(assignment.getAssignmentState(), equalTo(AssignmentState.STARTING));
            assertThat(assignment.getNodeRoutingTable(), is(aMapWithSize(1)));
            String assignedNode = assignment.getNodeRoutingTable().keySet().iterator().next();
            assertThat(assignment.getNodeRoutingTable().get(assignedNode).getCurrentAllocations(), equalTo(1));
            assertThat(assignment.getNodeRoutingTable().get(assignedNode).getTargetAllocations(), equalTo(1));
            assertThat(assignment.getNodeRoutingTable().get(assignedNode).getState(), equalTo(RoutingState.STARTING));
            assertThat(assignment.getReason().isPresent(), is(false));
            assignedNodes.add(assignedNode);
        }
        {
            TrainedModelAssignment assignment = result.getDeploymentAssignment(modelId2);
            assertThat(assignment, is(notNullValue()));
            assertThat(assignment.getAssignmentState(), equalTo(AssignmentState.STARTED));
            assertThat(assignment.getNodeRoutingTable(), is(aMapWithSize(1)));
            String assignedNode = assignment.getNodeRoutingTable().keySet().iterator().next();
            assertThat(assignment.getNodeRoutingTable().get(assignedNode).getCurrentAllocations(), equalTo(1));
            assertThat(assignment.getNodeRoutingTable().get(assignedNode).getTargetAllocations(), equalTo(1));
            assertThat(assignment.getNodeRoutingTable().get(assignedNode).getState(), equalTo(RoutingState.STARTED));
            assertThat(assignment.getReason().isPresent(), is(false));
            assignedNodes.add(assignedNode);
        }

        assertThat(assignedNodes, containsInAnyOrder("node-1", "node-2"));
    }

    public void testRebalance_GivenModelUsingAllCpu_FittingLowPriorityModelCanStart() throws Exception {
        long nodeMemoryBytes = ByteSizeValue.ofGb(1).getBytes();
        DiscoveryNode node1 = buildNode("node-1", nodeMemoryBytes, 7);

        Map<DiscoveryNode, NodeLoad> nodeLoads = new HashMap<>();
        nodeLoads.put(node1, NodeLoad.builder("node-1").setMaxMemory(nodeMemoryBytes).build());

        String modelId1 = "model-1";
        StartTrainedModelDeploymentAction.TaskParams taskParams1 = lowPriorityParams(modelId1, ByteSizeValue.ofMb(250).getBytes());
        String modelId2 = "model-2";
        StartTrainedModelDeploymentAction.TaskParams taskParams2 = normalPriorityParams(modelId2, ByteSizeValue.ofMb(300).getBytes(), 1, 1);
        TrainedModelAssignmentMetadata currentMetadata = TrainedModelAssignmentMetadata.Builder.empty()
            .addNewAssignment(modelId1, TrainedModelAssignment.Builder.empty(taskParams1))
            .addNewAssignment(modelId2, TrainedModelAssignment.Builder.empty(taskParams2))
            .build();

        TrainedModelAssignmentMetadata result = new TrainedModelAssignmentRebalancer(
            currentMetadata,
            nodeLoads,
            Map.of(List.of(), List.of(node1)),
            Optional.empty()
        ).rebalance().build();

        {
            TrainedModelAssignment assignment = result.getDeploymentAssignment(modelId1);
            assertThat(assignment, is(notNullValue()));
            assertThat(assignment.getAssignmentState(), equalTo(AssignmentState.STARTING));
            assertThat(assignment.getNodeRoutingTable(), is(anEmptyMap()));
            assertThat(assignment.getReason().isPresent(), is(true));
            assertThat(
                assignment.getReason().get(),
                containsString("Could not assign (more) allocations on node [node-1]. Reason: This node has insufficient available memory.")
            );
        }
        {
            TrainedModelAssignment assignment = result.getDeploymentAssignment(modelId2);
            assertThat(assignment, is(notNullValue()));
            assertThat(assignment.getAssignmentState(), equalTo(AssignmentState.STARTING));
            assertThat(assignment.getNodeRoutingTable(), is(aMapWithSize(1)));
            assertThat(assignment.getNodeRoutingTable(), hasKey("node-1"));
            assertThat(assignment.getNodeRoutingTable().get("node-1").getCurrentAllocations(), equalTo(1));
            assertThat(assignment.getNodeRoutingTable().get("node-1").getTargetAllocations(), equalTo(1));
            assertThat(assignment.getNodeRoutingTable().get("node-1").getState(), equalTo(RoutingState.STARTING));
            assertThat(assignment.getReason().isPresent(), is(false));
        }
    }

    public void testRebalance_GivenMultipleLowPriorityModels_AndMultipleNodes() throws Exception {
        long nodeMemoryBytes = ByteSizeValue.ofGb(1).getBytes();
        DiscoveryNode node1 = buildNode("node-1", nodeMemoryBytes, 1);
        DiscoveryNode node2 = buildNode("node-2", nodeMemoryBytes, 1);

        Map<DiscoveryNode, NodeLoad> nodeLoads = new HashMap<>();
        nodeLoads.put(node1, NodeLoad.builder("node-1").setMaxMemory(nodeMemoryBytes).build());
        nodeLoads.put(node2, NodeLoad.builder("node-2").setMaxMemory(nodeMemoryBytes).build());

        String modelId1 = "model-1";
        StartTrainedModelDeploymentAction.TaskParams taskParams1 = lowPriorityParams(modelId1, ByteSizeValue.ofMb(100).getBytes());
        String modelId2 = "model-2";
        StartTrainedModelDeploymentAction.TaskParams taskParams2 = lowPriorityParams(modelId2, ByteSizeValue.ofMb(100).getBytes());
        TrainedModelAssignmentMetadata currentMetadata = TrainedModelAssignmentMetadata.Builder.empty()
            .addNewAssignment(modelId1, TrainedModelAssignment.Builder.empty(taskParams1))
            .addNewAssignment(modelId2, TrainedModelAssignment.Builder.empty(taskParams2))
            .build();

        TrainedModelAssignmentMetadata result = new TrainedModelAssignmentRebalancer(
            currentMetadata,
            nodeLoads,
            Map.of(List.of(), List.of(node1, node2)),
            Optional.empty()
        ).rebalance().build();

        {
            TrainedModelAssignment assignment = result.getDeploymentAssignment(modelId1);
            assertThat(assignment, is(notNullValue()));
            assertThat(assignment.getAssignmentState(), equalTo(AssignmentState.STARTING));
            assertThat(assignment.getNodeRoutingTable(), is(aMapWithSize(1)));
            assertThat(assignment.getNodeRoutingTable(), hasKey("node-1"));
            assertThat(assignment.getNodeRoutingTable().get("node-1").getCurrentAllocations(), equalTo(1));
            assertThat(assignment.getNodeRoutingTable().get("node-1").getTargetAllocations(), equalTo(1));
            assertThat(assignment.getNodeRoutingTable().get("node-1").getState(), equalTo(RoutingState.STARTING));
            assertThat(assignment.getReason().isPresent(), is(false));
        }
        {
            TrainedModelAssignment assignment = result.getDeploymentAssignment(modelId2);
            assertThat(assignment, is(notNullValue()));
            assertThat(assignment.getAssignmentState(), equalTo(AssignmentState.STARTING));
            assertThat(assignment.getNodeRoutingTable(), is(aMapWithSize(1)));
            assertThat(assignment.getNodeRoutingTable(), hasKey("node-1"));
            assertThat(assignment.getNodeRoutingTable().get("node-1").getCurrentAllocations(), equalTo(1));
            assertThat(assignment.getNodeRoutingTable().get("node-1").getTargetAllocations(), equalTo(1));
            assertThat(assignment.getNodeRoutingTable().get("node-1").getState(), equalTo(RoutingState.STARTING));
            assertThat(assignment.getReason().isPresent(), is(false));
        }
    }

    public void testRebalance_GivenNormalPriorityModelToLoad_EvictsLowPriorityModel() throws Exception {
        long nodeMemoryBytes = ByteSizeValue.ofGb(1).getBytes();
        DiscoveryNode node1 = buildNode("node-1", nodeMemoryBytes, 1);

        Map<DiscoveryNode, NodeLoad> nodeLoads = new HashMap<>();
        nodeLoads.put(node1, NodeLoad.builder("node-1").setMaxMemory(nodeMemoryBytes).build());

        String modelId1 = "model-1";
        StartTrainedModelDeploymentAction.TaskParams taskParams1 = lowPriorityParams(modelId1, ByteSizeValue.ofMb(300).getBytes());
        String modelId2 = "model-2";
        StartTrainedModelDeploymentAction.TaskParams taskParams2 = normalPriorityParams(modelId2, ByteSizeValue.ofMb(300).getBytes(), 1, 1);
        TrainedModelAssignmentMetadata currentMetadata = TrainedModelAssignmentMetadata.Builder.empty()
            .addNewAssignment(
                modelId1,
                TrainedModelAssignment.Builder.empty(taskParams1).addRoutingEntry("node-1", new RoutingInfo(1, 1, RoutingState.STARTED, ""))
            )
            .build();

        TrainedModelAssignmentMetadata result = new TrainedModelAssignmentRebalancer(
            currentMetadata,
            nodeLoads,
            Map.of(List.of(), List.of(node1)),
            Optional.of(taskParams2)
        ).rebalance().build();

        {
            TrainedModelAssignment assignment = result.getDeploymentAssignment(modelId1);
            assertThat(assignment, is(notNullValue()));
            assertThat(assignment.getAssignmentState(), equalTo(AssignmentState.STARTING));
            assertThat(assignment.getNodeRoutingTable(), is(anEmptyMap()));
            assertThat(assignment.getReason().isPresent(), is(true));
            assertThat(
                assignment.getReason().get(),
                containsString("Could not assign (more) allocations on node [node-1]. Reason: This node has insufficient available memory.")
            );
        }
        {
            TrainedModelAssignment assignment = result.getDeploymentAssignment(modelId2);
            assertThat(assignment, is(notNullValue()));
            assertThat(assignment.getAssignmentState(), equalTo(AssignmentState.STARTING));
            assertThat(assignment.getNodeRoutingTable(), is(aMapWithSize(1)));
            assertThat(assignment.getNodeRoutingTable(), hasKey("node-1"));
            assertThat(assignment.getNodeRoutingTable().get("node-1").getCurrentAllocations(), equalTo(1));
            assertThat(assignment.getNodeRoutingTable().get("node-1").getTargetAllocations(), equalTo(1));
            assertThat(assignment.getNodeRoutingTable().get("node-1").getState(), equalTo(RoutingState.STARTING));
            assertThat(assignment.getReason().isPresent(), is(false));
        }
    }

    public void testRebalance_GivenNormalPriorityModelToLoad_AndLowPriorityModelCanStay() throws Exception {
        long nodeMemoryBytes = ByteSizeValue.ofGb(1).getBytes();
        DiscoveryNode node1 = buildNode("node-1", nodeMemoryBytes, 4);
        DiscoveryNode node2 = buildNode("node-2", nodeMemoryBytes, 2);

        Map<DiscoveryNode, NodeLoad> nodeLoads = new HashMap<>();
        nodeLoads.put(node1, NodeLoad.builder("node-1").setMaxMemory(nodeMemoryBytes).build());
        nodeLoads.put(node1, NodeLoad.builder("node-2").setMaxMemory(nodeMemoryBytes).build());

        String modelId1 = "model-1";
        StartTrainedModelDeploymentAction.TaskParams taskParams1 = lowPriorityParams(modelId1, ByteSizeValue.ofMb(1).getBytes());
        String modelId2 = "model-2";
        StartTrainedModelDeploymentAction.TaskParams taskParams2 = normalPriorityParams(modelId2, ByteSizeValue.ofMb(1).getBytes(), 1, 4);
        TrainedModelAssignmentMetadata currentMetadata = TrainedModelAssignmentMetadata.Builder.empty()
            .addNewAssignment(
                modelId1,
                TrainedModelAssignment.Builder.empty(taskParams1).addRoutingEntry("node-1", new RoutingInfo(1, 1, RoutingState.STARTED, ""))
            )
            .build();

        TrainedModelAssignmentMetadata result = new TrainedModelAssignmentRebalancer(
            currentMetadata,
            nodeLoads,
            Map.of(List.of(), List.of(node1, node2)),
            Optional.of(taskParams2)
        ).rebalance().build();

        {
            TrainedModelAssignment assignment = result.getDeploymentAssignment(modelId1);
            assertThat(assignment, is(notNullValue()));
            assertThat(assignment.getAssignmentState(), equalTo(AssignmentState.STARTED));
            assertThat(assignment.getNodeRoutingTable(), is(aMapWithSize(1)));
            assertThat(assignment.getNodeRoutingTable(), hasKey("node-1"));
            assertThat(assignment.getNodeRoutingTable().get("node-1").getCurrentAllocations(), equalTo(1));
            assertThat(assignment.getNodeRoutingTable().get("node-1").getTargetAllocations(), equalTo(1));
            assertThat(assignment.getNodeRoutingTable().get("node-1").getState(), equalTo(RoutingState.STARTED));
            assertThat(assignment.getReason().isPresent(), is(false));
        }
        {
            TrainedModelAssignment assignment = result.getDeploymentAssignment(modelId2);
            assertThat(assignment, is(notNullValue()));
            assertThat(assignment.getAssignmentState(), equalTo(AssignmentState.STARTING));
            assertThat(assignment.getNodeRoutingTable(), is(aMapWithSize(1)));
            assertThat(assignment.getNodeRoutingTable(), hasKey("node-1"));
            assertThat(assignment.getNodeRoutingTable().get("node-1").getCurrentAllocations(), equalTo(1));
            assertThat(assignment.getNodeRoutingTable().get("node-1").getTargetAllocations(), equalTo(1));
            assertThat(assignment.getNodeRoutingTable().get("node-1").getState(), equalTo(RoutingState.STARTING));
            assertThat(assignment.getReason().isPresent(), is(false));
        }
    }

    public void testRebalance_GivenNormalPriorityModelToLoad_AndLowPriorityModelMustRelocate() throws Exception {
        long nodeMemoryBytes = ByteSizeValue.ofGb(1).getBytes();
        DiscoveryNode node1 = buildNode("node-1", nodeMemoryBytes, 4);
        DiscoveryNode node2 = buildNode("node-2", nodeMemoryBytes, 2);

        Map<DiscoveryNode, NodeLoad> nodeLoads = new HashMap<>();
        nodeLoads.put(node1, NodeLoad.builder("node-1").setMaxMemory(nodeMemoryBytes).build());
        nodeLoads.put(node2, NodeLoad.builder("node-2").setMaxMemory(nodeMemoryBytes).build());

        String modelId1 = "model-1";
        StartTrainedModelDeploymentAction.TaskParams taskParams1 = lowPriorityParams(modelId1, ByteSizeValue.ofMb(300).getBytes());
        String modelId2 = "model-2";
        StartTrainedModelDeploymentAction.TaskParams taskParams2 = normalPriorityParams(modelId2, ByteSizeValue.ofMb(300).getBytes(), 1, 4);
        TrainedModelAssignmentMetadata currentMetadata = TrainedModelAssignmentMetadata.Builder.empty()
            .addNewAssignment(
                modelId1,
                TrainedModelAssignment.Builder.empty(taskParams1).addRoutingEntry("node-1", new RoutingInfo(1, 1, RoutingState.STARTED, ""))
            )
            .build();

        TrainedModelAssignmentMetadata result = new TrainedModelAssignmentRebalancer(
            currentMetadata,
            nodeLoads,
            Map.of(List.of(), List.of(node1, node2)),
            Optional.of(taskParams2)
        ).rebalance().build();

        {
            TrainedModelAssignment assignment = result.getDeploymentAssignment(modelId1);
            assertThat(assignment, is(notNullValue()));
            assertThat(assignment.getAssignmentState(), equalTo(AssignmentState.STARTING));
            assertThat(assignment.getNodeRoutingTable(), is(aMapWithSize(1)));
            assertThat(assignment.getNodeRoutingTable(), hasKey("node-2"));
            assertThat(assignment.getNodeRoutingTable().get("node-2").getCurrentAllocations(), equalTo(1));
            assertThat(assignment.getNodeRoutingTable().get("node-2").getTargetAllocations(), equalTo(1));
            assertThat(assignment.getNodeRoutingTable().get("node-2").getState(), equalTo(RoutingState.STARTING));
            assertThat(assignment.getReason().isPresent(), is(false));
        }
        {
            TrainedModelAssignment assignment = result.getDeploymentAssignment(modelId2);
            assertThat(assignment, is(notNullValue()));
            assertThat(assignment.getAssignmentState(), equalTo(AssignmentState.STARTING));
            assertThat(assignment.getNodeRoutingTable(), is(aMapWithSize(1)));
            assertThat(assignment.getNodeRoutingTable(), hasKey("node-1"));
            assertThat(assignment.getNodeRoutingTable().get("node-1").getCurrentAllocations(), equalTo(1));
            assertThat(assignment.getNodeRoutingTable().get("node-1").getTargetAllocations(), equalTo(1));
            assertThat(assignment.getNodeRoutingTable().get("node-1").getState(), equalTo(RoutingState.STARTING));
            assertThat(assignment.getReason().isPresent(), is(false));
        }
    }

    private static StartTrainedModelDeploymentAction.TaskParams lowPriorityParams(String deploymentId, long modelSize) {
        return lowPriorityParams(deploymentId, deploymentId, modelSize);
    }

    private static StartTrainedModelDeploymentAction.TaskParams lowPriorityParams(String deploymentId, String modelId, long modelSize) {
        return new StartTrainedModelDeploymentAction.TaskParams(
            modelId,
            deploymentId,
            modelSize,
            1,
            1,
            1024,
            ByteSizeValue.ofBytes(modelSize),
            Priority.LOW
        );
    }

    private static StartTrainedModelDeploymentAction.TaskParams normalPriorityParams(
        String deploymentId,
        long modelSize,
        int numberOfAllocations,
        int threadsPerAllocation
    ) {
        return normalPriorityParams(deploymentId, deploymentId, modelSize, numberOfAllocations, threadsPerAllocation);
    }

    private static StartTrainedModelDeploymentAction.TaskParams normalPriorityParams(
        String deploymentId,
        String modelId,
        long modelSize,
        int numberOfAllocations,
        int threadsPerAllocation
    ) {
        return new StartTrainedModelDeploymentAction.TaskParams(
            modelId,
            deploymentId,
            modelSize,
            numberOfAllocations,
            threadsPerAllocation,
            1024,
            ByteSizeValue.ofBytes(modelSize),
            Priority.NORMAL
        );
    }

    private static DiscoveryNode buildNode(String name, long nativeMemory, int allocatedProcessors) {
        return TestDiscoveryNode.create(
            name,
            name,
            buildNewFakeTransportAddress(),
            MapBuilder.<String, String>newMapBuilder()
                .put(MachineLearning.MACHINE_MEMORY_NODE_ATTR, String.valueOf(nativeMemory))
                .put(MachineLearning.MAX_JVM_SIZE_NODE_ATTR, String.valueOf(10))
                .put(MachineLearning.ALLOCATED_PROCESSORS_NODE_ATTR, String.valueOf(allocatedProcessors))
                .map(),
            DiscoveryNodeRole.roles()
        );
    }
}
