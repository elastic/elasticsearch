/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.assignment.planning;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.inference.assignment.Priority;
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingInfo;
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingState;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignment;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;

public class AllocationReducerTests extends ESTestCase {

    public void testReduceTo_ValueEqualToCurrentAllocations() {
        Map<List<String>, Collection<DiscoveryNode>> nodesByZone = Map.of(List.of("z"), List.of(buildNode("n")));
        TrainedModelAssignment assignment = createAssignment("d", "m", 2, Map.of("n", 2));
        expectThrows(IllegalArgumentException.class, () -> new AllocationReducer(assignment, nodesByZone).reduceTo(2));
    }

    public void testReduceTo_ValueLargerThanCurrentAllocations() {
        Map<List<String>, Collection<DiscoveryNode>> nodesByZone = Map.of(List.of("z"), List.of(buildNode("n")));
        TrainedModelAssignment assignment = createAssignment("d", "m", 2, Map.of("n", 2));
        expectThrows(IllegalArgumentException.class, () -> new AllocationReducer(assignment, nodesByZone).reduceTo(3));
    }

    public void testReduceTo_GivenOneZone_OneAssignment_ReductionByOne() {
        Map<List<String>, Collection<DiscoveryNode>> nodesByZone = Map.of(List.of("z"), List.of(buildNode("n")));
        TrainedModelAssignment assignment = createAssignment("d", "m", 2, Map.of("n", 2));

        TrainedModelAssignment updatedAssignment = new AllocationReducer(assignment, nodesByZone).reduceTo(1).build();

        assertThat(updatedAssignment.getTaskParams().getNumberOfAllocations(), equalTo(1));

        Map<String, RoutingInfo> routingTable = updatedAssignment.getNodeRoutingTable();
        assertThat(routingTable, aMapWithSize(1));
        assertThat(routingTable, hasKey("n"));
        assertThat(routingTable.get("n").getTargetAllocations(), equalTo(1));
    }

    public void testReduceTo_GivenOneZone_OneAssignment_ReductionByMany() {
        Map<List<String>, Collection<DiscoveryNode>> nodesByZone = Map.of(List.of("z"), List.of(buildNode("n")));
        TrainedModelAssignment assignment = createAssignment("d", "m", 5, Map.of("n", 5));

        TrainedModelAssignment updatedAssignment = new AllocationReducer(assignment, nodesByZone).reduceTo(2).build();

        assertThat(updatedAssignment.getTaskParams().getNumberOfAllocations(), equalTo(2));

        Map<String, RoutingInfo> routingTable = updatedAssignment.getNodeRoutingTable();
        assertThat(routingTable, aMapWithSize(1));
        assertThat(routingTable, hasKey("n"));
        assertThat(routingTable.get("n").getTargetAllocations(), equalTo(2));
    }

    public void testReduceTo_GivenOneZone_MultipleAssignments_RemovableAssignments() {
        Map<List<String>, Collection<DiscoveryNode>> nodesByZone = Map.of(
            List.of("z"),
            List.of(buildNode("n_1"), buildNode("n_2"), buildNode("n_3"))
        );
        TrainedModelAssignment assignment = createAssignment("d", "m", 6, Map.of("n_1", 3, "n_2", 2, "n_3", 1));

        TrainedModelAssignment updatedAssignment = new AllocationReducer(assignment, nodesByZone).reduceTo(3).build();

        assertThat(updatedAssignment.getTaskParams().getNumberOfAllocations(), equalTo(3));

        Map<String, RoutingInfo> routingTable = updatedAssignment.getNodeRoutingTable();
        assertThat(routingTable, aMapWithSize(1));
        assertThat(routingTable, hasKey("n_1"));
        assertThat(routingTable.get("n_1").getTargetAllocations(), equalTo(3));
    }

    public void testReduceTo_GivenOneZone_MultipleAssignments_NonRemovableAssignments() {
        Map<List<String>, Collection<DiscoveryNode>> nodesByZone = Map.of(
            List.of("z"),
            List.of(buildNode("n_1"), buildNode("n_2"), buildNode("n_3"))
        );
        TrainedModelAssignment assignment = createAssignment("d", "m", 6, Map.of("n_1", 2, "n_2", 2, "n_3", 2));

        TrainedModelAssignment updatedAssignment = new AllocationReducer(assignment, nodesByZone).reduceTo(5).build();

        assertThat(updatedAssignment.getTaskParams().getNumberOfAllocations(), equalTo(5));
        assertThat(updatedAssignment.totalTargetAllocations(), equalTo(5));

        Map<String, RoutingInfo> routingTable = updatedAssignment.getNodeRoutingTable();
        assertThat(routingTable, aMapWithSize(3));
        assertThat(routingTable, hasKey("n_1"));
        assertThat(routingTable, hasKey("n_2"));
        assertThat(routingTable, hasKey("n_3"));
    }

    public void testReduceTo_GivenTwoZones_RemovableAssignments() {
        Map<List<String>, Collection<DiscoveryNode>> nodesByZone = Map.of(
            List.of("z_1"),
            List.of(buildNode("n_1"), buildNode("n_2")),
            List.of("z_2"),
            List.of(buildNode("n_3"))
        );
        TrainedModelAssignment assignment = createAssignment("d", "m", 5, Map.of("n_1", 3, "n_2", 1, "n_3", 1));

        TrainedModelAssignment updatedAssignment = new AllocationReducer(assignment, nodesByZone).reduceTo(4).build();

        assertThat(updatedAssignment.getTaskParams().getNumberOfAllocations(), equalTo(4));

        Map<String, RoutingInfo> routingTable = updatedAssignment.getNodeRoutingTable();
        assertThat(routingTable, aMapWithSize(2));
        assertThat(routingTable, hasKey("n_1"));
        assertThat(routingTable.get("n_1").getTargetAllocations(), equalTo(3));
        assertThat(routingTable, hasKey("n_3"));
        assertThat(routingTable.get("n_3").getTargetAllocations(), equalTo(1));
    }

    public void testReduceTo_GivenTwoZones_NonRemovableAssignments() {
        Map<List<String>, Collection<DiscoveryNode>> nodesByZone = Map.of(
            List.of("z_1"),
            List.of(buildNode("n_1")),
            List.of("z_2"),
            List.of(buildNode("n_2"))
        );
        TrainedModelAssignment assignment = createAssignment("d", "m", 6, Map.of("n_1", 3, "n_2", 3));

        TrainedModelAssignment updatedAssignment = new AllocationReducer(assignment, nodesByZone).reduceTo(4).build();

        assertThat(updatedAssignment.getTaskParams().getNumberOfAllocations(), equalTo(4));

        Map<String, RoutingInfo> routingTable = updatedAssignment.getNodeRoutingTable();
        assertThat(routingTable, aMapWithSize(2));
        assertThat(routingTable, hasKey("n_1"));
        assertThat(routingTable.get("n_1").getTargetAllocations(), equalTo(2));
        assertThat(routingTable, hasKey("n_2"));
        assertThat(routingTable.get("n_2").getTargetAllocations(), equalTo(2));
    }

    public void testReduceTo_GivenTwoZones_WithSameAssignmentsOfOneAllocationEach() {
        Map<List<String>, Collection<DiscoveryNode>> nodesByZone = Map.of(
            List.of("z_1"),
            List.of(buildNode("n_1")),
            List.of("z_2"),
            List.of(buildNode("n_2"))
        );
        TrainedModelAssignment assignment = createAssignment("d", "m", 2, Map.of("n_1", 1, "n_2", 1));

        TrainedModelAssignment updatedAssignment = new AllocationReducer(assignment, nodesByZone).reduceTo(1).build();

        assertThat(updatedAssignment.getTaskParams().getNumberOfAllocations(), equalTo(1));

        Map<String, RoutingInfo> routingTable = updatedAssignment.getNodeRoutingTable();
        assertThat(routingTable, aMapWithSize(1));
        assertThat(routingTable, hasKey("n_1"));
        assertThat(routingTable.get(routingTable.keySet().iterator().next()).getTargetAllocations(), equalTo(1));
    }

    private static TrainedModelAssignment createAssignment(
        String deploymentId,
        String modelId,
        int numberOfAllocations,
        Map<String, Integer> allocationsByNode
    ) {
        TrainedModelAssignment.Builder builder = TrainedModelAssignment.Builder.empty(
            new StartTrainedModelDeploymentAction.TaskParams(
                modelId,
                deploymentId,
                randomNonNegativeLong(),
                numberOfAllocations,
                randomIntBetween(1, 16),
                1024,
                null,
                Priority.NORMAL,
                randomNonNegativeLong(),
                randomNonNegativeLong()
            ),
            null
        );
        allocationsByNode.entrySet()
            .stream()
            .forEach(
                e -> builder.addRoutingEntry(
                    e.getKey(),
                    new RoutingInfo(randomIntBetween(1, e.getValue()), e.getValue(), RoutingState.STARTED, "")
                )
            );
        return builder.build();
    }

    private static DiscoveryNode buildNode(String nodeId) {
        return DiscoveryNodeUtils.create(nodeId, nodeId);
    }
}
