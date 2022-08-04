/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.assignment;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentTaskParamsTests;
import org.elasticsearch.xpack.core.ml.stats.CountAccumulator;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class TrainedModelAssignmentTests extends AbstractSerializingTestCase<TrainedModelAssignment> {

    public static TrainedModelAssignment randomInstance() {
        TrainedModelAssignment.Builder builder = TrainedModelAssignment.Builder.empty(randomParams());
        List<String> nodes = Stream.generate(() -> randomAlphaOfLength(10)).limit(randomInt(5)).toList();
        for (String node : nodes) {
            builder.addRoutingEntry(node, RoutingInfoTests.randomInstance());
        }
        builder.setAssignmentState(randomFrom(AssignmentState.values()));
        if (randomBoolean()) {
            builder.setReason(randomAlphaOfLength(10));
        }
        return builder.build();
    }

    @Override
    protected TrainedModelAssignment doParseInstance(XContentParser parser) throws IOException {
        return TrainedModelAssignment.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<TrainedModelAssignment> instanceReader() {
        return TrainedModelAssignment::new;
    }

    @Override
    protected TrainedModelAssignment createTestInstance() {
        return randomInstance();
    }

    public void testBuilderAddingExistingRoute() {
        TrainedModelAssignment.Builder assignment = TrainedModelAssignment.Builder.empty(randomParams());
        String addingNode = "new-node";
        assignment.addRoutingEntry(addingNode, RoutingInfoTests.randomInstance());

        expectThrows(ResourceAlreadyExistsException.class, () -> assignment.addRoutingEntry("new-node", RoutingInfoTests.randomInstance()));
    }

    public void testBuilderUpdatingMissingRoute() {
        TrainedModelAssignment.Builder assignment = TrainedModelAssignment.Builder.empty(randomParams());
        String addingNode = "new-node";
        expectThrows(
            ResourceNotFoundException.class,
            () -> assignment.updateExistingRoutingEntry(addingNode, RoutingInfoTests.randomInstance())
        );
    }

    public void testGetStartedNodes() {
        String startedNode1 = "started-node-1";
        String startedNode2 = "started-node-2";
        String nodeInAnotherState1 = "another-state-node-1";
        String nodeInAnotherState2 = "another-state-node-2";
        TrainedModelAssignment allocation = TrainedModelAssignment.Builder.empty(randomParams())
            .addRoutingEntry(startedNode1, RoutingInfoTests.randomInstance(RoutingState.STARTED))
            .addRoutingEntry(startedNode2, RoutingInfoTests.randomInstance(RoutingState.STARTED))
            .addRoutingEntry(
                nodeInAnotherState1,
                RoutingInfoTests.randomInstance(
                    randomFrom(RoutingState.STARTING, RoutingState.STOPPING, RoutingState.STOPPED, RoutingState.FAILED)
                )
            )
            .addRoutingEntry(
                nodeInAnotherState2,
                RoutingInfoTests.randomInstance(
                    randomFrom(RoutingState.STARTING, RoutingState.STOPPING, RoutingState.STOPPED, RoutingState.FAILED)
                )
            )
            .build();
        assertThat(allocation.getStartedNodes(), arrayContainingInAnyOrder(startedNode1, startedNode2));
    }

    public void testCalculateAllocationStatus_GivenNoAllocations() {
        assertThat(
            TrainedModelAssignment.Builder.empty(randomTaskParams(5)).build().calculateAllocationStatus().get(),
            equalTo(new AllocationStatus(0, 5))
        );
    }

    public void testCalculateAllocationStatus_GivenStoppingAssignment() {
        TrainedModelAssignment.Builder builder = TrainedModelAssignment.Builder.empty(randomTaskParams(5));
        builder.addRoutingEntry("node-1", new RoutingInfo(1, 2, RoutingState.STARTED, ""));
        builder.addRoutingEntry("node-2", new RoutingInfo(2, 1, RoutingState.STARTED, ""));
        assertThat(builder.stopAssignment("test").build().calculateAllocationStatus().isEmpty(), is(true));
    }

    public void testCalculateAllocationStatus_GivenPartiallyAllocated() {
        TrainedModelAssignment.Builder builder = TrainedModelAssignment.Builder.empty(randomTaskParams(5));
        builder.addRoutingEntry("node-1", new RoutingInfo(1, 2, RoutingState.STARTED, ""));
        builder.addRoutingEntry("node-2", new RoutingInfo(2, 1, RoutingState.STARTED, ""));
        builder.addRoutingEntry("node-3", new RoutingInfo(3, 3, RoutingState.STARTING, ""));
        assertThat(builder.build().calculateAllocationStatus().get(), equalTo(new AllocationStatus(3, 5)));
    }

    public void testCalculateAllocationStatus_GivenFullyAllocated() {
        TrainedModelAssignment.Builder builder = TrainedModelAssignment.Builder.empty(randomTaskParams(5));
        builder.addRoutingEntry("node-1", new RoutingInfo(4, 4, RoutingState.STARTED, ""));
        builder.addRoutingEntry("node-2", new RoutingInfo(1, 1, RoutingState.STARTED, ""));
        assertThat(builder.build().calculateAllocationStatus().get(), equalTo(new AllocationStatus(5, 5)));
    }

    public void testCalculateAssignmentState_GivenNoStartedAssignments() {
        TrainedModelAssignment.Builder builder = TrainedModelAssignment.Builder.empty(randomTaskParams(5));
        builder.addRoutingEntry("node-1", new RoutingInfo(4, 4, RoutingState.STARTING, ""));
        builder.addRoutingEntry("node-2", new RoutingInfo(1, 1, RoutingState.STARTING, ""));
        assertThat(builder.calculateAssignmentState(), equalTo(AssignmentState.STARTING));
    }

    public void testCalculateAssignmentState_GivenOneStartedAssignment() {
        TrainedModelAssignment.Builder builder = TrainedModelAssignment.Builder.empty(randomTaskParams(5));
        builder.addRoutingEntry("node-1", new RoutingInfo(4, 4, RoutingState.STARTING, ""));
        builder.addRoutingEntry("node-2", new RoutingInfo(1, 1, RoutingState.STARTED, ""));
        assertThat(builder.calculateAssignmentState(), equalTo(AssignmentState.STARTED));
    }

    public void testCalculateAndSetAssignmentState_GivenStoppingAssignment() {
        TrainedModelAssignment.Builder builder = TrainedModelAssignment.Builder.empty(randomTaskParams(5));
        builder.addRoutingEntry("node-1", new RoutingInfo(4, 4, RoutingState.STARTED, ""));
        builder.addRoutingEntry("node-2", new RoutingInfo(1, 1, RoutingState.STARTED, ""));
        assertThat(
            builder.stopAssignment("test").calculateAndSetAssignmentState().build().getAssignmentState(),
            equalTo(AssignmentState.STOPPING)
        );
    }

    public void testSelectRandomStartedNodeWeighedOnAllocations_GivenNoStartedAllocations() {
        TrainedModelAssignment.Builder builder = TrainedModelAssignment.Builder.empty(randomTaskParams(5));
        builder.addRoutingEntry("node-1", new RoutingInfo(4, 4, RoutingState.STARTING, ""));
        builder.addRoutingEntry("node-2", new RoutingInfo(1, 1, RoutingState.STOPPED, ""));
        TrainedModelAssignment assignment = builder.build();

        assertThat(assignment.selectRandomStartedNodeWeighedOnAllocations().isEmpty(), is(true));
    }

    public void testSelectRandomStartedNodeWeighedOnAllocations_GivenSingleStartedNode() {
        TrainedModelAssignment.Builder builder = TrainedModelAssignment.Builder.empty(randomTaskParams(5));
        builder.addRoutingEntry("node-1", new RoutingInfo(4, 4, RoutingState.STARTED, ""));
        TrainedModelAssignment assignment = builder.build();

        Optional<String> node = assignment.selectRandomStartedNodeWeighedOnAllocations();

        assertThat(node.isPresent(), is(true));
        assertThat(node.get(), equalTo("node-1"));
    }

    public void testSelectRandomStartedNodeWeighedOnAllocations_GivenMultipleStartedNodes() {
        TrainedModelAssignment.Builder builder = TrainedModelAssignment.Builder.empty(randomTaskParams(6));
        builder.addRoutingEntry("node-1", new RoutingInfo(1, 1, RoutingState.STARTED, ""));
        builder.addRoutingEntry("node-2", new RoutingInfo(2, 2, RoutingState.STARTED, ""));
        builder.addRoutingEntry("node-3", new RoutingInfo(3, 3, RoutingState.STARTED, ""));
        TrainedModelAssignment assignment = builder.build();

        final long selectionCount = 10000;
        final CountAccumulator countsPerNodeAccumulator = new CountAccumulator();
        for (int i = 0; i < selectionCount; i++) {
            Optional<String> node = assignment.selectRandomStartedNodeWeighedOnAllocations();
            assertThat(node.isPresent(), is(true));
            countsPerNodeAccumulator.add(node.get(), 1L);
        }

        Map<String, Long> countsPerNode = countsPerNodeAccumulator.asMap();
        assertThat(countsPerNode.keySet(), contains("node-1", "node-2", "node-3"));
        assertThat(countsPerNode.get("node-1") + countsPerNode.get("node-2") + countsPerNode.get("node-3"), equalTo(selectionCount));

        assertValueWithinPercentageOfExpectedRatio(countsPerNode.get("node-1"), selectionCount, 1.0 / 6.0, 0.2);
        assertValueWithinPercentageOfExpectedRatio(countsPerNode.get("node-2"), selectionCount, 2.0 / 6.0, 0.2);
        assertValueWithinPercentageOfExpectedRatio(countsPerNode.get("node-3"), selectionCount, 3.0 / 6.0, 0.2);
    }

    public void testSelectRandomStartedNodeWeighedOnAllocations_GivenMultipleStartedNodesWithZeroAllocations() {
        TrainedModelAssignment.Builder builder = TrainedModelAssignment.Builder.empty(randomTaskParams(6));
        builder.addRoutingEntry("node-1", new RoutingInfo(0, 0, RoutingState.STARTED, ""));
        builder.addRoutingEntry("node-2", new RoutingInfo(0, 0, RoutingState.STARTED, ""));
        builder.addRoutingEntry("node-3", new RoutingInfo(0, 0, RoutingState.STARTED, ""));
        TrainedModelAssignment assignment = builder.build();
        final long selectionCount = 1000;
        Set<String> selectedNodes = new HashSet<>();
        for (int i = 0; i < selectionCount; i++) {
            Optional<String> selectedNode = assignment.selectRandomStartedNodeWeighedOnAllocations();
            assertThat(selectedNode.isPresent(), is(true));
            selectedNodes.add(selectedNode.get());
        }

        assertThat(selectedNodes, contains("node-1", "node-2", "node-3"));
    }

    public void testIsSatisfied_GivenEnoughAllocations() {
        TrainedModelAssignment.Builder builder = TrainedModelAssignment.Builder.empty(randomTaskParams(6));
        builder.addRoutingEntry("node-1", new RoutingInfo(1, 1, RoutingState.STARTED, ""));
        builder.addRoutingEntry("node-2", new RoutingInfo(2, 2, RoutingState.STARTED, ""));
        builder.addRoutingEntry("node-3", new RoutingInfo(3, 3, RoutingState.STARTED, ""));
        TrainedModelAssignment assignment = builder.build();
        assertThat(assignment.isSatisfied(Sets.newHashSet("node-1", "node-2", "node-3")), is(true));
    }

    public void testIsSatisfied_GivenEnoughAllocations_ButOneNodeIsNotAssignable() {
        TrainedModelAssignment.Builder builder = TrainedModelAssignment.Builder.empty(randomTaskParams(6));
        builder.addRoutingEntry("node-1", new RoutingInfo(1, 1, RoutingState.STARTED, ""));
        builder.addRoutingEntry("node-2", new RoutingInfo(2, 2, RoutingState.STARTED, ""));
        builder.addRoutingEntry("node-3", new RoutingInfo(3, 3, RoutingState.STARTED, ""));
        TrainedModelAssignment assignment = builder.build();
        assertThat(assignment.isSatisfied(Sets.newHashSet("node-2", "node-3")), is(false));
    }

    public void testIsSatisfied_GivenEnoughAllocations_ButOneNodeIsNeitherStartingNorStarted() {
        TrainedModelAssignment.Builder builder = TrainedModelAssignment.Builder.empty(randomTaskParams(6));
        builder.addRoutingEntry(
            "node-1",
            new RoutingInfo(1, 1, randomFrom(RoutingState.FAILED, RoutingState.STOPPING, RoutingState.STOPPED), "")
        );
        builder.addRoutingEntry("node-2", new RoutingInfo(2, 2, RoutingState.STARTED, ""));
        builder.addRoutingEntry("node-3", new RoutingInfo(3, 3, RoutingState.STARTED, ""));
        TrainedModelAssignment assignment = builder.build();
        assertThat(assignment.isSatisfied(Sets.newHashSet("node-1", "node-2", "node-3")), is(false));
    }

    public void testIsSatisfied_GivenNotEnoughAllocations() {
        TrainedModelAssignment.Builder builder = TrainedModelAssignment.Builder.empty(randomTaskParams(7));
        builder.addRoutingEntry("node-1", new RoutingInfo(1, 1, RoutingState.STARTED, ""));
        builder.addRoutingEntry("node-2", new RoutingInfo(2, 2, RoutingState.STARTED, ""));
        builder.addRoutingEntry("node-3", new RoutingInfo(3, 3, RoutingState.STARTED, ""));
        TrainedModelAssignment assignment = builder.build();
        assertThat(assignment.isSatisfied(Sets.newHashSet("node-1", "node-2", "node-3")), is(false));
    }

    public void testMaxAssignedAllocations() {
        TrainedModelAssignment assignment = TrainedModelAssignment.Builder.empty(randomTaskParams(10))
            .addRoutingEntry("node-1", new RoutingInfo(1, 2, RoutingState.STARTED, ""))
            .addRoutingEntry("node-2", new RoutingInfo(2, 1, RoutingState.STARTED, ""))
            .addRoutingEntry("node-3", new RoutingInfo(3, 3, RoutingState.STARTING, ""))
            .build();
        assertThat(assignment.getMaxAssignedAllocations(), equalTo(6));

        TrainedModelAssignment assignmentAfterRemovingNode = TrainedModelAssignment.Builder.fromAssignment(assignment)
            .removeRoutingEntry("node-1")
            .build();
        assertThat(assignmentAfterRemovingNode.getMaxAssignedAllocations(), equalTo(6));
        assertThat(assignmentAfterRemovingNode.totalCurrentAllocations(), equalTo(5));
    }

    private void assertValueWithinPercentageOfExpectedRatio(long value, long totalCount, double ratio, double tolerance) {
        double expected = totalCount * ratio;
        double lowerBound = (1.0 - tolerance) * expected;
        double upperBound = (1.0 + tolerance) * expected;
        logger.info("Checked that: {} <= {} <= {}", lowerBound, value, upperBound);
        assertThat((double) value, greaterThanOrEqualTo(lowerBound));
        assertThat((double) value, lessThanOrEqualTo(upperBound));
    }

    private static StartTrainedModelDeploymentAction.TaskParams randomTaskParams(int numberOfAllocations) {
        long modelSize = randomNonNegativeLong();
        return new StartTrainedModelDeploymentAction.TaskParams(
            randomAlphaOfLength(10),
            modelSize,
            randomIntBetween(1, 8),
            numberOfAllocations,
            randomIntBetween(1, 10000),
            randomBoolean() ? null : ByteSizeValue.ofBytes(randomLongBetween(0, modelSize + 1))
        );
    }

    private static StartTrainedModelDeploymentAction.TaskParams randomParams() {
        return StartTrainedModelDeploymentTaskParamsTests.createRandom();
    }
}
