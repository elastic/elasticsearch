/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.assignment;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentTaskParamsTests;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class TrainedModelAssignmentTests extends AbstractSerializingTestCase<TrainedModelAssignment> {

    public static TrainedModelAssignment randomInstance() {
        TrainedModelAssignment.Builder builder = TrainedModelAssignment.Builder.empty(randomParams());
        List<String> nodes = Stream.generate(() -> randomAlphaOfLength(10)).limit(randomInt(5)).collect(Collectors.toList());
        for (String node : nodes) {
            if (randomBoolean()) {
                builder.addNewFailedRoutingEntry(node, randomAlphaOfLength(10));
            } else {
                builder.addNewRoutingEntry(node);
            }
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

    public void testBuilderChanged() {
        TrainedModelAssignment original = randomInstance();
        TrainedModelAssignment.Builder builder = TrainedModelAssignment.Builder.fromAssignment(original);
        assertThat(builder.isChanged(), is(false));
        String addingNode = "foo";

        assertUnchanged(builder, b -> b.removeRoutingEntry(addingNode));

        if (randomBoolean()) {
            builder.addNewRoutingEntry(addingNode);
        } else {
            builder.addNewFailedRoutingEntry(addingNode, "test failed");
        }
        assertThat(builder.isChanged(), is(true));

        TrainedModelAssignment.Builder builderWithNode = TrainedModelAssignment.Builder.fromAssignment(builder.build());
        assertThat(builderWithNode.isChanged(), is(false));

        builderWithNode.removeRoutingEntry(addingNode);
        assertThat(builderWithNode.isChanged(), is(true));
    }

    public void testBuilderAddingExistingRoute() {
        TrainedModelAssignment original = randomInstance();
        TrainedModelAssignment.Builder builder = TrainedModelAssignment.Builder.fromAssignment(original);
        String addingNode = "new-node";
        if (randomBoolean()) {
            builder.addNewRoutingEntry(addingNode);
        } else {
            builder.addNewFailedRoutingEntry(addingNode, "test failed");
        }
        expectThrows(ResourceAlreadyExistsException.class, () -> builder.addNewFailedRoutingEntry("new-node", "anything"));
        expectThrows(ResourceAlreadyExistsException.class, () -> builder.addNewRoutingEntry("new-node"));
    }

    public void testBuilderUpdatingMissingRoute() {
        TrainedModelAssignment original = randomInstance();
        TrainedModelAssignment.Builder builder = TrainedModelAssignment.Builder.fromAssignment(original);
        String addingNode = "new-node";
        expectThrows(
            ResourceNotFoundException.class,
            () -> builder.updateExistingRoutingEntry(addingNode, RoutingStateAndReasonTests.randomInstance())
        );
    }

    public void testGetStartedNodes() {
        String startedNode1 = "started-node-1";
        String startedNode2 = "started-node-2";
        String nodeInAnotherState1 = "another-state-node-1";
        String nodeInAnotherState2 = "another-state-node-2";
        TrainedModelAssignment allocation = TrainedModelAssignment.Builder.empty(randomParams())
            .addNewRoutingEntry(startedNode1)
            .addNewRoutingEntry(startedNode2)
            .addNewRoutingEntry(nodeInAnotherState1)
            .addNewRoutingEntry(nodeInAnotherState2)
            .updateExistingRoutingEntry(startedNode1, new RoutingStateAndReason(RoutingState.STARTED, ""))
            .updateExistingRoutingEntry(startedNode2, new RoutingStateAndReason(RoutingState.STARTED, ""))
            .updateExistingRoutingEntry(
                nodeInAnotherState1,
                new RoutingStateAndReason(
                    randomFrom(RoutingState.STARTING, RoutingState.FAILED, RoutingState.STOPPED, RoutingState.STOPPING),
                    randomAlphaOfLength(10)
                )
            )
            .updateExistingRoutingEntry(
                nodeInAnotherState2,
                new RoutingStateAndReason(
                    randomFrom(RoutingState.STARTING, RoutingState.FAILED, RoutingState.STOPPED, RoutingState.STOPPING),
                    randomAlphaOfLength(10)
                )
            )
            .build();
        assertThat(allocation.getStartedNodes(), arrayContainingInAnyOrder(startedNode1, startedNode2));
    }

    public void testCalculateAllocationStatus() {
        List<DiscoveryNode> nodes = Stream.generate(TrainedModelAssignmentTests::buildNode).limit(5).collect(Collectors.toList());
        final boolean includeNodes = randomBoolean();
        assertThat(
            TrainedModelAssignment.Builder.empty(randomParams())
                .build()
                .calculateAllocationStatus(includeNodes ? nodes : List.of())
                .orElseThrow(),
            equalTo(new AllocationStatus(0, includeNodes ? 5 : 0))
        );
        assertThat(
            TrainedModelAssignment.Builder.empty(randomParams())
                .stopAssignment("test")
                .build()
                .calculateAllocationStatus(includeNodes ? nodes : List.of())
                .isPresent(),
            is(false)
        );

        {
            TrainedModelAssignment.Builder builder = TrainedModelAssignment.Builder.empty(randomParams());
            int count = randomInt(4);
            for (int i = 0; i < count; i++) {
                builder.addRoutingEntry(nodes.get(i).getId(), RoutingState.STARTED);
            }
            assertThat(builder.build().calculateAllocationStatus(nodes).orElseThrow(), equalTo(new AllocationStatus(count, 5)));
        }
        {
            TrainedModelAssignment.Builder builder = TrainedModelAssignment.Builder.empty(randomParams());
            for (DiscoveryNode node : nodes) {
                builder.addRoutingEntry(
                    node.getId(),
                    randomFrom(RoutingState.FAILED, RoutingState.STOPPED, RoutingState.STARTING, RoutingState.STOPPING)
                );
            }
            int count = randomIntBetween(1, 4);
            for (int i = 0; i < count; i++) {
                builder.addRoutingEntry(nodes.get(i).getId(), RoutingState.STARTED);
            }
            assertThat(builder.build().calculateAllocationStatus(nodes).orElseThrow(), equalTo(new AllocationStatus(count, 5)));
        }
        {
            TrainedModelAssignment.Builder builder = TrainedModelAssignment.Builder.empty(randomParams());
            for (DiscoveryNode node : nodes) {
                builder.addRoutingEntry(node.getId(), RoutingState.STARTED);
            }
            assertThat(builder.build().calculateAllocationStatus(nodes).orElseThrow(), equalTo(new AllocationStatus(5, 5)));
        }
    }

    public void testCalculateAllocationState() {
        List<DiscoveryNode> nodes = Stream.generate(TrainedModelAssignmentTests::buildNode).limit(5).collect(Collectors.toList());
        assertThat(TrainedModelAssignment.Builder.empty(randomParams()).calculateAssignmentState(), equalTo(AssignmentState.STARTING));
        assertThat(
            TrainedModelAssignment.Builder.empty(randomParams()).stopAssignment("test").calculateAssignmentState(),
            equalTo(AssignmentState.STOPPING)
        );

        {
            TrainedModelAssignment.Builder builder = TrainedModelAssignment.Builder.empty(randomParams());
            int count = randomInt(4);
            for (int i = 0; i < count; i++) {
                builder.addRoutingEntry(
                    nodes.get(i).getId(),
                    randomFrom(RoutingState.FAILED, RoutingState.STOPPED, RoutingState.STARTING, RoutingState.STOPPING)
                );
            }
            assertThat(builder.calculateAssignmentState(), equalTo(AssignmentState.STARTING));
        }
        {
            TrainedModelAssignment.Builder builder = TrainedModelAssignment.Builder.empty(randomParams());
            for (DiscoveryNode node : nodes) {
                builder.addRoutingEntry(
                    node.getId(),
                    randomFrom(RoutingState.FAILED, RoutingState.STOPPED, RoutingState.STARTING, RoutingState.STOPPING)
                );
            }
            int count = randomIntBetween(1, 4);
            for (int i = 0; i < count; i++) {
                builder.addRoutingEntry(nodes.get(i).getId(), RoutingState.STARTED);
            }
            assertThat(builder.calculateAssignmentState(), equalTo(AssignmentState.STARTED));
        }
        {
            TrainedModelAssignment.Builder builder = TrainedModelAssignment.Builder.empty(randomParams());
            for (DiscoveryNode node : nodes) {
                builder.addRoutingEntry(node.getId(), RoutingState.STARTED);
            }
            assertThat(builder.calculateAssignmentState(), equalTo(AssignmentState.STARTED));
        }
    }

    private static DiscoveryNode buildNode() {
        return new DiscoveryNode(
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            buildNewFakeTransportAddress(),
            Map.of(),
            DiscoveryNodeRole.roles(),
            Version.CURRENT
        );
    }

    private static StartTrainedModelDeploymentAction.TaskParams randomParams() {
        return StartTrainedModelDeploymentTaskParamsTests.createRandom();
    }

    private static void assertUnchanged(
        TrainedModelAssignment.Builder builder,
        Function<TrainedModelAssignment.Builder, TrainedModelAssignment.Builder> function
    ) {
        function.apply(builder);
        assertThat(builder.isChanged(), is(false));
    }

}
