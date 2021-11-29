/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.allocation;

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

public class TrainedModelAllocationTests extends AbstractSerializingTestCase<TrainedModelAllocation> {

    public static TrainedModelAllocation randomInstance() {
        TrainedModelAllocation.Builder builder = TrainedModelAllocation.Builder.empty(randomParams());
        List<String> nodes = Stream.generate(() -> randomAlphaOfLength(10)).limit(randomInt(5)).collect(Collectors.toList());
        for (String node : nodes) {
            if (randomBoolean()) {
                builder.addNewFailedRoutingEntry(node, randomAlphaOfLength(10));
            } else {
                builder.addNewRoutingEntry(node);
            }
        }
        builder.setAllocationState(randomFrom(AllocationState.values()));
        if (randomBoolean()) {
            builder.setReason(randomAlphaOfLength(10));
        }
        return builder.build();
    }

    @Override
    protected TrainedModelAllocation doParseInstance(XContentParser parser) throws IOException {
        return TrainedModelAllocation.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<TrainedModelAllocation> instanceReader() {
        return TrainedModelAllocation::new;
    }

    @Override
    protected TrainedModelAllocation createTestInstance() {
        return randomInstance();
    }

    public void testBuilderChanged() {
        TrainedModelAllocation original = randomInstance();
        TrainedModelAllocation.Builder builder = TrainedModelAllocation.Builder.fromAllocation(original);
        assertThat(builder.isChanged(), is(false));
        String addingNode = "foo";

        assertUnchanged(builder, b -> b.removeRoutingEntry(addingNode));

        if (randomBoolean()) {
            builder.addNewRoutingEntry(addingNode);
        } else {
            builder.addNewFailedRoutingEntry(addingNode, "test failed");
        }
        assertThat(builder.isChanged(), is(true));

        TrainedModelAllocation.Builder builderWithNode = TrainedModelAllocation.Builder.fromAllocation(builder.build());
        assertThat(builderWithNode.isChanged(), is(false));

        builderWithNode.removeRoutingEntry(addingNode);
        assertThat(builderWithNode.isChanged(), is(true));
    }

    public void testBuilderAddingExistingRoute() {
        TrainedModelAllocation original = randomInstance();
        TrainedModelAllocation.Builder builder = TrainedModelAllocation.Builder.fromAllocation(original);
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
        TrainedModelAllocation original = randomInstance();
        TrainedModelAllocation.Builder builder = TrainedModelAllocation.Builder.fromAllocation(original);
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
        TrainedModelAllocation allocation = TrainedModelAllocation.Builder.empty(randomParams())
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
        List<DiscoveryNode> nodes = Stream.generate(TrainedModelAllocationTests::buildNode).limit(5).collect(Collectors.toList());
        final boolean includeNodes = randomBoolean();
        assertThat(
            TrainedModelAllocation.Builder.empty(randomParams())
                .build()
                .calculateAllocationStatus(includeNodes ? nodes : List.of())
                .orElseThrow(),
            equalTo(new AllocationStatus(0, includeNodes ? 5 : 0))
        );
        assertThat(
            TrainedModelAllocation.Builder.empty(randomParams())
                .stopAllocation("test")
                .build()
                .calculateAllocationStatus(includeNodes ? nodes : List.of())
                .isPresent(),
            is(false)
        );

        {
            TrainedModelAllocation.Builder builder = TrainedModelAllocation.Builder.empty(randomParams());
            int count = randomInt(4);
            for (int i = 0; i < count; i++) {
                builder.addRoutingEntry(nodes.get(i).getId(), RoutingState.STARTED);
            }
            assertThat(builder.build().calculateAllocationStatus(nodes).orElseThrow(), equalTo(new AllocationStatus(count, 5)));
        }
        {
            TrainedModelAllocation.Builder builder = TrainedModelAllocation.Builder.empty(randomParams());
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
            TrainedModelAllocation.Builder builder = TrainedModelAllocation.Builder.empty(randomParams());
            for (DiscoveryNode node : nodes) {
                builder.addRoutingEntry(node.getId(), RoutingState.STARTED);
            }
            assertThat(builder.build().calculateAllocationStatus(nodes).orElseThrow(), equalTo(new AllocationStatus(5, 5)));
        }
    }

    public void testCalculateAllocationState() {
        List<DiscoveryNode> nodes = Stream.generate(TrainedModelAllocationTests::buildNode).limit(5).collect(Collectors.toList());
        assertThat(TrainedModelAllocation.Builder.empty(randomParams()).calculateAllocationState(), equalTo(AllocationState.STARTING));
        assertThat(
            TrainedModelAllocation.Builder.empty(randomParams()).stopAllocation("test").calculateAllocationState(),
            equalTo(AllocationState.STOPPING)
        );

        {
            TrainedModelAllocation.Builder builder = TrainedModelAllocation.Builder.empty(randomParams());
            int count = randomInt(4);
            for (int i = 0; i < count; i++) {
                builder.addRoutingEntry(
                    nodes.get(i).getId(),
                    randomFrom(RoutingState.FAILED, RoutingState.STOPPED, RoutingState.STARTING, RoutingState.STOPPING)
                );
            }
            assertThat(builder.calculateAllocationState(), equalTo(AllocationState.STARTING));
        }
        {
            TrainedModelAllocation.Builder builder = TrainedModelAllocation.Builder.empty(randomParams());
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
            assertThat(builder.calculateAllocationState(), equalTo(AllocationState.STARTED));
        }
        {
            TrainedModelAllocation.Builder builder = TrainedModelAllocation.Builder.empty(randomParams());
            for (DiscoveryNode node : nodes) {
                builder.addRoutingEntry(node.getId(), RoutingState.STARTED);
            }
            assertThat(builder.calculateAllocationState(), equalTo(AllocationState.STARTED));
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
        TrainedModelAllocation.Builder builder,
        Function<TrainedModelAllocation.Builder, TrainedModelAllocation.Builder> function
    ) {
        function.apply(builder);
        assertThat(builder.isChanged(), is(false));
    }

}
