/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.allocation;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.action.UpdateTrainedModelAllocationStateAction;
import org.elasticsearch.xpack.core.ml.inference.allocation.AllocationState;
import org.elasticsearch.xpack.core.ml.inference.allocation.RoutingState;
import org.elasticsearch.xpack.core.ml.inference.allocation.RoutingStateAndReason;
import org.elasticsearch.xpack.core.ml.inference.allocation.TrainedModelAllocation;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.job.NodeLoadDetector;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;
import org.junit.Before;

import java.util.Collections;
import java.util.Set;
import java.util.function.Function;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TrainedModelAllocationClusterServiceTests extends ESTestCase {

    private ClusterService clusterService;
    private NodeLoadDetector nodeLoadDetector;

    @Before
    public void setupObjects() {
        clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Sets.newHashSet(MachineLearning.MAX_MACHINE_MEMORY_PERCENT, MachineLearning.USE_AUTO_MACHINE_MEMORY_PERCENT)
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        MlMemoryTracker memoryTracker = mock(MlMemoryTracker.class);
        when(memoryTracker.isRecentlyRefreshed()).thenReturn(true);
        nodeLoadDetector = new NodeLoadDetector(memoryTracker);
    }

    public void testUpdateModelRoutingTable() {
        String modelId = "existing-model";
        String nodeId = "ml-node-with-room";
        ClusterState currentState = ClusterState.builder(new ClusterName("testUpdateModelRoutingTable"))
            .nodes(DiscoveryNodes.builder().add(buildNode("ml-node-with-room", true, ByteSizeValue.ofGb(4).getBytes())).build())
            .metadata(
                Metadata.builder()
                    .putCustom(
                        TrainedModelAllocationMetadata.NAME,
                        TrainedModelAllocationMetadata.Builder.empty().addNewAllocation(newParams(modelId, 10_000L))
                            .addNode(modelId, nodeId)
                            .build()
                    )
                    .build()
            )
            .build();

        assertThatStoppingAllocationPreventsMutation(
            state -> TrainedModelAllocationClusterService.updateModelRoutingTable(
                state,
                new UpdateTrainedModelAllocationStateAction.Request(nodeId, modelId, new RoutingStateAndReason(RoutingState.STARTED, ""))
            ),
            currentState
        );

        ClusterState newState = TrainedModelAllocationClusterService.updateModelRoutingTable(
            currentState,
            new UpdateTrainedModelAllocationStateAction.Request(nodeId, modelId, new RoutingStateAndReason(RoutingState.STARTED, ""))
        );
        assertThat(
            TrainedModelAllocationMetadata.fromState(newState).getModelAllocation(modelId).getNodeRoutingTable().get(nodeId).getState(),
            equalTo(RoutingState.STARTED)
        );

        expectThrows(
            ResourceNotFoundException.class,
            () -> TrainedModelAllocationClusterService.updateModelRoutingTable(
                currentState,
                new UpdateTrainedModelAllocationStateAction.Request(
                    "missingNode",
                    modelId,
                    new RoutingStateAndReason(RoutingState.STARTED, "")
                )
            )
        );
        expectThrows(
            ResourceNotFoundException.class,
            () -> TrainedModelAllocationClusterService.updateModelRoutingTable(
                currentState,
                new UpdateTrainedModelAllocationStateAction.Request(
                    nodeId,
                    "missingModel",
                    new RoutingStateAndReason(RoutingState.STARTED, "")
                )
            )
        );

        // TEST Stopped

        // We should allow a "stopped" update on missing models and nodes as entries may have already been deleted
        TrainedModelAllocationClusterService.updateModelRoutingTable(
            currentState,
            new UpdateTrainedModelAllocationStateAction.Request("missingNode", modelId, new RoutingStateAndReason(RoutingState.STOPPED, ""))
        );
        TrainedModelAllocationClusterService.updateModelRoutingTable(
            currentState,
            new UpdateTrainedModelAllocationStateAction.Request(nodeId, "missingModel", new RoutingStateAndReason(RoutingState.STOPPED, ""))
        );

        ClusterState updateState = TrainedModelAllocationClusterService.updateModelRoutingTable(
            currentState,
            new UpdateTrainedModelAllocationStateAction.Request(nodeId, modelId, new RoutingStateAndReason(RoutingState.STOPPED, ""))
        );
        assertThat(
            TrainedModelAllocationMetadata.fromState(updateState).getModelAllocation(modelId).getNodeRoutingTable(),
            not(hasKey(nodeId))
        );
    }

    public void testRemoveAllocation() {
        ClusterState clusterStateWithoutAllocation = ClusterState.builder(new ClusterName("testRemoveAllocation"))
            .metadata(Metadata.builder().build())
            .build();
        String modelId = "remove-allocation";

        expectThrows(
            ResourceNotFoundException.class,
            () -> TrainedModelAllocationClusterService.removeAllocation(clusterStateWithoutAllocation, modelId)
        );

        ClusterState clusterStateWithAllocation = ClusterState.builder(new ClusterName("testRemoveAllocation"))
            .metadata(
                Metadata.builder()
                    .putCustom(
                        TrainedModelAllocationMetadata.NAME,
                        TrainedModelAllocationMetadata.Builder.empty().addNewAllocation(newParams(modelId, randomNonNegativeLong())).build()
                    )
                    .build()
            )
            .build();
        assertThat(TrainedModelAllocationMetadata.fromState(clusterStateWithAllocation).getModelAllocation(modelId), is(not(nullValue())));

        ClusterState modified = TrainedModelAllocationClusterService.removeAllocation(clusterStateWithAllocation, modelId);
        assertThat(TrainedModelAllocationMetadata.fromState(modified).getModelAllocation(modelId), is(nullValue()));
    }

    public void testRemoveAllAllocations() {
        ClusterState clusterStateWithoutAllocation = ClusterState.builder(new ClusterName("testRemoveAllAllocations"))
            .metadata(Metadata.builder().build())
            .build();
        assertThat(
            TrainedModelAllocationClusterService.removeAllAllocations(clusterStateWithoutAllocation),
            equalTo(clusterStateWithoutAllocation)
        );

        ClusterState clusterStateWithAllocations = ClusterState.builder(new ClusterName("testRemoveAllAllocations"))
            .metadata(
                Metadata.builder()
                    .putCustom(
                        TrainedModelAllocationMetadata.NAME,
                        TrainedModelAllocationMetadataTests.randomInstance()
                    )
                    .build()
            )
            .build();
        ClusterState modified = TrainedModelAllocationClusterService.removeAllAllocations(clusterStateWithAllocations);
        assertThat(TrainedModelAllocationMetadata.fromState(modified).modelAllocations(), is(anEmptyMap()));
    }

    public void testCreateAllocation() {
        ClusterState currentState = ClusterState.builder(new ClusterName("testCreateAllocation"))
            .nodes(
                DiscoveryNodes.builder()
                    .add(buildNode("ml-node-with-room", true, ByteSizeValue.ofGb(4).getBytes()))
                    .add(buildNode("ml-node-without-room", true, 1000L))
                    .add(buildNode("not-ml-node", false, ByteSizeValue.ofGb(4).getBytes()))
                    .add(buildNode("ml-node-shutting-down", true, ByteSizeValue.ofGb(4).getBytes()))
                    .add(buildOldNode("old-ml-node-with-room", true, ByteSizeValue.ofGb(4).getBytes()))
                    .build()
            )
            .metadata(Metadata.builder().putCustom(NodesShutdownMetadata.TYPE, shutdownMetadata("ml-node-shutting-down")))
            .build();

        TrainedModelAllocationClusterService trainedModelAllocationClusterService = createClusterService();
        ClusterState newState = trainedModelAllocationClusterService.createModelAllocation(currentState, newParams("new-model", 150));
        TrainedModelAllocation createdAllocation = TrainedModelAllocationMetadata.fromState(newState).getModelAllocation("new-model");

        assertThat(createdAllocation, is(not(nullValue())));
        assertThat(createdAllocation.getNodeRoutingTable().keySet(), hasSize(2));
        assertThat(createdAllocation.getNodeRoutingTable(), hasKey("ml-node-with-room"));
        assertThat(createdAllocation.getNodeRoutingTable().get("ml-node-with-room").getState(), equalTo(RoutingState.STARTING));
        assertThat(createdAllocation.getNodeRoutingTable(), hasKey("ml-node-without-room"));
        assertThat(createdAllocation.getNodeRoutingTable().get("ml-node-without-room").getState(), equalTo(RoutingState.FAILED));
        assertThat(
            createdAllocation.getNodeRoutingTable().get("ml-node-without-room").getReason(),
            containsString("This node has insufficient available memory.")
        );

        expectThrows(
            ResourceAlreadyExistsException.class,
            () -> trainedModelAllocationClusterService.createModelAllocation(newState, newParams("new-model", 150))
        );
    }

    public void testCreateAllocationWhileResetModeIsTrue() {
        ClusterState currentState = ClusterState.builder(new ClusterName("testCreateAllocation"))
            .nodes(
                DiscoveryNodes.builder()
                    .add(buildNode("ml-node-with-room", true, ByteSizeValue.ofGb(4).getBytes()))
                    .build()
            )
            .metadata(Metadata.builder().putCustom(MlMetadata.TYPE, new MlMetadata.Builder().isResetMode(true).build()))
            .build();
        TrainedModelAllocationClusterService trainedModelAllocationClusterService = createClusterService();
        expectThrows(
            ElasticsearchStatusException.class,
            () -> trainedModelAllocationClusterService.createModelAllocation(currentState, newParams("new-model", 150))
        );

        ClusterState stateWithoutReset = ClusterState.builder(new ClusterName("testCreateAllocation"))
            .nodes(
                DiscoveryNodes.builder()
                    .add(buildNode("ml-node-with-room", true, ByteSizeValue.ofGb(4).getBytes()))
                    .build()
            )
            .metadata(Metadata.builder().putCustom(MlMetadata.TYPE, new MlMetadata.Builder().isResetMode(false).build()))
            .build();
        // Shouldn't throw
        trainedModelAllocationClusterService.createModelAllocation(stateWithoutReset, newParams("new-model", 150));
    }

    public void testAddRemoveAllocationNodes() {
        ClusterState currentState = ClusterState.builder(new ClusterName("testAddRemoveAllocationNodes"))
            .nodes(
                DiscoveryNodes.builder()
                    .add(buildNode("ml-node-with-room", true, ByteSizeValue.ofGb(4).getBytes()))
                    .add(buildNode("new-ml-node-with-room", true, ByteSizeValue.ofGb(4).getBytes()))
                    .add(buildNode("ml-node-without-room", true, 1000L))
                    .add(buildNode("not-ml-node", false, ByteSizeValue.ofGb(4).getBytes()))
                    .add(buildNode("ml-node-shutting-down", true, ByteSizeValue.ofGb(4).getBytes()))
                    .add(buildOldNode("old-versioned-ml-node-with-room", true, ByteSizeValue.ofGb(4).getBytes()))
                    .build()
            )
            .metadata(
                Metadata.builder()
                    .putCustom(NodesShutdownMetadata.TYPE, shutdownMetadata("ml-node-shutting-down"))
                    .putCustom(
                        TrainedModelAllocationMetadata.NAME,
                        TrainedModelAllocationMetadata.Builder.empty().addNewAllocation(newParams("model-1", 10_000))
                            .addNode("model-1", "ml-node-with-room")
                            .updateAllocation("model-1", "ml-node-with-room", new RoutingStateAndReason(RoutingState.STARTED, ""))
                            .addNode("model-1", "old-ml-node-with-room")
                            .updateAllocation("model-1", "old-ml-node-with-room", new RoutingStateAndReason(RoutingState.STARTED, ""))
                            .addNode("model-1", "ml-node-shutting-down")
                            .addNewAllocation(newParams("model-2", 10_000))
                            .addNode("model-2", "old-ml-node-with-room")
                            .updateAllocation("model-2", "old-ml-node-with-room", new RoutingStateAndReason(RoutingState.STARTED, ""))
                            .build()
                    )
            )
            .build();
        TrainedModelAllocationClusterService trainedModelAllocationClusterService = createClusterService();

        // Stopping shouldn't cause any updates
        assertThatStoppingAllocationPreventsMutation(
            trainedModelAllocationClusterService::addRemoveAllocationNodes,
            currentState
        );

        ClusterState modified = trainedModelAllocationClusterService.addRemoveAllocationNodes(currentState);
        TrainedModelAllocationMetadata trainedModelAllocationMetadata = TrainedModelAllocationMetadata.fromState(modified);
        assertThat(trainedModelAllocationMetadata.modelAllocations().keySet(), hasSize(2));
        assertThat(trainedModelAllocationMetadata.modelAllocations(), allOf(hasKey("model-1"), hasKey("model-2")));

        assertThat(trainedModelAllocationMetadata.getModelAllocation("model-1").getNodeRoutingTable().keySet(), hasSize(3));
        assertThat(
            trainedModelAllocationMetadata.getModelAllocation("model-1").getNodeRoutingTable(),
            allOf(hasKey("ml-node-with-room"), hasKey("new-ml-node-with-room"), hasKey("ml-node-without-room"))
        );
        assertNodeState(trainedModelAllocationMetadata, "model-1", "ml-node-with-room", RoutingState.STARTED);
        assertNodeState(trainedModelAllocationMetadata, "model-1", "new-ml-node-with-room", RoutingState.STARTING);
        assertNodeState(trainedModelAllocationMetadata, "model-1", "ml-node-without-room", RoutingState.FAILED);

        assertThat(trainedModelAllocationMetadata.getModelAllocation("model-2").getNodeRoutingTable().keySet(), hasSize(3));
        assertThat(
            trainedModelAllocationMetadata.getModelAllocation("model-2").getNodeRoutingTable(),
            allOf(hasKey("ml-node-with-room"), hasKey("new-ml-node-with-room"), hasKey("ml-node-without-room"))
        );
        assertNodeState(trainedModelAllocationMetadata, "model-2", "ml-node-with-room", RoutingState.STARTING);
        assertNodeState(trainedModelAllocationMetadata, "model-2", "new-ml-node-with-room", RoutingState.STARTING);
        assertNodeState(trainedModelAllocationMetadata, "model-2", "ml-node-without-room", RoutingState.FAILED);
    }

    public void testShouldAllocateModels() {
        String mlNode1 = "ml-node-with-room";
        String mlNode2 = "new-ml-node-with-room";
        DiscoveryNode mlNode1Node = buildNode(mlNode1, true, ByteSizeValue.ofGb(4).getBytes());
        DiscoveryNode mlNode2Node = buildNode(mlNode2, true, ByteSizeValue.ofGb(4).getBytes());
        ClusterState stateWithTwoNodes = ClusterState.builder(new ClusterName("testShouldAllocateModels"))
            .nodes(DiscoveryNodes.builder().add(mlNode1Node).add(mlNode2Node))
            .build();
        ClusterState stateWithOneNode = ClusterState.builder(new ClusterName("testShouldAllocateModels"))
            .nodes(DiscoveryNodes.builder().add(mlNode1Node))
            .build();
        ClusterState stateWithOneNodeNotMl = ClusterState.builder(new ClusterName("testShouldAllocateModels"))
            .nodes(DiscoveryNodes.builder().add(mlNode1Node).add(buildNode("not-ml-node", false, ByteSizeValue.ofGb(4).getBytes())))
            .build();

        // No metadata in the new state means no allocations, so no updates
        assertThat(
            TrainedModelAllocationClusterService.shouldAllocateModels(
                new ClusterChangedEvent(
                    "test",
                    ClusterState.builder(randomFrom(stateWithOneNodeNotMl, stateWithOneNode, stateWithTwoNodes)).build(),
                    ClusterState.builder(randomFrom(stateWithOneNodeNotMl, stateWithOneNode, stateWithTwoNodes))
                        .metadata(
                            Metadata.builder()
                                .putCustom(
                                    TrainedModelAllocationMetadata.NAME,
                                    TrainedModelAllocationMetadata.Builder.empty().addNewAllocation(newParams(mlNode1, 100)).build()
                                )
                                .build()
                        )
                        .build()
                )
            ),
            is(false)
        );

        // Even with metadata changes, unless there are node changes, do nothing
        ClusterState randomState = randomFrom(stateWithOneNodeNotMl, stateWithOneNode, stateWithTwoNodes);
        assertThat(
            TrainedModelAllocationClusterService.shouldAllocateModels(
                new ClusterChangedEvent(
                    "test",
                    ClusterState.builder(randomState)
                        .metadata(
                            Metadata.builder()
                                .putCustom(TrainedModelAllocationMetadata.NAME, TrainedModelAllocationMetadataTests.randomInstance())
                                .build()
                        )
                        .build(),
                    ClusterState.builder(randomState)
                        .metadata(
                            Metadata.builder()
                                .putCustom(TrainedModelAllocationMetadata.NAME, TrainedModelAllocationMetadataTests.randomInstance())
                                .build()
                        )
                        .build()
                )
            ),
            is(false)
        );

        // If the node removed is not even an ML node, we should not attempt to re-allocate
        assertThat(
            TrainedModelAllocationClusterService.shouldAllocateModels(
                new ClusterChangedEvent(
                    "test",
                    ClusterState.builder(stateWithOneNode)
                        .metadata(
                            Metadata.builder()
                                .putCustom(
                                    TrainedModelAllocationMetadata.NAME,
                                    TrainedModelAllocationMetadata.Builder.empty().addNewAllocation(newParams(mlNode1, 100)).build()
                                )
                                .build()
                        )
                        .build(),
                    ClusterState.builder(stateWithOneNodeNotMl)
                        .metadata(
                            Metadata.builder()
                                .putCustom(
                                    TrainedModelAllocationMetadata.NAME,
                                    TrainedModelAllocationMetadata.Builder.empty().addNewAllocation(newParams(mlNode1, 100)).build()
                                )
                                .build()
                        )
                        .build()
                )
            ),
            is(false)
        );

        // If the node removed is an ML node, but no models are allocated to it, we should not attempt to re-allocate
        assertThat(
            TrainedModelAllocationClusterService.shouldAllocateModels(
                new ClusterChangedEvent(
                    "test",
                    ClusterState.builder(stateWithOneNode)
                        .metadata(
                            Metadata.builder()
                                .putCustom(
                                    TrainedModelAllocationMetadata.NAME,
                                    TrainedModelAllocationMetadata.Builder.empty().addNewAllocation(newParams(mlNode1, 100)).build()
                                )
                                .build()
                        )
                        .build(),
                    ClusterState.builder(stateWithTwoNodes)
                        .metadata(
                            Metadata.builder()
                                .putCustom(
                                    TrainedModelAllocationMetadata.NAME,
                                    TrainedModelAllocationMetadata.Builder.empty().addNewAllocation(newParams(mlNode1, 100)).build()
                                )
                                .build()
                        )
                        .build()
                )
            ),
            is(false)
        );

        // If a new ML node is added, we should attempt to re-allocate
        assertThat(
            TrainedModelAllocationClusterService.shouldAllocateModels(
                new ClusterChangedEvent(
                    "test",
                    ClusterState.builder(stateWithTwoNodes)
                        .metadata(
                            Metadata.builder()
                                .putCustom(
                                    TrainedModelAllocationMetadata.NAME,
                                    TrainedModelAllocationMetadata.Builder.empty().addNewAllocation(newParams(mlNode1, 100)).build()
                                )
                                .build()
                        )
                        .build(),
                    ClusterState.builder(stateWithOneNode)
                        .metadata(
                            Metadata.builder()
                                .putCustom(
                                    TrainedModelAllocationMetadata.NAME,
                                    TrainedModelAllocationMetadata.Builder.empty().addNewAllocation(newParams(mlNode1, 100)).build()
                                )
                                .build()
                        )
                        .build()
                )
            ),
            is(true)
        );

        // If a new ML node is added, but allocation is stopping, we should not re-allocate
        assertThat(
            TrainedModelAllocationClusterService.shouldAllocateModels(
                new ClusterChangedEvent(
                    "test",
                    ClusterState.builder(stateWithTwoNodes)
                        .metadata(
                            Metadata.builder()
                                .putCustom(
                                    TrainedModelAllocationMetadata.NAME,
                                    TrainedModelAllocationMetadata.Builder.empty()
                                        .addNewAllocation(newParams(mlNode1, 100))
                                        .setAllocationToStopping(mlNode1)
                                        .build()
                                )
                                .build()
                        )
                        .build(),
                    ClusterState.builder(stateWithOneNode)
                        .metadata(
                            Metadata.builder()
                                .putCustom(
                                    TrainedModelAllocationMetadata.NAME,
                                    TrainedModelAllocationMetadata.Builder.empty().addNewAllocation(newParams(mlNode1, 100)).build()
                                )
                                .build()
                        )
                        .build()
                )
            ),
            is(false)
        );

        // If a new ML node is added, but its shutting down, don't re-allocate
        assertThat(
            TrainedModelAllocationClusterService.shouldAllocateModels(
                new ClusterChangedEvent(
                    "test",
                    ClusterState.builder(stateWithTwoNodes)
                        .metadata(
                            Metadata.builder()
                                .putCustom(
                                    TrainedModelAllocationMetadata.NAME,
                                    TrainedModelAllocationMetadata.Builder.empty().addNewAllocation(newParams(mlNode1, 100)).build()
                                )
                                .putCustom(NodesShutdownMetadata.TYPE, shutdownMetadata(mlNode2))
                                .build()
                        )
                        .build(),
                    ClusterState.builder(stateWithOneNode)
                        .metadata(
                            Metadata.builder()
                                .putCustom(
                                    TrainedModelAllocationMetadata.NAME,
                                    TrainedModelAllocationMetadata.Builder.empty().addNewAllocation(newParams(mlNode1, 100)).build()
                                )
                                .build()
                        )
                        .build()
                )
            ),
            is(false)
        );

        // If a ML node is removed and its routed to, re-allocate
        assertThat(
            TrainedModelAllocationClusterService.shouldAllocateModels(
                new ClusterChangedEvent(
                    "test",
                    ClusterState.builder(stateWithOneNode)
                        .metadata(
                            Metadata.builder()
                                .putCustom(
                                    TrainedModelAllocationMetadata.NAME,
                                    TrainedModelAllocationMetadata.Builder.empty().addNewAllocation(newParams("model-1", 100))
                                        .addNode("model-1", mlNode1)
                                        .addNewAllocation(newParams("model-2", 100))
                                        .addNode("model-2", mlNode1)
                                        .addNode("model-2", mlNode2)
                                        .build()
                                )
                                .build()
                        )
                        .build(),
                    ClusterState.builder(stateWithTwoNodes)
                        .metadata(
                            Metadata.builder()
                                .putCustom(
                                    TrainedModelAllocationMetadata.NAME,
                                    TrainedModelAllocationMetadata.Builder.empty().addNewAllocation(newParams("model-1", 100))
                                        .addNode("model-1", mlNode1)
                                        .addNewAllocation(newParams("model-2", 100))
                                        .addNode("model-2", mlNode1)
                                        .addNode("model-2", mlNode2)
                                        .build()
                                )
                                .build()
                        )
                        .build()
                )
            ),
            is(true)
        );

        // If a ML node is removed and its routed to, but the allocation is stopping, don't re-allocate
        assertThat(
            TrainedModelAllocationClusterService.shouldAllocateModels(
                new ClusterChangedEvent(
                    "test",
                    ClusterState.builder(stateWithOneNode)
                        .metadata(
                            Metadata.builder()
                                .putCustom(
                                    TrainedModelAllocationMetadata.NAME,
                                    TrainedModelAllocationMetadata.Builder.empty()
                                        .addNewAllocation(newParams("model-1", 100))
                                        .addNode("model-1", mlNode1)
                                        .addNewAllocation(newParams("model-2", 100))
                                        .addNode("model-2", mlNode1)
                                        .addNode("model-2", mlNode2)
                                        .setAllocationToStopping("model-2")
                                        .build()
                                )
                                .build()
                        )
                        .build(),
                    ClusterState.builder(stateWithTwoNodes)
                        .metadata(
                            Metadata.builder()
                                .putCustom(
                                    TrainedModelAllocationMetadata.NAME,
                                    TrainedModelAllocationMetadata.Builder.empty()
                                        .addNewAllocation(newParams("model-1", 100))
                                        .addNode("model-1", mlNode1)
                                        .addNewAllocation(newParams("model-2", 100))
                                        .addNode("model-2", mlNode1)
                                        .addNode("model-2", mlNode2)
                                        .build()
                                )
                                .build()
                        )
                        .build()
                )
            ),
            is(false)
        );
    }

    public void testSetAllocationToStopping() {
        ClusterState clusterStateWithoutAllocation = ClusterState.builder(new ClusterName("testSetAllocationToStopping"))
            .metadata(Metadata.builder().build())
            .build();
        String modelId = "stopping-allocation";

        expectThrows(
            ResourceNotFoundException.class,
            () -> TrainedModelAllocationClusterService.setToStopping(clusterStateWithoutAllocation, modelId)
        );

        ClusterState clusterStateWithAllocation = ClusterState.builder(new ClusterName("testSetAllocationToStopping"))
            .metadata(
                Metadata.builder()
                    .putCustom(
                        TrainedModelAllocationMetadata.NAME,
                        TrainedModelAllocationMetadata.Builder.empty().addNewAllocation(newParams(modelId, randomNonNegativeLong())).build()
                    )
                    .build()
            )
            .build();
        TrainedModelAllocationMetadata before = TrainedModelAllocationMetadata.fromState(clusterStateWithAllocation);
        assertThat(before.getModelAllocation(modelId), is(not(nullValue())));
        assertThat(before.getModelAllocation(modelId).getAllocationState(), equalTo(AllocationState.STARTED));

        ClusterState modified = TrainedModelAllocationClusterService.setToStopping(clusterStateWithAllocation, modelId);
        assertThat(
            TrainedModelAllocationMetadata.fromState(modified).getModelAllocation(modelId).getAllocationState(),
            equalTo(AllocationState.STOPPING)
        );
    }

    private void assertThatStoppingAllocationPreventsMutation(
        Function<ClusterState, ClusterState> mutationFunction,
        ClusterState original
    ) {
        TrainedModelAllocationMetadata tempMetadata = TrainedModelAllocationMetadata.fromState(original);
        if (tempMetadata.modelAllocations().isEmpty()) {
            return;
        }
        TrainedModelAllocationMetadata.Builder builder = TrainedModelAllocationMetadata.builder(original);
        for (String modelId : tempMetadata.modelAllocations().keySet()) {
            builder.setAllocationToStopping(modelId);
        }
        TrainedModelAllocationMetadata metadataWithStopping = builder.build();
        ClusterState originalWithStoppingAllocations = ClusterState.builder(original)
            .metadata(Metadata.builder(original.metadata()).putCustom(TrainedModelAllocationMetadata.NAME, metadataWithStopping).build())
            .build();

        assertThat(
            "setting all allocations to stopping did not prevent mutation",
            TrainedModelAllocationMetadata.fromState(mutationFunction.apply(originalWithStoppingAllocations)),
            equalTo(metadataWithStopping)
        );
    }

    private TrainedModelAllocationClusterService createClusterService() {
        return new TrainedModelAllocationClusterService(Settings.EMPTY, clusterService, nodeLoadDetector);
    }

    private static DiscoveryNode buildNode(String name, boolean isML, long nativeMemory) {
        return buildNode(name, isML, nativeMemory, Version.CURRENT);
    }

    private static DiscoveryNode buildNode(String name, boolean isML, long nativeMemory, Version version) {
        return new DiscoveryNode(
            name,
            name,
            buildNewFakeTransportAddress(),
            MapBuilder.<String, String>newMapBuilder()
                .put(MachineLearning.MACHINE_MEMORY_NODE_ATTR, String.valueOf(nativeMemory))
                .put(MachineLearning.MAX_JVM_SIZE_NODE_ATTR, String.valueOf(10))
                .put(MachineLearning.MAX_OPEN_JOBS_NODE_ATTR, String.valueOf(10))
                .map(),
            isML ? DiscoveryNodeRole.roles() : Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.MASTER_ROLE),
            version
        );
    }

    private static DiscoveryNode buildOldNode(String name, boolean isML, long nativeMemory) {
        return buildNode(name, isML, nativeMemory, Version.V_7_15_0);
    }

    private static StartTrainedModelDeploymentAction.TaskParams newParams(String modelId, long modelSize) {
        return new StartTrainedModelDeploymentAction.TaskParams(modelId, "test-index", modelSize);
    }

    private static void assertNodeState(TrainedModelAllocationMetadata metadata, String modelId, String nodeId, RoutingState routingState) {
        assertThat(metadata.getModelAllocation(modelId).getNodeRoutingTable().get(nodeId).getState(), equalTo(routingState));
    }

    private static NodesShutdownMetadata shutdownMetadata(String nodeId) {
        return new NodesShutdownMetadata(
            Collections.singletonMap(
                nodeId,
                SingleNodeShutdownMetadata.builder()
                    .setType(SingleNodeShutdownMetadata.Type.REMOVE)
                    .setStartedAtMillis(randomNonNegativeLong())
                    .setReason("tests")
                    .setNodeId(nodeId)
                    .build()
            )
        );
    }
}
