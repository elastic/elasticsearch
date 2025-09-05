/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.assignment;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.message.Message;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.version.CompatibilityVersionsUtils;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Strings;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.MachineLearningField;
import org.elasticsearch.xpack.core.ml.MlConfigVersion;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.CreateTrainedModelAssignmentAction;
import org.elasticsearch.xpack.core.ml.action.StartDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.action.UpdateTrainedModelAssignmentRoutingInfoAction;
import org.elasticsearch.xpack.core.ml.inference.assignment.AdaptiveAllocationsSettings;
import org.elasticsearch.xpack.core.ml.inference.assignment.AssignmentState;
import org.elasticsearch.xpack.core.ml.inference.assignment.Priority;
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingInfo;
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingInfoUpdate;
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingState;
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingStateAndReason;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignment;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignmentMetadata;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.autoscaling.NodeAvailabilityZoneMapper;
import org.elasticsearch.xpack.ml.autoscaling.NodeFakeAvailabilityZoneMapper;
import org.elasticsearch.xpack.ml.autoscaling.NodeRealAvailabilityZoneMapper;
import org.elasticsearch.xpack.ml.job.NodeLoadDetector;
import org.elasticsearch.xpack.ml.job.task.OpenJobPersistentTasksExecutorTests;
import org.elasticsearch.xpack.ml.notifications.SystemAuditor;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;
import org.elasticsearch.xpack.ml.test.MockAppender;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Map.entry;
import static org.elasticsearch.core.Strings.format;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TrainedModelAssignmentClusterServiceTests extends ESTestCase {

    private ClusterService clusterService;
    private ThreadPool threadPool;
    private NodeLoadDetector nodeLoadDetector;
    private SystemAuditor systemAuditor;
    private NodeAvailabilityZoneMapper nodeAvailabilityZoneMapper;
    private Client client;
    private static MockAppender appender;
    private static final Logger testLogger1 = LogManager.getLogger(TrainedModelAssignmentClusterService.class);

    @Before
    public void setupObjects() throws IllegalAccessException {
        clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Sets.newHashSet(
                MachineLearning.MAX_MACHINE_MEMORY_PERCENT,
                MachineLearningField.USE_AUTO_MACHINE_MEMORY_PERCENT,
                MachineLearning.MAX_OPEN_JOBS_PER_NODE,
                MachineLearningField.MAX_LAZY_ML_NODES,
                MachineLearning.MAX_ML_NODE_SIZE,
                MachineLearning.ALLOCATED_PROCESSORS_SCALE
            )
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        threadPool = mock(ThreadPool.class);

        MlMemoryTracker memoryTracker = mock(MlMemoryTracker.class);
        when(memoryTracker.isRecentlyRefreshed()).thenReturn(true);
        nodeLoadDetector = new NodeLoadDetector(memoryTracker);

        systemAuditor = mock(SystemAuditor.class);
        client = mock(Client.class);

        appender = new MockAppender("trace_appender");
        appender.start();
        Loggers.addAppender(testLogger1, appender);
    }

    @After
    public void cleanup() {
        appender.stop();
        Loggers.removeAppender(testLogger1, appender);
    }

    public void testLogMlNodeHeterogeneity_GivenZeroOrOneArchitectures_ThenNothing() throws InterruptedException {
        Set<String> architecturesSet = new HashSet<>(randomList(0, 1, () -> randomAlphaOfLength(10)));

        final ActionListener<Set<String>> underTestListener = TrainedModelAssignmentClusterService.getArchitecturesSetActionListener();

        underTestListener.onResponse(architecturesSet);

        LogEvent lastEvent = appender.getLastEventAndReset();
        assertNull(lastEvent);
    }

    public void testLogMlNodeHeterogeneity_GivenTwoArchitecture_ThenWarn() throws InterruptedException {
        String nodeArch = randomAlphaOfLength(10);
        Set<String> architecturesSet = Set.of(nodeArch, nodeArch + "2"); // architectures must be different

        final ActionListener<Set<String>> underTestListener = TrainedModelAssignmentClusterService.getArchitecturesSetActionListener();
        underTestListener.onResponse(architecturesSet);

        LogEvent lastEvent = appender.getLastEventAndReset();

        assertEquals(Level.WARN, lastEvent.getLevel());

        Message m = lastEvent.getMessage();
        String fm = m.getFormattedMessage();
        String expected = Strings.format(
            "Heterogeneous platform architectures were detected among ML nodes. "
                + "This will prevent the deployment of some trained models. Distinct platform architectures detected: %s",
            architecturesSet
        );

        assertEquals(expected, fm);
    }

    public void testLogMlNodeHeterogeneity_GivenFailure_ThenError() throws InterruptedException {
        RuntimeException e = new RuntimeException("Test Runtime Exception");
        final ActionListener<Set<String>> underTestListener = TrainedModelAssignmentClusterService.getArchitecturesSetActionListener();
        underTestListener.onFailure(e);

        LogEvent lastEvent = appender.getLastEventAndReset();

        assertEquals(Level.ERROR, lastEvent.getLevel());

        Message m = lastEvent.getMessage();
        String fm = m.getFormattedMessage();

        assertEquals("Failed to detect heterogeneity among ML nodes with exception: ", fm);
        assertEquals(e, lastEvent.getThrown());
    }

    public void testClusterChanged_GivenNodesAdded_ThenLogMlNodeHeterogeneityCalled() {
        nodeAvailabilityZoneMapper = randomFrom(mock(NodeRealAvailabilityZoneMapper.class), mock(NodeFakeAvailabilityZoneMapper.class));
        TrainedModelAssignmentClusterService serviceSpy = spy(createClusterService(randomInt(5)));
        doNothing().when(serviceSpy).logMlNodeHeterogeneity();
        doReturn(false).when(serviceSpy).eventStateHasGlobalBlockStateNotRecoveredBlock(any());

        ClusterChangedEvent mockNodesAddedEvent = mock(ClusterChangedEvent.class);
        ClusterState mockState = mock(ClusterState.class);
        doReturn(mockState).when(mockNodesAddedEvent).state();
        Metadata mockMetadata = mock(Metadata.class);
        doReturn(mockMetadata).when(mockState).metadata();

        doReturn(true).when(mockNodesAddedEvent).localNodeMaster();
        doReturn(true).when(mockNodesAddedEvent).nodesAdded();

        serviceSpy.clusterChanged(mockNodesAddedEvent);
        Mockito.verify(serviceSpy).logMlNodeHeterogeneity();
        Mockito.verify(mockNodesAddedEvent).nodesAdded();
    }

    public void testStopPlatformSpecificModelsInHeterogeneousClusters_GivenMultipleMlNodeArchitectures_ThenCallSetToStopping() {
        nodeAvailabilityZoneMapper = randomFrom(mock(NodeRealAvailabilityZoneMapper.class), mock(NodeFakeAvailabilityZoneMapper.class));
        TrainedModelAssignmentClusterService serviceSpy = spy(createClusterService(randomInt(5)));

        Set<String> architecturesSet = new HashSet<>(randomList(2, 5, () -> randomAlphaOfLength(10)));
        ClusterState mockUpdatedState = mock(ClusterState.class);
        ClusterState mockClusterState = mock(ClusterState.class);
        StartTrainedModelDeploymentAction.TaskParams mockModelToAdd = mock(StartTrainedModelDeploymentAction.TaskParams.class);
        Optional<StartTrainedModelDeploymentAction.TaskParams> optionalModelToAdd = Optional.of(mockModelToAdd);
        String modelId = randomAlphaOfLength(10);
        String deploymentId = randomAlphaOfLength(10);
        when(mockModelToAdd.getModelId()).thenReturn(modelId);
        when(mockModelToAdd.getDeploymentId()).thenReturn(deploymentId);

        String reasonToStop = format(
            "ML nodes in this cluster have multiple platform architectures, "
                + "but can only have one for this model ([%s]); "
                + "detected architectures: %s",
            modelId,
            architecturesSet
        );

        doReturn(mockUpdatedState).when(serviceSpy).callSetToStopping(reasonToStop, deploymentId, mockClusterState);

        ClusterState updatedMockClusterState = serviceSpy.stopPlatformSpecificModelsInHeterogeneousClusters(
            mockUpdatedState,
            architecturesSet,
            optionalModelToAdd,
            mockClusterState
        );

        verify(serviceSpy).callSetToStopping(reasonToStop, deploymentId, mockClusterState);
    }

    public void testUpdateModelRoutingTable() {
        String modelId = "existing-model";
        String nodeId = "ml-node-with-room";
        String startedNode = "started-ml-node-with-room";
        ClusterState currentState = ClusterState.builder(new ClusterName("testUpdateModelRoutingTable"))
            .nodes(
                DiscoveryNodes.builder()
                    .add(buildNode(nodeId, true, ByteSizeValue.ofGb(4).getBytes(), 8))
                    .add(buildNode(startedNode, true, ByteSizeValue.ofGb(4).getBytes(), 8))
                    .build()
            )
            .putCompatibilityVersions(nodeId, CompatibilityVersionsUtils.staticCurrent())
            .metadata(
                Metadata.builder()
                    .putCustom(
                        TrainedModelAssignmentMetadata.NAME,
                        TrainedModelAssignmentMetadata.Builder.empty()
                            .addNewAssignment(
                                modelId,
                                TrainedModelAssignment.Builder.empty(newParams(modelId, 10_000L), null)
                                    .addRoutingEntry(nodeId, new RoutingInfo(1, 1, RoutingState.STARTING, ""))
                                    .addRoutingEntry(startedNode, new RoutingInfo(1, 1, RoutingState.STARTING, ""))
                            )
                            .build()
                    )
                    .build()
            )
            .build();

        assertThatStoppingAssignmentPreventsMutation(
            state -> TrainedModelAssignmentClusterService.updateModelRoutingTable(
                state,
                new UpdateTrainedModelAssignmentRoutingInfoAction.Request(nodeId, modelId, started())
            ),
            currentState
        );

        assertThat(
            TrainedModelAssignmentMetadata.fromState(currentState).getDeploymentAssignment(modelId).getAssignmentState(),
            equalTo(AssignmentState.STARTING)
        );

        ClusterState newState = TrainedModelAssignmentClusterService.updateModelRoutingTable(
            currentState,
            new UpdateTrainedModelAssignmentRoutingInfoAction.Request(startedNode, modelId, started())
        );
        assertThat(
            TrainedModelAssignmentMetadata.fromState(newState)
                .getDeploymentAssignment(modelId)
                .getNodeRoutingTable()
                .get(startedNode)
                .getState(),
            equalTo(RoutingState.STARTED)
        );
        assertThat(
            TrainedModelAssignmentMetadata.fromState(newState).getDeploymentAssignment(modelId).getAssignmentState(),
            equalTo(AssignmentState.STARTED)
        );

        expectThrows(
            ResourceNotFoundException.class,
            () -> TrainedModelAssignmentClusterService.updateModelRoutingTable(
                newState,
                new UpdateTrainedModelAssignmentRoutingInfoAction.Request("missingNode", modelId, started())
            )
        );
        expectThrows(
            ResourceNotFoundException.class,
            () -> TrainedModelAssignmentClusterService.updateModelRoutingTable(
                newState,
                new UpdateTrainedModelAssignmentRoutingInfoAction.Request(nodeId, "missingModel", started())
            )
        );

        // TEST Stopped

        // We should allow a "stopped" update on missing models and nodes as entries may have already been deleted
        TrainedModelAssignmentClusterService.updateModelRoutingTable(
            newState,
            new UpdateTrainedModelAssignmentRoutingInfoAction.Request(
                "missingNode",
                modelId,
                RoutingInfoUpdate.updateStateAndReason(new RoutingStateAndReason(RoutingState.STOPPED, ""))
            )
        );
        TrainedModelAssignmentClusterService.updateModelRoutingTable(
            newState,
            new UpdateTrainedModelAssignmentRoutingInfoAction.Request(
                nodeId,
                "missingModel",
                RoutingInfoUpdate.updateStateAndReason(new RoutingStateAndReason(RoutingState.STOPPED, ""))
            )
        );

        ClusterState updateState = TrainedModelAssignmentClusterService.updateModelRoutingTable(
            newState,
            new UpdateTrainedModelAssignmentRoutingInfoAction.Request(
                nodeId,
                modelId,
                RoutingInfoUpdate.updateStateAndReason(new RoutingStateAndReason(RoutingState.STOPPED, ""))
            )
        );
        assertThat(
            TrainedModelAssignmentMetadata.fromState(updateState).getDeploymentAssignment(modelId).getNodeRoutingTable(),
            not(hasKey(nodeId))
        );
        assertThat(
            TrainedModelAssignmentMetadata.fromState(updateState).getDeploymentAssignment(modelId).getAssignmentState(),
            equalTo(AssignmentState.STARTED)
        );
    }

    public void testRemoveAssignment() {
        ClusterState clusterStateWithoutAssignment = ClusterState.builder(new ClusterName("testRemoveAssignment"))
            .metadata(Metadata.builder().build())
            .build();
        String modelId = "remove-assignment";

        expectThrows(
            ResourceNotFoundException.class,
            () -> TrainedModelAssignmentClusterService.removeAssignment(clusterStateWithoutAssignment, modelId)
        );

        ClusterState clusterStateWithAssignment = ClusterState.builder(new ClusterName("testRemoveAssignment"))
            .nodes(DiscoveryNodes.builder().add(buildNode("test-node", true, ByteSizeValue.ofGb(4).getBytes(), 8)).build())
            .putCompatibilityVersions("test-node", CompatibilityVersionsUtils.staticCurrent())
            .metadata(
                Metadata.builder()
                    .putCustom(
                        TrainedModelAssignmentMetadata.NAME,
                        TrainedModelAssignmentMetadata.Builder.empty()
                            .addNewAssignment(
                                modelId,
                                TrainedModelAssignment.Builder.empty(newParams(modelId, randomNonNegativeLong()), null)
                            )
                            .build()
                    )
                    .build()
            )
            .build();
        assertThat(
            TrainedModelAssignmentMetadata.fromState(clusterStateWithAssignment).getDeploymentAssignment(modelId),
            is(not(nullValue()))
        );

        ClusterState modified = TrainedModelAssignmentClusterService.removeAssignment(clusterStateWithAssignment, modelId);
        assertThat(TrainedModelAssignmentMetadata.fromState(modified).getDeploymentAssignment(modelId), is(nullValue()));
    }

    public void testRemoveAllAssignments() {
        ClusterState clusterStateWithoutAssignment = ClusterState.builder(new ClusterName("testRemoveAllAssignments"))
            .metadata(Metadata.builder().build())
            .build();
        assertThat(
            TrainedModelAssignmentClusterService.removeAllAssignments(clusterStateWithoutAssignment),
            equalTo(clusterStateWithoutAssignment)
        );

        ClusterState clusterStateWithAssignments = ClusterState.builder(new ClusterName("testRemoveAllAssignments"))
            .nodes(DiscoveryNodes.builder().add(buildNode("test-node", true, ByteSizeValue.ofGb(4).getBytes(), 8)).build())
            .putCompatibilityVersions("test-node", CompatibilityVersionsUtils.staticCurrent())
            .metadata(
                Metadata.builder()
                    .putCustom(TrainedModelAssignmentMetadata.NAME, TrainedModelAssignmentMetadataTests.randomInstance())
                    .build()
            )
            .build();
        ClusterState modified = TrainedModelAssignmentClusterService.removeAllAssignments(clusterStateWithAssignments);
        assertThat(TrainedModelAssignmentMetadata.fromState(modified).allAssignments(), is(anEmptyMap()));
    }

    public void testCreateAssignment_GivenModelCannotByFullyAllocated_AndScalingIsPossible() throws Exception {
        Settings settings = Settings.EMPTY;
        ClusterSettings clusterSettings = new ClusterSettings(
            settings,
            Set.of(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING)
        );
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder()
            .add(buildNode("ml-node-with-room", true, ByteSizeValue.ofGb(4).getBytes(), 2))
            .add(buildNode("ml-node-without-room", true, 1000L, 2))
            .add(buildNode("not-ml-node", false, ByteSizeValue.ofGb(4).getBytes(), 2))
            .add(buildNode("ml-node-shutting-down", true, ByteSizeValue.ofGb(4).getBytes(), 2))
            .add(buildOldNode("old-ml-node-with-room", true, ByteSizeValue.ofGb(4).getBytes(), 2))
            .build();
        nodeAvailabilityZoneMapper = randomFrom(
            new NodeRealAvailabilityZoneMapper(settings, clusterSettings, discoveryNodes),
            new NodeFakeAvailabilityZoneMapper(settings, clusterSettings, discoveryNodes)
        );

        ClusterState currentState = ClusterState.builder(new ClusterName("testCreateAssignment"))
            .nodes(discoveryNodes)
            .metadata(Metadata.builder().putCustom(NodesShutdownMetadata.TYPE, shutdownMetadata("ml-node-shutting-down")))
            .build();

        TrainedModelAssignmentClusterService trainedModelAssignmentClusterService = createClusterService(5);
        ClusterState newState = trainedModelAssignmentClusterService.createModelAssignment(
            currentState,
            new CreateTrainedModelAssignmentAction.Request(newParams("new-model", 150, 4, 1), null)
        );
        TrainedModelAssignment createdAssignment = TrainedModelAssignmentMetadata.fromState(newState).getDeploymentAssignment("new-model");

        assertThat(createdAssignment, is(not(nullValue())));
        assertThat(createdAssignment.getNodeRoutingTable().keySet(), hasSize(1));
        assertThat(createdAssignment.getNodeRoutingTable(), hasKey("ml-node-with-room"));
        assertThat(createdAssignment.getNodeRoutingTable().get("ml-node-with-room").getState(), equalTo(RoutingState.STARTING));
        assertThat(createdAssignment.getReason().isPresent(), is(true));
        assertThat(
            createdAssignment.getReason().get(),
            containsString("Could not assign (more) allocations on node [ml-node-without-room]")
        );
        assertThat(createdAssignment.getAssignmentState(), equalTo(AssignmentState.STARTING));

        expectThrows(
            ResourceAlreadyExistsException.class,
            () -> trainedModelAssignmentClusterService.createModelAssignment(
                newState,
                new CreateTrainedModelAssignmentAction.Request(newParams("new-model", 150), null)
            )
        );
    }

    public void testCreateAssignment_GivenModelCannotByFullyAllocated_AndScalingIsNotPossible() {
        Settings settings = Settings.EMPTY;
        ClusterSettings clusterSettings = new ClusterSettings(
            settings,
            Set.of(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING)
        );
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder()
            .add(buildNode("ml-node-with-room", true, ByteSizeValue.ofGb(4).getBytes(), 2))
            .add(buildNode("ml-node-without-room", true, 1000L, 2))
            .add(buildNode("not-ml-node", false, ByteSizeValue.ofGb(4).getBytes(), 2))
            .add(buildNode("ml-node-shutting-down", true, ByteSizeValue.ofGb(4).getBytes(), 2))
            .add(buildOldNode("old-ml-node-with-room", true, ByteSizeValue.ofGb(4).getBytes(), 2))
            .build();
        nodeAvailabilityZoneMapper = randomFrom(
            new NodeRealAvailabilityZoneMapper(settings, clusterSettings, discoveryNodes),
            new NodeFakeAvailabilityZoneMapper(settings, clusterSettings, discoveryNodes)
        );
        ClusterState currentState = ClusterState.builder(new ClusterName("testCreateAssignment"))
            .nodes(discoveryNodes)
            .metadata(Metadata.builder().putCustom(NodesShutdownMetadata.TYPE, shutdownMetadata("ml-node-shutting-down")))
            .build();

        TrainedModelAssignmentClusterService trainedModelAssignmentClusterService = createClusterService(0);
        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> trainedModelAssignmentClusterService.createModelAssignment(
                currentState,
                new CreateTrainedModelAssignmentAction.Request(newParams("new-model", 150, 4, 1), null)
            )
        );

        assertThat(
            e.getMessage(),
            equalTo("Could not start deployment because there are not enough resources to provide all requested allocations")
        );
    }

    public void testCreateAssignmentWhileResetModeIsTrue() throws InterruptedException {
        Settings settings = Settings.EMPTY;
        ClusterSettings clusterSettings = new ClusterSettings(
            settings,
            Set.of(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING)
        );
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder()
            .add(buildNode("ml-node-with-room", true, ByteSizeValue.ofGb(4).getBytes(), 8))
            .build();
        nodeAvailabilityZoneMapper = randomFrom(
            new NodeRealAvailabilityZoneMapper(settings, clusterSettings, discoveryNodes),
            new NodeFakeAvailabilityZoneMapper(settings, clusterSettings, discoveryNodes)
        );

        ClusterState currentState = ClusterState.builder(new ClusterName("testCreateAssignment"))
            .nodes(discoveryNodes)
            .putCompatibilityVersions("ml-node-with-room", CompatibilityVersionsUtils.staticCurrent())
            .metadata(Metadata.builder().putCustom(MlMetadata.TYPE, new MlMetadata.Builder().isResetMode(true).build()))
            .build();
        when(clusterService.state()).thenReturn(currentState);
        TrainedModelAssignmentClusterService trainedModelAssignmentClusterService = createClusterService(0);

        CountDownLatch latch = new CountDownLatch(1);
        trainedModelAssignmentClusterService.createNewModelAssignment(
            new CreateTrainedModelAssignmentAction.Request(newParams("new-model", 150), null),
            new LatchedActionListener<>(
                ActionListener.wrap(
                    trainedModelAssignment -> fail("assignment should have failed to be created because reset mode is set"),
                    e -> {
                        assertThat(e, is(instanceOf(ElasticsearchStatusException.class)));
                        assertThat(((ElasticsearchStatusException) e).status(), equalTo(RestStatus.CONFLICT));
                        assertThat(
                            e.getMessage(),
                            equalTo("cannot create new assignment [new-model] for model [new-model] while feature reset is in progress.")
                        );
                    }
                ),
                latch
            )
        );
        latch.await();
    }

    public void testHaveMlNodesChanged_ReturnsFalseWhenPreviouslyShuttingDownNode_IsMarkedAsReturning_ButIsNotAPresentNode() {
        String model1 = "model-1";
        String shuttingDownNode = "ml-shutting-down-node";
        String mlNode1 = "ml-node-with-room";

        ClusterState stateWithShuttingDownNodeAndMlNode1 = createClusterState(
            List.of(shuttingDownNode, mlNode1),
            Metadata.builder()
                .putCustom(
                    TrainedModelAssignmentMetadata.NAME,
                    TrainedModelAssignmentMetadata.Builder.empty()
                        .addNewAssignment(
                            model1,
                            TrainedModelAssignment.Builder.empty(newParams(model1, 100), null)
                                .addRoutingEntry(mlNode1, new RoutingInfo(1, 1, RoutingState.STARTING, ""))
                        )
                        .build()
                )
                .putCustom(NodesShutdownMetadata.TYPE, shutdownMetadata(shuttingDownNode))
                .build()
        );

        ClusterState stateWithMlNode1 = ClusterState.builder(stateWithShuttingDownNodeAndMlNode1)
            .nodes(DiscoveryNodes.builder(stateWithShuttingDownNodeAndMlNode1.nodes()).remove(shuttingDownNode).build())
            .metadata(
                Metadata.builder(stateWithShuttingDownNodeAndMlNode1.metadata())
                    .putCustom(NodesShutdownMetadata.TYPE, NodesShutdownMetadata.EMPTY)
                    .build()
            )
            .build();

        var shutdownEvent = new ClusterChangedEvent("test", stateWithMlNode1, stateWithShuttingDownNodeAndMlNode1);
        var metadata = TrainedModelAssignmentMetadata.fromState(shutdownEvent.state());

        assertFalse(TrainedModelAssignmentClusterService.haveMlNodesChanged(shutdownEvent, metadata));
    }

    public void testHaveMlNodesChanged_ReturnsTrueWhenNodeShutsDownAndWasRoutedTo() {
        String model1 = "model-1";
        String mlNode1 = "ml-node-with-room";
        String mlNode2 = "new-ml-node-with-room";

        ClusterState stateWithOneNode = createClusterState(
            List.of(mlNode1),
            Metadata.builder()
                .putCustom(
                    TrainedModelAssignmentMetadata.NAME,
                    TrainedModelAssignmentMetadata.Builder.empty()
                        .addNewAssignment(
                            model1,
                            TrainedModelAssignment.Builder.empty(newParams(model1, 100), null)
                                .addRoutingEntry(mlNode1, new RoutingInfo(1, 1, RoutingState.STARTING, ""))
                        )
                        .build()
                )
                .putCustom(NodesShutdownMetadata.TYPE, shutdownMetadata(mlNode1))
                .build()
        );

        ClusterState stateWithTwoNodes = createClusterState(
            List.of(mlNode1, mlNode2),
            Metadata.builder()
                .putCustom(
                    TrainedModelAssignmentMetadata.NAME,
                    TrainedModelAssignmentMetadata.Builder.empty()
                        .addNewAssignment(
                            model1,
                            TrainedModelAssignment.Builder.empty(newParams(model1, 100), null)
                                .addRoutingEntry(mlNode1, new RoutingInfo(1, 1, RoutingState.STARTING, ""))
                        )
                        .build()
                )
                .build()
        );

        var shutdownEvent = new ClusterChangedEvent("test", stateWithOneNode, stateWithTwoNodes);
        var metadata = TrainedModelAssignmentMetadata.fromState(shutdownEvent.state());

        assertThat(TrainedModelAssignmentClusterService.haveMlNodesChanged(shutdownEvent, metadata), is(true));
    }

    public void testHaveMlNodesChanged_ReturnsFalseWhenNodeShutsDownAndWasRoutedTo_ButRouteIsStopping() {
        String model1 = "model-1";
        String mlNode1 = "ml-node-with-room";
        String mlNode2 = "new-ml-node-with-room";

        ClusterState stateWithOneNode = createClusterState(
            List.of(mlNode1),
            Metadata.builder()
                .putCustom(
                    TrainedModelAssignmentMetadata.NAME,
                    TrainedModelAssignmentMetadata.Builder.empty()
                        .addNewAssignment(
                            model1,
                            TrainedModelAssignment.Builder.empty(newParams(model1, 100), null)
                                .addRoutingEntry(mlNode1, new RoutingInfo(1, 1, RoutingState.STOPPING, ""))
                        )
                        .build()
                )
                .putCustom(NodesShutdownMetadata.TYPE, shutdownMetadata(mlNode1))
                .build()
        );

        ClusterState stateWithTwoNodes = createClusterState(
            List.of(mlNode1, mlNode2),
            Metadata.builder()
                .putCustom(
                    TrainedModelAssignmentMetadata.NAME,
                    TrainedModelAssignmentMetadata.Builder.empty()
                        .addNewAssignment(
                            model1,
                            TrainedModelAssignment.Builder.empty(newParams(model1, 100), null)
                                .addRoutingEntry(mlNode1, new RoutingInfo(1, 1, RoutingState.STARTING, ""))
                        )
                        .build()
                )
                .build()
        );

        var shutdownEvent = new ClusterChangedEvent("test", stateWithOneNode, stateWithTwoNodes);
        var metadata = TrainedModelAssignmentMetadata.fromState(shutdownEvent.state());

        assertThat(TrainedModelAssignmentClusterService.haveMlNodesChanged(shutdownEvent, metadata), is(false));
    }

    public void testDetectReasonToRebalanceModels() {
        String model1 = "model-1";
        String model2 = "model-2";
        String mlNode1 = "ml-node-with-room";
        String mlNode2 = "new-ml-node-with-room";
        DiscoveryNode mlNode1Node = buildNode(mlNode1, true, ByteSizeValue.ofGb(4).getBytes(), 8);
        DiscoveryNode mlNode2Node = buildNode(mlNode2, true, ByteSizeValue.ofGb(4).getBytes(), 8);
        ClusterState stateWithTwoNodes = ClusterState.builder(new ClusterName("testDetectReasonToRebalanceModels"))
            .nodes(DiscoveryNodes.builder().add(mlNode1Node).add(mlNode2Node))
            .build();
        ClusterState stateWithOneNode = ClusterState.builder(new ClusterName("testDetectReasonToRebalanceModels"))
            .nodes(DiscoveryNodes.builder().add(mlNode1Node))
            .build();
        ClusterState stateWithOneNodeNotMl = ClusterState.builder(new ClusterName("testDetectReasonToRebalanceModels"))
            .nodes(DiscoveryNodes.builder().add(mlNode1Node).add(buildNode("not-ml-node", false, ByteSizeValue.ofGb(4).getBytes(), 8)))
            .build();

        assertThat(
            TrainedModelAssignmentClusterService.detectReasonToRebalanceModels(
                new ClusterChangedEvent(
                    "test",
                    ClusterState.builder(randomFrom(stateWithOneNodeNotMl, stateWithOneNode, stateWithTwoNodes)).build(),
                    ClusterState.builder(randomFrom(stateWithOneNodeNotMl, stateWithOneNode, stateWithTwoNodes))
                        .metadata(
                            Metadata.builder()
                                .putCustom(
                                    TrainedModelAssignmentMetadata.NAME,
                                    TrainedModelAssignmentMetadata.Builder.empty()
                                        .addNewAssignment(model1, TrainedModelAssignment.Builder.empty(newParams(model1, 100), null))
                                        .build()
                                )
                                .build()
                        )
                        .build()
                )
            ),
            equalTo(Optional.empty())
        );

        // Even with metadata changes, unless there are node changes, do nothing
        ClusterState randomState = randomFrom(stateWithOneNodeNotMl, stateWithOneNode, stateWithTwoNodes);
        assertThat(
            TrainedModelAssignmentClusterService.detectReasonToRebalanceModels(
                new ClusterChangedEvent(
                    "test",
                    ClusterState.builder(randomState)
                        .metadata(
                            Metadata.builder()
                                .putCustom(TrainedModelAssignmentMetadata.NAME, TrainedModelAssignmentMetadataTests.randomInstance())
                                .build()
                        )
                        .build(),
                    ClusterState.builder(randomState)
                        .metadata(
                            Metadata.builder()
                                .putCustom(TrainedModelAssignmentMetadata.NAME, TrainedModelAssignmentMetadataTests.randomInstance())
                                .build()
                        )
                        .build()
                )
            ),
            equalTo(Optional.empty())
        );

        // If the node removed is not even an ML node, we should not attempt to re-allocate
        assertThat(
            TrainedModelAssignmentClusterService.detectReasonToRebalanceModels(
                new ClusterChangedEvent(
                    "test",
                    ClusterState.builder(stateWithOneNode)
                        .metadata(
                            Metadata.builder()
                                .putCustom(
                                    TrainedModelAssignmentMetadata.NAME,
                                    TrainedModelAssignmentMetadata.Builder.empty()
                                        .addNewAssignment(model1, TrainedModelAssignment.Builder.empty(newParams(model1, 100), null))
                                        .build()
                                )
                                .build()
                        )
                        .build(),
                    ClusterState.builder(stateWithOneNodeNotMl)
                        .metadata(
                            Metadata.builder()
                                .putCustom(
                                    TrainedModelAssignmentMetadata.NAME,
                                    TrainedModelAssignmentMetadata.Builder.empty()
                                        .addNewAssignment(model1, TrainedModelAssignment.Builder.empty(newParams(model1, 100), null))
                                        .build()
                                )
                                .build()
                        )
                        .build()
                )
            ),
            equalTo(Optional.empty())
        );

        // If the node removed is an ML node, but no models are allocated to it, we should not attempt to re-allocate
        assertThat(
            TrainedModelAssignmentClusterService.detectReasonToRebalanceModels(
                new ClusterChangedEvent(
                    "test",
                    ClusterState.builder(stateWithOneNode)
                        .metadata(
                            Metadata.builder()
                                .putCustom(
                                    TrainedModelAssignmentMetadata.NAME,
                                    TrainedModelAssignmentMetadata.Builder.empty()
                                        .addNewAssignment(model1, TrainedModelAssignment.Builder.empty(newParams(model1, 100), null))
                                        .build()
                                )
                                .build()
                        )
                        .build(),
                    ClusterState.builder(stateWithTwoNodes)
                        .metadata(
                            Metadata.builder()
                                .putCustom(
                                    TrainedModelAssignmentMetadata.NAME,
                                    TrainedModelAssignmentMetadata.Builder.empty()
                                        .addNewAssignment(model1, TrainedModelAssignment.Builder.empty(newParams(model1, 100), null))
                                        .build()
                                )
                                .build()
                        )
                        .build()
                )
            ),
            equalTo(Optional.empty())
        );

        // If a new ML node is added, we should attempt to re-allocate
        assertThat(
            TrainedModelAssignmentClusterService.detectReasonToRebalanceModels(
                new ClusterChangedEvent(
                    "test",
                    ClusterState.builder(stateWithTwoNodes)
                        .metadata(
                            Metadata.builder()
                                .putCustom(
                                    TrainedModelAssignmentMetadata.NAME,
                                    TrainedModelAssignmentMetadata.Builder.empty()
                                        .addNewAssignment(model1, TrainedModelAssignment.Builder.empty(newParams(model1, 100), null))
                                        .build()
                                )
                                .build()
                        )
                        .build(),
                    ClusterState.builder(stateWithOneNode)
                        .metadata(
                            Metadata.builder()
                                .putCustom(
                                    TrainedModelAssignmentMetadata.NAME,
                                    TrainedModelAssignmentMetadata.Builder.empty()
                                        .addNewAssignment(model1, TrainedModelAssignment.Builder.empty(newParams(model1, 100), null))
                                        .build()
                                )
                                .build()
                        )
                        .build()
                )
            ),
            equalTo(Optional.of("nodes changed"))
        );

        // If a new ML node is added, but allocation is stopping, we should not re-allocate
        assertThat(
            TrainedModelAssignmentClusterService.detectReasonToRebalanceModels(
                new ClusterChangedEvent(
                    "test",
                    ClusterState.builder(stateWithTwoNodes)
                        .metadata(
                            Metadata.builder()
                                .putCustom(
                                    TrainedModelAssignmentMetadata.NAME,
                                    TrainedModelAssignmentMetadata.Builder.empty()
                                        .addNewAssignment(
                                            model1,
                                            TrainedModelAssignment.Builder.empty(newParams(model1, 100), null).stopAssignment("test")
                                        )
                                        .build()
                                )
                                .build()
                        )
                        .build(),
                    ClusterState.builder(stateWithOneNode)
                        .metadata(
                            Metadata.builder()
                                .putCustom(
                                    TrainedModelAssignmentMetadata.NAME,
                                    TrainedModelAssignmentMetadata.Builder.empty()
                                        .addNewAssignment(model1, TrainedModelAssignment.Builder.empty(newParams(model1, 100), null))
                                        .build()
                                )
                                .build()
                        )
                        .build()
                )
            ),
            equalTo(Optional.empty())
        );

        // If a new ML node is added, but its shutting down, don't re-allocate
        assertThat(
            TrainedModelAssignmentClusterService.detectReasonToRebalanceModels(
                new ClusterChangedEvent(
                    "test",
                    ClusterState.builder(stateWithTwoNodes)
                        .metadata(
                            Metadata.builder()
                                .putCustom(
                                    TrainedModelAssignmentMetadata.NAME,
                                    TrainedModelAssignmentMetadata.Builder.empty()
                                        .addNewAssignment(model1, TrainedModelAssignment.Builder.empty(newParams(model1, 100), null))
                                        .build()
                                )
                                .putCustom(NodesShutdownMetadata.TYPE, shutdownMetadata(mlNode2))
                                .build()
                        )
                        .build(),
                    ClusterState.builder(stateWithOneNode)
                        .metadata(
                            Metadata.builder()
                                .putCustom(
                                    TrainedModelAssignmentMetadata.NAME,
                                    TrainedModelAssignmentMetadata.Builder.empty()
                                        .addNewAssignment(model1, TrainedModelAssignment.Builder.empty(newParams(model1, 100), null))
                                        .build()
                                )
                                .build()
                        )
                        .build()
                )
            ),
            equalTo(Optional.empty())
        );

        // If a ML node is removed and its routed to, re-allocate
        assertThat(
            TrainedModelAssignmentClusterService.detectReasonToRebalanceModels(
                new ClusterChangedEvent(
                    "test",
                    ClusterState.builder(stateWithOneNode)
                        .metadata(
                            Metadata.builder()
                                .putCustom(
                                    TrainedModelAssignmentMetadata.NAME,
                                    TrainedModelAssignmentMetadata.Builder.empty()
                                        .addNewAssignment(
                                            model1,
                                            TrainedModelAssignment.Builder.empty(newParams(model1, 100), null)
                                                .addRoutingEntry(mlNode1, new RoutingInfo(1, 1, RoutingState.STARTING, ""))
                                        )
                                        .addNewAssignment(
                                            model2,
                                            TrainedModelAssignment.Builder.empty(newParams("model-2", 100), null)
                                                .addRoutingEntry(mlNode1, new RoutingInfo(1, 1, RoutingState.STARTING, ""))
                                                .addRoutingEntry(mlNode2, new RoutingInfo(1, 1, RoutingState.STARTING, ""))
                                        )
                                        .build()
                                )
                                .build()
                        )
                        .build(),
                    ClusterState.builder(stateWithTwoNodes)
                        .metadata(
                            Metadata.builder()
                                .putCustom(
                                    TrainedModelAssignmentMetadata.NAME,
                                    TrainedModelAssignmentMetadata.Builder.empty()
                                        .addNewAssignment(
                                            model1,
                                            TrainedModelAssignment.Builder.empty(newParams(model1, 100), null)
                                                .addRoutingEntry(mlNode1, new RoutingInfo(1, 1, RoutingState.STARTING, ""))
                                        )
                                        .addNewAssignment(
                                            model2,
                                            TrainedModelAssignment.Builder.empty(newParams("model-2", 100), null)
                                                .addRoutingEntry(mlNode1, new RoutingInfo(1, 1, RoutingState.STARTING, ""))
                                                .addRoutingEntry(mlNode2, new RoutingInfo(1, 1, RoutingState.STARTING, ""))
                                        )
                                        .build()
                                )
                                .build()
                        )
                        .build()
                )
            ),
            equalTo(Optional.of("nodes changed"))
        );

        // If a ML node is removed and its routed to, but the allocation is stopping, don't re-allocate
        assertThat(
            TrainedModelAssignmentClusterService.detectReasonToRebalanceModels(
                new ClusterChangedEvent(
                    "test",
                    ClusterState.builder(stateWithOneNode)
                        .metadata(
                            Metadata.builder()
                                .putCustom(
                                    TrainedModelAssignmentMetadata.NAME,
                                    TrainedModelAssignmentMetadata.Builder.empty()
                                        .addNewAssignment(
                                            model1,
                                            TrainedModelAssignment.Builder.empty(newParams(model1, 100), null)
                                                .addRoutingEntry(mlNode1, new RoutingInfo(1, 1, RoutingState.STARTING, ""))
                                        )
                                        .addNewAssignment(
                                            model2,
                                            TrainedModelAssignment.Builder.empty(newParams("model-2", 100), null)
                                                .addRoutingEntry(mlNode1, new RoutingInfo(1, 1, RoutingState.STARTING, ""))
                                                .addRoutingEntry(mlNode2, new RoutingInfo(1, 1, RoutingState.STARTING, ""))
                                                .stopAssignment("test")
                                        )
                                        .build()
                                )
                                .build()
                        )
                        .build(),
                    ClusterState.builder(stateWithTwoNodes)
                        .metadata(
                            Metadata.builder()
                                .putCustom(
                                    TrainedModelAssignmentMetadata.NAME,
                                    TrainedModelAssignmentMetadata.Builder.empty()
                                        .addNewAssignment(
                                            model1,
                                            TrainedModelAssignment.Builder.empty(newParams(model1, 100), null)
                                                .addRoutingEntry(mlNode1, new RoutingInfo(1, 1, RoutingState.STARTING, ""))
                                        )
                                        .addNewAssignment(
                                            model2,
                                            TrainedModelAssignment.Builder.empty(newParams("model-2", 100), null)
                                                .addRoutingEntry(mlNode1, new RoutingInfo(1, 1, RoutingState.STARTING, ""))
                                                .addRoutingEntry(mlNode2, new RoutingInfo(1, 1, RoutingState.STARTING, ""))
                                        )
                                        .build()
                                )
                                .build()
                        )
                        .build()
                )
            ),
            equalTo(Optional.empty())
        );
    }

    public void testDetectReasonToRebalanceModels_WithNodeShutdowns() {
        String clusterName = "testDetectReasonToRebalanceModels_WithNodeShutdowns";
        String model1 = "model-1";
        DiscoveryNode mlNode1 = buildNode("ml-node-1", true, ByteSizeValue.ofGb(4).getBytes(), 8);
        DiscoveryNode mlNode2 = buildNode("ml-node-2", true, ByteSizeValue.ofGb(4).getBytes(), 8);
        DiscoveryNode esNode1 = buildNode("es-node-1", false, ByteSizeValue.ofGb(4).getBytes(), 8);
        DiscoveryNode esNode2 = buildNode("es-node-2", false, ByteSizeValue.ofGb(4).getBytes(), 8);
        DiscoveryNode esNode3 = buildNode("es-node-3", false, ByteSizeValue.ofGb(4).getBytes(), 8);

        TrainedModelAssignmentMetadata fullModelAllocation = TrainedModelAssignmentMetadata.Builder.empty()
            .addNewAssignment(
                model1,
                TrainedModelAssignment.Builder.empty(newParams(model1, 100), null)
                    .addRoutingEntry(mlNode1.getId(), new RoutingInfo(1, 1, RoutingState.STARTED, ""))
                    .addRoutingEntry(mlNode2.getId(), new RoutingInfo(1, 1, RoutingState.STARTED, ""))
            )
            .build();

        ClusterState fullyAllocated = csBuilderWithNodes(clusterName, mlNode1, mlNode2, esNode1, esNode2, esNode3).metadata(
            Metadata.builder().putCustom(TrainedModelAssignmentMetadata.NAME, fullModelAllocation).build()
        ).build();

        // reallocate when the node is marked for shutdown
        var previousState = fullyAllocated;
        var currentState = ClusterState.builder(fullyAllocated)
            .metadata(
                Metadata.builder()
                    .putCustom(TrainedModelAssignmentMetadata.NAME, fullModelAllocation)
                    .putCustom(NodesShutdownMetadata.TYPE, nodesShutdownMetadata(mlNode1))
                    .build()
            )
            .build();

        assertThat(
            TrainedModelAssignmentClusterService.detectReasonToRebalanceModels(
                new ClusterChangedEvent("test", currentState, previousState)
            ),
            equalTo(Optional.of("nodes changed"))
        );

        previousState = currentState;

        // mlNode1 node is now removed we but we have already
        // reallocated on the node shutdown change
        currentState = csBuilderWithNodes(clusterName, mlNode2, esNode1, esNode2, esNode3).metadata(
            Metadata.builder()
                .putCustom(TrainedModelAssignmentMetadata.NAME, fullModelAllocation)
                .putCustom(NodesShutdownMetadata.TYPE, nodesShutdownMetadata(mlNode1))
                .build()
        ).build();

        assertThat(
            TrainedModelAssignmentClusterService.detectReasonToRebalanceModels(
                new ClusterChangedEvent("test", currentState, previousState)
            ),
            equalTo(Optional.empty())
        );

        previousState = currentState;

        // mlNode1 has returned but is still marked as shutdown
        currentState = csBuilderWithNodes(clusterName, mlNode1, mlNode2, esNode1, esNode2, esNode3).metadata(
            Metadata.builder()
                .putCustom(TrainedModelAssignmentMetadata.NAME, fullModelAllocation)
                .putCustom(NodesShutdownMetadata.TYPE, nodesShutdownMetadata(mlNode1))
                .build()
        ).build();

        assertThat(
            TrainedModelAssignmentClusterService.detectReasonToRebalanceModels(
                new ClusterChangedEvent("test", currentState, previousState)
            ),
            equalTo(Optional.empty())
        );

        previousState = currentState;

        // mlNode1 no longer marked for shutdown
        currentState = csBuilderWithNodes(clusterName, mlNode1, mlNode2, esNode1, esNode2, esNode3).metadata(
            Metadata.builder().putCustom(TrainedModelAssignmentMetadata.NAME, fullModelAllocation).build()
        ).build();

        assertThat(
            TrainedModelAssignmentClusterService.detectReasonToRebalanceModels(
                new ClusterChangedEvent("test", currentState, previousState)
            ),
            equalTo(Optional.of("nodes changed"))
        );

        previousState = currentState;

        // now an ES node is marked for shutdown
        currentState = csBuilderWithNodes(clusterName, mlNode1, mlNode2, esNode1, esNode2, esNode3).metadata(
            Metadata.builder()
                .putCustom(TrainedModelAssignmentMetadata.NAME, fullModelAllocation)
                .putCustom(NodesShutdownMetadata.TYPE, nodesShutdownMetadata(esNode1))
                .build()
        ).build();

        assertThat(
            TrainedModelAssignmentClusterService.detectReasonToRebalanceModels(
                new ClusterChangedEvent("test", currentState, previousState)
            ),
            equalTo(Optional.empty())
        );

        previousState = currentState;

        // The ES node is removed
        currentState = csBuilderWithNodes(clusterName, mlNode1, mlNode2, esNode2, esNode3).metadata(
            Metadata.builder()
                .putCustom(TrainedModelAssignmentMetadata.NAME, fullModelAllocation)
                .putCustom(NodesShutdownMetadata.TYPE, nodesShutdownMetadata(esNode1))
                .build()
        ).build();

        assertThat(
            TrainedModelAssignmentClusterService.detectReasonToRebalanceModels(
                new ClusterChangedEvent("test", currentState, previousState)
            ),
            equalTo(Optional.empty())
        );

        previousState = currentState;

        // The ES node returns
        currentState = csBuilderWithNodes(clusterName, mlNode1, mlNode2, esNode1, esNode2, esNode3).metadata(
            Metadata.builder()
                .putCustom(TrainedModelAssignmentMetadata.NAME, fullModelAllocation)
                .putCustom(NodesShutdownMetadata.TYPE, nodesShutdownMetadata(esNode1))
                .build()
        ).build();

        assertThat(
            TrainedModelAssignmentClusterService.detectReasonToRebalanceModels(
                new ClusterChangedEvent("test", currentState, previousState)
            ),
            equalTo(Optional.empty())
        );

        previousState = currentState;

        // The ES node is no longer marked as shutdown
        currentState = csBuilderWithNodes(clusterName, mlNode1, mlNode2, esNode1, esNode2, esNode3).metadata(
            Metadata.builder()
                .putCustom(TrainedModelAssignmentMetadata.NAME, fullModelAllocation)
                .putCustom(NodesShutdownMetadata.TYPE, nodesShutdownMetadata(esNode1))
                .build()
        ).build();

        assertThat(
            TrainedModelAssignmentClusterService.detectReasonToRebalanceModels(
                new ClusterChangedEvent("test", currentState, previousState)
            ),
            equalTo(Optional.empty())
        );

        // shutdown and node removed in the same event
        previousState = fullyAllocated;
        currentState = csBuilderWithNodes(clusterName, mlNode2, esNode1, esNode2, esNode3).metadata(
            Metadata.builder()
                .putCustom(TrainedModelAssignmentMetadata.NAME, fullModelAllocation)
                .putCustom(NodesShutdownMetadata.TYPE, nodesShutdownMetadata(mlNode1))
                .build()
        ).build();

        assertThat(
            TrainedModelAssignmentClusterService.detectReasonToRebalanceModels(
                new ClusterChangedEvent("test", currentState, previousState)
            ),
            equalTo(Optional.of("nodes changed"))
        );

        previousState = currentState;

        // node comes back and the shutdown is removed
        currentState = fullyAllocated;

        assertThat(
            TrainedModelAssignmentClusterService.detectReasonToRebalanceModels(
                new ClusterChangedEvent("test", currentState, previousState)
            ),
            equalTo(Optional.of("nodes changed"))
        );
    }

    public void testDetectReasonToRebalanceModels_GivenSingleMlJobStopped() {
        String modelId = "model-1";
        String mlNodeId = "ml-node-1";
        DiscoveryNode mlNode = buildNode(mlNodeId, true, ByteSizeValue.ofGb(4).getBytes(), 8);

        PersistentTasksCustomMetadata.Builder tasksWithJobBuilder = PersistentTasksCustomMetadata.builder();
        OpenJobPersistentTasksExecutorTests.addJobTask(
            "anomaly-detection-job",
            mlNodeId,
            randomFrom(JobState.CLOSING, JobState.OPENED, JobState.OPENING, null),
            tasksWithJobBuilder
        );

        ClusterState previousState = ClusterState.builder(new ClusterName("test_cluster"))
            .nodes(DiscoveryNodes.builder().add(mlNode))
            .metadata(
                Metadata.builder()
                    .putCustom(PersistentTasksCustomMetadata.TYPE, tasksWithJobBuilder.build())
                    .putCustom(
                        TrainedModelAssignmentMetadata.NAME,
                        TrainedModelAssignmentMetadata.Builder.empty()
                            .addNewAssignment(modelId, TrainedModelAssignment.Builder.empty(newParams(modelId, 100), null))
                            .build()
                    )
                    .build()
            )
            .build();

        ClusterState currentState = ClusterState.builder(new ClusterName("test_cluster"))
            .nodes(DiscoveryNodes.builder().add(mlNode))
            .metadata(
                Metadata.builder()
                    .putCustom(PersistentTasksCustomMetadata.TYPE, PersistentTasksCustomMetadata.builder().build())
                    .putCustom(
                        TrainedModelAssignmentMetadata.NAME,
                        TrainedModelAssignmentMetadata.Builder.empty()
                            .addNewAssignment(modelId, TrainedModelAssignment.Builder.empty(newParams(modelId, 100), null))
                            .build()
                    )
                    .build()
            )
            .build();

        assertThat(
            TrainedModelAssignmentClusterService.detectReasonToRebalanceModels(
                new ClusterChangedEvent("test", currentState, previousState)
            ),
            equalTo(Optional.of("ML [anomaly detection] job stopped"))
        );
    }

    public void testDetectReasonToRebalanceModels_GivenOutdatedAssignments() {
        String modelId = "model-1";
        String mlNodeId = "ml-node-1";
        DiscoveryNode mlNode = buildNode(mlNodeId, true, ByteSizeValue.ofGb(4).getBytes(), 8);

        TrainedModelAssignmentMetadata modelMetadata = TrainedModelAssignmentMetadata.Builder.empty()
            .addNewAssignment(
                modelId,
                TrainedModelAssignment.Builder.empty(newParams(modelId, 100), null)
                    .addRoutingEntry(mlNodeId, new RoutingInfo(0, 0, RoutingState.STARTED, ""))
            )
            .build();

        ClusterState previousState = ClusterState.builder(new ClusterName("test_cluster"))
            .nodes(DiscoveryNodes.builder().add(mlNode))
            .metadata(Metadata.builder().putCustom(TrainedModelAssignmentMetadata.NAME, modelMetadata).build())
            .build();

        // A non ML-node is added
        ClusterState currentState = ClusterState.builder(new ClusterName("test_cluster"))
            .nodes(DiscoveryNodes.builder().add(mlNode).add(buildNode("non-ml-node", false, ByteSizeValue.ofGb(4).getBytes(), 8)))
            .metadata(Metadata.builder().putCustom(TrainedModelAssignmentMetadata.NAME, modelMetadata).build())
            .build();

        assertThat(
            TrainedModelAssignmentClusterService.detectReasonToRebalanceModels(
                new ClusterChangedEvent("test", currentState, previousState)
            ),
            equalTo(Optional.of("outdated assignments detected"))
        );
    }

    public void testDetectReasonToRebalanceModels_GivenMultipleMlJobsStopped() {
        String modelId = "model-1";
        String mlNodeId = "ml-node-1";
        DiscoveryNode mlNode = buildNode(mlNodeId, true, ByteSizeValue.ofGb(4).getBytes(), 8);

        PersistentTasksCustomMetadata.Builder previousTasksBuilder = PersistentTasksCustomMetadata.builder();
        OpenJobPersistentTasksExecutorTests.addJobTask(
            "anomaly-detection-job1",
            mlNodeId,
            randomFrom(JobState.CLOSING, JobState.OPENED, JobState.OPENING, null),
            previousTasksBuilder
        );
        OpenJobPersistentTasksExecutorTests.addJobTask(
            "anomaly-detection-job2",
            mlNodeId,
            randomFrom(JobState.CLOSING, JobState.OPENED, JobState.OPENING, null),
            previousTasksBuilder
        );
        OpenJobPersistentTasksExecutorTests.addJobTask(
            "anomaly-detection-job3",
            mlNodeId,
            randomFrom(JobState.CLOSING, JobState.OPENED, JobState.OPENING, null),
            previousTasksBuilder
        );
        previousTasksBuilder.addTask(
            MlTasks.dataFrameAnalyticsTaskId("dfa-1"),
            MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME,
            new StartDataFrameAnalyticsAction.TaskParams("dfa-1", MlConfigVersion.CURRENT, true),
            new PersistentTasksCustomMetadata.Assignment(mlNodeId, "test assignment")
        );

        PersistentTasksCustomMetadata.Builder currentTasksBuilder = PersistentTasksCustomMetadata.builder();
        OpenJobPersistentTasksExecutorTests.addJobTask(
            "anomaly-detection-job2",
            mlNodeId,
            randomFrom(JobState.CLOSING, JobState.OPENED, JobState.OPENING, null),
            currentTasksBuilder
        );
        OpenJobPersistentTasksExecutorTests.addJobTask(
            "anomaly-detection-job3",
            mlNodeId,
            randomFrom(JobState.CLOSING, JobState.OPENED, JobState.OPENING, null),
            currentTasksBuilder
        );

        ClusterState previousState = ClusterState.builder(new ClusterName("test_cluster"))
            .nodes(DiscoveryNodes.builder().add(mlNode))
            .metadata(
                Metadata.builder()
                    .putCustom(PersistentTasksCustomMetadata.TYPE, previousTasksBuilder.build())
                    .putCustom(
                        TrainedModelAssignmentMetadata.NAME,
                        TrainedModelAssignmentMetadata.Builder.empty()
                            .addNewAssignment(modelId, TrainedModelAssignment.Builder.empty(newParams(modelId, 100), null))
                            .build()
                    )
                    .build()
            )
            .build();

        ClusterState currentState = ClusterState.builder(new ClusterName("test_cluster"))
            .nodes(DiscoveryNodes.builder().add(mlNode))
            .metadata(
                Metadata.builder()
                    .putCustom(PersistentTasksCustomMetadata.TYPE, currentTasksBuilder.build())
                    .putCustom(
                        TrainedModelAssignmentMetadata.NAME,
                        TrainedModelAssignmentMetadata.Builder.empty()
                            .addNewAssignment(modelId, TrainedModelAssignment.Builder.empty(newParams(modelId, 100), null))
                            .build()
                    )
                    .build()
            )
            .build();

        assertThat(
            TrainedModelAssignmentClusterService.detectReasonToRebalanceModels(
                new ClusterChangedEvent("test", currentState, previousState)
            ),
            equalTo(Optional.of("ML [anomaly detection, data frame analytics] jobs stopped"))
        );
    }

    public void testDetectReasonToRebalanceModels_GivenMlJobsStarted() {
        String modelId = "model-1";
        String mlNodeId = "ml-node-1";
        DiscoveryNode mlNode = buildNode(mlNodeId, true, ByteSizeValue.ofGb(4).getBytes(), 8);

        PersistentTasksCustomMetadata.Builder previousTasksBuilder = PersistentTasksCustomMetadata.builder();
        OpenJobPersistentTasksExecutorTests.addJobTask(
            "anomaly-detection-job1",
            mlNodeId,
            randomFrom(JobState.CLOSING, JobState.OPENED, JobState.OPENING, null),
            previousTasksBuilder
        );
        previousTasksBuilder.addTask(
            MlTasks.dataFrameAnalyticsTaskId("dfa-1"),
            MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME,
            new StartDataFrameAnalyticsAction.TaskParams("dfa-1", MlConfigVersion.CURRENT, true),
            new PersistentTasksCustomMetadata.Assignment(mlNodeId, "test assignment")
        );

        PersistentTasksCustomMetadata.Builder currentTasksBuilder = PersistentTasksCustomMetadata.builder();
        OpenJobPersistentTasksExecutorTests.addJobTask(
            "anomaly-detection-job1",
            mlNodeId,
            randomFrom(JobState.CLOSING, JobState.OPENED, JobState.OPENING, null),
            currentTasksBuilder
        );
        OpenJobPersistentTasksExecutorTests.addJobTask(
            "anomaly-detection-job2",
            mlNodeId,
            randomFrom(JobState.CLOSING, JobState.OPENED, JobState.OPENING, null),
            currentTasksBuilder
        );
        currentTasksBuilder.addTask(
            MlTasks.dataFrameAnalyticsTaskId("dfa-1"),
            MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME,
            new StartDataFrameAnalyticsAction.TaskParams("dfa-1", MlConfigVersion.CURRENT, true),
            new PersistentTasksCustomMetadata.Assignment(mlNodeId, "test assignment")
        );

        ClusterState previousState = ClusterState.builder(new ClusterName("test_cluster"))
            .nodes(DiscoveryNodes.builder().add(mlNode))
            .metadata(
                Metadata.builder()
                    .putCustom(PersistentTasksCustomMetadata.TYPE, previousTasksBuilder.build())
                    .putCustom(
                        TrainedModelAssignmentMetadata.NAME,
                        TrainedModelAssignmentMetadata.Builder.empty()
                            .addNewAssignment(modelId, TrainedModelAssignment.Builder.empty(newParams(modelId, 100), null))
                            .build()
                    )
                    .build()
            )
            .build();

        ClusterState currentState = ClusterState.builder(new ClusterName("test_cluster"))
            .nodes(DiscoveryNodes.builder().add(mlNode))
            .metadata(
                Metadata.builder()
                    .putCustom(PersistentTasksCustomMetadata.TYPE, currentTasksBuilder.build())
                    .putCustom(
                        TrainedModelAssignmentMetadata.NAME,
                        TrainedModelAssignmentMetadata.Builder.empty()
                            .addNewAssignment(modelId, TrainedModelAssignment.Builder.empty(newParams(modelId, 100), null))
                            .build()
                    )
                    .build()
            )
            .build();

        assertThat(
            TrainedModelAssignmentClusterService.detectReasonToRebalanceModels(
                new ClusterChangedEvent("test", currentState, previousState)
            ),
            equalTo(Optional.empty())
        );
    }

    public void testAreAssignedNodesRemoved_GivenRemovedNodeThatIsRouted() {
        String modelId = "existing-model";
        String nodeId1 = "node-1";
        String nodeId2 = "node-2";
        Metadata metadata = Metadata.builder()
            .putCustom(
                TrainedModelAssignmentMetadata.NAME,
                TrainedModelAssignmentMetadata.Builder.empty()
                    .addNewAssignment(
                        modelId,
                        TrainedModelAssignment.Builder.empty(newParams(modelId, 10_000L), null)
                            .addRoutingEntry(nodeId1, new RoutingInfo(1, 1, RoutingState.STARTED, ""))
                            .addRoutingEntry(nodeId2, new RoutingInfo(1, 1, RoutingState.STARTED, ""))
                    )
                    .build()
            )
            .build();
        DiscoveryNode node1 = buildNode(nodeId1, true, ByteSizeValue.ofGb(4).getBytes(), 8);
        DiscoveryNode node2 = buildNode(nodeId2, true, ByteSizeValue.ofGb(4).getBytes(), 8);
        ClusterState previousState = ClusterState.builder(new ClusterName("testAreAssignedNodesRemoved"))
            .nodes(DiscoveryNodes.builder().add(node1).add(node2).build())
            .metadata(metadata)
            .build();
        ClusterState currentState = ClusterState.builder(new ClusterName("testAreAssignedNodesRemoved"))
            .nodes(DiscoveryNodes.builder().add(node1).build())
            .metadata(metadata)
            .build();
        ClusterChangedEvent event = new ClusterChangedEvent("test", currentState, previousState);

        assertThat(TrainedModelAssignmentClusterService.areAssignedNodesRemoved(event), is(true));
    }

    public void testAreAssignedNodesRemoved_GivenRemovedNodeThatIsNotRouted() {
        String modelId = "existing-model";
        String nodeId1 = "node-1";
        String nodeId2 = "node-2";
        Metadata metadata = Metadata.builder()
            .putCustom(
                TrainedModelAssignmentMetadata.NAME,
                TrainedModelAssignmentMetadata.Builder.empty()
                    .addNewAssignment(
                        modelId,
                        TrainedModelAssignment.Builder.empty(newParams(modelId, 10_000L), null)
                            .addRoutingEntry(nodeId1, new RoutingInfo(1, 1, RoutingState.STARTED, ""))
                    )
                    .build()
            )
            .build();
        DiscoveryNode node1 = buildNode(nodeId1, true, ByteSizeValue.ofGb(4).getBytes(), 8);
        DiscoveryNode node2 = buildNode(nodeId2, true, ByteSizeValue.ofGb(4).getBytes(), 8);
        ClusterState previousState = ClusterState.builder(new ClusterName("testAreAssignedNodesRemoved"))
            .nodes(DiscoveryNodes.builder().add(node1).add(node2).build())
            .metadata(metadata)
            .build();
        ClusterState currentState = ClusterState.builder(new ClusterName("testAreAssignedNodesRemoved"))
            .nodes(DiscoveryNodes.builder().add(node1).build())
            .metadata(metadata)
            .build();
        ClusterChangedEvent event = new ClusterChangedEvent("test", currentState, previousState);

        assertThat(TrainedModelAssignmentClusterService.areAssignedNodesRemoved(event), is(false));
    }

    public void testAreAssignedNodesRemoved_GivenShuttingDownNodeThatIsRouted() {
        String modelId = "existing-model";
        String nodeId1 = "node-1";
        String nodeId2 = "node-2";
        TrainedModelAssignmentMetadata trainedModelAssignmentMetadata = TrainedModelAssignmentMetadata.Builder.empty()
            .addNewAssignment(
                modelId,
                TrainedModelAssignment.Builder.empty(newParams(modelId, 10_000L), null)
                    .addRoutingEntry(nodeId1, new RoutingInfo(1, 1, RoutingState.STARTED, ""))
                    .addRoutingEntry(nodeId2, new RoutingInfo(1, 1, RoutingState.STARTED, ""))
            )
            .build();
        DiscoveryNode node1 = buildNode(nodeId1, true, ByteSizeValue.ofGb(4).getBytes(), 8);
        DiscoveryNode node2 = buildNode(nodeId2, true, ByteSizeValue.ofGb(4).getBytes(), 8);
        ClusterState previousState = ClusterState.builder(new ClusterName("testAreAssignedNodesRemoved"))
            .nodes(DiscoveryNodes.builder().add(node1).add(node2).build())
            .metadata(Metadata.builder().putCustom(TrainedModelAssignmentMetadata.NAME, trainedModelAssignmentMetadata))
            .build();
        ClusterState currentState = ClusterState.builder(new ClusterName("testAreAssignedNodesRemoved"))
            .nodes(DiscoveryNodes.builder().add(node1).add(node2).build())
            .metadata(
                Metadata.builder()
                    .putCustom(TrainedModelAssignmentMetadata.NAME, trainedModelAssignmentMetadata)
                    .putCustom(
                        NodesShutdownMetadata.TYPE,
                        new NodesShutdownMetadata(
                            Map.of(
                                nodeId1,
                                SingleNodeShutdownMetadata.builder()
                                    .setNodeId(nodeId1)
                                    .setNodeEphemeralId(nodeId1)
                                    .setType(SingleNodeShutdownMetadata.Type.REMOVE)
                                    .setStartedAtMillis(System.currentTimeMillis())
                                    .setReason("test")
                                    .build()
                            )
                        )
                    )
            )
            .build();
        ClusterChangedEvent event = new ClusterChangedEvent("test", currentState, previousState);

        assertThat(TrainedModelAssignmentClusterService.areAssignedNodesRemoved(event), is(true));
    }

    public void testAreAssignedNodesRemoved_GivenShuttingDownNodeThatIsNotRouted() {
        String modelId = "existing-model";
        String nodeId1 = "node-1";
        String nodeId2 = "node-2";
        TrainedModelAssignmentMetadata trainedModelAssignmentMetadata = TrainedModelAssignmentMetadata.Builder.empty()
            .addNewAssignment(
                modelId,
                TrainedModelAssignment.Builder.empty(newParams(modelId, 10_000L), null)
                    .addRoutingEntry(nodeId2, new RoutingInfo(1, 1, RoutingState.STARTED, ""))
            )
            .build();
        DiscoveryNode node1 = buildNode(nodeId1, true, ByteSizeValue.ofGb(4).getBytes(), 8);
        DiscoveryNode node2 = buildNode(nodeId2, true, ByteSizeValue.ofGb(4).getBytes(), 8);
        ClusterState previousState = ClusterState.builder(new ClusterName("testAreAssignedNodesRemoved"))
            .nodes(DiscoveryNodes.builder().add(node1).add(node2).build())
            .metadata(Metadata.builder().putCustom(TrainedModelAssignmentMetadata.NAME, trainedModelAssignmentMetadata))
            .build();
        ClusterState currentState = ClusterState.builder(new ClusterName("testAreAssignedNodesRemoved"))
            .nodes(DiscoveryNodes.builder().add(node1).add(node2).build())
            .metadata(
                Metadata.builder()
                    .putCustom(TrainedModelAssignmentMetadata.NAME, trainedModelAssignmentMetadata)
                    .putCustom(
                        NodesShutdownMetadata.TYPE,
                        new NodesShutdownMetadata(
                            Map.of(
                                nodeId1,
                                SingleNodeShutdownMetadata.builder()
                                    .setNodeId(nodeId1)
                                    .setNodeEphemeralId(nodeId1)
                                    .setType(SingleNodeShutdownMetadata.Type.REMOVE)
                                    .setStartedAtMillis(System.currentTimeMillis())
                                    .setReason("test")
                                    .build()
                            )
                        )
                    )
            )
            .build();
        ClusterChangedEvent event = new ClusterChangedEvent("test", currentState, previousState);

        assertThat(TrainedModelAssignmentClusterService.areAssignedNodesRemoved(event), is(false));
    }

    public void testRemoveRoutingToUnassignableNodes_RemovesRouteForRemovedNodes() {
        String modelId1 = "model-1";
        String modelId2 = "model-2";
        String nodeId1 = "node-1";
        String nodeId2 = "node-2";
        String nodeId3 = "node-3";
        Metadata metadata = Metadata.builder()
            .putCustom(
                TrainedModelAssignmentMetadata.NAME,
                TrainedModelAssignmentMetadata.Builder.empty()
                    .addNewAssignment(
                        modelId1,
                        TrainedModelAssignment.Builder.empty(newParams(modelId1, 10_000L), null)
                            .addRoutingEntry(nodeId1, new RoutingInfo(1, 1, RoutingState.STARTED, ""))
                            .addRoutingEntry(nodeId2, new RoutingInfo(1, 1, RoutingState.STARTED, ""))
                    )
                    .addNewAssignment(
                        modelId2,
                        TrainedModelAssignment.Builder.empty(newParams(modelId2, 10_000L), null)
                            .addRoutingEntry(nodeId1, new RoutingInfo(1, 1, RoutingState.STARTED, ""))
                            .addRoutingEntry(nodeId2, new RoutingInfo(1, 1, RoutingState.STARTED, ""))
                    )
                    .build()
            )
            // This node should not affect the assignments because it is not routed to
            .putCustom(
                NodesShutdownMetadata.TYPE,
                new NodesShutdownMetadata(
                    Map.of(
                        nodeId3,
                        SingleNodeShutdownMetadata.builder()
                            .setNodeId(nodeId3)
                            .setNodeEphemeralId(nodeId3)
                            .setType(SingleNodeShutdownMetadata.Type.REMOVE)
                            .setStartedAtMillis(System.currentTimeMillis())
                            .setReason("test")
                            .build()
                    )
                )
            )
            .build();
        // This simulates node2 being non-existent but not shutting down
        ClusterState currentState = createClusterState(List.of(nodeId1, nodeId3), metadata);

        ClusterState resultState = TrainedModelAssignmentClusterService.removeRoutingToUnassignableNodes(currentState);

        TrainedModelAssignmentMetadata trainedModelAssignmentMetadata = TrainedModelAssignmentMetadata.fromState(resultState);
        assertThat(trainedModelAssignmentMetadata.allAssignments(), is(aMapWithSize(2)));
        for (String modelId : List.of(modelId1, modelId2)) {
            TrainedModelAssignment assignment = trainedModelAssignmentMetadata.getDeploymentAssignment(modelId);
            assertThat(assignment, is(notNullValue()));
            assertThat(assignment.getNodeRoutingTable(), is(aMapWithSize(1)));
            assertThat(assignment.getNodeRoutingTable(), hasKey(nodeId1));
            assertThat(assignment.getNodeRoutingTable(), not(hasKey(nodeId3)));

        }
    }

    public void testRemoveRoutingToUnassignableNodes_AddsAStoppingRouteForShuttingDownNodes() {
        String modelId1 = "model-1";
        String modelId2 = "model-2";
        String nodeId1 = "node-1";
        String nodeId2 = "node-2";
        String nodeId3 = "node-3";
        Metadata metadata = Metadata.builder()
            .putCustom(
                TrainedModelAssignmentMetadata.NAME,
                TrainedModelAssignmentMetadata.Builder.empty()
                    .addNewAssignment(
                        modelId1,
                        TrainedModelAssignment.Builder.empty(newParams(modelId1, 10_000L), null)
                            .addRoutingEntry(nodeId1, new RoutingInfo(1, 1, RoutingState.STARTED, ""))
                            .addRoutingEntry(nodeId2, new RoutingInfo(1, 1, RoutingState.STARTED, ""))
                            .addRoutingEntry(nodeId3, new RoutingInfo(1, 1, RoutingState.STARTED, ""))
                    )
                    .addNewAssignment(
                        modelId2,
                        TrainedModelAssignment.Builder.empty(newParams(modelId2, 10_000L), null)
                            .addRoutingEntry(nodeId1, new RoutingInfo(1, 1, RoutingState.STARTED, ""))
                            .addRoutingEntry(nodeId2, new RoutingInfo(1, 1, RoutingState.STARTED, ""))
                            .addRoutingEntry(nodeId3, new RoutingInfo(1, 1, RoutingState.STARTED, ""))
                    )
                    .build()
            )
            .putCustom(
                NodesShutdownMetadata.TYPE,
                new NodesShutdownMetadata(
                    Map.of(
                        nodeId3,
                        SingleNodeShutdownMetadata.builder()
                            .setNodeId(nodeId3)
                            .setNodeEphemeralId(nodeId3)
                            .setType(SingleNodeShutdownMetadata.Type.REMOVE)
                            .setStartedAtMillis(System.currentTimeMillis())
                            .setReason("test")
                            .build()
                    )
                )
            )
            .build();
        // This simulates node2 being non-existent but not shutting down
        ClusterState currentState = createClusterState(List.of(nodeId1, nodeId3), metadata);
        ClusterState resultState = TrainedModelAssignmentClusterService.removeRoutingToUnassignableNodes(currentState);

        TrainedModelAssignmentMetadata trainedModelAssignmentMetadata = TrainedModelAssignmentMetadata.fromState(resultState);
        assertThat(trainedModelAssignmentMetadata.allAssignments(), is(aMapWithSize(2)));

        for (String modelId : List.of(modelId1, modelId2)) {
            TrainedModelAssignment assignment = trainedModelAssignmentMetadata.getDeploymentAssignment(modelId);
            assertThat(assignment, is(notNullValue()));
            assertThat(assignment.getNodeRoutingTable(), is(aMapWithSize(2)));
            assertThat(assignment.getNodeRoutingTable(), hasKey(nodeId1));
            assertThat(assignment.getNodeRoutingTable().get(nodeId1).getState(), is(RoutingState.STARTED));
            assertThat(assignment.getNodeRoutingTable(), hasKey(nodeId3));
            assertThat(assignment.getNodeRoutingTable().get(nodeId3).getState(), is(RoutingState.STOPPING));
            assertThat(assignment.getNodeRoutingTable(), not(hasKey(nodeId2)));
        }
    }

    public void testRemoveRoutingToUnassignableNodes_IgnoresARouteThatIsStoppedForShuttingDownNode() {
        String modelId1 = "model-1";
        String modelId2 = "model-2";
        String nodeId1 = "node-1";
        String nodeId2 = "node-2";
        String nodeId3 = "node-3";
        Metadata metadata = Metadata.builder()
            .putCustom(
                TrainedModelAssignmentMetadata.NAME,
                TrainedModelAssignmentMetadata.Builder.empty()
                    .addNewAssignment(
                        modelId1,
                        TrainedModelAssignment.Builder.empty(newParams(modelId1, 10_000L), null)
                            .addRoutingEntry(nodeId1, new RoutingInfo(1, 1, RoutingState.STARTED, ""))
                            .addRoutingEntry(nodeId2, new RoutingInfo(1, 1, RoutingState.STARTED, ""))
                            .addRoutingEntry(nodeId3, new RoutingInfo(1, 1, RoutingState.STOPPED, ""))
                    )
                    .addNewAssignment(
                        modelId2,
                        TrainedModelAssignment.Builder.empty(newParams(modelId2, 10_000L), null)
                            .addRoutingEntry(nodeId1, new RoutingInfo(1, 1, RoutingState.STARTED, ""))
                            .addRoutingEntry(nodeId2, new RoutingInfo(1, 1, RoutingState.STARTED, ""))
                            .addRoutingEntry(nodeId3, new RoutingInfo(1, 1, RoutingState.STOPPED, ""))
                    )
                    .build()
            )
            .putCustom(
                NodesShutdownMetadata.TYPE,
                new NodesShutdownMetadata(
                    Map.of(
                        nodeId3,
                        SingleNodeShutdownMetadata.builder()
                            .setNodeId(nodeId3)
                            .setNodeEphemeralId(nodeId3)
                            .setType(SingleNodeShutdownMetadata.Type.REMOVE)
                            .setStartedAtMillis(System.currentTimeMillis())
                            .setReason("test")
                            .build()
                    )
                )
            )
            .build();
        // This simulates node2 being non-existent but not shutting down
        ClusterState currentState = createClusterState(List.of(nodeId1, nodeId3), metadata);

        ClusterState resultState = TrainedModelAssignmentClusterService.removeRoutingToUnassignableNodes(currentState);

        TrainedModelAssignmentMetadata trainedModelAssignmentMetadata = TrainedModelAssignmentMetadata.fromState(resultState);
        assertThat(trainedModelAssignmentMetadata.allAssignments(), is(aMapWithSize(2)));

        for (String modelId : List.of(modelId1, modelId2)) {
            TrainedModelAssignment assignment = trainedModelAssignmentMetadata.getDeploymentAssignment(modelId);
            assertThat(assignment, is(notNullValue()));
            assertThat(assignment.getNodeRoutingTable(), is(aMapWithSize(2)));
            assertThat(assignment.getNodeRoutingTable(), hasKey(nodeId1));
            assertThat(assignment.getNodeRoutingTable().get(nodeId1).getState(), is(RoutingState.STARTED));
            assertThat(assignment.getNodeRoutingTable(), hasKey(nodeId3));
            assertThat(assignment.getNodeRoutingTable().get(nodeId3).getState(), is(RoutingState.STOPPED));
            assertThat(assignment.getNodeRoutingTable(), not(hasKey(nodeId2)));
        }
    }

    public void testSetShuttingDownNodeRoutesToStopping_GivenAnAssignmentRoutedToShuttingDownNode_ItSetsShuttingDownNodeRouteToStopping() {
        var availableNode = "node-1";
        var availableNodeModelId = "available-model-id";
        StartTrainedModelDeploymentAction.TaskParams taskParamsRunning = newParams(availableNodeModelId, 100);

        var shuttingDownNodeId = "shutting-down-1";
        var shuttingDownModelId = "id1";
        StartTrainedModelDeploymentAction.TaskParams taskParamsShuttingDown = newParams(shuttingDownModelId, 100);

        TrainedModelAssignmentMetadata currentMetadata = TrainedModelAssignmentMetadata.Builder.empty()
            .addNewAssignment(
                availableNodeModelId,
                TrainedModelAssignment.Builder.empty(taskParamsRunning, null)
                    .addRoutingEntry(availableNode, new RoutingInfo(1, 1, RoutingState.STARTED, ""))
            )
            .addNewAssignment(
                shuttingDownModelId,
                TrainedModelAssignment.Builder.empty(taskParamsShuttingDown, null)
                    .addRoutingEntry(shuttingDownNodeId, new RoutingInfo(1, 1, RoutingState.STARTED, ""))
            )
            .build();

        TrainedModelAssignmentMetadata.Builder rebalanced = TrainedModelAssignmentMetadata.Builder.empty()
            .addNewAssignment(
                availableNodeModelId,
                TrainedModelAssignment.Builder.empty(taskParamsRunning, null)
                    .addRoutingEntry(availableNode, new RoutingInfo(1, 1, RoutingState.STARTED, ""))
            )
            .addNewAssignment(
                shuttingDownModelId,
                TrainedModelAssignment.Builder.empty(taskParamsRunning, null)
                    .addRoutingEntry(availableNode, new RoutingInfo(1, 1, RoutingState.STARTING, ""))
            );

        TrainedModelAssignmentMetadata result = TrainedModelAssignmentClusterService.setShuttingDownNodeRoutesToStopping(
            currentMetadata,
            Set.of(shuttingDownNodeId),
            rebalanced
        ).build();

        TrainedModelAssignment assignment = result.getDeploymentAssignment(shuttingDownModelId);
        assertThat(assignment, is(notNullValue()));
        assertThat(assignment.getAssignmentState(), equalTo(AssignmentState.STARTING));
        assertThat(assignment.getNodeRoutingTable().get(availableNode).getState(), is(RoutingState.STARTING));
        assertThat(assignment.getNodeRoutingTable().get(shuttingDownNodeId).getState(), is(RoutingState.STOPPING));
        assertThat(assignment.getReason().isPresent(), is(false));
    }

    public
        void
        testSetShuttingDownNodeRoutesToStopping_GivenTwoAssignmentsWithOneOnAShuttingDownNode_ItSetsShuttingDownNodeRouteToStopping() {
        var availableNode = "node-1";

        var shuttingDownModelId = "id1";
        StartTrainedModelDeploymentAction.TaskParams taskParamsShuttingDown = newParams(shuttingDownModelId, 300);

        var notShuttingDownModelId = "id2";
        StartTrainedModelDeploymentAction.TaskParams taskParamsNotShuttingDown = newParams(notShuttingDownModelId, 300);

        var shuttingDownNodeId = "shutting-down-1";
        TrainedModelAssignmentMetadata currentMetadata = TrainedModelAssignmentMetadata.Builder.empty()
            .addNewAssignment(
                shuttingDownModelId,
                TrainedModelAssignment.Builder.empty(taskParamsShuttingDown, null)
                    .addRoutingEntry(shuttingDownNodeId, new RoutingInfo(1, 1, RoutingState.STARTED, ""))
            )
            .addNewAssignment(
                notShuttingDownModelId,
                TrainedModelAssignment.Builder.empty(taskParamsNotShuttingDown, null)
                    .addRoutingEntry(availableNode, new RoutingInfo(1, 1, RoutingState.STARTED, ""))
            )
            .build();

        TrainedModelAssignmentMetadata.Builder rebalanced = TrainedModelAssignmentMetadata.Builder.empty()
            .addNewAssignment(
                shuttingDownModelId,
                TrainedModelAssignment.Builder.empty(taskParamsShuttingDown, null)
                    .addRoutingEntry(availableNode, new RoutingInfo(1, 1, RoutingState.STARTING, ""))
            )
            .addNewAssignment(
                notShuttingDownModelId,
                TrainedModelAssignment.Builder.empty(taskParamsNotShuttingDown, null)
                    .addRoutingEntry(availableNode, new RoutingInfo(1, 1, RoutingState.STARTED, ""))
            );

        TrainedModelAssignmentMetadata result = TrainedModelAssignmentClusterService.setShuttingDownNodeRoutesToStopping(
            currentMetadata,
            Set.of(shuttingDownNodeId),
            rebalanced
        ).build();

        TrainedModelAssignment shuttingDownAssignment = result.getDeploymentAssignment(shuttingDownModelId);
        assertThat(shuttingDownAssignment, is(notNullValue()));
        assertThat(shuttingDownAssignment.getAssignmentState(), equalTo(AssignmentState.STARTING));
        assertThat(shuttingDownAssignment.getNodeRoutingTable().get(availableNode).getState(), is(RoutingState.STARTING));
        assertThat(shuttingDownAssignment.getNodeRoutingTable().get(shuttingDownNodeId).getState(), is(RoutingState.STOPPING));
        assertThat(shuttingDownAssignment.getReason().isPresent(), is(false));

        TrainedModelAssignment assignment = result.getDeploymentAssignment(notShuttingDownModelId);
        assertThat(assignment, is(notNullValue()));
        // assignment state is set to starting by default
        assertThat(assignment.getAssignmentState(), equalTo(AssignmentState.STARTING));
        assertThat(assignment.getNodeRoutingTable().get(availableNode).getState(), is(RoutingState.STARTED));
        assertThat(assignment.getNodeRoutingTable().get(shuttingDownNodeId), is(nullValue()));
        assertThat(assignment.getReason().isPresent(), is(false));
    }

    public
        void
        testSetShuttingDownNodeRoutesToStopping_GivenShuttingDownNodeWithNoAssociatedAssignments_ItDoesNotMarkAnyAssignmentsAsStopping() {
        var availableNode = "node-1";

        var shuttingDownNodeId = "shutting-down-1";
        var modelId = "id1";
        StartTrainedModelDeploymentAction.TaskParams taskParamsShuttingDown = newParams(modelId, 300);

        var disappearingNodeId = "disappearingNode";
        TrainedModelAssignmentMetadata currentMetadata = TrainedModelAssignmentMetadata.Builder.empty()
            .addNewAssignment(
                modelId,
                TrainedModelAssignment.Builder.empty(taskParamsShuttingDown, null)
                    .addRoutingEntry(disappearingNodeId, new RoutingInfo(1, 1, RoutingState.STARTED, ""))
            )
            .build();

        TrainedModelAssignmentMetadata.Builder rebalanced = TrainedModelAssignmentMetadata.Builder.empty()
            .addNewAssignment(
                modelId,
                TrainedModelAssignment.Builder.empty(taskParamsShuttingDown, null)
                    .addRoutingEntry(availableNode, new RoutingInfo(1, 1, RoutingState.STARTED, ""))
            );

        TrainedModelAssignmentMetadata result = TrainedModelAssignmentClusterService.setShuttingDownNodeRoutesToStopping(
            currentMetadata,
            Set.of(shuttingDownNodeId),
            rebalanced
        ).build();

        TrainedModelAssignment assignment = result.getDeploymentAssignment(modelId);
        assertThat(assignment, is(notNullValue()));
        // assignment state is set to starting by default
        assertThat(assignment.getAssignmentState(), equalTo(AssignmentState.STARTING));
        assertThat(assignment.getNodeRoutingTable().get(availableNode).getState(), is(RoutingState.STARTED));
        assertThat(assignment.getNodeRoutingTable().get(shuttingDownNodeId), is(nullValue()));
        assertThat(assignment.getNodeRoutingTable().get(disappearingNodeId), is(nullValue()));
        assertThat(assignment.getReason().isPresent(), is(false));
    }

    public void testSetShuttingDownNodeRoutesToStopping_GivenAssignmentDoesNotExist_ItSetsAssignmentStateToStoppingAndRouteToStopping() {
        var shuttingDownNodeId = "shutting-down-1";
        var modelId = "id1";
        StartTrainedModelDeploymentAction.TaskParams taskParamsShuttingDown = newParams(modelId, 300);

        TrainedModelAssignmentMetadata currentMetadata = TrainedModelAssignmentMetadata.Builder.empty()
            .addNewAssignment(
                modelId,
                TrainedModelAssignment.Builder.empty(taskParamsShuttingDown, null)
                    .addRoutingEntry(shuttingDownNodeId, new RoutingInfo(1, 1, RoutingState.STARTED, ""))
            )
            .build();

        TrainedModelAssignmentMetadata result = TrainedModelAssignmentClusterService.setShuttingDownNodeRoutesToStopping(
            currentMetadata,
            Set.of(shuttingDownNodeId),
            TrainedModelAssignmentMetadata.Builder.empty()
        ).build();

        TrainedModelAssignment assignment = result.getDeploymentAssignment(modelId);
        assertThat(assignment, is(notNullValue()));
        assertThat(assignment.getAssignmentState(), equalTo(AssignmentState.STOPPING));
        assertThat(assignment.getNodeRoutingTable().get(shuttingDownNodeId).getState(), is(RoutingState.STOPPING));
        assertThat(assignment.getReason().isPresent(), is(true));
        assertThat(assignment.getReason().get(), is("nodes changed"));
    }

    private static ClusterState createClusterState(List<String> nodeIds, Metadata metadata) {
        DiscoveryNode[] nodes = nodeIds.stream()
            .map(id -> buildNode(id, true, ByteSizeValue.ofGb(4).getBytes(), 8))
            .toArray(DiscoveryNode[]::new);

        ClusterState.Builder csBuilder = csBuilderWithNodes("test", nodes);
        nodeIds.forEach(id -> csBuilder.putCompatibilityVersions(id, CompatibilityVersionsUtils.staticCurrent()));

        return csBuilder.metadata(metadata).build();
    }

    private static ClusterState.Builder csBuilderWithNodes(String name, DiscoveryNode... nodes) {
        var csBuilder = ClusterState.builder(new ClusterName(name));
        var nodeBuilder = DiscoveryNodes.builder();
        for (var node : nodes) {
            nodeBuilder.add(node);
        }
        csBuilder.nodes(nodeBuilder);
        return csBuilder;
    }

    private NodesShutdownMetadata nodesShutdownMetadata(DiscoveryNode nodeToShutdown) {
        return new NodesShutdownMetadata(
            Map.of(
                nodeToShutdown.getId(),
                SingleNodeShutdownMetadata.builder()
                    .setNodeId(nodeToShutdown.getId())
                    .setNodeEphemeralId(nodeToShutdown.getEphemeralId())
                    .setStartedAtMillis(1L)
                    .setType(SingleNodeShutdownMetadata.Type.RESTART)
                    .setReason("because this cannot be null")
                    .build()
            )
        );
    }

    public void testSetAllocationToStopping() {
        ClusterState clusterStateWithoutAllocation = ClusterState.builder(new ClusterName("testSetAllocationToStopping"))
            .metadata(Metadata.builder().build())
            .build();
        String modelId = "stopping-allocation";

        expectThrows(
            ResourceNotFoundException.class,
            () -> TrainedModelAssignmentClusterService.setToStopping(clusterStateWithoutAllocation, modelId, "test")
        );

        ClusterState clusterStateWithAllocation = ClusterState.builder(new ClusterName("testSetAllocationToStopping"))
            .nodes(DiscoveryNodes.builder().add(buildNode("test-node", true, ByteSizeValue.ofGb(4).getBytes(), 8)).build())
            .putCompatibilityVersions("test-node", CompatibilityVersionsUtils.staticCurrent())
            .metadata(
                Metadata.builder()
                    .putCustom(
                        TrainedModelAssignmentMetadata.NAME,
                        TrainedModelAssignmentMetadata.Builder.empty()
                            .addNewAssignment(
                                modelId,
                                TrainedModelAssignment.Builder.empty(newParams(modelId, randomNonNegativeLong()), null)
                            )
                            .build()
                    )
                    .build()
            )
            .build();
        TrainedModelAssignmentMetadata before = TrainedModelAssignmentMetadata.fromState(clusterStateWithAllocation);
        assertThat(before.getDeploymentAssignment(modelId), is(not(nullValue())));
        assertThat(before.getDeploymentAssignment(modelId).getAssignmentState(), equalTo(AssignmentState.STARTING));

        ClusterState modified = TrainedModelAssignmentClusterService.setToStopping(clusterStateWithAllocation, modelId, "test");
        assertThat(
            TrainedModelAssignmentMetadata.fromState(modified).getDeploymentAssignment(modelId).getAssignmentState(),
            equalTo(AssignmentState.STOPPING)
        );
    }

    static NodesShutdownMetadata shutdownMetadata(String nodeId) {
        return new NodesShutdownMetadata(
            Collections.singletonMap(
                nodeId,
                SingleNodeShutdownMetadata.builder()
                    .setType(SingleNodeShutdownMetadata.Type.REMOVE)
                    .setStartedAtMillis(randomNonNegativeLong())
                    .setReason("tests")
                    .setNodeId(nodeId)
                    .setNodeEphemeralId(nodeId)
                    .build()
            )
        );
    }

    private void assertThatStoppingAssignmentPreventsMutation(
        Function<ClusterState, ClusterState> mutationFunction,
        ClusterState original
    ) {
        TrainedModelAssignmentMetadata tempMetadata = TrainedModelAssignmentMetadata.fromState(original);
        if (tempMetadata.allAssignments().isEmpty()) {
            return;
        }
        TrainedModelAssignmentMetadata.Builder builder = TrainedModelAssignmentMetadata.builder(original);
        for (String modelId : tempMetadata.allAssignments().keySet()) {
            builder.getAssignment(modelId).stopAssignment("test");
        }
        TrainedModelAssignmentMetadata metadataWithStopping = builder.build();
        ClusterState originalWithStoppingAllocations = ClusterState.builder(original)
            .metadata(Metadata.builder(original.metadata()).putCustom(TrainedModelAssignmentMetadata.NAME, metadataWithStopping).build())
            .build();

        assertThat(
            "setting all allocations to stopping did not prevent mutation",
            TrainedModelAssignmentMetadata.fromState(mutationFunction.apply(originalWithStoppingAllocations)),
            equalTo(metadataWithStopping)
        );
    }

    public void testHasUpdates() {
        var assignment = TrainedModelAssignment.Builder.empty(newParams("foo", 10_000L, 1, 1), null).build();
        assertFalse(TrainedModelAssignmentClusterService.hasUpdates(1, null, assignment));
        assertTrue(TrainedModelAssignmentClusterService.hasUpdates(2, null, assignment));

        var adaptiveAllocations = new AdaptiveAllocationsSettings(true, 1, 4);
        assignment = TrainedModelAssignment.Builder.empty(newParams("foo", 10_000L, 1, 1), adaptiveAllocations).build();
        assertFalse(TrainedModelAssignmentClusterService.hasUpdates(null, new AdaptiveAllocationsSettings(true, 1, 4), assignment));
        assertTrue(TrainedModelAssignmentClusterService.hasUpdates(null, new AdaptiveAllocationsSettings(true, 0, 4), assignment));

        assertFalse(TrainedModelAssignmentClusterService.hasUpdates(1, new AdaptiveAllocationsSettings(true, 1, 4), assignment));
        assertTrue(TrainedModelAssignmentClusterService.hasUpdates(1, new AdaptiveAllocationsSettings(true, 0, 4), assignment));
    }

    private TrainedModelAssignmentClusterService createClusterService(int maxLazyNodes) {
        return new TrainedModelAssignmentClusterService(
            Settings.builder().put(MachineLearningField.MAX_LAZY_ML_NODES.getKey(), maxLazyNodes).build(),
            clusterService,
            threadPool,
            nodeLoadDetector,
            systemAuditor,
            nodeAvailabilityZoneMapper,
            client
        );
    }

    private static DiscoveryNode buildNode(String name, boolean isML, long nativeMemory, int allocatedProcessors) {
        return buildNode(name, isML, nativeMemory, allocatedProcessors, VersionInformation.CURRENT, MlConfigVersion.CURRENT);
    }

    private static DiscoveryNode buildNode(
        String name,
        boolean isML,
        long nativeMemory,
        int allocatedProcessors,
        VersionInformation versionInfo,
        MlConfigVersion mlConfigVersion
    ) {
        return DiscoveryNodeUtils.builder(name)
            .name(name)
            .attributes(
                Map.ofEntries(
                    entry(MachineLearning.MACHINE_MEMORY_NODE_ATTR, String.valueOf(nativeMemory)),
                    entry(MachineLearning.MAX_JVM_SIZE_NODE_ATTR, String.valueOf(10)),
                    entry(MachineLearning.ALLOCATED_PROCESSORS_NODE_ATTR, String.valueOf(allocatedProcessors)),
                    entry(MachineLearning.ML_CONFIG_VERSION_NODE_ATTR, mlConfigVersion.toString())
                )
            )
            .roles(isML ? DiscoveryNodeRole.roles() : Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.MASTER_ROLE))
            .version(versionInfo)
            .build();
    }

    private static RoutingInfoUpdate started() {
        return RoutingInfoUpdate.updateStateAndReason(new RoutingStateAndReason(RoutingState.STARTED, ""));
    }

    private static DiscoveryNode buildOldNode(String name, boolean isML, long nativeMemory, int allocatedProcessors) {
        return buildNode(
            name,
            isML,
            nativeMemory,
            allocatedProcessors,
            VersionInformation.inferVersions(Version.V_7_15_0),
            MlConfigVersion.V_7_15_0
        );
    }

    private static StartTrainedModelDeploymentAction.TaskParams newParams(String modelId, long modelSize) {
        return newParams(modelId, modelSize, 1, 1);
    }

    private static StartTrainedModelDeploymentAction.TaskParams newParams(
        String modelId,
        long modelSize,
        int numberOfAllocations,
        int threadsPerAllocation
    ) {
        return new StartTrainedModelDeploymentAction.TaskParams(
            modelId,
            modelId,
            modelSize,
            numberOfAllocations,
            threadsPerAllocation,
            1024,
            ByteSizeValue.ofBytes(modelSize),
            Priority.NORMAL,
            0L,
            0L
        );
    }

    protected <T> void assertAsync(
        Consumer<ActionListener<T>> function,
        T expected,
        CheckedConsumer<T, ? extends Exception> onAnswer,
        Consumer<Exception> onException
    ) throws InterruptedException {

        CountDownLatch latch = new CountDownLatch(1);

        LatchedActionListener<T> listener = new LatchedActionListener<>(ActionListener.wrap(r -> {
            if (expected == null) {
                fail("expected an exception but got a response");
            } else {
                assertThat(r, equalTo(expected));
            }
            if (onAnswer != null) {
                onAnswer.accept(r);
            }
        }, e -> {
            if (onException == null) {
                logger.error("got unexpected exception", e);
                fail("got unexpected exception: " + e.getMessage());
            } else {
                onException.accept(e);
            }
        }), latch);

        function.accept(listener);
        latch.countDown();
        assertTrue("timed out after 20s", latch.await(20, TimeUnit.SECONDS));
    }

}
