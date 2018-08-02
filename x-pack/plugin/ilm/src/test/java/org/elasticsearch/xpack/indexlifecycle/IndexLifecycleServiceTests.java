/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.indexlifecycle.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.indexlifecycle.LifecyclePolicy;
import org.elasticsearch.xpack.core.indexlifecycle.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleSettings;
import org.elasticsearch.xpack.core.indexlifecycle.MockAction;
import org.elasticsearch.xpack.core.indexlifecycle.OperationMode;
import org.elasticsearch.xpack.core.indexlifecycle.Phase;
import org.elasticsearch.xpack.core.indexlifecycle.ShrinkAction;
import org.elasticsearch.xpack.core.indexlifecycle.Step;
import org.elasticsearch.xpack.core.indexlifecycle.TestLifecycleType;
import org.elasticsearch.xpack.core.scheduler.SchedulerEngine;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;

import static org.elasticsearch.node.Node.NODE_MASTER_SETTING;
import static org.elasticsearch.xpack.core.indexlifecycle.AbstractStepTestCase.randomStepKey;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class IndexLifecycleServiceTests extends ESTestCase {

    private ClusterService clusterService;
    private IndexLifecycleService indexLifecycleService;
    private String nodeId;
    private DiscoveryNode masterNode;
    private IndicesAdminClient indicesClient;
    private long now;

    @Before
    public void prepareServices() {
        nodeId = randomAlphaOfLength(10);
        ExecutorService executorService = mock(ExecutorService.class);
        clusterService = mock(ClusterService.class);
        masterNode = DiscoveryNode.createLocal(settings(Version.CURRENT)
                .put(NODE_MASTER_SETTING.getKey(), true).build(),
            new TransportAddress(TransportAddress.META_ADDRESS, 9300), nodeId);
        now = randomNonNegativeLong();
        Clock clock = Clock.fixed(Instant.ofEpochMilli(now), ZoneId.of(randomFrom(ZoneId.getAvailableZoneIds())));

        doAnswer(invocationOnMock -> null).when(clusterService).addListener(any());
        doAnswer(invocationOnMock -> {
            Runnable runnable = (Runnable) invocationOnMock.getArguments()[0];
            runnable.run();
            return null;
        }).when(executorService).execute(any());

        Client client = mock(Client.class);
        AdminClient adminClient = mock(AdminClient.class);
        indicesClient = mock(IndicesAdminClient.class);
        when(client.admin()).thenReturn(adminClient);
        when(adminClient.indices()).thenReturn(indicesClient);
        when(client.settings()).thenReturn(Settings.EMPTY);

        indexLifecycleService = new IndexLifecycleService(Settings.EMPTY, client, clusterService, clock, () -> now);
        Mockito.verify(clusterService).addListener(indexLifecycleService);
        Mockito.verify(clusterService).addStateApplier(indexLifecycleService);
    }

    @After
    public void cleanup() {
        indexLifecycleService.close();
    }

    public void testOnlyChangesStateOnMasterAndMetadataExists() {
        boolean isMaster = randomBoolean();
        String localNodeId = isMaster ? nodeId : nodeId + "not_master";
        MetaData.Builder metaData = MetaData.builder()
            .persistentSettings(settings(Version.CURRENT)
                .put(LifecycleSettings.LIFECYCLE_POLL_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(3)).build());
        if (isMaster == false) {
                metaData.putCustom(IndexLifecycleMetadata.TYPE, IndexLifecycleMetadata.EMPTY);
        }
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .metaData(metaData)
            .nodes(DiscoveryNodes.builder().localNodeId(localNodeId).masterNodeId(nodeId).add(masterNode).build())
            .build();
        ClusterChangedEvent event = new ClusterChangedEvent("_source", state, state);

        indexLifecycleService.applyClusterState(event);
        indexLifecycleService.clusterChanged(event);
        verify(clusterService, times(1)).addListener(any());
        verify(clusterService, times(1)).addStateApplier(any());
        Mockito.verifyNoMoreInteractions(clusterService);
        assertNull(indexLifecycleService.getScheduler());
    }

    public void testOnlyChangesStateOnMasterWhenMetadataChanges() {
        int numPolicies = randomIntBetween(1, 5);
        IndexLifecycleMetadata lifecycleMetadata = IndexLifecycleMetadataTests.createTestInstance(numPolicies, OperationMode.RUNNING);
        IndexLifecycleMetadata newLifecycleMetadata = randomValueOtherThan(lifecycleMetadata,
            () -> IndexLifecycleMetadataTests.createTestInstance(numPolicies, OperationMode.RUNNING));
        MetaData previousMetadata = MetaData.builder()
            .persistentSettings(settings(Version.CURRENT)
                .put(LifecycleSettings.LIFECYCLE_POLL_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(3)).build())
            .putCustom(IndexLifecycleMetadata.TYPE, lifecycleMetadata)
            .build();
        MetaData newMetaData = MetaData.builder(previousMetadata).putCustom(IndexLifecycleMetadata.TYPE, newLifecycleMetadata).build();

        ClusterState previousState = ClusterState.builder(ClusterName.DEFAULT)
            .metaData(previousMetadata)
            .nodes(DiscoveryNodes.builder().localNodeId(nodeId).masterNodeId(nodeId).add(masterNode).build())
            .build();
        ClusterState newState = ClusterState.builder(previousState).metaData(newMetaData).build();
        ClusterChangedEvent event = new ClusterChangedEvent("_source", previousState, previousState);

        Mockito.reset(clusterService);
        PolicyStepsRegistry policyStepsRegistry = indexLifecycleService.getPolicyRegistry();
        indexLifecycleService.applyClusterState(event);
        indexLifecycleService.clusterChanged(event);
        Mockito.verifyZeroInteractions(clusterService);
        assertNotNull(indexLifecycleService.getScheduler());
        assertEquals(1, indexLifecycleService.getScheduler().jobCount());
        assertNotNull(indexLifecycleService.getScheduledJob());
        assertThat(policyStepsRegistry.getLifecyclePolicyMap().keySet(), equalTo(lifecycleMetadata.getPolicyMetadatas().keySet()));

        event = new ClusterChangedEvent("_source", newState, previousState);
        indexLifecycleService.applyClusterState(event);
        assertThat(policyStepsRegistry.getLifecyclePolicyMap().keySet(), equalTo(newLifecycleMetadata.getPolicyMetadatas().keySet()));
    }

    public void testElectUnElectMaster() {
        int numberOfPolicies = randomIntBetween(1, 5);
        IndexLifecycleMetadata lifecycleMetadata = IndexLifecycleMetadataTests.createTestInstance(numberOfPolicies, OperationMode.RUNNING);
        Map<String, LifecyclePolicyMetadata> expectedPolicyMap = lifecycleMetadata.getPolicyMetadatas();
        MetaData metaData = MetaData.builder()
            .persistentSettings(settings(Version.CURRENT)
                .put(LifecycleSettings.LIFECYCLE_POLL_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(3)).build())
                .putCustom(IndexLifecycleMetadata.TYPE, lifecycleMetadata)
            .build();

        // First check that when the node has never been master the scheduler
        // and job are not set up
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .metaData(metaData)
            .nodes(DiscoveryNodes.builder().localNodeId(nodeId + "not").masterNodeId(nodeId).add(masterNode).build())
            .build();
        ClusterChangedEvent event = new ClusterChangedEvent("_source", state, state);

        indexLifecycleService.applyClusterState(event);
        indexLifecycleService.clusterChanged(event);
        verify(clusterService, times(1)).addListener(any());
        verify(clusterService, times(1)).addStateApplier(any());
        Mockito.verifyNoMoreInteractions(clusterService);
        assertNull(indexLifecycleService.getScheduler());
        assertNull(indexLifecycleService.getScheduledJob());

        Mockito.reset(clusterService);
        state = ClusterState.builder(ClusterName.DEFAULT)
                .metaData(metaData)
                .nodes(DiscoveryNodes.builder().localNodeId(nodeId).masterNodeId(nodeId).add(masterNode).build())
                .build();
        event = new ClusterChangedEvent("_source", state, state);

        // Check that when the node is first elected as master it sets up
        // the scheduler job and steps registry
        indexLifecycleService.applyClusterState(event);
        indexLifecycleService.clusterChanged(event);
        Mockito.verifyZeroInteractions(clusterService);
        assertNotNull(indexLifecycleService.getScheduler());
        assertEquals(1, indexLifecycleService.getScheduler().jobCount());
        assertNotNull(indexLifecycleService.getScheduledJob());
        assertThat(indexLifecycleService.getPolicyRegistry().getLifecyclePolicyMap(), equalTo(expectedPolicyMap));

        Mockito.reset(clusterService);
        state = ClusterState.builder(ClusterName.DEFAULT)
                .metaData(metaData)
                .nodes(DiscoveryNodes.builder().localNodeId(nodeId + "not").masterNodeId(nodeId).add(masterNode).build())
                .build();
        event = new ClusterChangedEvent("_source", state, state);

        indexLifecycleService.applyClusterState(event);
        // Check that when the node is un-elected as master it cancels the job and cleans up steps registry
        indexLifecycleService.clusterChanged(event);
        Mockito.verifyZeroInteractions(clusterService);
        assertNotNull(indexLifecycleService.getScheduler());
        assertEquals(0, indexLifecycleService.getScheduler().jobCount());
        assertNull(indexLifecycleService.getScheduledJob());
        assertThat(indexLifecycleService.getPolicyRegistry().getLifecyclePolicyMap(), equalTo(expectedPolicyMap));

        Mockito.reset(clusterService);
        state = ClusterState.builder(ClusterName.DEFAULT)
                .metaData(metaData)
                .nodes(DiscoveryNodes.builder().localNodeId(nodeId).masterNodeId(nodeId).add(masterNode).build())
                .build();
        event = new ClusterChangedEvent("_source", state, state);

        // Check that when the node is re-elected as master it re-starts the job and populates the registry
        indexLifecycleService.applyClusterState(event);
        indexLifecycleService.clusterChanged(event);
        Mockito.verifyZeroInteractions(clusterService);
        assertNotNull(indexLifecycleService.getScheduler());
        assertEquals(1, indexLifecycleService.getScheduler().jobCount());
        assertNotNull(indexLifecycleService.getScheduledJob());
        assertThat(indexLifecycleService.getPolicyRegistry().getLifecyclePolicyMap(), equalTo(expectedPolicyMap));
    }

    public void testSchedulerInitializationAndUpdate() {
        TimeValue pollInterval = TimeValue.timeValueSeconds(randomIntBetween(1, 59));
        MetaData metaData = MetaData.builder()
            .putCustom(IndexLifecycleMetadata.TYPE, IndexLifecycleMetadata.EMPTY)
            .persistentSettings(settings(Version.CURRENT).build())
            .build();
        MetaData updatedPollMetaData = MetaData.builder(metaData).persistentSettings(settings(Version.CURRENT)
            .put(LifecycleSettings.LIFECYCLE_POLL_INTERVAL_SETTING.getKey(), pollInterval).build())
            .build();
        ClusterState previousState = ClusterState.builder(ClusterName.DEFAULT)
            .metaData(metaData)
            .nodes(DiscoveryNodes.builder().localNodeId(nodeId).masterNodeId(nodeId).add(masterNode).build())
            .build();
        ClusterState currentState = ClusterState.builder(previousState)
            .metaData(updatedPollMetaData)
            .build();
        ClusterChangedEvent event = new ClusterChangedEvent("_source", currentState, previousState);

        ClusterChangedEvent noChangeEvent = new ClusterChangedEvent("_source", previousState, previousState);
        indexLifecycleService.applyClusterState(noChangeEvent);
        indexLifecycleService.clusterChanged(noChangeEvent);
        assertThat(indexLifecycleService.getScheduler().jobCount(), equalTo(1));
        assertThat(((TimeValueSchedule) indexLifecycleService.getScheduledJob().getSchedule()).getInterval(),
                equalTo(TimeValue.timeValueSeconds(3)));
        indexLifecycleService.applyClusterState(event);
        indexLifecycleService.clusterChanged(event);
        assertThat(indexLifecycleService.getScheduler().jobCount(), equalTo(1));
        assertThat(((TimeValueSchedule) indexLifecycleService.getScheduledJob().getSchedule()).getInterval(), equalTo(pollInterval));
        noChangeEvent = new ClusterChangedEvent("_source", currentState, currentState);
        indexLifecycleService.applyClusterState(noChangeEvent);
        indexLifecycleService.clusterChanged(noChangeEvent);
        assertThat(indexLifecycleService.getScheduler().jobCount(), equalTo(1));
        assertThat(((TimeValueSchedule) indexLifecycleService.getScheduledJob().getSchedule()).getInterval(), equalTo(pollInterval));

        verify(clusterService, times(1)).addListener(any());
        verify(clusterService, times(1)).addStateApplier(any());
        verify(clusterService, never()).submitStateUpdateTask(anyString(), any(ClusterStateUpdateTask.class));
        Mockito.verifyNoMoreInteractions(clusterService);
    }

    public void testStoppedModeSkip() {
        String policyName = randomAlphaOfLengthBetween(1, 20);
        IndexLifecycleRunnerTests.MockClusterStateActionStep mockStep =
            new IndexLifecycleRunnerTests.MockClusterStateActionStep(randomStepKey(), randomStepKey());
        MockAction mockAction = new MockAction(Collections.singletonList(mockStep));
        Phase phase = new Phase("phase", TimeValue.ZERO, Collections.singletonMap("action", mockAction));
        LifecyclePolicy policy = new LifecyclePolicy(TestLifecycleType.INSTANCE, policyName,
                Collections.singletonMap(phase.getName(), phase));
        SortedMap<String, LifecyclePolicyMetadata> policyMap = new TreeMap<>();
        policyMap.put(policyName, new LifecyclePolicyMetadata(policy, Collections.emptyMap()));
        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        IndexMetaData indexMetadata = IndexMetaData.builder(index.getName())
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME_SETTING.getKey(), policyName))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        ImmutableOpenMap.Builder<String, IndexMetaData> indices = ImmutableOpenMap.<String, IndexMetaData> builder()
            .fPut(index.getName(), indexMetadata);
        MetaData metaData = MetaData.builder()
            .putCustom(IndexLifecycleMetadata.TYPE, new IndexLifecycleMetadata(policyMap, OperationMode.STOPPED))
            .indices(indices.build())
            .persistentSettings(settings(Version.CURRENT).build())
            .build();
        ClusterState currentState = ClusterState.builder(ClusterName.DEFAULT)
            .metaData(metaData)
            .nodes(DiscoveryNodes.builder().localNodeId(nodeId).masterNodeId(nodeId).add(masterNode).build())
            .build();
        indexLifecycleService.triggerPolicies(currentState, randomBoolean());
        assertThat(mockStep.getExecuteCount(), equalTo(0L));
    }

    public void testRequestedStopOnShrink() {
        Step.StepKey mockShrinkStep = new Step.StepKey(randomAlphaOfLength(4), ShrinkAction.NAME, randomAlphaOfLength(5));
        String policyName = randomAlphaOfLengthBetween(1, 20);
        IndexLifecycleRunnerTests.MockClusterStateActionStep mockStep =
            new IndexLifecycleRunnerTests.MockClusterStateActionStep(mockShrinkStep, randomStepKey());
        MockAction mockAction = new MockAction(Collections.singletonList(mockStep));
        Phase phase = new Phase("phase", TimeValue.ZERO, Collections.singletonMap("action", mockAction));
        LifecyclePolicy policy = new LifecyclePolicy(TestLifecycleType.INSTANCE, policyName,
            Collections.singletonMap(phase.getName(), phase));
        SortedMap<String, LifecyclePolicyMetadata> policyMap = new TreeMap<>();
        policyMap.put(policyName, new LifecyclePolicyMetadata(policy, Collections.emptyMap()));
        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        IndexMetaData indexMetadata = IndexMetaData.builder(index.getName())
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME_SETTING.getKey(), policyName)
                .put(LifecycleSettings.LIFECYCLE_PHASE, mockShrinkStep.getPhase())
                .put(LifecycleSettings.LIFECYCLE_ACTION, mockShrinkStep.getAction())
                .put(LifecycleSettings.LIFECYCLE_STEP, mockShrinkStep.getName()))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        ImmutableOpenMap.Builder<String, IndexMetaData> indices = ImmutableOpenMap.<String, IndexMetaData> builder()
            .fPut(index.getName(), indexMetadata);
        MetaData metaData = MetaData.builder()
            .putCustom(IndexLifecycleMetadata.TYPE, new IndexLifecycleMetadata(policyMap, OperationMode.STOPPING))
            .indices(indices.build())
            .persistentSettings(settings(Version.CURRENT).build())
            .build();
        ClusterState currentState = ClusterState.builder(ClusterName.DEFAULT)
            .metaData(metaData)
            .nodes(DiscoveryNodes.builder().localNodeId(nodeId).masterNodeId(nodeId).add(masterNode).build())
            .build();

        ClusterChangedEvent event = new ClusterChangedEvent("_source", currentState, ClusterState.EMPTY_STATE);
        SetOnce<Boolean> executedShrink = new SetOnce<>();
        doAnswer(invocationOnMock -> {
            executedShrink.set(true);
            return null;
        }).when(clusterService).submitStateUpdateTask(anyString(), any(ExecuteStepsUpdateTask.class));
        indexLifecycleService.applyClusterState(new ClusterChangedEvent("change", currentState, ClusterState.EMPTY_STATE));
        indexLifecycleService.clusterChanged(event);
        assertTrue(executedShrink.get());
    }

    public void testRequestedStopOnSafeAction() {
        String policyName = randomAlphaOfLengthBetween(1, 20);
        Step.StepKey currentStepKey = randomStepKey();
        IndexLifecycleRunnerTests.MockClusterStateActionStep mockStep =
            new IndexLifecycleRunnerTests.MockClusterStateActionStep(currentStepKey, randomStepKey());
        MockAction mockAction = new MockAction(Collections.singletonList(mockStep));
        Phase phase = new Phase("phase", TimeValue.ZERO, Collections.singletonMap("action", mockAction));
        LifecyclePolicy policy = new LifecyclePolicy(TestLifecycleType.INSTANCE, policyName,
            Collections.singletonMap(phase.getName(), phase));
        SortedMap<String, LifecyclePolicyMetadata> policyMap = new TreeMap<>();
        policyMap.put(policyName, new LifecyclePolicyMetadata(policy, Collections.emptyMap()));
        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        IndexMetaData indexMetadata = IndexMetaData.builder(index.getName())
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME_SETTING.getKey(), policyName)
                .put(LifecycleSettings.LIFECYCLE_PHASE, currentStepKey.getPhase())
                .put(LifecycleSettings.LIFECYCLE_ACTION, currentStepKey.getAction())
                .put(LifecycleSettings.LIFECYCLE_STEP, currentStepKey.getName()))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        ImmutableOpenMap.Builder<String, IndexMetaData> indices = ImmutableOpenMap.<String, IndexMetaData> builder()
            .fPut(index.getName(), indexMetadata);
        MetaData metaData = MetaData.builder()
            .putCustom(IndexLifecycleMetadata.TYPE, new IndexLifecycleMetadata(policyMap, OperationMode.STOPPING))
            .indices(indices.build())
            .persistentSettings(settings(Version.CURRENT).build())
            .build();
        ClusterState currentState = ClusterState.builder(ClusterName.DEFAULT)
            .metaData(metaData)
            .nodes(DiscoveryNodes.builder().localNodeId(nodeId).masterNodeId(nodeId).add(masterNode).build())
            .build();

        ClusterChangedEvent event = new ClusterChangedEvent("_source", currentState, ClusterState.EMPTY_STATE);

        SetOnce<Boolean> ranPolicy = new SetOnce<>();
        SetOnce<Boolean> moveToMaintenance = new SetOnce<>();
        doAnswer(invocationOnMock -> {
            ranPolicy.set(true);
            throw new AssertionError("invalid invocation");
        }).when(clusterService).submitStateUpdateTask(anyString(), any(ExecuteStepsUpdateTask.class));

        doAnswer(invocationOnMock -> {
            OperationModeUpdateTask task = (OperationModeUpdateTask) invocationOnMock.getArguments()[1];
            assertThat(task.getOperationMode(), equalTo(OperationMode.STOPPED));
            moveToMaintenance.set(true);
            return null;
        }).when(clusterService).submitStateUpdateTask(anyString(), any(OperationModeUpdateTask.class));

        indexLifecycleService.clusterChanged(event);
        assertNull(ranPolicy.get());
        assertTrue(moveToMaintenance.get());
    }

    public void testTriggeredDifferentJob() {
        SchedulerEngine.Event schedulerEvent = new SchedulerEngine.Event("foo", randomLong(), randomLong());
        indexLifecycleService.triggered(schedulerEvent);
        Mockito.verifyZeroInteractions(indicesClient, clusterService);
    }
}
