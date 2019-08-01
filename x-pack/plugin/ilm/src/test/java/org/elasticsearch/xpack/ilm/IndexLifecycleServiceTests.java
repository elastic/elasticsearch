/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ilm;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.component.Lifecycle.State;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecycleExecutionState;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.MockAction;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.ShrinkAction;
import org.elasticsearch.xpack.core.ilm.ShrinkStep;
import org.elasticsearch.xpack.core.ilm.Step;
import org.elasticsearch.xpack.core.scheduler.SchedulerEngine;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Collections;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;

import static org.elasticsearch.node.Node.NODE_MASTER_SETTING;
import static org.elasticsearch.xpack.core.ilm.AbstractStepTestCase.randomStepKey;
import static org.elasticsearch.xpack.core.ilm.LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY;
import static org.elasticsearch.xpack.core.ilm.LifecyclePolicyTestsUtils.newTestLifecyclePolicy;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IndexLifecycleServiceTests extends ESTestCase {

    private ClusterService clusterService;
    private IndexLifecycleService indexLifecycleService;
    private String nodeId;
    private DiscoveryNode masterNode;
    private IndicesAdminClient indicesClient;
    private long now;
    private ThreadPool threadPool;

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
        Settings settings = Settings.builder().put(LifecycleSettings.LIFECYCLE_POLL_INTERVAL, "1s").build();
        when(clusterService.getClusterSettings()).thenReturn(new ClusterSettings(settings,
            Collections.singleton(LifecycleSettings.LIFECYCLE_POLL_INTERVAL_SETTING)));
        when(clusterService.lifecycleState()).thenReturn(State.STARTED);

        Client client = mock(Client.class);
        AdminClient adminClient = mock(AdminClient.class);
        indicesClient = mock(IndicesAdminClient.class);
        when(client.admin()).thenReturn(adminClient);
        when(adminClient.indices()).thenReturn(indicesClient);
        when(client.settings()).thenReturn(Settings.EMPTY);

        threadPool = new TestThreadPool("test");
        indexLifecycleService = new IndexLifecycleService(Settings.EMPTY, client, clusterService, threadPool,
            clock, () -> now, null);
        Mockito.verify(clusterService).addListener(indexLifecycleService);
        Mockito.verify(clusterService).addStateApplier(indexLifecycleService);
    }

    @After
    public void cleanup() {
        when(clusterService.lifecycleState()).thenReturn(randomFrom(State.STOPPED, State.CLOSED));
        indexLifecycleService.close();
        threadPool.shutdownNow();
    }


    public void testStoppedModeSkip() {
        String policyName = randomAlphaOfLengthBetween(1, 20);
        IndexLifecycleRunnerTests.MockClusterStateActionStep mockStep =
            new IndexLifecycleRunnerTests.MockClusterStateActionStep(randomStepKey(), randomStepKey());
        MockAction mockAction = new MockAction(Collections.singletonList(mockStep));
        Phase phase = new Phase("phase", TimeValue.ZERO, Collections.singletonMap("action", mockAction));
        LifecyclePolicy policy = newTestLifecyclePolicy(policyName, Collections.singletonMap(phase.getName(), phase));
        SortedMap<String, LifecyclePolicyMetadata> policyMap = new TreeMap<>();
        policyMap.put(policyName, new LifecyclePolicyMetadata(policy, Collections.emptyMap(),
            randomNonNegativeLong(), randomNonNegativeLong()));
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
        ClusterChangedEvent event = new ClusterChangedEvent("_source", currentState, ClusterState.EMPTY_STATE);
        indexLifecycleService.applyClusterState(event);
        indexLifecycleService.triggerPolicies(currentState, randomBoolean());
        assertThat(mockStep.getExecuteCount(), equalTo(0L));
    }

    public void testRequestedStopOnShrink() {
        Step.StepKey mockShrinkStep = new Step.StepKey(randomAlphaOfLength(4), ShrinkAction.NAME, ShrinkStep.NAME);
        String policyName = randomAlphaOfLengthBetween(1, 20);
        IndexLifecycleRunnerTests.MockClusterStateActionStep mockStep =
            new IndexLifecycleRunnerTests.MockClusterStateActionStep(mockShrinkStep, randomStepKey());
        MockAction mockAction = new MockAction(Collections.singletonList(mockStep));
        Phase phase = new Phase("phase", TimeValue.ZERO, Collections.singletonMap("action", mockAction));
        LifecyclePolicy policy = newTestLifecyclePolicy(policyName, Collections.singletonMap(phase.getName(), phase));
        SortedMap<String, LifecyclePolicyMetadata> policyMap = new TreeMap<>();
        policyMap.put(policyName, new LifecyclePolicyMetadata(policy, Collections.emptyMap(),
            randomNonNegativeLong(), randomNonNegativeLong()));
        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhase(mockShrinkStep.getPhase());
        lifecycleState.setAction(mockShrinkStep.getAction());
        lifecycleState.setStep(mockShrinkStep.getName());
        IndexMetaData indexMetadata = IndexMetaData.builder(index.getName())
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME_SETTING.getKey(), policyName))
            .putCustom(ILM_CUSTOM_METADATA_KEY, lifecycleState.build().asMap())
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
        SetOnce<Boolean> changedOperationMode = new SetOnce<>();
        doAnswer(invocationOnMock -> {
            changedOperationMode.set(true);
            return null;
        }).when(clusterService).submitStateUpdateTask(eq("ilm_operation_mode_update"), any(OperationModeUpdateTask.class));
        indexLifecycleService.applyClusterState(event);
        indexLifecycleService.triggerPolicies(currentState, true);
        assertNull(changedOperationMode.get());
    }

    public void testRequestedStopInShrinkActionButNotShrinkStep() {
        // test all the shrink action steps that ILM can be stopped during (basically all of them minus the actual shrink)
        ShrinkAction action = new ShrinkAction(1);
        action.toSteps(mock(Client.class), "warm", randomStepKey()).stream()
            .map(sk -> sk.getKey().getName())
            .filter(name -> name.equals(ShrinkStep.NAME) == false)
            .forEach(this::verifyCanStopWithStep);
    }

    // Check that ILM can stop when in the shrink action on the provided step
    private void verifyCanStopWithStep(String stoppableStep) {
        Step.StepKey mockShrinkStep = new Step.StepKey(randomAlphaOfLength(4), ShrinkAction.NAME, stoppableStep);
        String policyName = randomAlphaOfLengthBetween(1, 20);
        IndexLifecycleRunnerTests.MockClusterStateActionStep mockStep =
            new IndexLifecycleRunnerTests.MockClusterStateActionStep(mockShrinkStep, randomStepKey());
        MockAction mockAction = new MockAction(Collections.singletonList(mockStep));
        Phase phase = new Phase("phase", TimeValue.ZERO, Collections.singletonMap("action", mockAction));
        LifecyclePolicy policy = newTestLifecyclePolicy(policyName, Collections.singletonMap(phase.getName(), phase));
        SortedMap<String, LifecyclePolicyMetadata> policyMap = new TreeMap<>();
        policyMap.put(policyName, new LifecyclePolicyMetadata(policy, Collections.emptyMap(),
            randomNonNegativeLong(), randomNonNegativeLong()));
        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhase(mockShrinkStep.getPhase());
        lifecycleState.setAction(mockShrinkStep.getAction());
        lifecycleState.setStep(mockShrinkStep.getName());
        IndexMetaData indexMetadata = IndexMetaData.builder(index.getName())
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME_SETTING.getKey(), policyName))
            .putCustom(ILM_CUSTOM_METADATA_KEY, lifecycleState.build().asMap())
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
        SetOnce<Boolean> changedOperationMode = new SetOnce<>();
        doAnswer(invocationOnMock -> {
            changedOperationMode.set(true);
            return null;
        }).when(clusterService).submitStateUpdateTask(eq("ilm_operation_mode_update"), any(OperationModeUpdateTask.class));
        indexLifecycleService.applyClusterState(event);
        indexLifecycleService.triggerPolicies(currentState, true);
        assertTrue(changedOperationMode.get());
    }

    public void testRequestedStopOnSafeAction() {
        String policyName = randomAlphaOfLengthBetween(1, 20);
        Step.StepKey currentStepKey = randomStepKey();
        IndexLifecycleRunnerTests.MockClusterStateActionStep mockStep =
            new IndexLifecycleRunnerTests.MockClusterStateActionStep(currentStepKey, randomStepKey());
        MockAction mockAction = new MockAction(Collections.singletonList(mockStep));
        Phase phase = new Phase("phase", TimeValue.ZERO, Collections.singletonMap("action", mockAction));
        LifecyclePolicy policy = newTestLifecyclePolicy(policyName, Collections.singletonMap(phase.getName(), phase));
        SortedMap<String, LifecyclePolicyMetadata> policyMap = new TreeMap<>();
        policyMap.put(policyName, new LifecyclePolicyMetadata(policy, Collections.emptyMap(),
            randomNonNegativeLong(), randomNonNegativeLong()));
        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhase(currentStepKey.getPhase());
        lifecycleState.setAction(currentStepKey.getAction());
        lifecycleState.setStep(currentStepKey.getName());
        IndexMetaData indexMetadata = IndexMetaData.builder(index.getName())
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME_SETTING.getKey(), policyName))
            .putCustom(ILM_CUSTOM_METADATA_KEY, lifecycleState.build().asMap())
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
        }).when(clusterService).submitStateUpdateTask(eq("ilm_operation_mode_update"), any(OperationModeUpdateTask.class));

        indexLifecycleService.applyClusterState(event);
        indexLifecycleService.triggerPolicies(currentState, randomBoolean());
        assertNull(ranPolicy.get());
        assertTrue(moveToMaintenance.get());
    }

    public void testTriggeredDifferentJob() {
        Mockito.reset(clusterService);
        SchedulerEngine.Event schedulerEvent = new SchedulerEngine.Event("foo", randomLong(), randomLong());
        indexLifecycleService.triggered(schedulerEvent);
        Mockito.verifyZeroInteractions(indicesClient, clusterService);
    }
}
