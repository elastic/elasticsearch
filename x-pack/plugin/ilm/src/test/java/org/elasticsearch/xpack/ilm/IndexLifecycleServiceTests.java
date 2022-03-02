/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ilm;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.client.internal.AdminClient;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.IndicesAdminClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.component.Lifecycle.State;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.NodeRoles;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ilm.CheckShrinkReadyStep;
import org.elasticsearch.xpack.core.ilm.GenerateUniqueIndexNameStep;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.MockAction;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.SetSingleNodeAllocateStep;
import org.elasticsearch.xpack.core.ilm.ShrinkAction;
import org.elasticsearch.xpack.core.ilm.ShrinkStep;
import org.elasticsearch.xpack.core.ilm.ShrunkShardsAllocatedStep;
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
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static java.time.Clock.systemUTC;
import static org.elasticsearch.cluster.metadata.LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY;
import static org.elasticsearch.gateway.GatewayService.STATE_NOT_RECOVERED_BLOCK;
import static org.elasticsearch.xpack.core.ilm.AbstractStepTestCase.randomStepKey;
import static org.elasticsearch.xpack.ilm.LifecyclePolicyTestsUtils.newTestLifecyclePolicy;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
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
        masterNode = DiscoveryNode.createLocal(
            NodeRoles.masterNode(settings(Version.CURRENT).build()),
            new TransportAddress(TransportAddress.META_ADDRESS, 9300),
            nodeId
        );
        now = randomNonNegativeLong();
        Clock clock = Clock.fixed(Instant.ofEpochMilli(now), ZoneId.of(randomFrom(ZoneId.getAvailableZoneIds())));

        doAnswer(invocationOnMock -> null).when(clusterService).addListener(any());
        doAnswer(invocationOnMock -> {
            Runnable runnable = (Runnable) invocationOnMock.getArguments()[0];
            runnable.run();
            return null;
        }).when(executorService).execute(any());
        Settings settings = Settings.builder().put(LifecycleSettings.LIFECYCLE_POLL_INTERVAL, "1s").build();
        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(settings, Collections.singleton(LifecycleSettings.LIFECYCLE_POLL_INTERVAL_SETTING))
        );
        when(clusterService.lifecycleState()).thenReturn(State.STARTED);

        Client client = mock(Client.class);
        AdminClient adminClient = mock(AdminClient.class);
        indicesClient = mock(IndicesAdminClient.class);
        when(client.admin()).thenReturn(adminClient);
        when(adminClient.indices()).thenReturn(indicesClient);
        when(client.settings()).thenReturn(Settings.EMPTY);

        threadPool = new TestThreadPool("test");
        indexLifecycleService = new IndexLifecycleService(
            Settings.EMPTY,
            client,
            clusterService,
            threadPool,
            clock,
            () -> now,
            null,
            null,
            null
        );
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
        IndexLifecycleRunnerTests.MockClusterStateActionStep mockStep = new IndexLifecycleRunnerTests.MockClusterStateActionStep(
            randomStepKey(),
            randomStepKey()
        );
        MockAction mockAction = new MockAction(Collections.singletonList(mockStep));
        Phase phase = new Phase("phase", TimeValue.ZERO, Collections.singletonMap("action", mockAction));
        LifecyclePolicy policy = newTestLifecyclePolicy(policyName, Collections.singletonMap(phase.getName(), phase));
        SortedMap<String, LifecyclePolicyMetadata> policyMap = new TreeMap<>();
        policyMap.put(
            policyName,
            new LifecyclePolicyMetadata(policy, Collections.emptyMap(), randomNonNegativeLong(), randomNonNegativeLong())
        );
        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        IndexMetadata indexMetadata = IndexMetadata.builder(index.getName())
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();
        ImmutableOpenMap.Builder<String, IndexMetadata> indices = ImmutableOpenMap.<String, IndexMetadata>builder()
            .fPut(index.getName(), indexMetadata);
        Metadata metadata = Metadata.builder()
            .putCustom(IndexLifecycleMetadata.TYPE, new IndexLifecycleMetadata(policyMap, OperationMode.STOPPED))
            .indices(indices.build())
            .persistentSettings(settings(Version.CURRENT).build())
            .build();
        ClusterState currentState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
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
        IndexLifecycleRunnerTests.MockClusterStateActionStep mockStep = new IndexLifecycleRunnerTests.MockClusterStateActionStep(
            mockShrinkStep,
            randomStepKey()
        );
        MockAction mockAction = new MockAction(Collections.singletonList(mockStep));
        Phase phase = new Phase("phase", TimeValue.ZERO, Collections.singletonMap("action", mockAction));
        LifecyclePolicy policy = newTestLifecyclePolicy(policyName, Collections.singletonMap(phase.getName(), phase));
        SortedMap<String, LifecyclePolicyMetadata> policyMap = new TreeMap<>();
        policyMap.put(
            policyName,
            new LifecyclePolicyMetadata(policy, Collections.emptyMap(), randomNonNegativeLong(), randomNonNegativeLong())
        );
        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhase(mockShrinkStep.getPhase());
        lifecycleState.setAction(mockShrinkStep.getAction());
        lifecycleState.setStep(mockShrinkStep.getName());
        IndexMetadata indexMetadata = IndexMetadata.builder(index.getName())
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .putCustom(ILM_CUSTOM_METADATA_KEY, lifecycleState.build().asMap())
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();
        ImmutableOpenMap.Builder<String, IndexMetadata> indices = ImmutableOpenMap.<String, IndexMetadata>builder()
            .fPut(index.getName(), indexMetadata);
        Metadata metadata = Metadata.builder()
            .putCustom(IndexLifecycleMetadata.TYPE, new IndexLifecycleMetadata(policyMap, OperationMode.STOPPING))
            .indices(indices.build())
            .persistentSettings(settings(Version.CURRENT).build())
            .build();
        ClusterState currentState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .nodes(DiscoveryNodes.builder().localNodeId(nodeId).masterNodeId(nodeId).add(masterNode).build())
            .build();

        ClusterChangedEvent event = new ClusterChangedEvent("_source", currentState, ClusterState.EMPTY_STATE);
        SetOnce<Boolean> changedOperationMode = new SetOnce<>();
        doAnswer(invocationOnMock -> {
            changedOperationMode.set(true);
            return null;
        }).when(clusterService).submitStateUpdateTask(eq("ilm_operation_mode_update"), any(OperationModeUpdateTask.class), any());
        indexLifecycleService.applyClusterState(event);
        indexLifecycleService.triggerPolicies(currentState, true);
        assertNull(changedOperationMode.get());
    }

    public void testRequestedStopInShrinkActionButNotShrinkStep() {
        // test all the shrink action steps that ILM can be stopped during (basically all of them minus the actual shrink)
        ShrinkAction action = new ShrinkAction(1, null);
        action.toSteps(mock(Client.class), "warm", randomStepKey())
            .stream()
            .map(sk -> sk.getKey().getName())
            .filter(name -> name.equals(ShrinkStep.NAME) == false)
            .forEach(this::verifyCanStopWithStep);
    }

    // Check that ILM can stop when in the shrink action on the provided step
    private void verifyCanStopWithStep(String stoppableStep) {
        Step.StepKey mockShrinkStep = new Step.StepKey(randomAlphaOfLength(4), ShrinkAction.NAME, stoppableStep);
        String policyName = randomAlphaOfLengthBetween(1, 20);
        IndexLifecycleRunnerTests.MockClusterStateActionStep mockStep = new IndexLifecycleRunnerTests.MockClusterStateActionStep(
            mockShrinkStep,
            randomStepKey()
        );
        MockAction mockAction = new MockAction(Collections.singletonList(mockStep));
        Phase phase = new Phase("phase", TimeValue.ZERO, Collections.singletonMap("action", mockAction));
        LifecyclePolicy policy = newTestLifecyclePolicy(policyName, Collections.singletonMap(phase.getName(), phase));
        SortedMap<String, LifecyclePolicyMetadata> policyMap = new TreeMap<>();
        policyMap.put(
            policyName,
            new LifecyclePolicyMetadata(policy, Collections.emptyMap(), randomNonNegativeLong(), randomNonNegativeLong())
        );
        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhase(mockShrinkStep.getPhase());
        lifecycleState.setAction(mockShrinkStep.getAction());
        lifecycleState.setStep(mockShrinkStep.getName());
        IndexMetadata indexMetadata = IndexMetadata.builder(index.getName())
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .putCustom(ILM_CUSTOM_METADATA_KEY, lifecycleState.build().asMap())
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();
        ImmutableOpenMap.Builder<String, IndexMetadata> indices = ImmutableOpenMap.<String, IndexMetadata>builder()
            .fPut(index.getName(), indexMetadata);
        Metadata metadata = Metadata.builder()
            .putCustom(IndexLifecycleMetadata.TYPE, new IndexLifecycleMetadata(policyMap, OperationMode.STOPPING))
            .indices(indices.build())
            .persistentSettings(settings(Version.CURRENT).build())
            .build();
        ClusterState currentState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .nodes(DiscoveryNodes.builder().localNodeId(nodeId).masterNodeId(nodeId).add(masterNode).build())
            .build();

        ClusterChangedEvent event = new ClusterChangedEvent("_source", currentState, ClusterState.EMPTY_STATE);
        SetOnce<Boolean> changedOperationMode = new SetOnce<>();
        doAnswer(invocationOnMock -> {
            changedOperationMode.set(true);
            return null;
        }).when(clusterService)
            .submitStateUpdateTask(
                eq("ilm_operation_mode_update[stopped]"),
                eq(OperationModeUpdateTask.ilmMode(OperationMode.STOPPED)),
                any()
            );
        indexLifecycleService.applyClusterState(event);
        indexLifecycleService.triggerPolicies(currentState, true);
        assertTrue(changedOperationMode.get());
    }

    public void testRequestedStopOnSafeAction() {
        String policyName = randomAlphaOfLengthBetween(1, 20);
        Step.StepKey currentStepKey = randomStepKey();
        IndexLifecycleRunnerTests.MockClusterStateActionStep mockStep = new IndexLifecycleRunnerTests.MockClusterStateActionStep(
            currentStepKey,
            randomStepKey()
        );
        MockAction mockAction = new MockAction(Collections.singletonList(mockStep));
        Phase phase = new Phase("phase", TimeValue.ZERO, Collections.singletonMap("action", mockAction));
        LifecyclePolicy policy = newTestLifecyclePolicy(policyName, Collections.singletonMap(phase.getName(), phase));
        SortedMap<String, LifecyclePolicyMetadata> policyMap = new TreeMap<>();
        policyMap.put(
            policyName,
            new LifecyclePolicyMetadata(policy, Collections.emptyMap(), randomNonNegativeLong(), randomNonNegativeLong())
        );
        Index index = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhase(currentStepKey.getPhase());
        lifecycleState.setAction(currentStepKey.getAction());
        lifecycleState.setStep(currentStepKey.getName());
        IndexMetadata indexMetadata = IndexMetadata.builder(index.getName())
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .putCustom(ILM_CUSTOM_METADATA_KEY, lifecycleState.build().asMap())
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();
        ImmutableOpenMap.Builder<String, IndexMetadata> indices = ImmutableOpenMap.<String, IndexMetadata>builder()
            .fPut(index.getName(), indexMetadata);
        Metadata metadata = Metadata.builder()
            .putCustom(IndexLifecycleMetadata.TYPE, new IndexLifecycleMetadata(policyMap, OperationMode.STOPPING))
            .indices(indices.build())
            .persistentSettings(settings(Version.CURRENT).build())
            .build();
        ClusterState currentState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .nodes(DiscoveryNodes.builder().localNodeId(nodeId).masterNodeId(nodeId).add(masterNode).build())
            .build();

        ClusterChangedEvent event = new ClusterChangedEvent("_source", currentState, ClusterState.EMPTY_STATE);

        SetOnce<Boolean> ranPolicy = new SetOnce<>();
        SetOnce<Boolean> moveToMaintenance = new SetOnce<>();
        doAnswer(invocationOnMock -> {
            ranPolicy.set(true);
            throw new AssertionError("invalid invocation");
        }).when(clusterService).submitStateUpdateTask(anyString(), any(), eq(IndexLifecycleRunner.ILM_TASK_CONFIG), any());

        doAnswer(invocationOnMock -> {
            OperationModeUpdateTask task = (OperationModeUpdateTask) invocationOnMock.getArguments()[1];
            assertThat(task.getILMOperationMode(), equalTo(OperationMode.STOPPED));
            moveToMaintenance.set(true);
            return null;
        }).when(clusterService).submitStateUpdateTask(eq("ilm_operation_mode_update[stopped]"), any(OperationModeUpdateTask.class), any());

        indexLifecycleService.applyClusterState(event);
        indexLifecycleService.triggerPolicies(currentState, randomBoolean());
        assertNull(ranPolicy.get());
        assertTrue(moveToMaintenance.get());
    }

    public void testExceptionStillProcessesOtherIndices() {
        doTestExceptionStillProcessesOtherIndices(false);
    }

    public void testExceptionStillProcessesOtherIndicesOnMaster() {
        doTestExceptionStillProcessesOtherIndices(true);
    }

    @SuppressWarnings("unchecked")
    public void doTestExceptionStillProcessesOtherIndices(boolean useOnMaster) {
        String policy1 = randomAlphaOfLengthBetween(1, 20);
        Step.StepKey i1currentStepKey = randomStepKey();
        final Step i1mockStep;
        if (useOnMaster) {
            i1mockStep = new IndexLifecycleRunnerTests.MockAsyncActionStep(i1currentStepKey, randomStepKey());
        } else {
            i1mockStep = new IndexLifecycleRunnerTests.MockClusterStateActionStep(i1currentStepKey, randomStepKey());
        }
        MockAction i1mockAction = new MockAction(Collections.singletonList(i1mockStep));
        Phase i1phase = new Phase("phase", TimeValue.ZERO, Collections.singletonMap("action", i1mockAction));
        LifecyclePolicy i1policy = newTestLifecyclePolicy(policy1, Collections.singletonMap(i1phase.getName(), i1phase));
        Index index1 = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        LifecycleExecutionState.Builder i1lifecycleState = LifecycleExecutionState.builder();
        i1lifecycleState.setPhase(i1currentStepKey.getPhase());
        i1lifecycleState.setAction(i1currentStepKey.getAction());
        i1lifecycleState.setStep(i1currentStepKey.getName());

        String policy2 = randomValueOtherThan(policy1, () -> randomAlphaOfLengthBetween(1, 20));
        Step.StepKey i2currentStepKey = randomStepKey();
        final Step i2mockStep;
        if (useOnMaster) {
            i2mockStep = new IndexLifecycleRunnerTests.MockAsyncActionStep(i2currentStepKey, randomStepKey());
        } else {
            i2mockStep = new IndexLifecycleRunnerTests.MockClusterStateActionStep(i2currentStepKey, randomStepKey());
        }
        MockAction mockAction = new MockAction(Collections.singletonList(i2mockStep));
        Phase i2phase = new Phase("phase", TimeValue.ZERO, Collections.singletonMap("action", mockAction));
        LifecyclePolicy i2policy = newTestLifecyclePolicy(policy1, Collections.singletonMap(i2phase.getName(), i1phase));
        Index index2 = new Index(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        LifecycleExecutionState.Builder i2lifecycleState = LifecycleExecutionState.builder();
        i2lifecycleState.setPhase(i2currentStepKey.getPhase());
        i2lifecycleState.setAction(i2currentStepKey.getAction());
        i2lifecycleState.setStep(i2currentStepKey.getName());

        CountDownLatch stepLatch = new CountDownLatch(2);
        boolean failStep1 = randomBoolean();
        if (useOnMaster) {
            ((IndexLifecycleRunnerTests.MockAsyncActionStep) i1mockStep).setLatch(stepLatch);
            ((IndexLifecycleRunnerTests.MockAsyncActionStep) i1mockStep).setException(
                failStep1 ? new IllegalArgumentException("forcing a failure for index 1") : null
            );
            ((IndexLifecycleRunnerTests.MockAsyncActionStep) i2mockStep).setLatch(stepLatch);
            ((IndexLifecycleRunnerTests.MockAsyncActionStep) i2mockStep).setException(
                failStep1 ? null : new IllegalArgumentException("forcing a failure for index 2")
            );
        } else {
            ((IndexLifecycleRunnerTests.MockClusterStateActionStep) i1mockStep).setLatch(stepLatch);
            ((IndexLifecycleRunnerTests.MockClusterStateActionStep) i1mockStep).setException(
                failStep1 ? new IllegalArgumentException("forcing a failure for index 1") : null
            );
            ((IndexLifecycleRunnerTests.MockClusterStateActionStep) i1mockStep).setLatch(stepLatch);
            ((IndexLifecycleRunnerTests.MockClusterStateActionStep) i1mockStep).setException(
                failStep1 ? null : new IllegalArgumentException("forcing a failure for index 2")
            );
        }

        SortedMap<String, LifecyclePolicyMetadata> policyMap = new TreeMap<>();
        policyMap.put(
            policy1,
            new LifecyclePolicyMetadata(i1policy, Collections.emptyMap(), randomNonNegativeLong(), randomNonNegativeLong())
        );
        policyMap.put(
            policy2,
            new LifecyclePolicyMetadata(i2policy, Collections.emptyMap(), randomNonNegativeLong(), randomNonNegativeLong())
        );

        IndexMetadata i1indexMetadata = IndexMetadata.builder(index1.getName())
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, policy1))
            .putCustom(ILM_CUSTOM_METADATA_KEY, i1lifecycleState.build().asMap())
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();
        IndexMetadata i2indexMetadata = IndexMetadata.builder(index2.getName())
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, policy1))
            .putCustom(ILM_CUSTOM_METADATA_KEY, i2lifecycleState.build().asMap())
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();
        ImmutableOpenMap.Builder<String, IndexMetadata> indices = ImmutableOpenMap.<String, IndexMetadata>builder()
            .fPut(index1.getName(), i1indexMetadata)
            .fPut(index2.getName(), i2indexMetadata);

        Metadata metadata = Metadata.builder()
            .putCustom(IndexLifecycleMetadata.TYPE, new IndexLifecycleMetadata(policyMap, OperationMode.RUNNING))
            .indices(indices.build())
            .persistentSettings(settings(Version.CURRENT).build())
            .build();

        ClusterState currentState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .nodes(DiscoveryNodes.builder().localNodeId(nodeId).masterNodeId(nodeId).add(masterNode).build())
            .build();

        if (useOnMaster) {
            when(clusterService.state()).thenReturn(currentState);
            indexLifecycleService.onMaster(currentState);
        } else {
            indexLifecycleService.triggerPolicies(currentState, randomBoolean());
        }
        try {
            stepLatch.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.error("failure while waiting for step execution", e);
            fail("both steps should have been executed, even with an exception");
        }
    }

    public void testClusterChangedWaitsForTheStateToBeRecovered() {
        IndexLifecycleService ilmService = new IndexLifecycleService(
            Settings.EMPTY,
            mock(Client.class),
            clusterService,
            threadPool,
            systemUTC(),
            () -> now,
            null,
            null,
            null
        ) {

            @Override
            void onMaster(ClusterState clusterState) {
                fail("IndexLifecycleService ignored the global [state not recovered / initialized] cluster block");
            }

            @Override
            void triggerPolicies(ClusterState clusterState, boolean fromClusterStateChange) {
                fail("IndexLifecycleService ignored the global [state not recovered / initialized] cluster block");
            }
        };

        ClusterState currentState = ClusterState.builder(ClusterName.DEFAULT)
            .blocks(ClusterBlocks.builder().addGlobalBlock(STATE_NOT_RECOVERED_BLOCK).build())
            .build();
        ilmService.clusterChanged(new ClusterChangedEvent("_source", currentState, ClusterState.EMPTY_STATE));
    }

    public void testTriggeredDifferentJob() {
        Mockito.reset(clusterService);
        SchedulerEngine.Event schedulerEvent = new SchedulerEngine.Event("foo", randomLong(), randomLong());
        indexLifecycleService.triggered(schedulerEvent);
        Mockito.verifyNoMoreInteractions(indicesClient, clusterService);
    }

    public void testParsingOriginationDateBeforeIndexCreation() {
        Settings indexSettings = Settings.builder().put(LifecycleSettings.LIFECYCLE_PARSE_ORIGINATION_DATE, true).build();
        Index index = new Index("invalid_index_name", UUID.randomUUID().toString());
        expectThrows(
            IllegalArgumentException.class,
            "The parse origination date setting was configured for index "
                + index.getName()
                + " but the index name did not match the expected format",
            () -> indexLifecycleService.beforeIndexAddedToCluster(index, indexSettings)
        );

        // disabling the parsing origination date setting should prevent the validation from throwing exception
        try {
            indexLifecycleService.beforeIndexAddedToCluster(index, Settings.EMPTY);
        } catch (Exception e) {
            fail("Did not expect the before index validation to throw an exception as the parse origination date setting was not set");
        }
    }

    public void testIndicesOnShuttingDownNodesInDangerousStep() {
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).build();
        assertThat(IndexLifecycleService.indicesOnShuttingDownNodesInDangerousStep(state, "regular_node"), equalTo(Collections.emptySet()));
        assertThat(
            IndexLifecycleService.indicesOnShuttingDownNodesInDangerousStep(state, "shutdown_node"),
            equalTo(Collections.emptySet())
        );

        IndexMetadata nonDangerousIndex = IndexMetadata.builder("no_danger")
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, "mypolicy"))
            .putCustom(
                ILM_CUSTOM_METADATA_KEY,
                LifecycleExecutionState.builder()
                    .setPhase("warm")
                    .setAction("shrink")
                    .setStep(GenerateUniqueIndexNameStep.NAME)
                    .build()
                    .asMap()
            )
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();
        IndexMetadata dangerousIndex = IndexMetadata.builder("danger")
            .settings(
                settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, "mypolicy")
                    .put(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "_id", "shutdown_node")
            )
            .putCustom(
                ILM_CUSTOM_METADATA_KEY,
                LifecycleExecutionState.builder()
                    .setPhase("warm")
                    .setAction("shrink")
                    .setStep(
                        randomFrom(
                            SetSingleNodeAllocateStep.NAME,
                            CheckShrinkReadyStep.NAME,
                            ShrinkStep.NAME,
                            ShrunkShardsAllocatedStep.NAME
                        )
                    )
                    .build()
                    .asMap()
            )
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();
        ImmutableOpenMap.Builder<String, IndexMetadata> indices = ImmutableOpenMap.<String, IndexMetadata>builder()
            .fPut("no_danger", nonDangerousIndex)
            .fPut("danger", dangerousIndex);

        Metadata metadata = Metadata.builder()
            .putCustom(IndexLifecycleMetadata.TYPE, new IndexLifecycleMetadata(Collections.emptyMap(), OperationMode.RUNNING))
            .indices(indices.build())
            .persistentSettings(settings(Version.CURRENT).build())
            .build();

        state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .nodes(
                DiscoveryNodes.builder()
                    .localNodeId(nodeId)
                    .masterNodeId(nodeId)
                    .add(masterNode)
                    .add(
                        DiscoveryNode.createLocal(
                            NodeRoles.masterNode(settings(Version.CURRENT).build()),
                            new TransportAddress(TransportAddress.META_ADDRESS, 9301),
                            "regular_node"
                        )
                    )
                    .add(
                        DiscoveryNode.createLocal(
                            NodeRoles.masterNode(settings(Version.CURRENT).build()),
                            new TransportAddress(TransportAddress.META_ADDRESS, 9302),
                            "shutdown_node"
                        )
                    )
                    .build()
            )
            .build();

        // No danger yet, because no node is shutting down
        assertThat(IndexLifecycleService.indicesOnShuttingDownNodesInDangerousStep(state, "regular_node"), equalTo(Collections.emptySet()));
        assertThat(
            IndexLifecycleService.indicesOnShuttingDownNodesInDangerousStep(state, "shutdown_node"),
            equalTo(Collections.emptySet())
        );

        state = ClusterState.builder(state)
            .metadata(
                Metadata.builder(state.metadata())
                    .putCustom(
                        NodesShutdownMetadata.TYPE,
                        new NodesShutdownMetadata(
                            Collections.singletonMap(
                                "shutdown_node",
                                SingleNodeShutdownMetadata.builder()
                                    .setNodeId("shutdown_node")
                                    .setReason("shut down for test")
                                    .setStartedAtMillis(randomNonNegativeLong())
                                    .setType(SingleNodeShutdownMetadata.Type.RESTART)
                                    .build()
                            )
                        )
                    )
                    .build()
            )
            .build();

        assertThat(IndexLifecycleService.indicesOnShuttingDownNodesInDangerousStep(state, "regular_node"), equalTo(Collections.emptySet()));
        // No danger, because this is a "RESTART" type shutdown
        assertThat(
            "restart type shutdowns are not considered dangerous",
            IndexLifecycleService.indicesOnShuttingDownNodesInDangerousStep(state, "shutdown_node"),
            equalTo(Collections.emptySet())
        );

        final SingleNodeShutdownMetadata.Type type = randomFrom(
            SingleNodeShutdownMetadata.Type.REMOVE,
            SingleNodeShutdownMetadata.Type.REPLACE
        );
        final String targetNodeName = type == SingleNodeShutdownMetadata.Type.REPLACE ? randomAlphaOfLengthBetween(10, 20) : null;
        state = ClusterState.builder(state)
            .metadata(
                Metadata.builder(state.metadata())
                    .putCustom(
                        NodesShutdownMetadata.TYPE,
                        new NodesShutdownMetadata(
                            Collections.singletonMap(
                                "shutdown_node",
                                SingleNodeShutdownMetadata.builder()
                                    .setNodeId("shutdown_node")
                                    .setReason("shut down for test")
                                    .setStartedAtMillis(randomNonNegativeLong())
                                    .setType(type)
                                    .setTargetNodeName(targetNodeName)
                                    .build()
                            )
                        )
                    )
                    .build()
            )
            .build();

        // The dangerous index should be calculated as being in danger now
        assertThat(
            IndexLifecycleService.indicesOnShuttingDownNodesInDangerousStep(state, "shutdown_node"),
            equalTo(Collections.singleton("danger"))
        );
    }
}
