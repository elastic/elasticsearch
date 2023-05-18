/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ilm.AsyncActionStep;
import org.elasticsearch.xpack.core.ilm.AsyncWaitStep;
import org.elasticsearch.xpack.core.ilm.ClusterStateActionStep;
import org.elasticsearch.xpack.core.ilm.ClusterStateWaitStep;
import org.elasticsearch.xpack.core.ilm.ErrorStep;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecycleAction;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyTests;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.MigrateAction;
import org.elasticsearch.xpack.core.ilm.MockAction;
import org.elasticsearch.xpack.core.ilm.MockStep;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.PhaseCompleteStep;
import org.elasticsearch.xpack.core.ilm.PhaseExecutionInfo;
import org.elasticsearch.xpack.core.ilm.RolloverAction;
import org.elasticsearch.xpack.core.ilm.RolloverActionTests;
import org.elasticsearch.xpack.core.ilm.Step;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;
import org.elasticsearch.xpack.core.ilm.TerminalPolicyStep;
import org.elasticsearch.xpack.core.ilm.WaitForRolloverReadyStep;
import org.elasticsearch.xpack.ilm.history.ILMHistoryItem;
import org.elasticsearch.xpack.ilm.history.ILMHistoryStore;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;

import static java.util.stream.Collectors.toList;
import static org.elasticsearch.cluster.metadata.LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.awaitLatch;
import static org.elasticsearch.xpack.core.ilm.LifecycleSettings.LIFECYCLE_HISTORY_INDEX_ENABLED_SETTING;
import static org.elasticsearch.xpack.ilm.LifecyclePolicyTestsUtils.newTestLifecyclePolicy;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

public class IndexLifecycleRunnerTests extends ESTestCase {
    private static final NamedXContentRegistry REGISTRY;
    private ThreadPool threadPool;
    private Client noopClient;
    private NoOpHistoryStore historyStore;

    static {
        try (IndexLifecycle indexLifecycle = new IndexLifecycle(Settings.EMPTY)) {
            List<NamedXContentRegistry.Entry> entries = new ArrayList<>(indexLifecycle.getNamedXContent());
            REGISTRY = new NamedXContentRegistry(entries);
        }
    }

    @Before
    public void prepare() {
        threadPool = new TestThreadPool("test");
        noopClient = new NoOpClient(threadPool);
        ClusterSettings settings = new ClusterSettings(
            Settings.EMPTY,
            Sets.union(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS, Set.of(LIFECYCLE_HISTORY_INDEX_ENABLED_SETTING))
        );
        historyStore = new NoOpHistoryStore(noopClient, ClusterServiceUtils.createClusterService(threadPool, settings));
    }

    @After
    public void shutdown() {
        historyStore.close();
        noopClient.close();
        threadPool.shutdownNow();
    }

    public void testRunPolicyTerminalPolicyStep() {
        String policyName = "async_action_policy";
        TerminalPolicyStep step = TerminalPolicyStep.INSTANCE;
        PolicyStepsRegistry stepRegistry = createOneStepPolicyStepRegistry(policyName, step);
        ClusterService clusterService = mock(ClusterService.class);
        IndexLifecycleRunner runner = new IndexLifecycleRunner(stepRegistry, historyStore, clusterService, threadPool, () -> 0L);
        IndexMetadata indexMetadata = IndexMetadata.builder("my_index")
            .settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();

        runner.runPolicyAfterStateChange(policyName, indexMetadata);

        Mockito.verify(clusterService, times(1)).createTaskQueue(anyString(), any(), any());
        Mockito.verifyNoMoreInteractions(clusterService);
    }

    public void testRunPolicyPhaseCompletePolicyStep() {
        String policyName = "async_action_policy";
        PhaseCompleteStep step = PhaseCompleteStep.finalStep(randomAlphaOfLength(4));
        PolicyStepsRegistry stepRegistry = createOneStepPolicyStepRegistry(policyName, step);
        ClusterService clusterService = mock(ClusterService.class);
        IndexLifecycleRunner runner = new IndexLifecycleRunner(stepRegistry, historyStore, clusterService, threadPool, () -> 0L);
        IndexMetadata indexMetadata = IndexMetadata.builder("my_index")
            .settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();

        runner.runPolicyAfterStateChange(policyName, indexMetadata);
        runner.runPeriodicStep(policyName, Metadata.builder().put(indexMetadata, true).build(), indexMetadata);

        Mockito.verify(clusterService, times(1)).createTaskQueue(anyString(), any(), any());
        Mockito.verifyNoMoreInteractions(clusterService);
    }

    @SuppressWarnings("unchecked")
    private static MasterServiceTaskQueue<IndexLifecycleClusterStateUpdateTask> newMockTaskQueue(ClusterService clusterService) {
        final var masterServiceTaskQueue = mock(MasterServiceTaskQueue.class);
        when(clusterService.<IndexLifecycleClusterStateUpdateTask>createTaskQueue(eq("ilm-runner"), eq(Priority.NORMAL), any())).thenReturn(
            masterServiceTaskQueue
        );
        return masterServiceTaskQueue;
    }

    public void testRunPolicyPhaseCompleteWithMoreStepsPolicyStep() {
        String policyName = "async_action_policy";
        TerminalPolicyStep stop = TerminalPolicyStep.INSTANCE;
        PhaseCompleteStep step = new PhaseCompleteStep(new StepKey("cold", "complete", "complete"), stop.getKey());
        PolicyStepsRegistry stepRegistry = createOneStepPolicyStepRegistry(policyName, step);
        ClusterService clusterService = mock(ClusterService.class);
        MasterServiceTaskQueue<IndexLifecycleClusterStateUpdateTask> taskQueue = newMockTaskQueue(clusterService);
        IndexLifecycleRunner runner = new IndexLifecycleRunner(stepRegistry, historyStore, clusterService, threadPool, () -> 0L);
        IndexMetadata indexMetadata = IndexMetadata.builder("my_index")
            .settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();

        runner.runPolicyAfterStateChange(policyName, indexMetadata);
        runner.runPeriodicStep(policyName, Metadata.builder().put(indexMetadata, true).build(), indexMetadata);

        Mockito.verify(taskQueue, times(1)).submitTask(anyString(), any(), any());
    }

    public void testRunPolicyErrorStep() {
        String policyName = "async_action_policy";
        LifecyclePolicy policy = LifecyclePolicyTests.randomTimeseriesLifecyclePolicyWithAllPhases(policyName);
        String phaseName = randomFrom(policy.getPhases().keySet());
        Phase phase = policy.getPhases().get(phaseName);
        PhaseExecutionInfo phaseExecutionInfo = new PhaseExecutionInfo(policy.getName(), phase, 1, randomNonNegativeLong());
        String phaseJson = Strings.toString(phaseExecutionInfo);
        LifecycleAction action = randomValueOtherThan(MigrateAction.DISABLED, () -> randomFrom(phase.getActions().values()));
        Step step = randomFrom(action.toSteps(new NoOpClient(threadPool), phaseName, null, null));
        StepKey stepKey = step.getKey();

        PolicyStepsRegistry stepRegistry = createOneStepPolicyStepRegistry(policyName, step);
        ClusterService clusterService = mock(ClusterService.class);
        IndexLifecycleRunner runner = new IndexLifecycleRunner(stepRegistry, historyStore, clusterService, threadPool, () -> 0L);
        LifecycleExecutionState.Builder newState = LifecycleExecutionState.builder();
        newState.setFailedStep(stepKey.name());
        newState.setIsAutoRetryableError(false);
        newState.setPhase(stepKey.phase());
        newState.setAction(stepKey.action());
        newState.setStep(ErrorStep.NAME);
        newState.setPhaseDefinition(phaseJson);
        IndexMetadata indexMetadata = IndexMetadata.builder("test")
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .putCustom(ILM_CUSTOM_METADATA_KEY, newState.build().asMap())
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();

        runner.runPolicyAfterStateChange(policyName, indexMetadata);

        Mockito.verify(clusterService).createTaskQueue(anyString(), any(Priority.class), any());
        Mockito.verifyNoMoreInteractions(clusterService);
    }

    public void testRunPolicyErrorStepOnRetryableFailedStep() {
        String policyName = "rollover_policy";
        String phaseName = "hot";
        TimeValue after = TimeValue.parseTimeValue(randomTimeValue(0, 1000000000, "s", "m", "h", "d"), "test_after");
        Map<String, LifecycleAction> actions = new HashMap<>();
        RolloverAction action = RolloverActionTests.randomInstance();
        actions.put(RolloverAction.NAME, action);
        Phase phase = new Phase(phaseName, after, actions);
        PhaseExecutionInfo phaseExecutionInfo = new PhaseExecutionInfo(policyName, phase, 1, randomNonNegativeLong());
        String phaseJson = Strings.toString(phaseExecutionInfo);
        NoOpClient client = new NoOpClient(threadPool);
        List<Step> waitForRolloverStepList = action.toSteps(client, phaseName, null)
            .stream()
            .filter(s -> s.getKey().name().equals(WaitForRolloverReadyStep.NAME))
            .collect(toList());
        assertThat(waitForRolloverStepList.size(), is(1));
        Step waitForRolloverStep = waitForRolloverStepList.get(0);
        StepKey stepKey = waitForRolloverStep.getKey();

        PolicyStepsRegistry stepRegistry = createOneStepPolicyStepRegistry(policyName, waitForRolloverStep);
        ClusterService clusterService = mock(ClusterService.class);
        MasterServiceTaskQueue<IndexLifecycleClusterStateUpdateTask> taskQueue = newMockTaskQueue(clusterService);
        when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE);
        IndexLifecycleRunner runner = new IndexLifecycleRunner(stepRegistry, historyStore, clusterService, threadPool, () -> 0L);
        LifecycleExecutionState.Builder newState = LifecycleExecutionState.builder();
        newState.setFailedStep(stepKey.name());
        newState.setIsAutoRetryableError(true);
        newState.setPhase(stepKey.phase());
        newState.setAction(stepKey.action());
        newState.setStep(ErrorStep.NAME);
        newState.setPhaseDefinition(phaseJson);
        IndexMetadata indexMetadata = IndexMetadata.builder("test")
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .putCustom(ILM_CUSTOM_METADATA_KEY, newState.build().asMap())
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();

        runner.runPeriodicStep(policyName, Metadata.builder().put(indexMetadata, true).build(), indexMetadata);

        Mockito.verify(taskQueue, times(1)).submitTask(anyString(), any(), any());
    }

    public void testRunStateChangePolicyWithNoNextStep() throws Exception {
        String policyName = "foo";
        StepKey stepKey = new StepKey("phase", "action", "cluster_state_action_step");
        MockClusterStateActionStep step = new MockClusterStateActionStep(stepKey, null);
        PolicyStepsRegistry stepRegistry = createOneStepPolicyStepRegistry(policyName, step);
        ThreadPool threadPool = new TestThreadPool("name");
        IndexMetadata indexMetadata = IndexMetadata.builder("test")
            .settings(indexSettings(Version.CURRENT, 1, 1).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .build();
        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
        DiscoveryNode node = clusterService.localNode();
        IndexLifecycleMetadata ilm = new IndexLifecycleMetadata(Collections.emptyMap(), OperationMode.RUNNING);
        ClusterState state = ClusterState.builder(new ClusterName("cluster"))
            .metadata(Metadata.builder().put(indexMetadata, true).putCustom(IndexLifecycleMetadata.TYPE, ilm))
            .nodes(DiscoveryNodes.builder().add(node).masterNodeId(node.getId()).localNodeId(node.getId()))
            .build();
        ClusterServiceUtils.setState(clusterService, state);
        IndexLifecycleRunner runner = new IndexLifecycleRunner(stepRegistry, historyStore, clusterService, threadPool, () -> 0L);

        ClusterState before = clusterService.state();
        CountDownLatch latch = new CountDownLatch(1);
        step.setLatch(latch);
        runner.runPolicyAfterStateChange(policyName, indexMetadata);

        awaitLatch(latch, 5, TimeUnit.SECONDS);
        ClusterState after = clusterService.state();

        assertEquals(before, after);
        assertThat(step.getExecuteCount(), equalTo(1L));
        ClusterServiceUtils.awaitNoPendingTasks(clusterService);
        clusterService.close();
        threadPool.shutdownNow();
    }

    public void testRunStateChangePolicyWithNextStep() throws Exception {
        String policyName = "foo";
        StepKey stepKey = new StepKey("phase", "action", "cluster_state_action_step");
        StepKey nextStepKey = new StepKey("phase", "action", "next_cluster_state_action_step");
        MockClusterStateActionStep step = new MockClusterStateActionStep(stepKey, nextStepKey);
        MockClusterStateActionStep nextStep = new MockClusterStateActionStep(nextStepKey, null);
        MockPolicyStepsRegistry stepRegistry = createMultiStepPolicyStepRegistry(policyName, Arrays.asList(step, nextStep));
        stepRegistry.setResolver((i, k) -> {
            if (stepKey.equals(k)) {
                return step;
            } else if (nextStepKey.equals(k)) {
                return nextStep;
            } else {
                fail("should not try to retrieve different step");
                return null;
            }
        });
        ThreadPool threadPool = new TestThreadPool("name");
        LifecycleExecutionState les = LifecycleExecutionState.builder()
            .setPhase("phase")
            .setAction("action")
            .setStep("cluster_state_action_step")
            .build();
        IndexMetadata indexMetadata = IndexMetadata.builder("test")
            .settings(indexSettings(Version.CURRENT, 1, 1).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .putCustom(LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY, les.asMap())
            .build();
        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
        DiscoveryNode node = clusterService.localNode();
        IndexLifecycleMetadata ilm = new IndexLifecycleMetadata(Collections.emptyMap(), OperationMode.RUNNING);
        ClusterState state = ClusterState.builder(new ClusterName("cluster"))
            .metadata(Metadata.builder().put(indexMetadata, true).putCustom(IndexLifecycleMetadata.TYPE, ilm))
            .nodes(DiscoveryNodes.builder().add(node).masterNodeId(node.getId()).localNodeId(node.getId()))
            .build();
        ClusterServiceUtils.setState(clusterService, state);
        long stepTime = randomLong();
        IndexLifecycleRunner runner = new IndexLifecycleRunner(stepRegistry, historyStore, clusterService, threadPool, () -> stepTime);

        ClusterState before = clusterService.state();
        CountDownLatch latch = new CountDownLatch(1);
        nextStep.setLatch(latch);
        runner.runPolicyAfterStateChange(policyName, indexMetadata);

        awaitLatch(latch, 5, TimeUnit.SECONDS);

        // The cluster state can take a few extra milliseconds to update after the steps are executed
        assertBusy(() -> assertNotEquals(before, clusterService.state()));
        LifecycleExecutionState newExecutionState = clusterService.state()
            .metadata()
            .index(indexMetadata.getIndex())
            .getLifecycleExecutionState();
        assertThat(newExecutionState.phase(), equalTo("phase"));
        assertThat(newExecutionState.action(), equalTo("action"));
        assertThat(newExecutionState.step(), equalTo("next_cluster_state_action_step"));
        assertThat(newExecutionState.stepTime(), equalTo(stepTime));
        assertThat(step.getExecuteCount(), equalTo(1L));
        assertThat(nextStep.getExecuteCount(), equalTo(1L));
        ClusterServiceUtils.awaitNoPendingTasks(clusterService);
        clusterService.close();
        threadPool.shutdownNow();

        ILMHistoryItem historyItem = historyStore.getItems()
            .stream()
            .findFirst()
            .orElseThrow(() -> new AssertionError("failed to register ILM history"));
        assertThat(historyItem.toString(), containsString(Strings.format("""
            {
              "index": "test",
              "policy": "foo",
              "@timestamp": %s,
              "success": true,
              "state": {
                "phase": "phase",
                "action": "action",
                "step": "next_cluster_state_action_step",
                "step_time": "%s"
              }
            }""", stepTime, stepTime).replaceAll("\\s", "")));
    }

    public void testRunPeriodicPolicyWithFailureToReadPolicy() throws Exception {
        doTestRunPolicyWithFailureToReadPolicy(false, true);
    }

    public void testRunStateChangePolicyWithFailureToReadPolicy() throws Exception {
        doTestRunPolicyWithFailureToReadPolicy(false, false);
    }

    public void testRunAsyncActionPolicyWithFailureToReadPolicy() throws Exception {
        doTestRunPolicyWithFailureToReadPolicy(true, false);
    }

    public void doTestRunPolicyWithFailureToReadPolicy(boolean asyncAction, boolean periodicAction) throws Exception {
        String policyName = "foo";
        StepKey stepKey = new StepKey("phase", "action", "cluster_state_action_step");
        StepKey nextStepKey = new StepKey("phase", "action", "next_cluster_state_action_step");
        MockClusterStateActionStep step = new MockClusterStateActionStep(stepKey, nextStepKey);
        MockClusterStateActionStep nextStep = new MockClusterStateActionStep(nextStepKey, null);
        MockPolicyStepsRegistry stepRegistry = createOneStepPolicyStepRegistry(policyName, step);
        AtomicBoolean resolved = new AtomicBoolean(false);
        stepRegistry.setResolver((i, k) -> {
            resolved.set(true);
            throw new IllegalArgumentException("fake failure retrieving step");
        });
        ThreadPool threadPool = new TestThreadPool("name");
        LifecycleExecutionState les = LifecycleExecutionState.builder()
            .setPhase("phase")
            .setAction("action")
            .setStep("cluster_state_action_step")
            .build();
        IndexMetadata indexMetadata = IndexMetadata.builder("test")
            .settings(indexSettings(Version.CURRENT, 1, 1).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .putCustom(LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY, les.asMap())
            .build();
        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
        DiscoveryNode node = clusterService.localNode();
        IndexLifecycleMetadata ilm = new IndexLifecycleMetadata(Collections.emptyMap(), OperationMode.RUNNING);
        ClusterState state = ClusterState.builder(new ClusterName("cluster"))
            .metadata(Metadata.builder().put(indexMetadata, true).putCustom(IndexLifecycleMetadata.TYPE, ilm))
            .nodes(DiscoveryNodes.builder().add(node).masterNodeId(node.getId()).localNodeId(node.getId()))
            .build();
        ClusterServiceUtils.setState(clusterService, state);
        long stepTime = randomLong();
        IndexLifecycleRunner runner = new IndexLifecycleRunner(stepRegistry, historyStore, clusterService, threadPool, () -> stepTime);

        ClusterState before = clusterService.state();
        if (asyncAction) {
            runner.maybeRunAsyncAction(before, indexMetadata, policyName, stepKey);
        } else if (periodicAction) {
            runner.runPeriodicStep(policyName, Metadata.builder().put(indexMetadata, true).build(), indexMetadata);
        } else {
            runner.runPolicyAfterStateChange(policyName, indexMetadata);
        }

        // The cluster state can take a few extra milliseconds to update after the steps are executed
        assertBusy(() -> assertNotEquals(before, clusterService.state()));
        LifecycleExecutionState newExecutionState = clusterService.state()
            .metadata()
            .index(indexMetadata.getIndex())
            .getLifecycleExecutionState();
        assertThat(newExecutionState.phase(), equalTo("phase"));
        assertThat(newExecutionState.action(), equalTo("action"));
        assertThat(newExecutionState.step(), equalTo("cluster_state_action_step"));
        assertThat(step.getExecuteCount(), equalTo(0L));
        assertThat(nextStep.getExecuteCount(), equalTo(0L));
        assertThat(
            newExecutionState.stepInfo(),
            containsString("{\"type\":\"illegal_argument_exception\",\"reason\":\"fake failure retrieving step\"}")
        );
        ClusterServiceUtils.awaitNoPendingTasks(clusterService);
        clusterService.close();
        threadPool.shutdownNow();
    }

    public void testRunAsyncActionDoesNotRun() {
        String policyName = "foo";
        StepKey stepKey = new StepKey("phase", "action", "async_action_step");
        MockAsyncActionStep step = new MockAsyncActionStep(stepKey, null);
        PolicyStepsRegistry stepRegistry = createOneStepPolicyStepRegistry(policyName, step);
        ThreadPool threadPool = new TestThreadPool("name");
        IndexMetadata indexMetadata = IndexMetadata.builder("test")
            .settings(indexSettings(Version.CURRENT, 1, 1).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .build();
        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
        DiscoveryNode node = clusterService.localNode();
        IndexLifecycleMetadata ilm = new IndexLifecycleMetadata(Collections.emptyMap(), OperationMode.RUNNING);
        ClusterState state = ClusterState.builder(new ClusterName("cluster"))
            .metadata(Metadata.builder().put(indexMetadata, true).putCustom(IndexLifecycleMetadata.TYPE, ilm))
            .nodes(DiscoveryNodes.builder().add(node).masterNodeId(node.getId()).localNodeId(node.getId()))
            .build();
        ClusterServiceUtils.setState(clusterService, state);
        IndexLifecycleRunner runner = new IndexLifecycleRunner(stepRegistry, historyStore, clusterService, threadPool, () -> 0L);

        ClusterState before = clusterService.state();
        // State changes should not run AsyncAction steps
        runner.runPolicyAfterStateChange(policyName, indexMetadata);

        ClusterState after = clusterService.state();

        assertEquals(before, after);
        assertThat(step.getExecuteCount(), equalTo(0L));
        ClusterServiceUtils.awaitNoPendingTasks(clusterService);
        clusterService.close();
        threadPool.shutdownNow();
    }

    public void testRunStateChangePolicyWithAsyncActionNextStep() throws Exception {
        String policyName = "foo";
        StepKey stepKey = new StepKey("phase", "action", "cluster_state_action_step");
        StepKey nextStepKey = new StepKey("phase", "action", "async_action_step");
        MockClusterStateActionStep step = new MockClusterStateActionStep(stepKey, nextStepKey);
        MockAsyncActionStep nextStep = new MockAsyncActionStep(nextStepKey, null);
        MockPolicyStepsRegistry stepRegistry = createMultiStepPolicyStepRegistry(policyName, Arrays.asList(step, nextStep));
        stepRegistry.setResolver((i, k) -> {
            if (stepKey.equals(k)) {
                return step;
            } else if (nextStepKey.equals(k)) {
                return nextStep;
            } else {
                fail("should not try to retrieve different step");
                return null;
            }
        });
        ThreadPool threadPool = new TestThreadPool("name");
        LifecycleExecutionState les = LifecycleExecutionState.builder()
            .setPhase("phase")
            .setAction("action")
            .setStep("cluster_state_action_step")
            .build();
        IndexMetadata indexMetadata = IndexMetadata.builder("test")
            .settings(indexSettings(Version.CURRENT, 1, 1).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .putCustom(LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY, les.asMap())
            .build();
        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
        DiscoveryNode node = clusterService.localNode();
        IndexLifecycleMetadata ilm = new IndexLifecycleMetadata(Collections.emptyMap(), OperationMode.RUNNING);
        ClusterState state = ClusterState.builder(new ClusterName("cluster"))
            .metadata(Metadata.builder().put(indexMetadata, true).putCustom(IndexLifecycleMetadata.TYPE, ilm))
            .nodes(DiscoveryNodes.builder().add(node).masterNodeId(node.getId()).localNodeId(node.getId()))
            .build();
        logger.info("--> state: {}", state);
        ClusterServiceUtils.setState(clusterService, state);
        IndexLifecycleRunner runner = new IndexLifecycleRunner(stepRegistry, historyStore, clusterService, threadPool, () -> 0L);

        ClusterState before = clusterService.state();
        CountDownLatch latch = new CountDownLatch(1);
        step.setLatch(latch);
        CountDownLatch asyncLatch = new CountDownLatch(1);
        nextStep.setLatch(asyncLatch);
        runner.runPolicyAfterStateChange(policyName, indexMetadata);

        // Wait for the cluster state action step
        awaitLatch(latch, 5, TimeUnit.SECONDS);
        // Wait for the async action step
        awaitLatch(asyncLatch, 5, TimeUnit.SECONDS);
        ClusterState after = clusterService.state();

        assertNotEquals(before, after);
        assertThat(step.getExecuteCount(), equalTo(1L));
        assertThat(nextStep.getExecuteCount(), equalTo(1L));
        ClusterServiceUtils.awaitNoPendingTasks(clusterService);
        clusterService.close();
        threadPool.shutdownNow();

        ILMHistoryItem historyItem = historyStore.getItems()
            .stream()
            .findFirst()
            .orElseThrow(() -> new AssertionError("failed to register ILM history"));
        assertThat(historyItem.toString(), containsString("""
            {
              "index": "test",
              "policy": "foo",
              "@timestamp": 0,
              "success": true,
              "state": {
                "phase": "phase",
                "action": "action",
                "step": "async_action_step",
                "step_time": "0"
              }
            }""".replaceAll("\\s", "")));
    }

    public void testRunPeriodicStep() throws Exception {
        String policyName = "foo";
        StepKey stepKey = new StepKey("phase", "action", "cluster_state_action_step");
        StepKey nextStepKey = new StepKey("phase", "action", "async_action_step");
        MockAsyncWaitStep step = new MockAsyncWaitStep(stepKey, nextStepKey);
        MockAsyncWaitStep nextStep = new MockAsyncWaitStep(nextStepKey, null);
        MockPolicyStepsRegistry stepRegistry = createOneStepPolicyStepRegistry(policyName, step);
        stepRegistry.setResolver((i, k) -> {
            if (stepKey.equals(k)) {
                return step;
            } else if (nextStepKey.equals(k)) {
                return nextStep;
            } else {
                fail("should not try to retrieve different step");
                return null;
            }
        });
        ThreadPool threadPool = new TestThreadPool("name");
        LifecycleExecutionState les = LifecycleExecutionState.builder()
            .setPhase("phase")
            .setAction("action")
            .setStep("cluster_state_action_step")
            .build();
        IndexMetadata indexMetadata = IndexMetadata.builder("test")
            .settings(indexSettings(Version.CURRENT, 1, 1).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .putCustom(LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY, les.asMap())
            .build();
        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
        DiscoveryNode node = clusterService.localNode();
        IndexLifecycleMetadata ilm = new IndexLifecycleMetadata(Collections.emptyMap(), OperationMode.RUNNING);
        ClusterState state = ClusterState.builder(new ClusterName("cluster"))
            .metadata(Metadata.builder().put(indexMetadata, true).putCustom(IndexLifecycleMetadata.TYPE, ilm))
            .nodes(DiscoveryNodes.builder().add(node).masterNodeId(node.getId()).localNodeId(node.getId()))
            .build();
        logger.info("--> state: {}", state);
        ClusterServiceUtils.setState(clusterService, state);
        IndexLifecycleRunner runner = new IndexLifecycleRunner(stepRegistry, historyStore, clusterService, threadPool, () -> 0L);

        ClusterState before = clusterService.state();
        CountDownLatch latch = new CountDownLatch(1);
        step.setLatch(latch);
        runner.runPeriodicStep(policyName, Metadata.builder().put(indexMetadata, true).build(), indexMetadata);
        awaitLatch(latch, 5, TimeUnit.SECONDS);

        ClusterState after = clusterService.state();

        assertEquals(before, after);
        assertThat(step.getExecuteCount(), equalTo(1L));
        assertThat(nextStep.getExecuteCount(), equalTo(0L));
        ClusterServiceUtils.awaitNoPendingTasks(clusterService);
        clusterService.close();
        threadPool.shutdownNow();
    }

    public void testRunPolicyClusterStateActionStep() {
        String policyName = "cluster_state_action_policy";
        StepKey stepKey = new StepKey("phase", "action", "cluster_state_action_step");
        MockClusterStateActionStep step = new MockClusterStateActionStep(stepKey, null);
        PolicyStepsRegistry stepRegistry = createOneStepPolicyStepRegistry(policyName, step);
        ClusterService clusterService = mock(ClusterService.class);
        MasterServiceTaskQueue<IndexLifecycleClusterStateUpdateTask> taskQueue = newMockTaskQueue(clusterService);
        IndexLifecycleRunner runner = new IndexLifecycleRunner(stepRegistry, historyStore, clusterService, threadPool, () -> 0L);
        IndexMetadata indexMetadata = IndexMetadata.builder("my_index")
            .settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();

        runner.runPolicyAfterStateChange(policyName, indexMetadata);

        final ExecuteStepsUpdateTaskMatcher taskMatcher = new ExecuteStepsUpdateTaskMatcher(indexMetadata.getIndex(), policyName, step);
        Mockito.verify(taskQueue, Mockito.times(1))
            .submitTask(
                Mockito.eq("""
                    ilm-execute-cluster-state-steps [{"phase":"phase","action":"action","name":"cluster_state_action_step"} => null]"""),
                Mockito.argThat(taskMatcher),
                Mockito.eq(null)
            );
        Mockito.verifyNoMoreInteractions(taskQueue);
        Mockito.verify(clusterService, Mockito.times(1)).createTaskQueue(any(), any(), any());
        Mockito.verifyNoMoreInteractions(clusterService);
    }

    public void testRunPolicyClusterStateWaitStep() {
        String policyName = "cluster_state_action_policy";
        StepKey stepKey = new StepKey("phase", "action", "cluster_state_action_step");
        MockClusterStateWaitStep step = new MockClusterStateWaitStep(stepKey, null);
        PolicyStepsRegistry stepRegistry = createOneStepPolicyStepRegistry(policyName, step);
        ClusterService clusterService = mock(ClusterService.class);
        MasterServiceTaskQueue<IndexLifecycleClusterStateUpdateTask> taskQueue = newMockTaskQueue(clusterService);
        IndexLifecycleRunner runner = new IndexLifecycleRunner(stepRegistry, historyStore, clusterService, threadPool, () -> 0L);
        IndexMetadata indexMetadata = IndexMetadata.builder("my_index")
            .settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();

        runner.runPolicyAfterStateChange(policyName, indexMetadata);

        final ExecuteStepsUpdateTaskMatcher taskMatcher = new ExecuteStepsUpdateTaskMatcher(indexMetadata.getIndex(), policyName, step);
        Mockito.verify(taskQueue, Mockito.times(1))
            .submitTask(
                Mockito.eq("""
                    ilm-execute-cluster-state-steps [{"phase":"phase","action":"action","name":"cluster_state_action_step"} => null]"""),
                Mockito.argThat(taskMatcher),
                Mockito.eq(null)
            );
        Mockito.verifyNoMoreInteractions(taskQueue);
        Mockito.verify(clusterService, Mockito.times(1)).createTaskQueue(any(), any(), any());
        Mockito.verifyNoMoreInteractions(clusterService);
    }

    public void testRunPolicyAsyncActionStepClusterStateChangeIgnored() {
        String policyName = "async_action_policy";
        StepKey stepKey = new StepKey("phase", "action", "async_action_step");
        MockAsyncActionStep step = new MockAsyncActionStep(stepKey, null);
        Exception expectedException = new RuntimeException();
        step.setException(expectedException);
        PolicyStepsRegistry stepRegistry = createOneStepPolicyStepRegistry(policyName, step);
        ClusterService clusterService = mock(ClusterService.class);
        IndexLifecycleRunner runner = new IndexLifecycleRunner(stepRegistry, historyStore, clusterService, threadPool, () -> 0L);
        IndexMetadata indexMetadata = IndexMetadata.builder("my_index")
            .settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();

        runner.runPolicyAfterStateChange(policyName, indexMetadata);

        assertEquals(0, step.getExecuteCount());
        Mockito.verify(clusterService, Mockito.times(1)).createTaskQueue(any(), any(), any());
        Mockito.verifyNoMoreInteractions(clusterService);
    }

    public void testRunPolicyAsyncWaitStepClusterStateChangeIgnored() {
        String policyName = "async_wait_policy";
        StepKey stepKey = new StepKey("phase", "action", "async_wait_step");
        MockAsyncWaitStep step = new MockAsyncWaitStep(stepKey, null);
        Exception expectedException = new RuntimeException();
        step.setException(expectedException);
        PolicyStepsRegistry stepRegistry = createOneStepPolicyStepRegistry(policyName, step);
        ClusterService clusterService = mock(ClusterService.class);
        IndexLifecycleRunner runner = new IndexLifecycleRunner(stepRegistry, historyStore, clusterService, threadPool, () -> 0L);
        IndexMetadata indexMetadata = IndexMetadata.builder("my_index")
            .settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();

        runner.runPolicyAfterStateChange(policyName, indexMetadata);

        assertEquals(0, step.getExecuteCount());
        Mockito.verify(clusterService, Mockito.times(1)).createTaskQueue(any(), any(), any());
        Mockito.verifyNoMoreInteractions(clusterService);
    }

    public void testRunPolicyThatDoesntExist() {
        String policyName = "cluster_state_action_policy";
        ClusterService clusterService = mock(ClusterService.class);
        MasterServiceTaskQueue<IndexLifecycleClusterStateUpdateTask> taskQueue = newMockTaskQueue(clusterService);
        IndexLifecycleRunner runner = new IndexLifecycleRunner(
            new PolicyStepsRegistry(NamedXContentRegistry.EMPTY, null, null),
            historyStore,
            clusterService,
            threadPool,
            () -> 0L
        );
        IndexMetadata indexMetadata = IndexMetadata.builder("my_index")
            .settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();
        // verify that no exception is thrown
        runner.runPolicyAfterStateChange(policyName, indexMetadata);
        final SetStepInfoUpdateTaskMatcher taskMatcher = new SetStepInfoUpdateTaskMatcher(
            indexMetadata.getIndex(),
            policyName,
            null,
            (builder, params) -> {
                builder.startObject();
                builder.field("reason", "policy [does_not_exist] does not exist");
                builder.field("type", "illegal_argument_exception");
                builder.endObject();
                return builder;
            }
        );
        Mockito.verify(taskQueue, Mockito.times(1))
            .submitTask(
                Mockito.eq("ilm-set-step-info {policy [cluster_state_action_policy], index [my_index], currentStep [null]}"),
                Mockito.argThat(taskMatcher),
                Mockito.eq(null)
            );
        Mockito.verifyNoMoreInteractions(taskQueue);
        Mockito.verify(clusterService, Mockito.times(1)).createTaskQueue(any(), any(), any());
        Mockito.verifyNoMoreInteractions(clusterService);
    }

    public void testGetCurrentStep() {
        String policyName = "policy";
        StepKey firstStepKey = new StepKey("phase_1", "action_1", "step_1");
        StepKey secondStepKey = new StepKey("phase_1", "action_1", "step_2");
        Step firstStep = new MockStep(firstStepKey, secondStepKey);
        Map<String, Step> firstStepMap = new HashMap<>();
        firstStepMap.put(policyName, firstStep);
        Map<String, Map<StepKey, Step>> stepMap = new HashMap<>();
        Index index = new Index("test", "uuid");

        Step.StepKey MOCK_STEP_KEY = new Step.StepKey("mock", "mock", "mock");
        Client client = mock(Client.class);
        when(client.settings()).thenReturn(Settings.EMPTY);
        LifecyclePolicy policy = LifecyclePolicyTests.randomTimeseriesLifecyclePolicyWithAllPhases(policyName);
        LifecyclePolicyMetadata policyMetadata = new LifecyclePolicyMetadata(policy, Collections.emptyMap(), 1, randomNonNegativeLong());
        String phaseName = randomFrom(policy.getPhases().keySet());
        Phase phase = policy.getPhases().get(phaseName);
        PhaseExecutionInfo pei = new PhaseExecutionInfo(policy.getName(), phase, 1, randomNonNegativeLong());
        String phaseJson = Strings.toString(pei);
        LifecycleAction action = randomValueOtherThan(MigrateAction.DISABLED, () -> randomFrom(phase.getActions().values()));
        Step step = randomFrom(action.toSteps(client, phaseName, MOCK_STEP_KEY, null));
        Settings indexSettings = indexSettings(Version.CURRENT, 1, 0).put(LifecycleSettings.LIFECYCLE_NAME, policyName).build();
        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhaseDefinition(phaseJson);
        lifecycleState.setPhase(step.getKey().phase());
        lifecycleState.setAction(step.getKey().action());
        lifecycleState.setStep(step.getKey().name());
        IndexMetadata indexMetadata = IndexMetadata.builder(index.getName())
            .settings(indexSettings)
            .putCustom(ILM_CUSTOM_METADATA_KEY, lifecycleState.build().asMap())
            .build();
        SortedMap<String, LifecyclePolicyMetadata> metas = new TreeMap<>();
        metas.put(policyName, policyMetadata);
        PolicyStepsRegistry registry = new PolicyStepsRegistry(metas, firstStepMap, stepMap, REGISTRY, client, null);

        // First step is retrieved because there are no settings for the index
        IndexMetadata indexMetadataWithNoKey = IndexMetadata.builder(index.getName())
            .settings(indexSettings)
            .putCustom(ILM_CUSTOM_METADATA_KEY, LifecycleExecutionState.builder().build().asMap())
            .build();
        Step stepFromNoSettings = IndexLifecycleRunner.getCurrentStep(registry, policy.getName(), indexMetadataWithNoKey);
        assertEquals(firstStep, stepFromNoSettings);

        // The step that was written into the metadata is retrieved
        Step currentStep = IndexLifecycleRunner.getCurrentStep(registry, policy.getName(), indexMetadata);
        assertEquals(step.getKey(), currentStep.getKey());
    }

    public void testIsReadyToTransition() {
        String policyName = "async_action_policy";
        StepKey stepKey = new StepKey("phase", MockAction.NAME, MockAction.NAME);
        MockAsyncActionStep step = new MockAsyncActionStep(stepKey, null);
        SortedMap<String, LifecyclePolicyMetadata> lifecyclePolicyMap = new TreeMap<>(
            Collections.singletonMap(
                policyName,
                new LifecyclePolicyMetadata(
                    createPolicy(policyName, null, step.getKey()),
                    new HashMap<>(),
                    randomNonNegativeLong(),
                    randomNonNegativeLong()
                )
            )
        );
        Map<String, Step> firstStepMap = Collections.singletonMap(policyName, step);
        Map<StepKey, Step> policySteps = Collections.singletonMap(step.getKey(), step);
        Map<String, Map<StepKey, Step>> stepMap = Collections.singletonMap(policyName, policySteps);
        PolicyStepsRegistry policyStepsRegistry = new PolicyStepsRegistry(
            lifecyclePolicyMap,
            firstStepMap,
            stepMap,
            NamedXContentRegistry.EMPTY,
            null,
            null
        );
        ClusterService clusterService = mock(ClusterService.class);
        final AtomicLong now = new AtomicLong(5);
        IndexLifecycleRunner runner = new IndexLifecycleRunner(policyStepsRegistry, historyStore, clusterService, threadPool, now::get);
        IndexMetadata indexMetadata = IndexMetadata.builder("my_index")
            .settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();
        // With no time, always transition
        assertTrue(
            "index should be able to transition with no creation date",
            runner.isReadyToTransitionToThisPhase(policyName, indexMetadata, "phase")
        );

        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setIndexCreationDate(10L);
        indexMetadata = IndexMetadata.builder(indexMetadata)
            .settings(Settings.builder().put(indexMetadata.getSettings()).build())
            .putCustom(ILM_CUSTOM_METADATA_KEY, lifecycleState.build().asMap())
            .build();
        // Index is not old enough to transition
        assertFalse(
            "index is not able to transition if it isn't old enough",
            runner.isReadyToTransitionToThisPhase(policyName, indexMetadata, "phase")
        );

        // Set to the fuuuuuttuuuuuuurre
        now.set(Long.MAX_VALUE);
        assertTrue(
            "index should be able to transition past phase's age",
            runner.isReadyToTransitionToThisPhase(policyName, indexMetadata, "phase")
        );

        // Come back to the "present"
        now.set(5L);
        indexMetadata = IndexMetadata.builder(indexMetadata)
            .settings(Settings.builder().put(indexMetadata.getSettings()).put(IndexSettings.LIFECYCLE_ORIGINATION_DATE, 3L).build())
            .putCustom(ILM_CUSTOM_METADATA_KEY, lifecycleState.build().asMap())
            .build();
        assertTrue(
            "index should be able to transition due to the origination date indicating it's old enough",
            runner.isReadyToTransitionToThisPhase(policyName, indexMetadata, "phase")
        );
    }

    private static LifecyclePolicy createPolicy(String policyName, StepKey safeStep, StepKey unsafeStep) {
        Map<String, Phase> phases = new HashMap<>();
        if (safeStep != null) {
            assert MockAction.NAME.equals(safeStep.action()) : "The safe action needs to be MockAction.NAME";
            assert unsafeStep == null || safeStep.phase().equals(unsafeStep.phase()) == false
                : "safe and unsafe actions must be in different phases";
            Map<String, LifecycleAction> actions = new HashMap<>();
            List<Step> steps = Collections.singletonList(new MockStep(safeStep, null));
            MockAction safeAction = new MockAction(steps, true);
            actions.put(safeAction.getWriteableName(), safeAction);
            Phase phase = new Phase(safeStep.phase(), TimeValue.timeValueMillis(0), actions);
            phases.put(phase.getName(), phase);
        }
        if (unsafeStep != null) {
            assert MockAction.NAME.equals(unsafeStep.action()) : "The unsafe action needs to be MockAction.NAME";
            Map<String, LifecycleAction> actions = new HashMap<>();
            List<Step> steps = Collections.singletonList(new MockStep(unsafeStep, null));
            MockAction unsafeAction = new MockAction(steps, false);
            actions.put(unsafeAction.getWriteableName(), unsafeAction);
            Phase phase = new Phase(unsafeStep.phase(), TimeValue.timeValueMillis(0), actions);
            phases.put(phase.getName(), phase);
        }
        return newTestLifecyclePolicy(policyName, phases);
    }

    public static void assertClusterStateOnNextStep(
        ClusterState oldClusterState,
        Index index,
        StepKey currentStep,
        StepKey nextStep,
        ClusterState newClusterState,
        long now
    ) {
        assertNotSame(oldClusterState, newClusterState);
        Metadata newMetadata = newClusterState.metadata();
        assertNotSame(oldClusterState.metadata(), newMetadata);
        IndexMetadata newIndexMetadata = newMetadata.getIndexSafe(index);
        assertNotSame(oldClusterState.metadata().index(index), newIndexMetadata);
        LifecycleExecutionState newLifecycleState = newClusterState.metadata().index(index).getLifecycleExecutionState();
        LifecycleExecutionState oldLifecycleState = oldClusterState.metadata().index(index).getLifecycleExecutionState();
        assertNotSame(oldLifecycleState, newLifecycleState);
        assertEquals(nextStep.phase(), newLifecycleState.phase());
        assertEquals(nextStep.action(), newLifecycleState.action());
        assertEquals(nextStep.name(), newLifecycleState.step());
        if (currentStep.phase().equals(nextStep.phase())) {
            assertEquals(oldLifecycleState.phaseTime(), newLifecycleState.phaseTime());
        } else {
            assertEquals(now, newLifecycleState.phaseTime().longValue());
        }
        if (currentStep.action().equals(nextStep.action())) {
            assertEquals(oldLifecycleState.actionTime(), newLifecycleState.actionTime());
        } else {
            assertEquals(now, newLifecycleState.actionTime().longValue());
        }
        assertEquals(now, newLifecycleState.stepTime().longValue());
        assertEquals(null, newLifecycleState.failedStep());
        assertEquals(null, newLifecycleState.stepInfo());
    }

    static class MockAsyncActionStep extends AsyncActionStep {

        private Exception exception;
        private boolean indexSurvives = true;
        private long executeCount = 0;
        private CountDownLatch latch;

        MockAsyncActionStep(StepKey key, StepKey nextStepKey) {
            super(key, nextStepKey, null);
        }

        @Override
        public boolean isRetryable() {
            return false;
        }

        void setException(Exception exception) {
            this.exception = exception;
        }

        @Override
        public boolean indexSurvives() {
            return indexSurvives;
        }

        long getExecuteCount() {
            return executeCount;
        }

        public void setLatch(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void performAction(
            IndexMetadata indexMetadata,
            ClusterState currentState,
            ClusterStateObserver observer,
            ActionListener<Void> listener
        ) {
            executeCount++;
            if (latch != null) {
                latch.countDown();
            }
            if (exception == null) {
                listener.onResponse(null);
            } else {
                listener.onFailure(exception);
            }
        }

    }

    static class MockAsyncWaitStep extends AsyncWaitStep {

        private Exception exception;
        private long executeCount = 0;
        private ToXContentObject expectedInfo = null;
        private CountDownLatch latch;

        MockAsyncWaitStep(StepKey key, StepKey nextStepKey) {
            super(key, nextStepKey, null);
        }

        @Override
        public boolean isRetryable() {
            return false;
        }

        void setException(Exception exception) {
            this.exception = exception;
        }

        long getExecuteCount() {
            return executeCount;
        }

        public void setLatch(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void evaluateCondition(Metadata metadata, Index index, Listener listener, TimeValue masterTimeout) {
            executeCount++;
            if (latch != null) {
                latch.countDown();
            }
            if (exception == null) {
                listener.onResponse(true, expectedInfo);
            } else {
                listener.onFailure(exception);
            }
        }

    }

    static class MockClusterStateActionStep extends ClusterStateActionStep {

        private RuntimeException exception;
        private long executeCount = 0;
        private CountDownLatch latch;

        MockClusterStateActionStep(StepKey key, StepKey nextStepKey) {
            super(key, nextStepKey);
        }

        @Override
        public boolean isRetryable() {
            return false;
        }

        public void setException(RuntimeException exception) {
            this.exception = exception;
        }

        public void setLatch(CountDownLatch latch) {
            this.latch = latch;
        }

        public long getExecuteCount() {
            return executeCount;
        }

        @Override
        public ClusterState performAction(Index index, ClusterState clusterState) {
            executeCount++;
            if (latch != null) {
                latch.countDown();
            }
            if (exception != null) {
                throw exception;
            }
            return clusterState;
        }
    }

    static class MockClusterStateWaitStep extends ClusterStateWaitStep {

        private RuntimeException exception;
        private boolean willComplete;
        private long executeCount = 0;
        private ToXContentObject expectedInfo = null;

        MockClusterStateWaitStep(StepKey key, StepKey nextStepKey) {
            super(key, nextStepKey);
        }

        @Override
        public boolean isRetryable() {
            return false;
        }

        public void setException(RuntimeException exception) {
            this.exception = exception;
        }

        public void setWillComplete(boolean willComplete) {
            this.willComplete = willComplete;
        }

        void expectedInfo(ToXContentObject expectedInfo) {
            this.expectedInfo = expectedInfo;
        }

        public long getExecuteCount() {
            return executeCount;
        }

        @Override
        public Result isConditionMet(Index index, ClusterState clusterState) {
            executeCount++;
            if (exception != null) {
                throw exception;
            }
            return new Result(willComplete, expectedInfo);
        }

    }

    private static class SetStepInfoUpdateTaskMatcher implements ArgumentMatcher<SetStepInfoUpdateTask> {

        private Index index;
        private String policy;
        private StepKey currentStepKey;
        private ToXContentObject stepInfo;

        SetStepInfoUpdateTaskMatcher(Index index, String policy, StepKey currentStepKey, ToXContentObject stepInfo) {
            this.index = index;
            this.policy = policy;
            this.currentStepKey = currentStepKey;
            this.stepInfo = stepInfo;
        }

        @Override
        public boolean matches(SetStepInfoUpdateTask other) {
            if (other == null) {
                return false;
            }
            return Objects.equals(index, other.getIndex())
                && Objects.equals(policy, other.getPolicy())
                && Objects.equals(currentStepKey, other.getCurrentStepKey())
                && Objects.equals(xContentToString(stepInfo), xContentToString(other.getStepInfo()));
        }

        private String xContentToString(ToXContentObject xContent) {
            try {
                XContentBuilder builder = JsonXContent.contentBuilder();
                stepInfo.toXContent(builder, ToXContent.EMPTY_PARAMS);
                return BytesReference.bytes(builder).utf8ToString();
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }

    }

    private static class ExecuteStepsUpdateTaskMatcher implements ArgumentMatcher<ExecuteStepsUpdateTask> {

        private Index index;
        private String policy;
        private Step startStep;

        ExecuteStepsUpdateTaskMatcher(Index index, String policy, Step startStep) {
            this.index = index;
            this.policy = policy;
            this.startStep = startStep;
        }

        @Override
        public boolean matches(ExecuteStepsUpdateTask other) {
            if (other == null) {
                return false;
            }
            return Objects.equals(index, other.getIndex())
                && Objects.equals(policy, other.getPolicy())
                && Objects.equals(startStep, other.getStartStep());
        }

    }

    static final class RetryableMockStep extends MockStep {

        RetryableMockStep(StepKey stepKey, StepKey nextStepKey) {
            super(stepKey, nextStepKey);
        }

        @Override
        public boolean isRetryable() {
            return true;
        }
    }

    /** A real policy steps registry where getStep can be overridden so that JSON doesn't have to be parsed */
    public static class MockPolicyStepsRegistry extends PolicyStepsRegistry {
        private BiFunction<IndexMetadata, StepKey, Step> fn = null;
        private static Logger logger = LogManager.getLogger(MockPolicyStepsRegistry.class);

        MockPolicyStepsRegistry(
            SortedMap<String, LifecyclePolicyMetadata> lifecyclePolicyMap,
            Map<String, Step> firstStepMap,
            Map<String, Map<StepKey, Step>> stepMap,
            NamedXContentRegistry xContentRegistry,
            Client client
        ) {
            super(lifecyclePolicyMap, firstStepMap, stepMap, xContentRegistry, client, null);
        }

        public void setResolver(BiFunction<IndexMetadata, StepKey, Step> resolver) {
            this.fn = resolver;
        }

        @Override
        public Step getStep(IndexMetadata indexMetadata, StepKey stepKey) {
            if (fn == null) {
                logger.info("--> retrieving step {}", stepKey);
                return super.getStep(indexMetadata, stepKey);
            } else {
                logger.info("--> returning mock step");
                return fn.apply(indexMetadata, stepKey);
            }
        }
    }

    public static MockPolicyStepsRegistry createOneStepPolicyStepRegistry(String policyName, Step step) {
        return createMultiStepPolicyStepRegistry(policyName, Collections.singletonList(step));
    }

    public static MockPolicyStepsRegistry createMultiStepPolicyStepRegistry(String policyName, List<Step> steps) {
        LifecyclePolicy policy = new LifecyclePolicy(policyName, new HashMap<>());
        SortedMap<String, LifecyclePolicyMetadata> lifecyclePolicyMap = new TreeMap<>();
        lifecyclePolicyMap.put(policyName, new LifecyclePolicyMetadata(policy, new HashMap<>(), 1, 1));
        Map<String, Step> firstStepMap = new HashMap<>();
        firstStepMap.put(policyName, steps.get(0));
        Map<String, Map<StepKey, Step>> stepMap = new HashMap<>();
        Map<StepKey, Step> policySteps = new HashMap<>();
        steps.forEach(step -> policySteps.put(step.getKey(), step));
        stepMap.put(policyName, policySteps);
        Client client = mock(Client.class);
        when(client.settings()).thenReturn(Settings.EMPTY);
        return new MockPolicyStepsRegistry(lifecyclePolicyMap, firstStepMap, stepMap, REGISTRY, client);
    }

    private static class NoOpHistoryStore extends ILMHistoryStore {
        private static final Logger logger = LogManager.getLogger(NoOpHistoryStore.class);

        private final List<ILMHistoryItem> items = new CopyOnWriteArrayList<>();

        NoOpHistoryStore(Client noopClient, ClusterService clusterService) {
            super(noopClient, clusterService, clusterService.threadPool());
        }

        public List<ILMHistoryItem> getItems() {
            return items;
        }

        @Override
        public void putAsync(ILMHistoryItem item) {
            logger.info("--> adding ILM history item: [{}]", item);
            items.add(item);
        }
    }
}
