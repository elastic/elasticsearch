/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ilm.AsyncActionStep;
import org.elasticsearch.xpack.core.ilm.AsyncWaitStep;
import org.elasticsearch.xpack.core.ilm.ClusterStateActionStep;
import org.elasticsearch.xpack.core.ilm.ClusterStateWaitStep;
import org.elasticsearch.xpack.core.ilm.ErrorStep;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecycleAction;
import org.elasticsearch.xpack.core.ilm.LifecycleExecutionState;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyTests;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.MockAction;
import org.elasticsearch.xpack.core.ilm.MockStep;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.ilm.Phase;
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
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;

import static java.util.stream.Collectors.toList;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.awaitLatch;
import static org.elasticsearch.xpack.core.ilm.LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY;
import static org.elasticsearch.xpack.core.ilm.LifecyclePolicyTestsUtils.newTestLifecyclePolicy;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
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
        historyStore = new NoOpHistoryStore();
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
        IndexMetaData indexMetaData = IndexMetaData.builder("my_index").settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();

        runner.runPolicyAfterStateChange(policyName, indexMetaData);

        Mockito.verifyZeroInteractions(clusterService);
    }

    public void testRunPolicyErrorStep() {
        String policyName = "async_action_policy";
        LifecyclePolicy policy = LifecyclePolicyTests.randomTimeseriesLifecyclePolicyWithAllPhases(policyName);
        String phaseName = randomFrom(policy.getPhases().keySet());
        Phase phase = policy.getPhases().get(phaseName);
        PhaseExecutionInfo phaseExecutionInfo = new PhaseExecutionInfo(policy.getName(), phase, 1, randomNonNegativeLong());
        String phaseJson = Strings.toString(phaseExecutionInfo);
        LifecycleAction action = randomFrom(phase.getActions().values());
        Step step = randomFrom(action.toSteps(new NoOpClient(threadPool), phaseName, null));
        StepKey stepKey = step.getKey();

        PolicyStepsRegistry stepRegistry = createOneStepPolicyStepRegistry(policyName, step);
        ClusterService clusterService = mock(ClusterService.class);
        IndexLifecycleRunner runner = new IndexLifecycleRunner(stepRegistry, historyStore, clusterService, threadPool, () -> 0L);
        LifecycleExecutionState.Builder newState = LifecycleExecutionState.builder();
        newState.setFailedStep(stepKey.getName());
        newState.setIsAutoRetryableError(false);
        newState.setPhase(stepKey.getPhase());
        newState.setAction(stepKey.getAction());
        newState.setStep(ErrorStep.NAME);
        newState.setPhaseDefinition(phaseJson);
        IndexMetaData indexMetaData = IndexMetaData.builder("test")
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .putCustom(ILM_CUSTOM_METADATA_KEY, newState.build().asMap())
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5))
            .build();

        runner.runPolicyAfterStateChange(policyName, indexMetaData);

        Mockito.verifyZeroInteractions(clusterService);
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
        List<Step> waitForRolloverStepList =
            action.toSteps(client, phaseName, null).stream()
                .filter(s -> s.getKey().getName().equals(WaitForRolloverReadyStep.NAME))
                .collect(toList());
        assertThat(waitForRolloverStepList.size(), is(1));
        Step waitForRolloverStep = waitForRolloverStepList.get(0);
        StepKey stepKey = waitForRolloverStep.getKey();

        PolicyStepsRegistry stepRegistry = createOneStepPolicyStepRegistry(policyName, waitForRolloverStep);
        ClusterService clusterService = mock(ClusterService.class);
        IndexLifecycleRunner runner = new IndexLifecycleRunner(stepRegistry, historyStore, clusterService, threadPool, () -> 0L);
        LifecycleExecutionState.Builder newState = LifecycleExecutionState.builder();
        newState.setFailedStep(stepKey.getName());
        newState.setIsAutoRetryableError(true);
        newState.setPhase(stepKey.getPhase());
        newState.setAction(stepKey.getAction());
        newState.setStep(ErrorStep.NAME);
        newState.setPhaseDefinition(phaseJson);
        IndexMetaData indexMetaData = IndexMetaData.builder("test")
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .putCustom(ILM_CUSTOM_METADATA_KEY, newState.build().asMap())
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5))
            .build();

        runner.runPeriodicStep(policyName, indexMetaData);

        Mockito.verify(clusterService, times(1)).submitStateUpdateTask(any(), any());
    }

    public void testRunStateChangePolicyWithNoNextStep() throws Exception {
        String policyName = "foo";
        StepKey stepKey = new StepKey("phase", "action", "cluster_state_action_step");
        MockClusterStateActionStep step = new MockClusterStateActionStep(stepKey, null);
        PolicyStepsRegistry stepRegistry = createOneStepPolicyStepRegistry(policyName, step);
        ThreadPool threadPool = new TestThreadPool("name");
        IndexMetaData indexMetaData = IndexMetaData.builder("test")
            .settings(Settings.builder()
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .build();
        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
        DiscoveryNode node = clusterService.localNode();
        IndexLifecycleMetadata ilm = new IndexLifecycleMetadata(Collections.emptyMap(), OperationMode.RUNNING);
        ClusterState state = ClusterState.builder(new ClusterName("cluster"))
            .metaData(MetaData.builder()
                .put(indexMetaData, true)
                .putCustom(IndexLifecycleMetadata.TYPE, ilm))
            .nodes(DiscoveryNodes.builder()
                .add(node)
                .masterNodeId(node.getId())
                .localNodeId(node.getId()))
            .build();
        ClusterServiceUtils.setState(clusterService, state);
        IndexLifecycleRunner runner = new IndexLifecycleRunner(stepRegistry, historyStore, clusterService, threadPool, () -> 0L);

        ClusterState before = clusterService.state();
        CountDownLatch latch = new CountDownLatch(1);
        step.setLatch(latch);
        runner.runPolicyAfterStateChange(policyName, indexMetaData);

        awaitLatch(latch, 5, TimeUnit.SECONDS);
        ClusterState after = clusterService.state();

        assertEquals(before, after);
        assertThat(step.getExecuteCount(), equalTo(1L));
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
        IndexMetaData indexMetaData = IndexMetaData.builder("test")
            .settings(Settings.builder()
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .putCustom(LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY, les.asMap())
            .build();
        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
        DiscoveryNode node = clusterService.localNode();
        IndexLifecycleMetadata ilm = new IndexLifecycleMetadata(Collections.emptyMap(), OperationMode.RUNNING);
        ClusterState state = ClusterState.builder(new ClusterName("cluster"))
            .metaData(MetaData.builder()
                .put(indexMetaData, true)
                .putCustom(IndexLifecycleMetadata.TYPE, ilm))
            .nodes(DiscoveryNodes.builder()
                .add(node)
                .masterNodeId(node.getId())
                .localNodeId(node.getId()))
            .build();
        ClusterServiceUtils.setState(clusterService, state);
        long stepTime = randomLong();
        IndexLifecycleRunner runner = new IndexLifecycleRunner(stepRegistry, historyStore,
            clusterService, threadPool, () -> stepTime);

        ClusterState before = clusterService.state();
        CountDownLatch latch = new CountDownLatch(1);
        nextStep.setLatch(latch);
        runner.runPolicyAfterStateChange(policyName, indexMetaData);

        awaitLatch(latch, 5, TimeUnit.SECONDS);

        // The cluster state can take a few extra milliseconds to update after the steps are executed
        assertBusy(() -> assertNotEquals(before, clusterService.state()));
        LifecycleExecutionState newExecutionState = LifecycleExecutionState
            .fromIndexMetadata(clusterService.state().metaData().index(indexMetaData.getIndex()));
        assertThat(newExecutionState.getPhase(), equalTo("phase"));
        assertThat(newExecutionState.getAction(), equalTo("action"));
        assertThat(newExecutionState.getStep(), equalTo("next_cluster_state_action_step"));
        assertThat(newExecutionState.getStepTime(), equalTo(stepTime));
        assertThat(step.getExecuteCount(), equalTo(1L));
        assertThat(nextStep.getExecuteCount(), equalTo(1L));
        clusterService.close();
        threadPool.shutdownNow();

        ILMHistoryItem historyItem = historyStore.getItems().stream()
            .findFirst()
            .orElseThrow(() -> new AssertionError("failed to register ILM history"));
        assertThat(historyItem.toString(),
            containsString("{\"index\":\"test\",\"policy\":\"foo\",\"@timestamp\":" + stepTime +
                ",\"success\":true,\"state\":{\"phase\":\"phase\",\"action\":\"action\"," +
                "\"step\":\"next_cluster_state_action_step\",\"step_time\":\"" + stepTime + "\"}}"));
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
        IndexMetaData indexMetaData = IndexMetaData.builder("test")
            .settings(Settings.builder()
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .putCustom(LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY, les.asMap())
            .build();
        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
        DiscoveryNode node = clusterService.localNode();
        IndexLifecycleMetadata ilm = new IndexLifecycleMetadata(Collections.emptyMap(), OperationMode.RUNNING);
        ClusterState state = ClusterState.builder(new ClusterName("cluster"))
            .metaData(MetaData.builder()
                .put(indexMetaData, true)
                .putCustom(IndexLifecycleMetadata.TYPE, ilm))
            .nodes(DiscoveryNodes.builder()
                .add(node)
                .masterNodeId(node.getId())
                .localNodeId(node.getId()))
            .build();
        ClusterServiceUtils.setState(clusterService, state);
        long stepTime = randomLong();
        IndexLifecycleRunner runner = new IndexLifecycleRunner(stepRegistry, historyStore,
            clusterService, threadPool, () -> stepTime);

        ClusterState before = clusterService.state();
        if (asyncAction) {
            runner.maybeRunAsyncAction(before, indexMetaData, policyName, stepKey);
        } else if (periodicAction) {
            runner.runPeriodicStep(policyName, indexMetaData);
        } else {
            runner.runPolicyAfterStateChange(policyName, indexMetaData);
        }

        // The cluster state can take a few extra milliseconds to update after the steps are executed
        assertBusy(() -> assertNotEquals(before, clusterService.state()));
        LifecycleExecutionState newExecutionState = LifecycleExecutionState
            .fromIndexMetadata(clusterService.state().metaData().index(indexMetaData.getIndex()));
        assertThat(newExecutionState.getPhase(), equalTo("phase"));
        assertThat(newExecutionState.getAction(), equalTo("action"));
        assertThat(newExecutionState.getStep(), equalTo("cluster_state_action_step"));
        assertThat(step.getExecuteCount(), equalTo(0L));
        assertThat(nextStep.getExecuteCount(), equalTo(0L));
        assertThat(newExecutionState.getStepInfo(),
            containsString("{\"type\":\"illegal_argument_exception\",\"reason\":\"fake failure retrieving step\"}"));
        clusterService.close();
        threadPool.shutdownNow();
    }

    public void testRunAsyncActionDoesNotRun() {
        String policyName = "foo";
        StepKey stepKey = new StepKey("phase", "action", "async_action_step");
        MockAsyncActionStep step = new MockAsyncActionStep(stepKey, null);
        PolicyStepsRegistry stepRegistry = createOneStepPolicyStepRegistry(policyName, step);
        ThreadPool threadPool = new TestThreadPool("name");
        IndexMetaData indexMetaData = IndexMetaData.builder("test")
            .settings(Settings.builder()
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .build();
        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
        DiscoveryNode node = clusterService.localNode();
        IndexLifecycleMetadata ilm = new IndexLifecycleMetadata(Collections.emptyMap(), OperationMode.RUNNING);
        ClusterState state = ClusterState.builder(new ClusterName("cluster"))
            .metaData(MetaData.builder()
                .put(indexMetaData, true)
                .putCustom(IndexLifecycleMetadata.TYPE, ilm))
            .nodes(DiscoveryNodes.builder()
                .add(node)
                .masterNodeId(node.getId())
                .localNodeId(node.getId()))
            .build();
        ClusterServiceUtils.setState(clusterService, state);
        IndexLifecycleRunner runner = new IndexLifecycleRunner(stepRegistry, historyStore, clusterService, threadPool, () -> 0L);

        ClusterState before = clusterService.state();
        // State changes should not run AsyncAction steps
        runner.runPolicyAfterStateChange(policyName, indexMetaData);

        ClusterState after = clusterService.state();

        assertEquals(before, after);
        assertThat(step.getExecuteCount(), equalTo(0L));
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
        IndexMetaData indexMetaData = IndexMetaData.builder("test")
            .settings(Settings.builder()
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .putCustom(LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY, les.asMap())
            .build();
        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
        DiscoveryNode node = clusterService.localNode();
        IndexLifecycleMetadata ilm = new IndexLifecycleMetadata(Collections.emptyMap(), OperationMode.RUNNING);
        ClusterState state = ClusterState.builder(new ClusterName("cluster"))
            .metaData(MetaData.builder()
                .put(indexMetaData, true)
                .putCustom(IndexLifecycleMetadata.TYPE, ilm))
            .nodes(DiscoveryNodes.builder()
                .add(node)
                .masterNodeId(node.getId())
                .localNodeId(node.getId()))
            .build();
        logger.info("--> state: {}", state);
        ClusterServiceUtils.setState(clusterService, state);
        IndexLifecycleRunner runner = new IndexLifecycleRunner(stepRegistry, historyStore, clusterService, threadPool, () -> 0L);

        ClusterState before = clusterService.state();
        CountDownLatch latch = new CountDownLatch(1);
        step.setLatch(latch);
        CountDownLatch asyncLatch = new CountDownLatch(1);
        nextStep.setLatch(asyncLatch);
        runner.runPolicyAfterStateChange(policyName, indexMetaData);

        // Wait for the cluster state action step
        awaitLatch(latch, 5, TimeUnit.SECONDS);
        // Wait for the async action step
        awaitLatch(asyncLatch, 5, TimeUnit.SECONDS);
        ClusterState after = clusterService.state();

        assertNotEquals(before, after);
        assertThat(step.getExecuteCount(), equalTo(1L));
        assertThat(nextStep.getExecuteCount(), equalTo(1L));
        clusterService.close();
        threadPool.shutdownNow();

        ILMHistoryItem historyItem = historyStore.getItems().stream()
            .findFirst()
            .orElseThrow(() -> new AssertionError("failed to register ILM history"));
        assertThat(historyItem.toString(),
            containsString("{\"index\":\"test\",\"policy\":\"foo\",\"@timestamp\":0,\"success\":true," +
                "\"state\":{\"phase\":\"phase\",\"action\":\"action\",\"step\":\"async_action_step\",\"step_time\":\"0\"}}"));
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
        IndexMetaData indexMetaData = IndexMetaData.builder("test")
            .settings(Settings.builder()
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .putCustom(LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY, les.asMap())
            .build();
        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
        DiscoveryNode node = clusterService.localNode();
        IndexLifecycleMetadata ilm = new IndexLifecycleMetadata(Collections.emptyMap(), OperationMode.RUNNING);
        ClusterState state = ClusterState.builder(new ClusterName("cluster"))
            .metaData(MetaData.builder()
                .put(indexMetaData, true)
                .putCustom(IndexLifecycleMetadata.TYPE, ilm))
            .nodes(DiscoveryNodes.builder()
                .add(node)
                .masterNodeId(node.getId())
                .localNodeId(node.getId()))
            .build();
        logger.info("--> state: {}", state);
        ClusterServiceUtils.setState(clusterService, state);
        IndexLifecycleRunner runner = new IndexLifecycleRunner(stepRegistry, historyStore, clusterService, threadPool, () -> 0L);

        ClusterState before = clusterService.state();
        CountDownLatch latch = new CountDownLatch(1);
        step.setLatch(latch);
        runner.runPeriodicStep(policyName, indexMetaData);
        awaitLatch(latch, 5, TimeUnit.SECONDS);

        ClusterState after = clusterService.state();

        assertEquals(before, after);
        assertThat(step.getExecuteCount(), equalTo(1L));
        assertThat(nextStep.getExecuteCount(), equalTo(0L));
        clusterService.close();
        threadPool.shutdownNow();
    }

    public void testRunPolicyClusterStateActionStep() {
        String policyName = "cluster_state_action_policy";
        StepKey stepKey = new StepKey("phase", "action", "cluster_state_action_step");
        MockClusterStateActionStep step = new MockClusterStateActionStep(stepKey, null);
        PolicyStepsRegistry stepRegistry = createOneStepPolicyStepRegistry(policyName, step);
        ClusterService clusterService = mock(ClusterService.class);
        IndexLifecycleRunner runner = new IndexLifecycleRunner(stepRegistry, historyStore, clusterService, threadPool, () -> 0L);
        IndexMetaData indexMetaData = IndexMetaData.builder("my_index").settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();

        runner.runPolicyAfterStateChange(policyName, indexMetaData);

        Mockito.verify(clusterService, Mockito.times(1)).submitStateUpdateTask(Mockito.matches("ilm-execute-cluster-state-steps"),
                Mockito.argThat(new ExecuteStepsUpdateTaskMatcher(indexMetaData.getIndex(), policyName, step)));
        Mockito.verifyNoMoreInteractions(clusterService);
    }

    public void testRunPolicyClusterStateWaitStep() {
        String policyName = "cluster_state_action_policy";
        StepKey stepKey = new StepKey("phase", "action", "cluster_state_action_step");
        MockClusterStateWaitStep step = new MockClusterStateWaitStep(stepKey, null);
        step.setWillComplete(true);
        PolicyStepsRegistry stepRegistry = createOneStepPolicyStepRegistry(policyName, step);
        ClusterService clusterService = mock(ClusterService.class);
        IndexLifecycleRunner runner = new IndexLifecycleRunner(stepRegistry, historyStore, clusterService, threadPool, () -> 0L);
        IndexMetaData indexMetaData = IndexMetaData.builder("my_index").settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();

        runner.runPolicyAfterStateChange(policyName, indexMetaData);

        Mockito.verify(clusterService, Mockito.times(1)).submitStateUpdateTask(Mockito.matches("ilm-execute-cluster-state-steps"),
                Mockito.argThat(new ExecuteStepsUpdateTaskMatcher(indexMetaData.getIndex(), policyName, step)));
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
        IndexMetaData indexMetaData = IndexMetaData.builder("my_index").settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();

        runner.runPolicyAfterStateChange(policyName, indexMetaData);

        assertEquals(0, step.getExecuteCount());
        Mockito.verifyZeroInteractions(clusterService);
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
        IndexMetaData indexMetaData = IndexMetaData.builder("my_index").settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();

        runner.runPolicyAfterStateChange(policyName, indexMetaData);

        assertEquals(0, step.getExecuteCount());
        Mockito.verifyZeroInteractions(clusterService);
    }

    public void testRunPolicyThatDoesntExist() {
        String policyName = "cluster_state_action_policy";
        ClusterService clusterService = mock(ClusterService.class);
        IndexLifecycleRunner runner = new IndexLifecycleRunner(new PolicyStepsRegistry(NamedXContentRegistry.EMPTY, null),
            historyStore, clusterService, threadPool, () -> 0L);
        IndexMetaData indexMetaData = IndexMetaData.builder("my_index").settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        // verify that no exception is thrown
        runner.runPolicyAfterStateChange(policyName, indexMetaData);
        Mockito.verify(clusterService, Mockito.times(1)).submitStateUpdateTask(Mockito.matches("ilm-set-step-info"),
            Mockito.argThat(new SetStepInfoUpdateTaskMatcher(indexMetaData.getIndex(), policyName, null,
                (builder, params) -> {
                    builder.startObject();
                    builder.field("reason", "policy [does_not_exist] does not exist");
                    builder.field("type", "illegal_argument_exception");
                    builder.endObject();
                    return builder;
                })));
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
        LifecycleAction action = randomFrom(phase.getActions().values());
        Step step = randomFrom(action.toSteps(client, phaseName, MOCK_STEP_KEY));
        Settings indexSettings = Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .put("index.version.created", Version.CURRENT)
            .put(LifecycleSettings.LIFECYCLE_NAME, policyName)
            .build();
        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhaseDefinition(phaseJson);
        lifecycleState.setPhase(step.getKey().getPhase());
        lifecycleState.setAction(step.getKey().getAction());
        lifecycleState.setStep(step.getKey().getName());
        IndexMetaData indexMetaData = IndexMetaData.builder(index.getName())
            .settings(indexSettings)
            .putCustom(ILM_CUSTOM_METADATA_KEY, lifecycleState.build().asMap())
            .build();
        SortedMap<String, LifecyclePolicyMetadata> metas = new TreeMap<>();
        metas.put(policyName, policyMetadata);
        PolicyStepsRegistry registry = new PolicyStepsRegistry(metas, firstStepMap, stepMap, REGISTRY, client);

        // First step is retrieved because there are no settings for the index
        IndexMetaData indexMetaDataWithNoKey = IndexMetaData.builder(index.getName())
            .settings(indexSettings)
            .putCustom(ILM_CUSTOM_METADATA_KEY, LifecycleExecutionState.builder().build().asMap())
            .build();
        Step stepFromNoSettings = IndexLifecycleRunner.getCurrentStep(registry, policy.getName(), indexMetaDataWithNoKey);
        assertEquals(firstStep, stepFromNoSettings);

        // The step that was written into the metadata is retrieved
        Step currentStep = IndexLifecycleRunner.getCurrentStep(registry, policy.getName(), indexMetaData);
        assertEquals(step.getKey(), currentStep.getKey());
    }

    public void testIsReadyToTransition() {
        String policyName = "async_action_policy";
        StepKey stepKey = new StepKey("phase", MockAction.NAME, MockAction.NAME);
        MockAsyncActionStep step = new MockAsyncActionStep(stepKey, null);
        step.setWillComplete(true);
        SortedMap<String, LifecyclePolicyMetadata> lifecyclePolicyMap = new TreeMap<>(Collections.singletonMap(policyName,
            new LifecyclePolicyMetadata(createPolicy(policyName, null, step.getKey()), new HashMap<>(),
                randomNonNegativeLong(), randomNonNegativeLong())));
        Map<String, Step> firstStepMap = Collections.singletonMap(policyName, step);
        Map<StepKey, Step> policySteps = Collections.singletonMap(step.getKey(), step);
        Map<String, Map<StepKey, Step>> stepMap = Collections.singletonMap(policyName, policySteps);
        PolicyStepsRegistry policyStepsRegistry = new PolicyStepsRegistry(lifecyclePolicyMap, firstStepMap,
            stepMap, NamedXContentRegistry.EMPTY, null);
        ClusterService clusterService = mock(ClusterService.class);
        final AtomicLong now = new AtomicLong(5);
        IndexLifecycleRunner runner = new IndexLifecycleRunner(policyStepsRegistry, historyStore,
            clusterService, threadPool, now::get);
        IndexMetaData indexMetaData = IndexMetaData.builder("my_index").settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();
        // With no time, always transition
        assertTrue("index should be able to transition with no creation date",
            runner.isReadyToTransitionToThisPhase(policyName, indexMetaData, "phase"));

        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setIndexCreationDate(10L);
        indexMetaData = IndexMetaData.builder(indexMetaData)
            .settings(Settings.builder()
                .put(indexMetaData.getSettings())
                .build())
            .putCustom(ILM_CUSTOM_METADATA_KEY, lifecycleState.build().asMap())
            .build();
        // Index is not old enough to transition
        assertFalse("index is not able to transition if it isn't old enough",
            runner.isReadyToTransitionToThisPhase(policyName, indexMetaData, "phase"));

        // Set to the fuuuuuttuuuuuuurre
        now.set(Long.MAX_VALUE);
        assertTrue("index should be able to transition past phase's age",
            runner.isReadyToTransitionToThisPhase(policyName, indexMetaData, "phase"));

        // Come back to the "present"
        now.set(5L);
        indexMetaData = IndexMetaData.builder(indexMetaData)
            .settings(Settings.builder()
                .put(indexMetaData.getSettings())
                .put(LifecycleSettings.LIFECYCLE_ORIGINATION_DATE, 3L)
                .build())
            .putCustom(ILM_CUSTOM_METADATA_KEY, lifecycleState.build().asMap())
            .build();
        assertTrue("index should be able to transition due to the origination date indicating it's old enough",
            runner.isReadyToTransitionToThisPhase(policyName, indexMetaData, "phase"));
    }

    private static LifecyclePolicy createPolicy(String policyName, StepKey safeStep, StepKey unsafeStep) {
        Map<String, Phase> phases = new HashMap<>();
        if (safeStep != null) {
            assert MockAction.NAME.equals(safeStep.getAction()) : "The safe action needs to be MockAction.NAME";
            assert unsafeStep == null
                || safeStep.getPhase().equals(unsafeStep.getPhase()) == false : "safe and unsafe actions must be in different phases";
            Map<String, LifecycleAction> actions = new HashMap<>();
            List<Step> steps = Collections.singletonList(new MockStep(safeStep, null));
            MockAction safeAction = new MockAction(steps, true);
            actions.put(safeAction.getWriteableName(), safeAction);
            Phase phase = new Phase(safeStep.getPhase(), TimeValue.timeValueMillis(0), actions);
            phases.put(phase.getName(), phase);
        }
        if (unsafeStep != null) {
            assert MockAction.NAME.equals(unsafeStep.getAction()) : "The unsafe action needs to be MockAction.NAME";
            Map<String, LifecycleAction> actions = new HashMap<>();
            List<Step> steps = Collections.singletonList(new MockStep(unsafeStep, null));
            MockAction unsafeAction = new MockAction(steps, false);
            actions.put(unsafeAction.getWriteableName(), unsafeAction);
            Phase phase = new Phase(unsafeStep.getPhase(), TimeValue.timeValueMillis(0), actions);
            phases.put(phase.getName(), phase);
        }
        return newTestLifecyclePolicy(policyName, phases);
    }

    public static void assertClusterStateOnNextStep(ClusterState oldClusterState, Index index, StepKey currentStep, StepKey nextStep,
            ClusterState newClusterState, long now) {
        assertNotSame(oldClusterState, newClusterState);
        MetaData newMetadata = newClusterState.metaData();
        assertNotSame(oldClusterState.metaData(), newMetadata);
        IndexMetaData newIndexMetadata = newMetadata.getIndexSafe(index);
        assertNotSame(oldClusterState.metaData().index(index), newIndexMetadata);
        LifecycleExecutionState newLifecycleState = LifecycleExecutionState
            .fromIndexMetadata(newClusterState.metaData().index(index));
        LifecycleExecutionState oldLifecycleState = LifecycleExecutionState
            .fromIndexMetadata(oldClusterState.metaData().index(index));
        assertNotSame(oldLifecycleState, newLifecycleState);
        assertEquals(nextStep.getPhase(), newLifecycleState.getPhase());
        assertEquals(nextStep.getAction(), newLifecycleState.getAction());
        assertEquals(nextStep.getName(), newLifecycleState.getStep());
        if (currentStep.getPhase().equals(nextStep.getPhase())) {
            assertEquals(oldLifecycleState.getPhaseTime(), newLifecycleState.getPhaseTime());
        } else {
            assertEquals(now, newLifecycleState.getPhaseTime().longValue());
        }
        if (currentStep.getAction().equals(nextStep.getAction())) {
            assertEquals(oldLifecycleState.getActionTime(), newLifecycleState.getActionTime());
        } else {
            assertEquals(now, newLifecycleState.getActionTime().longValue());
        }
        assertEquals(now, newLifecycleState.getStepTime().longValue());
        assertEquals(null, newLifecycleState.getFailedStep());
        assertEquals(null, newLifecycleState.getStepInfo());
    }

    static class MockAsyncActionStep extends AsyncActionStep {

        private Exception exception;
        private boolean willComplete;
        private boolean indexSurvives = true;
        private long executeCount = 0;
        private CountDownLatch latch;

        MockAsyncActionStep(StepKey key, StepKey nextStepKey) {
            super(key, nextStepKey, null);
        }

        void setException(Exception exception) {
            this.exception = exception;
        }

        @Override
        public boolean indexSurvives() {
            return indexSurvives;
        }

        void setWillComplete(boolean willComplete) {
            this.willComplete = willComplete;
        }

        long getExecuteCount() {
            return executeCount;
        }

        public void setLatch(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void performAction(IndexMetaData indexMetaData, ClusterState currentState,
                                  ClusterStateObserver observer, Listener listener) {
            executeCount++;
            if (latch != null) {
                latch.countDown();
            }
            if (exception == null) {
                listener.onResponse(willComplete);
            } else {
                listener.onFailure(exception);
            }
        }

    }

    static class MockAsyncWaitStep extends AsyncWaitStep {

        private Exception exception;
        private boolean willComplete;
        private long executeCount = 0;
        private ToXContentObject expectedInfo = null;
        private CountDownLatch latch;

        MockAsyncWaitStep(StepKey key, StepKey nextStepKey) {
            super(key, nextStepKey, null);
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
        public void evaluateCondition(IndexMetaData indexMetaData, Listener listener) {
            executeCount++;
            if (latch != null) {
                latch.countDown();
            }
            if (exception == null) {
                listener.onResponse(willComplete, expectedInfo);
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

    private static class SetStepInfoUpdateTaskMatcher extends ArgumentMatcher<SetStepInfoUpdateTask> {

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
        public boolean matches(Object argument) {
            if (argument == null || argument instanceof SetStepInfoUpdateTask == false) {
                return false;
            }
            SetStepInfoUpdateTask task = (SetStepInfoUpdateTask) argument;
            return Objects.equals(index, task.getIndex()) &&
                    Objects.equals(policy, task.getPolicy())&&
                    Objects.equals(currentStepKey, task.getCurrentStepKey()) &&
                    Objects.equals(xContentToString(stepInfo), xContentToString(task.getStepInfo()));
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

    private static class ExecuteStepsUpdateTaskMatcher extends ArgumentMatcher<ExecuteStepsUpdateTask> {

        private Index index;
        private String policy;
        private Step startStep;

        ExecuteStepsUpdateTaskMatcher(Index index, String policy, Step startStep) {
            this.index = index;
            this.policy = policy;
            this.startStep = startStep;
        }

        @Override
        public boolean matches(Object argument) {
            if (argument == null || argument instanceof ExecuteStepsUpdateTask == false) {
                return false;
            }
            ExecuteStepsUpdateTask task = (ExecuteStepsUpdateTask) argument;
            return Objects.equals(index, task.getIndex()) &&
                    Objects.equals(policy, task.getPolicy()) &&
                    Objects.equals(startStep, task.getStartStep());
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
        private BiFunction<IndexMetaData, StepKey, Step> fn = null;
        private static Logger logger = LogManager.getLogger(MockPolicyStepsRegistry.class);

        MockPolicyStepsRegistry(SortedMap<String, LifecyclePolicyMetadata> lifecyclePolicyMap, Map<String, Step> firstStepMap,
                                Map<String, Map<StepKey, Step>> stepMap, NamedXContentRegistry xContentRegistry, Client client) {
            super(lifecyclePolicyMap, firstStepMap, stepMap, xContentRegistry, client);
        }

        public void setResolver(BiFunction<IndexMetaData, StepKey, Step> fn) {
            this.fn = fn;
        }

        @Override
        public Step getStep(IndexMetaData indexMetaData, StepKey stepKey) {
            if (fn == null) {
                logger.info("--> retrieving step {}", stepKey);
                return super.getStep(indexMetaData, stepKey);
            } else {
                logger.info("--> returning mock step");
                return fn.apply(indexMetaData, stepKey);
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

    private class NoOpHistoryStore extends ILMHistoryStore {

        private final List<ILMHistoryItem> items = new ArrayList<>();

        NoOpHistoryStore() {
            super(Settings.EMPTY, noopClient, null, null);
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
