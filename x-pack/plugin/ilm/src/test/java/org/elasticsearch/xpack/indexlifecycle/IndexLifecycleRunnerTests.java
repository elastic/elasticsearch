/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.indexlifecycle.AbstractStepTestCase;
import org.elasticsearch.xpack.core.indexlifecycle.AsyncActionStep;
import org.elasticsearch.xpack.core.indexlifecycle.AsyncWaitStep;
import org.elasticsearch.xpack.core.indexlifecycle.ClusterStateActionStep;
import org.elasticsearch.xpack.core.indexlifecycle.ClusterStateWaitStep;
import org.elasticsearch.xpack.core.indexlifecycle.ErrorStep;
import org.elasticsearch.xpack.core.indexlifecycle.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleAction;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleExecutionState;
import org.elasticsearch.xpack.core.indexlifecycle.LifecyclePolicy;
import org.elasticsearch.xpack.core.indexlifecycle.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.indexlifecycle.LifecyclePolicyTests;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleSettings;
import org.elasticsearch.xpack.core.indexlifecycle.MockAction;
import org.elasticsearch.xpack.core.indexlifecycle.MockStep;
import org.elasticsearch.xpack.core.indexlifecycle.OperationMode;
import org.elasticsearch.xpack.core.indexlifecycle.Phase;
import org.elasticsearch.xpack.core.indexlifecycle.PhaseExecutionInfo;
import org.elasticsearch.xpack.core.indexlifecycle.RolloverAction;
import org.elasticsearch.xpack.core.indexlifecycle.Step;
import org.elasticsearch.xpack.core.indexlifecycle.Step.StepKey;
import org.elasticsearch.xpack.core.indexlifecycle.TerminalPolicyStep;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.indexlifecycle.LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY;
import static org.elasticsearch.xpack.core.indexlifecycle.LifecyclePolicyTestsUtils.newTestLifecyclePolicy;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class IndexLifecycleRunnerTests extends ESTestCase {
    private static final NamedXContentRegistry REGISTRY = new NamedXContentRegistry(new IndexLifecycle(Settings.EMPTY).getNamedXContent());

    private PolicyStepsRegistry createOneStepPolicyStepRegistry(String policyName, Step step) {
        return createOneStepPolicyStepRegistry(policyName, step, "test");
    }

    private PolicyStepsRegistry createOneStepPolicyStepRegistry(String policyName, Step step, String indexName) {
        SortedMap<String, LifecyclePolicyMetadata> lifecyclePolicyMap = null; // Not used in this test
        Map<String, Step> firstStepMap = new HashMap<>();
        firstStepMap.put(policyName, step);
        Map<String, Map<StepKey, Step>> stepMap = new HashMap<>();
        Map<StepKey, Step> policySteps = new HashMap<>();
        policySteps.put(step.getKey(), step);
        stepMap.put(policyName, policySteps);
        Map<Index, List<Step>> indexSteps = new HashMap<>();
        List<Step> steps = new ArrayList<>();
        steps.add(step);
        Index index = new Index(indexName, indexName + "uuid");
        indexSteps.put(index, steps);
        return new PolicyStepsRegistry(lifecyclePolicyMap, firstStepMap, stepMap, NamedXContentRegistry.EMPTY, null);
    }

    public void testRunPolicyTerminalPolicyStep() {
        String policyName = "async_action_policy";
        TerminalPolicyStep step = TerminalPolicyStep.INSTANCE;
        PolicyStepsRegistry stepRegistry = createOneStepPolicyStepRegistry(policyName, step);
        ClusterService clusterService = mock(ClusterService.class);
        IndexLifecycleRunner runner = new IndexLifecycleRunner(stepRegistry, clusterService, () -> 0L);
        IndexMetaData indexMetaData = IndexMetaData.builder("my_index").settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();

        runner.runPolicy(policyName, indexMetaData, null, false);

        Mockito.verifyZeroInteractions(clusterService);
    }

    public void testRunPolicyErrorStep() {
        String policyName = "async_action_policy";
        StepKey stepKey = new StepKey("phase", "action", "cluster_state_action_step");
        MockClusterStateWaitStep step = new MockClusterStateWaitStep(stepKey, null);
        PolicyStepsRegistry stepRegistry = createOneStepPolicyStepRegistry(policyName, step);
        ClusterService clusterService = mock(ClusterService.class);
        IndexLifecycleRunner runner = new IndexLifecycleRunner(stepRegistry, clusterService, () -> 0L);
        LifecycleExecutionState.Builder newState = LifecycleExecutionState.builder();
        newState.setPhase(stepKey.getPhase());
        newState.setAction(stepKey.getAction());
        newState.setStep(ErrorStep.NAME);
        IndexMetaData indexMetaData = IndexMetaData.builder("my_index").settings(settings(Version.CURRENT))
            .putCustom(ILM_CUSTOM_METADATA_KEY, newState.build().asMap())
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();

        runner.runPolicy(policyName, indexMetaData, null, false);

        Mockito.verifyZeroInteractions(clusterService);
    }

    public void testRunPolicyClusterStateActionStep() {
        String policyName = "cluster_state_action_policy";
        StepKey stepKey = new StepKey("phase", "action", "cluster_state_action_step");
        MockClusterStateActionStep step = new MockClusterStateActionStep(stepKey, null);
        PolicyStepsRegistry stepRegistry = createOneStepPolicyStepRegistry(policyName, step);
        ClusterService clusterService = mock(ClusterService.class);
        IndexLifecycleRunner runner = new IndexLifecycleRunner(stepRegistry, clusterService, () -> 0L);
        IndexMetaData indexMetaData = IndexMetaData.builder("my_index").settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();

        runner.runPolicy(policyName, indexMetaData, null, randomBoolean());

        Mockito.verify(clusterService, Mockito.times(1)).submitStateUpdateTask(Mockito.matches("ILM"),
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
        IndexLifecycleRunner runner = new IndexLifecycleRunner(stepRegistry, clusterService, () -> 0L);
        IndexMetaData indexMetaData = IndexMetaData.builder("my_index").settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();

        runner.runPolicy(policyName, indexMetaData, null, randomBoolean());

        Mockito.verify(clusterService, Mockito.times(1)).submitStateUpdateTask(Mockito.matches("ILM"),
                Mockito.argThat(new ExecuteStepsUpdateTaskMatcher(indexMetaData.getIndex(), policyName, step)));
        Mockito.verifyNoMoreInteractions(clusterService);
    }

    public void testRunPolicyAsyncActionStepCompletes() {
        String policyName = "async_action_policy";
        StepKey stepKey = new StepKey("phase", "action", "async_action_step");
        MockAsyncActionStep step = new MockAsyncActionStep(stepKey, null);
        step.setWillComplete(true);
        PolicyStepsRegistry stepRegistry = createOneStepPolicyStepRegistry(policyName, step);
        ClusterService clusterService = mock(ClusterService.class);
        IndexLifecycleRunner runner = new IndexLifecycleRunner(stepRegistry, clusterService, () -> 0L);
        IndexMetaData indexMetaData = IndexMetaData.builder("my_index").settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();

        runner.runPolicy(policyName, indexMetaData, null, false);

        assertEquals(1, step.getExecuteCount());
        Mockito.verify(clusterService, Mockito.times(1)).submitStateUpdateTask(Mockito.matches("ILM"),
                Mockito.argThat(new MoveToNextStepUpdateTaskMatcher(indexMetaData.getIndex(), policyName, stepKey, null)));
        Mockito.verifyNoMoreInteractions(clusterService);
    }

    public void testRunPolicyAsyncActionStepCompletesIndexDestroyed() {
        String policyName = "async_action_policy";
        StepKey stepKey = new StepKey("phase", "action", "async_action_step");
        MockAsyncActionStep step = new MockAsyncActionStep(stepKey, null);
        step.setWillComplete(true);
        step.setIndexSurvives(false);
        PolicyStepsRegistry stepRegistry = createOneStepPolicyStepRegistry(policyName, step);
        ClusterService clusterService = mock(ClusterService.class);
        IndexLifecycleRunner runner = new IndexLifecycleRunner(stepRegistry, clusterService, () -> 0L);
        IndexMetaData indexMetaData = IndexMetaData.builder("my_index").settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();

        runner.runPolicy(policyName, indexMetaData, null, false);

        assertEquals(1, step.getExecuteCount());
        Mockito.verifyZeroInteractions(clusterService);
    }

    public void testRunPolicyAsyncActionStepNotComplete() {
        String policyName = "async_action_policy";
        StepKey stepKey = new StepKey("phase", "action", "async_action_step");
        MockAsyncActionStep step = new MockAsyncActionStep(stepKey, null);
        step.setWillComplete(false);
        PolicyStepsRegistry stepRegistry = createOneStepPolicyStepRegistry(policyName, step);
        ClusterService clusterService = mock(ClusterService.class);
        IndexLifecycleRunner runner = new IndexLifecycleRunner(stepRegistry, clusterService, () -> 0L);
        IndexMetaData indexMetaData = IndexMetaData.builder("my_index").settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();

        runner.runPolicy(policyName, indexMetaData, null, false);

        assertEquals(1, step.getExecuteCount());
        Mockito.verifyZeroInteractions(clusterService);
    }

    public void testRunPolicyAsyncActionStepFails() {
        String policyName = "async_action_policy";
        StepKey stepKey = new StepKey("phase", "action", "async_action_step");
        MockAsyncActionStep step = new MockAsyncActionStep(stepKey, null);
        Exception expectedException = new RuntimeException();
        step.setException(expectedException);
        PolicyStepsRegistry stepRegistry = createOneStepPolicyStepRegistry(policyName, step);
        ClusterService clusterService = mock(ClusterService.class);
        IndexLifecycleRunner runner = new IndexLifecycleRunner(stepRegistry, clusterService, () -> 0L);
        IndexMetaData indexMetaData = IndexMetaData.builder("my_index").settings(settings(Version.CURRENT))
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();

        runner.runPolicy(policyName, indexMetaData, null, false);

        assertEquals(1, step.getExecuteCount());
        Mockito.verify(clusterService, Mockito.times(1)).submitStateUpdateTask(Mockito.matches("ILM"),
                Mockito.argThat(new MoveToErrorStepUpdateTaskMatcher(indexMetaData.getIndex(), policyName, stepKey, expectedException)));
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
        IndexLifecycleRunner runner = new IndexLifecycleRunner(stepRegistry, clusterService, () -> 0L);
        IndexMetaData indexMetaData = IndexMetaData.builder("my_index").settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();

        runner.runPolicy(policyName, indexMetaData, null, true);

        assertEquals(0, step.getExecuteCount());
        Mockito.verifyZeroInteractions(clusterService);
    }

    public void testRunPolicyAsyncWaitStepCompletes() {
        String policyName = "async_wait_policy";
        StepKey stepKey = new StepKey("phase", "action", "async_wait_step");
        MockAsyncWaitStep step = new MockAsyncWaitStep(stepKey, null);
        step.setWillComplete(true);
        PolicyStepsRegistry stepRegistry = createOneStepPolicyStepRegistry(policyName, step);
        ClusterService clusterService = mock(ClusterService.class);
        IndexLifecycleRunner runner = new IndexLifecycleRunner(stepRegistry, clusterService, () -> 0L);
        IndexMetaData indexMetaData = IndexMetaData.builder("my_index").settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();

        runner.runPolicy(policyName, indexMetaData, null, false);

        assertEquals(1, step.getExecuteCount());
        Mockito.verify(clusterService, Mockito.times(1)).submitStateUpdateTask(Mockito.matches("ILM"),
                Mockito.argThat(new MoveToNextStepUpdateTaskMatcher(indexMetaData.getIndex(), policyName, stepKey, null)));
        Mockito.verifyNoMoreInteractions(clusterService);
    }

    public void testRunPolicyAsyncWaitStepNotComplete() {
        String policyName = "async_wait_policy";
        StepKey stepKey = new StepKey("phase", "action", "async_wait_step");
        MockAsyncWaitStep step = new MockAsyncWaitStep(stepKey, null);
        RandomStepInfo stepInfo = new RandomStepInfo(() -> randomAlphaOfLength(10));
        step.expectedInfo(stepInfo);
        step.setWillComplete(false);
        PolicyStepsRegistry stepRegistry = createOneStepPolicyStepRegistry(policyName, step);
        ClusterService clusterService = mock(ClusterService.class);
        IndexLifecycleRunner runner = new IndexLifecycleRunner(stepRegistry, clusterService, () -> 0L);
        IndexMetaData indexMetaData = IndexMetaData.builder("my_index").settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();

        runner.runPolicy(policyName, indexMetaData, null, false);

        assertEquals(1, step.getExecuteCount());
        Mockito.verify(clusterService, Mockito.times(1)).submitStateUpdateTask(Mockito.matches("ILM"),
                Mockito.argThat(new SetStepInfoUpdateTaskMatcher(indexMetaData.getIndex(), policyName, stepKey, stepInfo)));
        Mockito.verifyNoMoreInteractions(clusterService);
    }

    public void testRunPolicyAsyncWaitStepNotCompleteNoStepInfo() {
        String policyName = "async_wait_policy";
        StepKey stepKey = new StepKey("phase", "action", "async_wait_step");
        MockAsyncWaitStep step = new MockAsyncWaitStep(stepKey, null);
        RandomStepInfo stepInfo = null;
        step.expectedInfo(stepInfo);
        step.setWillComplete(false);
        PolicyStepsRegistry stepRegistry = createOneStepPolicyStepRegistry(policyName, step);
        ClusterService clusterService = mock(ClusterService.class);
        IndexLifecycleRunner runner = new IndexLifecycleRunner(stepRegistry, clusterService, () -> 0L);
        IndexMetaData indexMetaData = IndexMetaData.builder("my_index").settings(settings(Version.CURRENT))
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();

        runner.runPolicy(policyName, indexMetaData, null, false);

        assertEquals(1, step.getExecuteCount());
        Mockito.verifyZeroInteractions(clusterService);
    }

    public void testRunPolicyAsyncWaitStepFails() {
        String policyName = "async_wait_policy";
        StepKey stepKey = new StepKey("phase", "action", "async_wait_step");
        MockAsyncWaitStep step = new MockAsyncWaitStep(stepKey, null);
        Exception expectedException = new RuntimeException();
        step.setException(expectedException);
        PolicyStepsRegistry stepRegistry = createOneStepPolicyStepRegistry(policyName, step);
        ClusterService clusterService = mock(ClusterService.class);
        IndexLifecycleRunner runner = new IndexLifecycleRunner(stepRegistry, clusterService, () -> 0L);
        IndexMetaData indexMetaData = IndexMetaData.builder("my_index").settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();

        runner.runPolicy(policyName, indexMetaData, null, false);

        assertEquals(1, step.getExecuteCount());
        Mockito.verify(clusterService, Mockito.times(1)).submitStateUpdateTask(Mockito.matches("ILM"),
                Mockito.argThat(new MoveToErrorStepUpdateTaskMatcher(indexMetaData.getIndex(), policyName, stepKey, expectedException)));
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
        IndexLifecycleRunner runner = new IndexLifecycleRunner(stepRegistry, clusterService, () -> 0L);
        IndexMetaData indexMetaData = IndexMetaData.builder("my_index").settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();

        runner.runPolicy(policyName, indexMetaData, null, true);

        assertEquals(0, step.getExecuteCount());
        Mockito.verifyZeroInteractions(clusterService);
    }

    public void testRunPolicyWithNoStepsInRegistry() {
        String policyName = "cluster_state_action_policy";
        ClusterService clusterService = mock(ClusterService.class);
        IndexLifecycleRunner runner = new IndexLifecycleRunner(new PolicyStepsRegistry(NamedXContentRegistry.EMPTY, null),
            clusterService, () -> 0L);
        IndexMetaData indexMetaData = IndexMetaData.builder("my_index").settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        // verify that no exception is thrown
        runner.runPolicy(policyName, indexMetaData, null, randomBoolean());
        Mockito.verifyZeroInteractions(clusterService);
    }

    public void testRunPolicyUnknownStepType() {
        String policyName = "cluster_state_action_policy";
        StepKey stepKey = new StepKey("phase", "action", "cluster_state_action_step");
        MockStep step = new MockStep(stepKey, null);
        PolicyStepsRegistry stepRegistry = createOneStepPolicyStepRegistry(policyName, step);
        ClusterService clusterService = mock(ClusterService.class);
        IndexLifecycleRunner runner = new IndexLifecycleRunner(stepRegistry, clusterService, () -> 0L);
        IndexMetaData indexMetaData = IndexMetaData.builder("my_index").settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();

        IllegalStateException exception = expectThrows(IllegalStateException.class,
                () -> runner.runPolicy(policyName, indexMetaData, null, randomBoolean()));
        assertEquals("Step with key [" + stepKey + "] is not a recognised type: [" + step.getClass().getName() + "]",
                exception.getMessage());
        Mockito.verifyZeroInteractions(clusterService);
    }

    public void testGetCurrentStepKey() {
        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        StepKey stepKey = IndexLifecycleRunner.getCurrentStepKey(lifecycleState.build());
        assertNull(stepKey);

        String phase = randomAlphaOfLength(20);
        String action = randomAlphaOfLength(20);
        String step = randomAlphaOfLength(20);
        LifecycleExecutionState.Builder lifecycleState2 = LifecycleExecutionState.builder();
        lifecycleState2.setPhase(phase);
        lifecycleState2.setAction(action);
        lifecycleState2.setStep(step);
        stepKey = IndexLifecycleRunner.getCurrentStepKey(lifecycleState2.build());
        assertNotNull(stepKey);
        assertEquals(phase, stepKey.getPhase());
        assertEquals(action, stepKey.getAction());
        assertEquals(step, stepKey.getName());

        phase = randomAlphaOfLength(20);
        action = randomAlphaOfLength(20);
        step = null;
        LifecycleExecutionState.Builder lifecycleState3 = LifecycleExecutionState.builder();
        lifecycleState3.setPhase(phase);
        lifecycleState3.setAction(action);
        lifecycleState3.setStep(step);
        AssertionError error3 = expectThrows(AssertionError.class, () -> IndexLifecycleRunner.getCurrentStepKey(lifecycleState3.build()));
        assertEquals("Current phase is not empty: " + phase, error3.getMessage());

        phase = null;
        action = randomAlphaOfLength(20);
        step = null;
        LifecycleExecutionState.Builder lifecycleState4 = LifecycleExecutionState.builder();
        lifecycleState4.setPhase(phase);
        lifecycleState4.setAction(action);
        lifecycleState4.setStep(step);
        AssertionError error4 = expectThrows(AssertionError.class, () -> IndexLifecycleRunner.getCurrentStepKey(lifecycleState4.build()));
        assertEquals("Current action is not empty: " + action, error4.getMessage());

        phase = null;
        action = randomAlphaOfLength(20);
        step = randomAlphaOfLength(20);
        LifecycleExecutionState.Builder lifecycleState5 = LifecycleExecutionState.builder();
        lifecycleState5.setPhase(phase);
        lifecycleState5.setAction(action);
        lifecycleState5.setStep(step);
        AssertionError error5 = expectThrows(AssertionError.class, () -> IndexLifecycleRunner.getCurrentStepKey(lifecycleState5.build()));
        assertEquals(null, error5.getMessage());

        phase = null;
        action = null;
        step = randomAlphaOfLength(20);
        LifecycleExecutionState.Builder lifecycleState6 = LifecycleExecutionState.builder();
        lifecycleState6.setPhase(phase);
        lifecycleState6.setAction(action);
        lifecycleState6.setStep(step);
        AssertionError error6 = expectThrows(AssertionError.class, () -> IndexLifecycleRunner.getCurrentStepKey(lifecycleState6.build()));
        assertEquals(null, error6.getMessage());
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
        Mockito.when(client.settings()).thenReturn(Settings.EMPTY);
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
        Step stepFromNoSettings = IndexLifecycleRunner.getCurrentStep(registry, policy.getName(), indexMetaData,
            LifecycleExecutionState.builder().build());
        assertEquals(firstStep, stepFromNoSettings);

        // The step that was written into the metadata is retrieved
        Step currentStep = IndexLifecycleRunner.getCurrentStep(registry, policy.getName(), indexMetaData, lifecycleState.build());
        assertEquals(step.getKey(), currentStep.getKey());
    }

    public void testMoveClusterStateToNextStep() {
        String indexName = "my_index";
        LifecyclePolicy policy = randomValueOtherThanMany(p -> p.getPhases().size() == 0,
            () -> LifecyclePolicyTests.randomTestLifecyclePolicy("policy"));
        Phase nextPhase = policy.getPhases().values().stream().findFirst().get();
        List<LifecyclePolicyMetadata> policyMetadatas = Collections.singletonList(
            new LifecyclePolicyMetadata(policy, Collections.emptyMap(), randomNonNegativeLong(), randomNonNegativeLong()));
        StepKey currentStep = new StepKey("current_phase", "current_action", "current_step");
        StepKey nextStep = new StepKey(nextPhase.getName(), "next_action", "next_step");
        long now = randomNonNegativeLong();

        // test going from null lifecycle settings to next step
        ClusterState clusterState = buildClusterState(indexName,
            Settings.builder()
                .put(LifecycleSettings.LIFECYCLE_NAME, policy.getName()), LifecycleExecutionState.builder().build(), policyMetadatas);
        Index index = clusterState.metaData().index(indexName).getIndex();
        ClusterState newClusterState = IndexLifecycleRunner.moveClusterStateToNextStep(index, clusterState, currentStep, nextStep,
                () -> now);
        assertClusterStateOnNextStep(clusterState, index, currentStep, nextStep, newClusterState, now);

        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhase(currentStep.getPhase());
        lifecycleState.setAction(currentStep.getAction());
        lifecycleState.setStep(currentStep.getName());
        // test going from set currentStep settings to nextStep
        Builder indexSettingsBuilder = Settings.builder()
            .put(LifecycleSettings.LIFECYCLE_NAME, policy.getName());
        if (randomBoolean()) {
            lifecycleState.setStepInfo(randomAlphaOfLength(20));
        }

        clusterState = buildClusterState(indexName, indexSettingsBuilder, lifecycleState.build(), policyMetadatas);
        index = clusterState.metaData().index(indexName).getIndex();
        newClusterState = IndexLifecycleRunner.moveClusterStateToNextStep(index, clusterState, currentStep, nextStep, () -> now);
        assertClusterStateOnNextStep(clusterState, index, currentStep, nextStep, newClusterState, now);
    }

    public void testMoveClusterStateToNextStepSamePhase() {
        String indexName = "my_index";
        StepKey currentStep = new StepKey("current_phase", "current_action", "current_step");
        StepKey nextStep = new StepKey("current_phase", "next_action", "next_step");
        long now = randomNonNegativeLong();

        ClusterState clusterState = buildClusterState(indexName, Settings.builder(), LifecycleExecutionState.builder().build(),
            Collections.emptyList());
        Index index = clusterState.metaData().index(indexName).getIndex();
        ClusterState newClusterState = IndexLifecycleRunner.moveClusterStateToNextStep(index, clusterState, currentStep, nextStep,
                () -> now);
        assertClusterStateOnNextStep(clusterState, index, currentStep, nextStep, newClusterState, now);

        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhase(currentStep.getPhase());
        lifecycleState.setAction(currentStep.getAction());
        lifecycleState.setStep(currentStep.getName());
        if (randomBoolean()) {
            lifecycleState.setStepInfo(randomAlphaOfLength(20));
        }

        clusterState = buildClusterState(indexName, Settings.builder(), lifecycleState.build(), Collections.emptyList());
        index = clusterState.metaData().index(indexName).getIndex();
        newClusterState = IndexLifecycleRunner.moveClusterStateToNextStep(index, clusterState, currentStep, nextStep, () -> now);
        assertClusterStateOnNextStep(clusterState, index, currentStep, nextStep, newClusterState, now);
    }

    public void testMoveClusterStateToNextStepSameAction() {
        String indexName = "my_index";
        StepKey currentStep = new StepKey("current_phase", "current_action", "current_step");
        StepKey nextStep = new StepKey("current_phase", "current_action", "next_step");
        long now = randomNonNegativeLong();

        ClusterState clusterState = buildClusterState(indexName, Settings.builder(), LifecycleExecutionState.builder().build(),
            Collections.emptyList());
        Index index = clusterState.metaData().index(indexName).getIndex();
        ClusterState newClusterState = IndexLifecycleRunner.moveClusterStateToNextStep(index, clusterState, currentStep, nextStep,
                () -> now);
        assertClusterStateOnNextStep(clusterState, index, currentStep, nextStep, newClusterState, now);

        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhase(currentStep.getPhase());
        lifecycleState.setAction(currentStep.getAction());
        lifecycleState.setStep(currentStep.getName());
        if (randomBoolean()) {
            lifecycleState.setStepInfo(randomAlphaOfLength(20));
        }
        clusterState = buildClusterState(indexName, Settings.builder(), lifecycleState.build(), Collections.emptyList());
        index = clusterState.metaData().index(indexName).getIndex();
        newClusterState = IndexLifecycleRunner.moveClusterStateToNextStep(index, clusterState, currentStep, nextStep, () -> now);
        assertClusterStateOnNextStep(clusterState, index, currentStep, nextStep, newClusterState, now);
    }

    public void testSuccessfulValidatedMoveClusterStateToNextStep() {
        String indexName = "my_index";
        String policyName = "my_policy";
        LifecyclePolicy policy = randomValueOtherThanMany(p -> p.getPhases().size() == 0,
            () -> LifecyclePolicyTests.randomTestLifecyclePolicy(policyName));
        Phase nextPhase = policy.getPhases().values().stream().findFirst().get();
        List<LifecyclePolicyMetadata> policyMetadatas = Collections.singletonList(
            new LifecyclePolicyMetadata(policy, Collections.emptyMap(), randomNonNegativeLong(), randomNonNegativeLong()));
        StepKey currentStepKey = new StepKey("current_phase", "current_action", "current_step");
        StepKey nextStepKey = new StepKey(nextPhase.getName(), "next_action", "next_step");
        long now = randomNonNegativeLong();
        Step step = new MockStep(nextStepKey, nextStepKey);
        PolicyStepsRegistry stepRegistry = createOneStepPolicyStepRegistry(policyName, step, indexName);

        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhase(currentStepKey.getPhase());
        lifecycleState.setAction(currentStepKey.getAction());
        lifecycleState.setStep(currentStepKey.getName());

        Builder indexSettingsBuilder = Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, policyName);
        ClusterState clusterState = buildClusterState(indexName, indexSettingsBuilder, lifecycleState.build(), policyMetadatas);
        Index index = clusterState.metaData().index(indexName).getIndex();
        ClusterState newClusterState = IndexLifecycleRunner.moveClusterStateToStep(indexName, clusterState, currentStepKey,
            nextStepKey, () -> now, stepRegistry);
        assertClusterStateOnNextStep(clusterState, index, currentStepKey, nextStepKey, newClusterState, now);
    }

    public void testValidatedMoveClusterStateToNextStepWithoutPolicy() {
        String indexName = "my_index";
        String policyName = randomBoolean() ? null : "";
        StepKey currentStepKey = new StepKey("current_phase", "current_action", "current_step");
        StepKey nextStepKey = new StepKey("next_phase", "next_action", "next_step");
        long now = randomNonNegativeLong();
        Step step = new MockStep(nextStepKey, nextStepKey);
        PolicyStepsRegistry stepRegistry = createOneStepPolicyStepRegistry(policyName, step);

        Builder indexSettingsBuilder = Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, policyName);
        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhase(currentStepKey.getPhase());
        lifecycleState.setAction(currentStepKey.getAction());
        lifecycleState.setStep(currentStepKey.getName());

        ClusterState clusterState = buildClusterState(indexName, indexSettingsBuilder, lifecycleState.build(), Collections.emptyList());
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
            () -> IndexLifecycleRunner.moveClusterStateToStep(indexName, clusterState, currentStepKey,
                nextStepKey, () -> now, stepRegistry));
        assertThat(exception.getMessage(), equalTo("index [my_index] is not associated with an Index Lifecycle Policy"));
    }

    public void testValidatedMoveClusterStateToNextStepInvalidCurrentStep() {
        String indexName = "my_index";
        String policyName = "my_policy";
        StepKey currentStepKey = new StepKey("current_phase", "current_action", "current_step");
        StepKey notCurrentStepKey = new StepKey("not_current_phase", "not_current_action", "not_current_step");
        StepKey nextStepKey = new StepKey("next_phase", "next_action", "next_step");
        long now = randomNonNegativeLong();
        Step step = new MockStep(nextStepKey, nextStepKey);
        PolicyStepsRegistry stepRegistry = createOneStepPolicyStepRegistry(policyName, step);

        Builder indexSettingsBuilder = Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, policyName);
        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhase(currentStepKey.getPhase());
        lifecycleState.setAction(currentStepKey.getAction());
        lifecycleState.setStep(currentStepKey.getName());

        ClusterState clusterState = buildClusterState(indexName, indexSettingsBuilder, lifecycleState.build(), Collections.emptyList());
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
            () -> IndexLifecycleRunner.moveClusterStateToStep(indexName, clusterState, notCurrentStepKey,
                nextStepKey, () -> now, stepRegistry));
        assertThat(exception.getMessage(), equalTo("index [my_index] is not on current step " +
            "[{\"phase\":\"not_current_phase\",\"action\":\"not_current_action\",\"name\":\"not_current_step\"}]"));
    }

    public void testValidatedMoveClusterStateToNextStepInvalidNextStep() {
        String indexName = "my_index";
        String policyName = "my_policy";
        StepKey currentStepKey = new StepKey("current_phase", "current_action", "current_step");
        StepKey nextStepKey = new StepKey("next_phase", "next_action", "next_step");
        long now = randomNonNegativeLong();
        Step step = new MockStep(currentStepKey, nextStepKey);
        PolicyStepsRegistry stepRegistry = createOneStepPolicyStepRegistry(policyName, step);

        Builder indexSettingsBuilder = Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, policyName);
        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhase(currentStepKey.getPhase());
        lifecycleState.setAction(currentStepKey.getAction());
        lifecycleState.setStep(currentStepKey.getName());

        ClusterState clusterState = buildClusterState(indexName, indexSettingsBuilder, lifecycleState.build(), Collections.emptyList());
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
            () -> IndexLifecycleRunner.moveClusterStateToStep(indexName, clusterState, currentStepKey,
                nextStepKey, () -> now, stepRegistry));
        assertThat(exception.getMessage(),
            equalTo("step [{\"phase\":\"next_phase\",\"action\":\"next_action\",\"name\":\"next_step\"}] " +
                "for index [my_index] with policy [my_policy] does not exist"));
    }

    public void testMoveClusterStateToErrorStep() throws IOException {
        String indexName = "my_index";
        StepKey currentStep = new StepKey("current_phase", "current_action", "current_step");
        long now = randomNonNegativeLong();
        Exception cause = new ElasticsearchException("THIS IS AN EXPECTED CAUSE");

        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhase(currentStep.getPhase());
        lifecycleState.setAction(currentStep.getAction());
        lifecycleState.setStep(currentStep.getName());
        ClusterState clusterState = buildClusterState(indexName, Settings.builder(), lifecycleState.build(), Collections.emptyList());
        Index index = clusterState.metaData().index(indexName).getIndex();

        ClusterState newClusterState = IndexLifecycleRunner.moveClusterStateToErrorStep(index, clusterState, currentStep, cause, () -> now);
        assertClusterStateOnErrorStep(clusterState, index, currentStep, newClusterState, now,
            "{\"type\":\"exception\",\"reason\":\"THIS IS AN EXPECTED CAUSE\"}");

        cause = new IllegalArgumentException("non elasticsearch-exception");
        newClusterState = IndexLifecycleRunner.moveClusterStateToErrorStep(index, clusterState, currentStep, cause, () -> now);
        assertClusterStateOnErrorStep(clusterState, index, currentStep, newClusterState, now,
            "{\"type\":\"illegal_argument_exception\",\"reason\":\"non elasticsearch-exception\"}");
    }

    public void testMoveClusterStateToFailedStep() {
        String indexName = "my_index";
        String[] indices = new String[] { indexName };
        String policyName = "my_policy";
        long now = randomNonNegativeLong();
        StepKey failedStepKey = new StepKey("current_phase", "current_action", "current_step");
        StepKey errorStepKey = new StepKey(failedStepKey.getPhase(), failedStepKey.getAction(), ErrorStep.NAME);
        Step step = new MockStep(failedStepKey, null);
        PolicyStepsRegistry policyRegistry = createOneStepPolicyStepRegistry(policyName, step, indexName);
        Settings.Builder indexSettingsBuilder = Settings.builder()
                .put(LifecycleSettings.LIFECYCLE_NAME, policyName);
        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhase(errorStepKey.getPhase());
        lifecycleState.setAction(errorStepKey.getAction());
        lifecycleState.setStep(errorStepKey.getName());
        lifecycleState.setFailedStep(failedStepKey.getName());
        ClusterState clusterState = buildClusterState(indexName, indexSettingsBuilder, lifecycleState.build(), Collections.emptyList());
        Index index = clusterState.metaData().index(indexName).getIndex();
        IndexLifecycleRunner runner = new IndexLifecycleRunner(policyRegistry, null, () -> now);
        ClusterState nextClusterState = runner.moveClusterStateToFailedStep(clusterState, indices);
        IndexLifecycleRunnerTests.assertClusterStateOnNextStep(clusterState, index, errorStepKey, failedStepKey,
            nextClusterState, now);
    }

    public void testMoveClusterStateToFailedStepIndexNotFound() {
        String existingIndexName = "my_index";
        String invalidIndexName = "does_not_exist";
        ClusterState clusterState = buildClusterState(existingIndexName, Settings.builder(), LifecycleExecutionState.builder().build(),
            Collections.emptyList());
        IndexLifecycleRunner runner = new IndexLifecycleRunner(null, null, () -> 0L);
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
            () -> runner.moveClusterStateToFailedStep(clusterState, new String[] { invalidIndexName }));
        assertThat(exception.getMessage(), equalTo("index [" + invalidIndexName + "] does not exist"));
    }
//
    public void testMoveClusterStateToFailedStepInvalidPolicySetting() {
        String indexName = "my_index";
        String[] indices = new String[] { indexName };
        String policyName = "my_policy";
        long now = randomNonNegativeLong();
        StepKey failedStepKey = new StepKey("current_phase", "current_action", "current_step");
        StepKey errorStepKey = new StepKey(failedStepKey.getPhase(), failedStepKey.getAction(), ErrorStep.NAME);
        Step step = new MockStep(failedStepKey, null);
        PolicyStepsRegistry policyRegistry = createOneStepPolicyStepRegistry(policyName, step);
        Settings.Builder indexSettingsBuilder = Settings.builder()
            .put(LifecycleSettings.LIFECYCLE_NAME, (String) null);
        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhase(errorStepKey.getPhase());
        lifecycleState.setAction(errorStepKey.getAction());
        lifecycleState.setStep(errorStepKey.getName());
        lifecycleState.setFailedStep(failedStepKey.getName());
        ClusterState clusterState = buildClusterState(indexName, indexSettingsBuilder, lifecycleState.build(), Collections.emptyList());
        IndexLifecycleRunner runner = new IndexLifecycleRunner(policyRegistry, null, () -> now);
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
            () -> runner.moveClusterStateToFailedStep(clusterState, indices));
        assertThat(exception.getMessage(), equalTo("index [" + indexName + "] is not associated with an Index Lifecycle Policy"));
    }

    public void testMoveClusterStateToFailedNotOnError() {
        String indexName = "my_index";
        String[] indices = new String[] { indexName };
        String policyName = "my_policy";
        long now = randomNonNegativeLong();
        StepKey failedStepKey = new StepKey("current_phase", "current_action", "current_step");
        Step step = new MockStep(failedStepKey, null);
        PolicyStepsRegistry policyRegistry = createOneStepPolicyStepRegistry(policyName, step);
        Settings.Builder indexSettingsBuilder = Settings.builder()
            .put(LifecycleSettings.LIFECYCLE_NAME, (String) null);
        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhase(failedStepKey.getPhase());
        lifecycleState.setAction(failedStepKey.getAction());
        lifecycleState.setStep(failedStepKey.getName());
        ClusterState clusterState = buildClusterState(indexName, indexSettingsBuilder, lifecycleState.build(), Collections.emptyList());
        IndexLifecycleRunner runner = new IndexLifecycleRunner(policyRegistry, null, () -> now);
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
            () -> runner.moveClusterStateToFailedStep(clusterState, indices));
        assertThat(exception.getMessage(), equalTo("cannot retry an action for an index [" + indices[0]
            + "] that has not encountered an error when running a Lifecycle Policy"));
    }

    public void testAddStepInfoToClusterState() throws IOException {
        String indexName = "my_index";
        StepKey currentStep = new StepKey("current_phase", "current_action", "current_step");
        RandomStepInfo stepInfo = new RandomStepInfo(() -> randomAlphaOfLength(10));

        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhase(currentStep.getPhase());
        lifecycleState.setAction(currentStep.getAction());
        lifecycleState.setStep(currentStep.getName());
        ClusterState clusterState = buildClusterState(indexName, Settings.builder(), lifecycleState.build(), Collections.emptyList());
        Index index = clusterState.metaData().index(indexName).getIndex();
        ClusterState newClusterState = IndexLifecycleRunner.addStepInfoToClusterState(index, clusterState, stepInfo);
        assertClusterStateStepInfo(clusterState, index, currentStep, newClusterState, stepInfo);
        ClusterState runAgainClusterState = IndexLifecycleRunner.addStepInfoToClusterState(index, newClusterState, stepInfo);
        assertSame(newClusterState, runAgainClusterState);
    }

    @SuppressWarnings("unchecked")
    public void testSkipped() {
        String policy = randomAlphaOfLength(5);
        String index = randomAlphaOfLength(10);
        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhase(randomAlphaOfLength(5));
        lifecycleState.setAction(randomAlphaOfLength(5));
        lifecycleState.setStep(randomAlphaOfLength(5));
        ClusterState clusterState = buildClusterState(index,
            Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, policy).put(LifecycleSettings.LIFECYCLE_SKIP, true),
            lifecycleState.build(),
            Collections.emptyList());
        Step step = mock(randomFrom(TerminalPolicyStep.class, ClusterStateActionStep.class,
            ClusterStateWaitStep.class, AsyncActionStep.class, AsyncWaitStep.class));
        PolicyStepsRegistry stepRegistry = createOneStepPolicyStepRegistry(policy, step);
        ClusterService clusterService = mock(ClusterService.class);
        IndexLifecycleRunner runner = new IndexLifecycleRunner(stepRegistry, clusterService, () -> 0L);
        runner.runPolicy(policy, clusterState.metaData().index(index), clusterState, randomBoolean());
        Mockito.verifyZeroInteractions(clusterService);
    }

    private ClusterState buildClusterState(String indexName, Settings.Builder indexSettingsBuilder,
                                           LifecycleExecutionState lifecycleState,
                                           List<LifecyclePolicyMetadata> lifecyclePolicyMetadatas) {
        Settings indexSettings = indexSettingsBuilder.put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0).put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).build();
        IndexMetaData indexMetadata = IndexMetaData.builder(indexName)
            .settings(indexSettings)
            .putCustom(ILM_CUSTOM_METADATA_KEY, lifecycleState.asMap())
            .build();

        Map<String, LifecyclePolicyMetadata> lifecyclePolicyMetadatasMap = lifecyclePolicyMetadatas.stream()
                .collect(Collectors.toMap(LifecyclePolicyMetadata::getName, Function.identity()));
        IndexLifecycleMetadata indexLifecycleMetadata = new IndexLifecycleMetadata(lifecyclePolicyMetadatasMap, OperationMode.RUNNING);

        MetaData metadata = MetaData.builder().put(indexMetadata, true).putCustom(IndexLifecycleMetadata.TYPE, indexLifecycleMetadata)
                .build();
        return ClusterState.builder(new ClusterName("my_cluster")).metaData(metadata).build();
    }

    public void testSetPolicyForIndex() {
        long now = randomNonNegativeLong();
        String indexName = randomAlphaOfLength(10);
        String oldPolicyName = "old_policy";
        String newPolicyName = "new_policy";
        String phaseName = randomAlphaOfLength(10);
        StepKey currentStep = new StepKey(phaseName, MockAction.NAME, randomAlphaOfLength(10));
        LifecyclePolicy newPolicy = createPolicy(newPolicyName,
                new StepKey(phaseName, MockAction.NAME, randomAlphaOfLength(9)), null);
        LifecyclePolicy oldPolicy = createPolicy(oldPolicyName, currentStep, null);
        Settings.Builder indexSettingsBuilder = Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, oldPolicyName)
                .put(LifecycleSettings.LIFECYCLE_SKIP, true);
        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhase(currentStep.getPhase());
        lifecycleState.setAction(currentStep.getAction());
        lifecycleState.setStep(currentStep.getName());
        List<LifecyclePolicyMetadata> policyMetadatas = new ArrayList<>();
        policyMetadatas.add(new LifecyclePolicyMetadata(oldPolicy, Collections.emptyMap(),
            randomNonNegativeLong(), randomNonNegativeLong()));
        LifecyclePolicyMetadata newPolicyMetadata = new LifecyclePolicyMetadata(newPolicy, Collections.emptyMap(),
            randomNonNegativeLong(), randomNonNegativeLong());
        policyMetadatas.add(newPolicyMetadata);
        ClusterState clusterState = buildClusterState(indexName, indexSettingsBuilder, lifecycleState.build(), policyMetadatas);
        Index index = clusterState.metaData().index(indexName).getIndex();
        Index[] indices = new Index[] { index };
        List<String> failedIndexes = new ArrayList<>();

        ClusterState newClusterState = IndexLifecycleRunner.setPolicyForIndexes(newPolicyName, indices, clusterState, newPolicyMetadata,
                failedIndexes, () -> now);

        assertTrue(failedIndexes.isEmpty());
        assertClusterStateOnPolicy(clusterState, index, newPolicyName, currentStep, TerminalPolicyStep.KEY, newClusterState, now);
    }

    public void testSetPolicyForIndexNoCurrentPolicy() {
        long now = randomNonNegativeLong();
        String indexName = randomAlphaOfLength(10);
        String newPolicyName = "new_policy";
        LifecyclePolicy newPolicy = newTestLifecyclePolicy(newPolicyName, Collections.emptyMap());
        LifecyclePolicyMetadata newPolicyMetadata = new LifecyclePolicyMetadata(newPolicy, Collections.emptyMap(),
            randomNonNegativeLong(), randomNonNegativeLong());
        StepKey currentStep = new StepKey(null, null, null);
        Settings.Builder indexSettingsBuilder = Settings.builder();
        ClusterState clusterState = buildClusterState(indexName, indexSettingsBuilder, LifecycleExecutionState.builder().build(),
            Collections.singletonList(newPolicyMetadata));
        Index index = clusterState.metaData().index(indexName).getIndex();
        Index[] indices = new Index[] { index };
        List<String> failedIndexes = new ArrayList<>();

        ClusterState newClusterState = IndexLifecycleRunner.setPolicyForIndexes(newPolicyName, indices, clusterState, newPolicyMetadata,
                failedIndexes, () -> now);

        assertTrue(failedIndexes.isEmpty());
        assertClusterStateOnPolicy(clusterState, index, newPolicyName, currentStep, currentStep, newClusterState, now);
    }

    public void testSetPolicyForIndexIndexDoesntExist() {
        long now = randomNonNegativeLong();
        String indexName = randomAlphaOfLength(10);
        String oldPolicyName = "old_policy";
        String newPolicyName = "new_policy";
        LifecyclePolicy oldPolicy = newTestLifecyclePolicy(oldPolicyName, Collections.emptyMap());
        LifecyclePolicy newPolicy = newTestLifecyclePolicy(newPolicyName, Collections.emptyMap());
        LifecyclePolicyMetadata newPolicyMetadata = new LifecyclePolicyMetadata(newPolicy, Collections.emptyMap(),
            randomNonNegativeLong(), randomNonNegativeLong());
        StepKey currentStep = AbstractStepTestCase.randomStepKey();
        Settings.Builder indexSettingsBuilder = Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, oldPolicyName)
                .put(LifecycleSettings.LIFECYCLE_SKIP, true);
        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhase(currentStep.getPhase());
        lifecycleState.setAction(currentStep.getAction());
        lifecycleState.setStep(currentStep.getName());
        List<LifecyclePolicyMetadata> policyMetadatas = new ArrayList<>();
        policyMetadatas.add(new LifecyclePolicyMetadata(oldPolicy, Collections.emptyMap(),
            randomNonNegativeLong(), randomNonNegativeLong()));
        ClusterState clusterState = buildClusterState(indexName, indexSettingsBuilder, lifecycleState.build(), policyMetadatas);
        Index index = new Index("doesnt_exist", "im_not_here");
        Index[] indices = new Index[] { index };
        List<String> failedIndexes = new ArrayList<>();

        ClusterState newClusterState = IndexLifecycleRunner.setPolicyForIndexes(newPolicyName, indices, clusterState, newPolicyMetadata,
                failedIndexes, () -> now);

        assertEquals(1, failedIndexes.size());
        assertEquals("doesnt_exist", failedIndexes.get(0));
        assertSame(clusterState, newClusterState);
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

    public void testRemovePolicyForIndex() {
        String indexName = randomAlphaOfLength(10);
        String oldPolicyName = "old_policy";
        StepKey currentStep = new StepKey(randomAlphaOfLength(10), MockAction.NAME, randomAlphaOfLength(10));
        LifecyclePolicy oldPolicy = createPolicy(oldPolicyName, currentStep, null);
        Settings.Builder indexSettingsBuilder = Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, oldPolicyName)
                .put(LifecycleSettings.LIFECYCLE_SKIP, true);
        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhase(currentStep.getPhase());
        lifecycleState.setAction(currentStep.getAction());
        lifecycleState.setStep(currentStep.getName());
        List<LifecyclePolicyMetadata> policyMetadatas = new ArrayList<>();
        policyMetadatas.add(new LifecyclePolicyMetadata(oldPolicy, Collections.emptyMap(),
            randomNonNegativeLong(), randomNonNegativeLong()));
        ClusterState clusterState = buildClusterState(indexName, indexSettingsBuilder, lifecycleState.build(), policyMetadatas);
        Index index = clusterState.metaData().index(indexName).getIndex();
        Index[] indices = new Index[] { index };
        List<String> failedIndexes = new ArrayList<>();

        ClusterState newClusterState = IndexLifecycleRunner.removePolicyForIndexes(indices, clusterState, failedIndexes);

        assertTrue(failedIndexes.isEmpty());
        assertIndexNotManagedByILM(newClusterState, index);
    }

    public void testRemovePolicyForIndexNoCurrentPolicy() {
        String indexName = randomAlphaOfLength(10);
        Settings.Builder indexSettingsBuilder = Settings.builder();
        ClusterState clusterState = buildClusterState(indexName, indexSettingsBuilder, LifecycleExecutionState.builder().build(),
            Collections.emptyList());
        Index index = clusterState.metaData().index(indexName).getIndex();
        Index[] indices = new Index[] { index };
        List<String> failedIndexes = new ArrayList<>();

        ClusterState newClusterState = IndexLifecycleRunner.removePolicyForIndexes(indices, clusterState, failedIndexes);

        assertTrue(failedIndexes.isEmpty());
        assertIndexNotManagedByILM(newClusterState, index);
    }

    public void testRemovePolicyForIndexIndexDoesntExist() {
        String indexName = randomAlphaOfLength(10);
        String oldPolicyName = "old_policy";
        LifecyclePolicy oldPolicy = newTestLifecyclePolicy(oldPolicyName, Collections.emptyMap());
        StepKey currentStep = AbstractStepTestCase.randomStepKey();
        Settings.Builder indexSettingsBuilder = Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, oldPolicyName)
                .put(LifecycleSettings.LIFECYCLE_SKIP, true);
        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhase(currentStep.getPhase());
        lifecycleState.setAction(currentStep.getAction());
        lifecycleState.setStep(currentStep.getName());
        List<LifecyclePolicyMetadata> policyMetadatas = new ArrayList<>();
        policyMetadatas.add(new LifecyclePolicyMetadata(oldPolicy, Collections.emptyMap(),
            randomNonNegativeLong(), randomNonNegativeLong()));
        ClusterState clusterState = buildClusterState(indexName, indexSettingsBuilder, lifecycleState.build(), policyMetadatas);
        Index index = new Index("doesnt_exist", "im_not_here");
        Index[] indices = new Index[] { index };
        List<String> failedIndexes = new ArrayList<>();

        ClusterState newClusterState = IndexLifecycleRunner.removePolicyForIndexes(indices, clusterState, failedIndexes);

        assertEquals(1, failedIndexes.size());
        assertEquals("doesnt_exist", failedIndexes.get(0));
        assertSame(clusterState, newClusterState);
    }

    public void testRemovePolicyForIndexIndexInUnsafe() {
        String indexName = randomAlphaOfLength(10);
        String oldPolicyName = "old_policy";
        StepKey currentStep = new StepKey(randomAlphaOfLength(10), MockAction.NAME, randomAlphaOfLength(10));
        LifecyclePolicy oldPolicy = createPolicy(oldPolicyName, null, currentStep);
        Settings.Builder indexSettingsBuilder = Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, oldPolicyName)
                .put(LifecycleSettings.LIFECYCLE_SKIP, true);
        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhase(currentStep.getPhase());
        lifecycleState.setAction(currentStep.getAction());
        lifecycleState.setStep(currentStep.getName());
        List<LifecyclePolicyMetadata> policyMetadatas = new ArrayList<>();
        policyMetadatas.add(new LifecyclePolicyMetadata(oldPolicy, Collections.emptyMap(),
            randomNonNegativeLong(), randomNonNegativeLong()));
        ClusterState clusterState = buildClusterState(indexName, indexSettingsBuilder, lifecycleState.build(), policyMetadatas);
        Index index = clusterState.metaData().index(indexName).getIndex();
        Index[] indices = new Index[] { index };
        List<String> failedIndexes = new ArrayList<>();

        ClusterState newClusterState = IndexLifecycleRunner.removePolicyForIndexes(indices, clusterState, failedIndexes);

        assertTrue(failedIndexes.isEmpty());
        assertIndexNotManagedByILM(newClusterState, index);
    }

    public void testIsReadyToTransition() {
        String policyName = "async_action_policy";
        StepKey stepKey = new StepKey("phase", MockAction.NAME, MockAction.NAME);
        MockAsyncActionStep step = new MockAsyncActionStep(stepKey, null);
        step.setWillComplete(true);
        SortedMap<String, LifecyclePolicyMetadata> lifecyclePolicyMap = new TreeMap<>(Collections.singletonMap(policyName,
            new LifecyclePolicyMetadata(createPolicy(policyName, null, step.getKey()), new HashMap<>(),
                randomNonNegativeLong(), randomNonNegativeLong())));
        Index index = new Index("my_index",  "uuid");
        Map<String, Step> firstStepMap = Collections.singletonMap(policyName, step);
        Map<StepKey, Step> policySteps = Collections.singletonMap(step.getKey(), step);
        Map<String, Map<StepKey, Step>> stepMap = Collections.singletonMap(policyName, policySteps);
        Map<Index, List<Step>> indexSteps = Collections.singletonMap(index, Collections.singletonList(step));
        PolicyStepsRegistry policyStepsRegistry = new PolicyStepsRegistry(lifecyclePolicyMap, firstStepMap,
            stepMap, NamedXContentRegistry.EMPTY, null);
        ClusterService clusterService = mock(ClusterService.class);
        final AtomicLong now = new AtomicLong(5);
        IndexLifecycleRunner runner = new IndexLifecycleRunner(policyStepsRegistry, clusterService, now::get);
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
    }


    public static void assertIndexNotManagedByILM(ClusterState clusterState, Index index) {
        MetaData metadata = clusterState.metaData();
        assertNotNull(metadata);
        IndexMetaData indexMetadata = metadata.getIndexSafe(index);
        assertNotNull(indexMetadata);
        Settings indexSettings = indexMetadata.getSettings();
        assertNotNull(indexSettings);
        assertFalse(LifecycleSettings.LIFECYCLE_NAME_SETTING.exists(indexSettings));
        assertFalse(LifecycleSettings.LIFECYCLE_SKIP_SETTING.exists(indexSettings));
        assertFalse(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS_SETTING.exists(indexSettings));
    }

    public static void assertClusterStateOnPolicy(ClusterState oldClusterState, Index index, String expectedPolicy, StepKey previousStep,
            StepKey expectedStep, ClusterState newClusterState, long now) {
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
        assertEquals(expectedStep.getPhase(), newLifecycleState.getPhase());
        assertEquals(expectedStep.getAction(), newLifecycleState.getAction());
        assertEquals(expectedStep.getName(), newLifecycleState.getStep());
        if (Objects.equals(previousStep.getPhase(), expectedStep.getPhase())) {
            assertEquals(oldLifecycleState.getPhase(), newLifecycleState.getPhase());
        } else {
            assertEquals(now, newLifecycleState.getPhaseTime().longValue());
        }
        if (Objects.equals(previousStep.getAction(), expectedStep.getAction())) {
            assertEquals(oldLifecycleState.getActionTime(), newLifecycleState.getActionTime());
        } else {
            assertEquals(now, newLifecycleState.getActionTime().longValue());
        }
        if (Objects.equals(previousStep.getName(), expectedStep.getName())) {
            assertEquals(oldLifecycleState.getStepTime(), newLifecycleState.getStepTime());
        } else {
            assertEquals(now, newLifecycleState.getStepTime().longValue());
        }
        assertEquals(null, newLifecycleState.getFailedStep());
        assertEquals(null, newLifecycleState.getStepInfo());
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

    private void assertClusterStateOnErrorStep(ClusterState oldClusterState, Index index, StepKey currentStep,
                                               ClusterState newClusterState, long now, String expectedCauseValue) throws IOException {
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
        assertEquals(currentStep.getPhase(), newLifecycleState.getPhase());
        assertEquals(currentStep.getAction(), newLifecycleState.getAction());
        assertEquals(ErrorStep.NAME, newLifecycleState.getStep());
        assertEquals(currentStep.getName(), newLifecycleState.getFailedStep());
        assertEquals(expectedCauseValue, newLifecycleState.getStepInfo());
        assertEquals(oldLifecycleState.getPhaseTime(), newLifecycleState.getPhaseTime());
        assertEquals(oldLifecycleState.getActionTime(), newLifecycleState.getActionTime());
        assertEquals(now, newLifecycleState.getStepTime().longValue());
    }

    private void assertClusterStateStepInfo(ClusterState oldClusterState, Index index, StepKey currentStep, ClusterState newClusterState,
            ToXContentObject stepInfo) throws IOException {
        XContentBuilder stepInfoXContentBuilder = JsonXContent.contentBuilder();
        stepInfo.toXContent(stepInfoXContentBuilder, ToXContent.EMPTY_PARAMS);
        String expectedstepInfoValue = BytesReference.bytes(stepInfoXContentBuilder).utf8ToString();
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
        assertEquals(currentStep.getPhase(), newLifecycleState.getPhase());
        assertEquals(currentStep.getAction(), newLifecycleState.getAction());
        assertEquals(currentStep.getName(), newLifecycleState.getStep());
        assertEquals(expectedstepInfoValue, newLifecycleState.getStepInfo());
        assertEquals(oldLifecycleState.getPhaseTime(), newLifecycleState.getPhaseTime());
        assertEquals(oldLifecycleState.getActionTime(), newLifecycleState.getActionTime());
        assertEquals(newLifecycleState.getStepTime(), newLifecycleState.getStepTime());
    }

    private static class MockAsyncActionStep extends AsyncActionStep {

        private Exception exception;
        private boolean willComplete;
        private boolean indexSurvives = true;
        private long executeCount = 0;

        MockAsyncActionStep(StepKey key, StepKey nextStepKey) {
            super(key, nextStepKey, null);
        }

        void setException(Exception exception) {
            this.exception = exception;
        }

        void setIndexSurvives(boolean indexSurvives) {
            this.indexSurvives = indexSurvives;
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

        @Override
        public void performAction(IndexMetaData indexMetaData, ClusterState currentState, Listener listener) {
            executeCount++;
            if (exception == null) {
                listener.onResponse(willComplete);
            } else {
                listener.onFailure(exception);
            }
        }

    }

    private static class MockAsyncWaitStep extends AsyncWaitStep {

        private Exception exception;
        private boolean willComplete;
        private long executeCount = 0;
        private ToXContentObject expectedInfo = null;

        MockAsyncWaitStep(StepKey key, StepKey nextStepKey) {
            super(key, nextStepKey, null);
        }

        void setException(Exception exception) {
            this.exception = exception;
        }

        void setWillComplete(boolean willComplete) {
            this.willComplete = willComplete;
        }

        void expectedInfo(ToXContentObject expectedInfo) {
            this.expectedInfo = expectedInfo;
        }

        long getExecuteCount() {
            return executeCount;
        }

        @Override
        public void evaluateCondition(Index index, Listener listener) {
            executeCount++;
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

        MockClusterStateActionStep(StepKey key, StepKey nextStepKey) {
            super(key, nextStepKey);
        }

        public void setException(RuntimeException exception) {
            this.exception = exception;
        }

        public long getExecuteCount() {
            return executeCount;
        }

        @Override
        public ClusterState performAction(Index index, ClusterState clusterState) {
            executeCount++;
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

    private static class MoveToNextStepUpdateTaskMatcher extends ArgumentMatcher<MoveToNextStepUpdateTask> {

        private Index index;
        private String policy;
        private StepKey currentStepKey;
        private StepKey nextStepKey;

        MoveToNextStepUpdateTaskMatcher(Index index, String policy, StepKey currentStepKey, StepKey nextStepKey) {
            this.index = index;
            this.policy = policy;
            this.currentStepKey = currentStepKey;
            this.nextStepKey = nextStepKey;
        }

        @Override
        public boolean matches(Object argument) {
            if (argument == null || argument instanceof MoveToNextStepUpdateTask == false) {
                return false;
            }
            MoveToNextStepUpdateTask task = (MoveToNextStepUpdateTask) argument;
            return Objects.equals(index, task.getIndex()) &&
                    Objects.equals(policy, task.getPolicy()) &&
                    Objects.equals(currentStepKey, task.getCurrentStepKey()) &&
                    Objects.equals(nextStepKey, task.getNextStepKey());
        }

    }

    private static class MoveToErrorStepUpdateTaskMatcher extends ArgumentMatcher<MoveToErrorStepUpdateTask> {

        private Index index;
        private String policy;
        private StepKey currentStepKey;
        private Exception cause;

        MoveToErrorStepUpdateTaskMatcher(Index index, String policy, StepKey currentStepKey, Exception cause) {
            this.index = index;
            this.policy = policy;
            this.currentStepKey = currentStepKey;
            this.cause = cause;
        }

        @Override
        public boolean matches(Object argument) {
            if (argument == null || argument instanceof MoveToErrorStepUpdateTask == false) {
                return false;
            }
            MoveToErrorStepUpdateTask task = (MoveToErrorStepUpdateTask) argument;
            return Objects.equals(index, task.getIndex()) &&
                    Objects.equals(policy, task.getPolicy())&&
                    Objects.equals(currentStepKey, task.getCurrentStepKey()) &&
                    Objects.equals(cause.getClass(), task.getCause().getClass()) &&
                    Objects.equals(cause.getMessage(), task.getCause().getMessage());
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
                    Objects.equals(stepInfo, task.getStepInfo());
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
}
