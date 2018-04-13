/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.indexlifecycle.AsyncActionStep;
import org.elasticsearch.xpack.core.indexlifecycle.AsyncWaitStep;
import org.elasticsearch.xpack.core.indexlifecycle.ClusterStateWaitStep;
import org.elasticsearch.xpack.core.indexlifecycle.InitializePolicyContextStep;
import org.elasticsearch.xpack.core.indexlifecycle.LifecyclePolicy;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleSettings;
import org.elasticsearch.xpack.core.indexlifecycle.MockStep;
import org.elasticsearch.xpack.core.indexlifecycle.Step;
import org.elasticsearch.xpack.core.indexlifecycle.Step.StepKey;
import org.elasticsearch.xpack.core.indexlifecycle.TerminalPolicyStep;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;

public class IndexLifecycleRunnerTests extends ESTestCase {

    private PolicyStepsRegistry createOneStepPolicyStepRegistry(String policyName, Step step) {
        SortedMap<String, LifecyclePolicy> lifecyclePolicyMap = null; // Not used in this test
        Map<String, Step> firstStepMap = new HashMap<>();
        firstStepMap.put(policyName, step);
        Map<String, Map<StepKey, Step>> stepMap = new HashMap<>();
        Map<StepKey, Step> policySteps = new HashMap<>();
        policySteps.put(step.getKey(), step);
        stepMap.put(policyName, policySteps);
        return new PolicyStepsRegistry(lifecyclePolicyMap, firstStepMap, stepMap);
    }

    public void testRunPolicyTerminalPolicyStep() {
        String policyName = "async_action_policy";
        TerminalPolicyStep step = TerminalPolicyStep.INSTANCE;
        PolicyStepsRegistry stepRegistry = createOneStepPolicyStepRegistry(policyName, step);
        ClusterService clusterService = Mockito.mock(ClusterService.class);
        IndexLifecycleRunner runner = new IndexLifecycleRunner(stepRegistry, clusterService, () -> 0L);
        IndexMetaData indexMetaData = IndexMetaData.builder("my_index").settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        Settings indexSettings = Settings.builder().build();

        runner.runPolicy(policyName, indexMetaData, indexSettings, false);

        Mockito.verifyZeroInteractions(clusterService);
    }

    public void testRunPolicyClusterStateActionStep() {
        String policyName = "cluster_state_action_policy";
        StepKey stepKey = new StepKey("phase", "action", "cluster_state_action_step");
        MockInitializePolicyContextStep step = new MockInitializePolicyContextStep(stepKey, null);
        PolicyStepsRegistry stepRegistry = createOneStepPolicyStepRegistry(policyName, step);
        ClusterService clusterService = Mockito.mock(ClusterService.class);
        IndexLifecycleRunner runner = new IndexLifecycleRunner(stepRegistry, clusterService, () -> 0L);
        IndexMetaData indexMetaData = IndexMetaData.builder("my_index").settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        Settings indexSettings = Settings.builder().build();

        runner.runPolicy(policyName, indexMetaData, indexSettings, randomBoolean());

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
        ClusterService clusterService = Mockito.mock(ClusterService.class);
        IndexLifecycleRunner runner = new IndexLifecycleRunner(stepRegistry, clusterService, () -> 0L);
        IndexMetaData indexMetaData = IndexMetaData.builder("my_index").settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        Settings indexSettings = Settings.builder().build();

        runner.runPolicy(policyName, indexMetaData, indexSettings, randomBoolean());

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
        ClusterService clusterService = Mockito.mock(ClusterService.class);
        IndexLifecycleRunner runner = new IndexLifecycleRunner(stepRegistry, clusterService, () -> 0L);
        IndexMetaData indexMetaData = IndexMetaData.builder("my_index").settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        Settings indexSettings = Settings.builder().build();

        runner.runPolicy(policyName, indexMetaData, indexSettings, false);

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
        ClusterService clusterService = Mockito.mock(ClusterService.class);
        IndexLifecycleRunner runner = new IndexLifecycleRunner(stepRegistry, clusterService, () -> 0L);
        IndexMetaData indexMetaData = IndexMetaData.builder("my_index").settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        Settings indexSettings = Settings.builder().build();

        runner.runPolicy(policyName, indexMetaData, indexSettings, false);

        assertEquals(1, step.getExecuteCount());
        Mockito.verifyZeroInteractions(clusterService);
    }

    public void testRunPolicyAsyncActionStepNotComplete() {
        String policyName = "async_action_policy";
        StepKey stepKey = new StepKey("phase", "action", "async_action_step");
        MockAsyncActionStep step = new MockAsyncActionStep(stepKey, null);
        step.setWillComplete(false);
        PolicyStepsRegistry stepRegistry = createOneStepPolicyStepRegistry(policyName, step);
        ClusterService clusterService = Mockito.mock(ClusterService.class);
        IndexLifecycleRunner runner = new IndexLifecycleRunner(stepRegistry, clusterService, () -> 0L);
        IndexMetaData indexMetaData = IndexMetaData.builder("my_index").settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        Settings indexSettings = Settings.builder().build();

        runner.runPolicy(policyName, indexMetaData, indexSettings, false);

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
        ClusterService clusterService = Mockito.mock(ClusterService.class);
        IndexLifecycleRunner runner = new IndexLifecycleRunner(stepRegistry, clusterService, () -> 0L);
        IndexMetaData indexMetaData = IndexMetaData.builder("my_index").settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        Settings indexSettings = Settings.builder().build();

        RuntimeException exception = expectThrows(RuntimeException.class,
                () -> runner.runPolicy(policyName, indexMetaData, indexSettings, false));

        assertSame(expectedException, exception.getCause());
        assertEquals(1, step.getExecuteCount());
        Mockito.verifyZeroInteractions(clusterService);
    }

    public void testRunPolicyAsyncActionStepClusterStateChangeIgnored() {
        String policyName = "async_action_policy";
        StepKey stepKey = new StepKey("phase", "action", "async_action_step");
        MockAsyncActionStep step = new MockAsyncActionStep(stepKey, null);
        Exception expectedException = new RuntimeException();
        step.setException(expectedException);
        PolicyStepsRegistry stepRegistry = createOneStepPolicyStepRegistry(policyName, step);
        ClusterService clusterService = Mockito.mock(ClusterService.class);
        IndexLifecycleRunner runner = new IndexLifecycleRunner(stepRegistry, clusterService, () -> 0L);
        IndexMetaData indexMetaData = IndexMetaData.builder("my_index").settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        Settings indexSettings = Settings.builder().build();

        runner.runPolicy(policyName, indexMetaData, indexSettings, true);

        assertEquals(0, step.getExecuteCount());
        Mockito.verifyZeroInteractions(clusterService);
    }

    public void testRunPolicyAsyncWaitStepCompletes() {
        String policyName = "async_wait_policy";
        StepKey stepKey = new StepKey("phase", "action", "async_wait_step");
        MockAsyncWaitStep step = new MockAsyncWaitStep(stepKey, null);
        step.setWillComplete(true);
        PolicyStepsRegistry stepRegistry = createOneStepPolicyStepRegistry(policyName, step);
        ClusterService clusterService = Mockito.mock(ClusterService.class);
        IndexLifecycleRunner runner = new IndexLifecycleRunner(stepRegistry, clusterService, () -> 0L);
        IndexMetaData indexMetaData = IndexMetaData.builder("my_index").settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        Settings indexSettings = Settings.builder().build();

        runner.runPolicy(policyName, indexMetaData, indexSettings, false);

        assertEquals(1, step.getExecuteCount());
        Mockito.verify(clusterService, Mockito.times(1)).submitStateUpdateTask(Mockito.matches("ILM"),
                Mockito.argThat(new MoveToNextStepUpdateTaskMatcher(indexMetaData.getIndex(), policyName, stepKey, null)));
        Mockito.verifyNoMoreInteractions(clusterService);
    }

    public void testRunPolicyAsyncWaitStepNotComplete() {
        String policyName = "async_wait_policy";
        StepKey stepKey = new StepKey("phase", "action", "async_wait_step");
        MockAsyncWaitStep step = new MockAsyncWaitStep(stepKey, null);
        step.setWillComplete(false);
        PolicyStepsRegistry stepRegistry = createOneStepPolicyStepRegistry(policyName, step);
        ClusterService clusterService = Mockito.mock(ClusterService.class);
        IndexLifecycleRunner runner = new IndexLifecycleRunner(stepRegistry, clusterService, () -> 0L);
        IndexMetaData indexMetaData = IndexMetaData.builder("my_index").settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        Settings indexSettings = Settings.builder().build();

        runner.runPolicy(policyName, indexMetaData, indexSettings, false);

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
        ClusterService clusterService = Mockito.mock(ClusterService.class);
        IndexLifecycleRunner runner = new IndexLifecycleRunner(stepRegistry, clusterService, () -> 0L);
        IndexMetaData indexMetaData = IndexMetaData.builder("my_index").settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        Settings indexSettings = Settings.builder().build();

        RuntimeException exception = expectThrows(RuntimeException.class,
                () -> runner.runPolicy(policyName, indexMetaData, indexSettings, false));

        assertSame(expectedException, exception.getCause());
        assertEquals(1, step.getExecuteCount());
        Mockito.verifyZeroInteractions(clusterService);
    }

    public void testRunPolicyAsyncWaitStepClusterStateChangeIgnored() {
        String policyName = "async_wait_policy";
        StepKey stepKey = new StepKey("phase", "action", "async_wait_step");
        MockAsyncWaitStep step = new MockAsyncWaitStep(stepKey, null);
        Exception expectedException = new RuntimeException();
        step.setException(expectedException);
        PolicyStepsRegistry stepRegistry = createOneStepPolicyStepRegistry(policyName, step);
        ClusterService clusterService = Mockito.mock(ClusterService.class);
        IndexLifecycleRunner runner = new IndexLifecycleRunner(stepRegistry, clusterService, () -> 0L);
        IndexMetaData indexMetaData = IndexMetaData.builder("my_index").settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        Settings indexSettings = Settings.builder().build();

        runner.runPolicy(policyName, indexMetaData, indexSettings, true);

        assertEquals(0, step.getExecuteCount());
        Mockito.verifyZeroInteractions(clusterService);
    }

    public void testRunPolicyUnknownStepType() {
        String policyName = "cluster_state_action_policy";
        StepKey stepKey = new StepKey("phase", "action", "cluster_state_action_step");
        MockStep step = new MockStep(stepKey, null);
        PolicyStepsRegistry stepRegistry = createOneStepPolicyStepRegistry(policyName, step);
        ClusterService clusterService = Mockito.mock(ClusterService.class);
        IndexLifecycleRunner runner = new IndexLifecycleRunner(stepRegistry, clusterService, () -> 0L);
        IndexMetaData indexMetaData = IndexMetaData.builder("my_index").settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        Settings indexSettings = Settings.builder().build();

        IllegalStateException exception = expectThrows(IllegalStateException.class,
                () -> runner.runPolicy(policyName, indexMetaData, indexSettings, randomBoolean()));
        assertEquals("Step with key [" + stepKey + "] is not a recognised type: [" + step.getClass().getName() + "]",
                exception.getMessage());
        Mockito.verifyZeroInteractions(clusterService);
    }

    public void testGetCurrentStepKey() {
        Settings indexSettings = Settings.EMPTY;
        StepKey stepKey = IndexLifecycleRunner.getCurrentStepKey(indexSettings);
        assertNull(stepKey);
        
        String phase = randomAlphaOfLength(20);
        String action = randomAlphaOfLength(20);
        String step = randomAlphaOfLength(20);
        Settings indexSettings2 = Settings.builder()
                .put(LifecycleSettings.LIFECYCLE_PHASE, phase)
                .put(LifecycleSettings.LIFECYCLE_ACTION, action)
                .put(LifecycleSettings.LIFECYCLE_STEP, step)
                .build();
        stepKey = IndexLifecycleRunner.getCurrentStepKey(indexSettings2);
        assertNotNull(stepKey);
        assertEquals(phase, stepKey.getPhase());
        assertEquals(action, stepKey.getAction());
        assertEquals(step, stepKey.getName());
        
        phase = randomAlphaOfLength(20);
        action = randomAlphaOfLength(20);
        step = randomBoolean() ? null : "";
        Settings indexSettings3 = Settings.builder()
                .put(LifecycleSettings.LIFECYCLE_PHASE, phase)
                .put(LifecycleSettings.LIFECYCLE_ACTION, action)
                .put(LifecycleSettings.LIFECYCLE_STEP, step)
                .build();
        AssertionError error3 = expectThrows(AssertionError.class, () -> IndexLifecycleRunner.getCurrentStepKey(indexSettings3));
        assertEquals("Current phase is not empty: " + phase, error3.getMessage());
        
        phase = randomBoolean() ? null : "";
        action = randomAlphaOfLength(20);
        step = randomBoolean() ? null : "";
        Settings indexSettings4 = Settings.builder()
                .put(LifecycleSettings.LIFECYCLE_PHASE, phase)
                .put(LifecycleSettings.LIFECYCLE_ACTION, action)
                .put(LifecycleSettings.LIFECYCLE_STEP, step)
                .build();
        AssertionError error4 = expectThrows(AssertionError.class, () -> IndexLifecycleRunner.getCurrentStepKey(indexSettings4));
        assertEquals("Current action is not empty: " + action, error4.getMessage());
        
        phase = randomBoolean() ? null : "";
        action = randomAlphaOfLength(20);
        step = randomAlphaOfLength(20);
        Settings indexSettings5 = Settings.builder()
                .put(LifecycleSettings.LIFECYCLE_PHASE, phase)
                .put(LifecycleSettings.LIFECYCLE_ACTION, action)
                .put(LifecycleSettings.LIFECYCLE_STEP, step)
                .build();
        AssertionError error5 = expectThrows(AssertionError.class, () -> IndexLifecycleRunner.getCurrentStepKey(indexSettings5));
        assertEquals(null, error5.getMessage());
        
        phase = randomBoolean() ? null : "";
        action = randomBoolean() ? null : "";
        step = randomAlphaOfLength(20);
        Settings indexSettings6 = Settings.builder()
                .put(LifecycleSettings.LIFECYCLE_PHASE, phase)
                .put(LifecycleSettings.LIFECYCLE_ACTION, action)
                .put(LifecycleSettings.LIFECYCLE_STEP, step)
                .build();
        AssertionError error6 = expectThrows(AssertionError.class, () -> IndexLifecycleRunner.getCurrentStepKey(indexSettings6));
        assertEquals(null, error6.getMessage());
    }
    
    public void testGetCurrentStep() {
        SortedMap<String, LifecyclePolicy> lifecyclePolicyMap = null; // Not used in the methods tested here
        String policyName = "policy_1";
        String otherPolicyName = "other_policy";
        StepKey firstStepKey = new StepKey("phase_1", "action_1", "step_1");
        StepKey secondStepKey = new StepKey("phase_1", "action_1", "step_2");
        StepKey thirdStepKey = new StepKey("phase_1", "action_2", "step_1");
        StepKey fourthStepKey = new StepKey("phase_2", "action_1", "step_1");
        StepKey otherPolicyFirstStepKey = new StepKey("phase_1", "action_1", "step_1");
        StepKey otherPolicySecondStepKey = new StepKey("phase_1", "action_1", "step_2");
        Step firstStep = new MockStep(firstStepKey, secondStepKey);
        Step secondStep = new MockStep(secondStepKey, thirdStepKey);
        Step thirdStep = new MockStep(thirdStepKey, fourthStepKey);
        Step fourthStep = new MockStep(fourthStepKey, null);
        Step otherPolicyFirstStep = new MockStep(firstStepKey, secondStepKey);
        Step otherPolicySecondStep = new MockStep(secondStepKey, null);
        Map<String, Step> firstStepMap = new HashMap<>();
        firstStepMap.put(policyName, firstStep);
        firstStepMap.put(otherPolicyName, otherPolicyFirstStep);
        Map<String, Map<StepKey, Step>> stepMap = new HashMap<>();
        Map<StepKey, Step> policySteps = new HashMap<>();
        policySteps.put(firstStepKey, firstStep);
        policySteps.put(secondStepKey, secondStep);
        policySteps.put(thirdStepKey, thirdStep);
        policySteps.put(fourthStepKey, fourthStep);
        stepMap.put(policyName, policySteps);
        Map<StepKey, Step> otherPolicySteps = new HashMap<>();
        otherPolicySteps.put(otherPolicyFirstStepKey, otherPolicyFirstStep);
        otherPolicySteps.put(otherPolicySecondStepKey, otherPolicySecondStep);
        stepMap.put(otherPolicyName, otherPolicySteps);
        PolicyStepsRegistry registry = new PolicyStepsRegistry(lifecyclePolicyMap, firstStepMap, stepMap);

        Settings indexSettings = Settings.EMPTY;
        Step actualStep = IndexLifecycleRunner.getCurrentStep(registry, policyName, indexSettings);
        assertSame(firstStep, actualStep);

        indexSettings = Settings.builder()
                .put(LifecycleSettings.LIFECYCLE_PHASE, "phase_1")
                .put(LifecycleSettings.LIFECYCLE_ACTION, "action_1")
                .put(LifecycleSettings.LIFECYCLE_STEP, "step_1")
                .build();
        actualStep = IndexLifecycleRunner.getCurrentStep(registry, policyName, indexSettings);
        assertSame(firstStep, actualStep);

        indexSettings = Settings.builder()
                .put(LifecycleSettings.LIFECYCLE_PHASE, "phase_1")
                .put(LifecycleSettings.LIFECYCLE_ACTION, "action_1")
                .put(LifecycleSettings.LIFECYCLE_STEP, "step_2")
                .build();
        actualStep = IndexLifecycleRunner.getCurrentStep(registry, policyName, indexSettings);
        assertSame(secondStep, actualStep);

        indexSettings = Settings.builder()
                .put(LifecycleSettings.LIFECYCLE_PHASE, "phase_1")
                .put(LifecycleSettings.LIFECYCLE_ACTION, "action_2")
                .put(LifecycleSettings.LIFECYCLE_STEP, "step_1")
                .build();
        actualStep = IndexLifecycleRunner.getCurrentStep(registry, policyName, indexSettings);
        assertSame(thirdStep, actualStep);

        indexSettings = Settings.builder()
                .put(LifecycleSettings.LIFECYCLE_PHASE, "phase_2")
                .put(LifecycleSettings.LIFECYCLE_ACTION, "action_1")
                .put(LifecycleSettings.LIFECYCLE_STEP, "step_1")
                .build();
        actualStep = IndexLifecycleRunner.getCurrentStep(registry, policyName, indexSettings);
        assertSame(fourthStep, actualStep);

        indexSettings = Settings.builder()
                .put(LifecycleSettings.LIFECYCLE_PHASE, "phase_2")
                .put(LifecycleSettings.LIFECYCLE_ACTION, "action_1")
                .put(LifecycleSettings.LIFECYCLE_STEP, "step_1")
                .build();
        actualStep = IndexLifecycleRunner.getCurrentStep(registry, policyName, indexSettings);
        assertSame(fourthStep, actualStep);

        indexSettings = Settings.builder()
                .put(LifecycleSettings.LIFECYCLE_PHASE, "phase_1")
                .put(LifecycleSettings.LIFECYCLE_ACTION, "action_1")
                .put(LifecycleSettings.LIFECYCLE_STEP, "step_1")
                .build();
        actualStep = IndexLifecycleRunner.getCurrentStep(registry, otherPolicyName, indexSettings);
        assertSame(otherPolicyFirstStep, actualStep);

        indexSettings = Settings.builder()
                .put(LifecycleSettings.LIFECYCLE_PHASE, "phase_1")
                .put(LifecycleSettings.LIFECYCLE_ACTION, "action_1")
                .put(LifecycleSettings.LIFECYCLE_STEP, "step_2")
                .build();
        actualStep = IndexLifecycleRunner.getCurrentStep(registry, otherPolicyName, indexSettings);
        assertSame(otherPolicySecondStep, actualStep);

        Settings invalidIndexSettings = Settings.builder()
                .put(LifecycleSettings.LIFECYCLE_PHASE, "phase_1")
                .put(LifecycleSettings.LIFECYCLE_ACTION, "action_1")
                .put(LifecycleSettings.LIFECYCLE_STEP, "step_3")
                .build();
        IllegalStateException exception = expectThrows(IllegalStateException.class,
                () -> IndexLifecycleRunner.getCurrentStep(registry, policyName, invalidIndexSettings));
        assertEquals("step [[phase_1][action_1][step_3]] does not exist", exception.getMessage());

        exception = expectThrows(IllegalStateException.class,
                () -> IndexLifecycleRunner.getCurrentStep(registry, "policy_does_not_exist", invalidIndexSettings));
        assertEquals("policy [policy_does_not_exist] does not exist", exception.getMessage());
    }
    
    public void testMoveClusterStateToNextStep() {
        String indexName = "my_index";
        StepKey currentStep = new StepKey("current_phase", "current_action", "current_step");
        StepKey nextStep = new StepKey("next_phase", "next_action", "next_step");
        long now = randomNonNegativeLong();

        ClusterState clusterState = buildClusterState(indexName, Settings.builder());
        Index index = clusterState.metaData().index(indexName).getIndex();
        ClusterState newClusterState = IndexLifecycleRunner.moveClusterStateToNextStep(index, clusterState, currentStep, nextStep,
                () -> now);
        assertClusterStateOnNextStep(clusterState, index, currentStep, nextStep, newClusterState, now);

        clusterState = buildClusterState(indexName,
                Settings.builder().put(LifecycleSettings.LIFECYCLE_PHASE, currentStep.getPhase())
                        .put(LifecycleSettings.LIFECYCLE_ACTION, currentStep.getAction())
                        .put(LifecycleSettings.LIFECYCLE_STEP, currentStep.getName()));
        index = clusterState.metaData().index(indexName).getIndex();
        newClusterState = IndexLifecycleRunner.moveClusterStateToNextStep(index, clusterState, currentStep, nextStep, () -> now);
        assertClusterStateOnNextStep(clusterState, index, currentStep, nextStep, newClusterState, now);
    }

    public void testMoveClusterStateToNextStepSamePhase() {
        String indexName = "my_index";
        StepKey currentStep = new StepKey("current_phase", "current_action", "current_step");
        StepKey nextStep = new StepKey("current_phase", "next_action", "next_step");
        long now = randomNonNegativeLong();

        ClusterState clusterState = buildClusterState(indexName, Settings.builder());
        Index index = clusterState.metaData().index(indexName).getIndex();
        ClusterState newClusterState = IndexLifecycleRunner.moveClusterStateToNextStep(index, clusterState, currentStep, nextStep,
                () -> now);
        assertClusterStateOnNextStep(clusterState, index, currentStep, nextStep, newClusterState, now);

        clusterState = buildClusterState(indexName,
                Settings.builder().put(LifecycleSettings.LIFECYCLE_PHASE, currentStep.getPhase())
                        .put(LifecycleSettings.LIFECYCLE_ACTION, currentStep.getAction())
                        .put(LifecycleSettings.LIFECYCLE_STEP, currentStep.getName()));
        index = clusterState.metaData().index(indexName).getIndex();
        newClusterState = IndexLifecycleRunner.moveClusterStateToNextStep(index, clusterState, currentStep, nextStep, () -> now);
        assertClusterStateOnNextStep(clusterState, index, currentStep, nextStep, newClusterState, now);
    }

    public void testMoveClusterStateToNextStepSameAction() {
        String indexName = "my_index";
        StepKey currentStep = new StepKey("current_phase", "current_action", "current_step");
        StepKey nextStep = new StepKey("current_phase", "current_action", "next_step");
        long now = randomNonNegativeLong();

        ClusterState clusterState = buildClusterState(indexName, Settings.builder());
        Index index = clusterState.metaData().index(indexName).getIndex();
        ClusterState newClusterState = IndexLifecycleRunner.moveClusterStateToNextStep(index, clusterState, currentStep, nextStep,
                () -> now);
        assertClusterStateOnNextStep(clusterState, index, currentStep, nextStep, newClusterState, now);

        clusterState = buildClusterState(indexName,
                Settings.builder().put(LifecycleSettings.LIFECYCLE_PHASE, currentStep.getPhase())
                        .put(LifecycleSettings.LIFECYCLE_ACTION, currentStep.getAction())
                        .put(LifecycleSettings.LIFECYCLE_STEP, currentStep.getName()));
        index = clusterState.metaData().index(indexName).getIndex();
        newClusterState = IndexLifecycleRunner.moveClusterStateToNextStep(index, clusterState, currentStep, nextStep, () -> now);
        assertClusterStateOnNextStep(clusterState, index, currentStep, nextStep, newClusterState, now);
    }

    private ClusterState buildClusterState(String indexName, Settings.Builder indexSettingsBuilder) {
        Settings indexSettings = indexSettingsBuilder.put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0).put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).build();
        IndexMetaData indexMetadata = IndexMetaData.builder(indexName).settings(indexSettings)
                .build();
        MetaData metadata = MetaData.builder().put(indexMetadata, true).build();
        return ClusterState.builder(new ClusterName("my_cluster")).metaData(metadata).build();
    }

    private void assertClusterStateOnNextStep(ClusterState oldClusterState, Index index, StepKey currentStep, StepKey nextStep, ClusterState newClusterState, long now) {
        assertNotSame(oldClusterState, newClusterState);
        MetaData newMetadata = newClusterState.metaData();
        assertNotSame(oldClusterState.metaData(), newMetadata);
        IndexMetaData newIndexMetadata = newMetadata.getIndexSafe(index);
        assertNotSame(oldClusterState.metaData().index(index), newIndexMetadata);
        Settings newIndexSettings = newIndexMetadata.getSettings();
        assertNotSame(oldClusterState.metaData().index(index).getSettings(), newIndexSettings);
        assertEquals(nextStep.getPhase(), LifecycleSettings.LIFECYCLE_PHASE_SETTING.get(newIndexSettings));
        assertEquals(nextStep.getAction(), LifecycleSettings.LIFECYCLE_ACTION_SETTING.get(newIndexSettings));
        assertEquals(nextStep.getName(), LifecycleSettings.LIFECYCLE_STEP_SETTING.get(newIndexSettings));
        if (currentStep.getPhase().equals(nextStep.getPhase())) {
            assertEquals(LifecycleSettings.LIFECYCLE_PHASE_TIME_SETTING.get(oldClusterState.metaData().index(index).getSettings()),
                    LifecycleSettings.LIFECYCLE_PHASE_TIME_SETTING.get(newIndexSettings));
        } else {
            assertEquals(now, (long) LifecycleSettings.LIFECYCLE_PHASE_TIME_SETTING.get(newIndexSettings));
        }
        if (currentStep.getAction().equals(nextStep.getAction())) {
            assertEquals(LifecycleSettings.LIFECYCLE_ACTION_TIME_SETTING.get(oldClusterState.metaData().index(index).getSettings()),
                    LifecycleSettings.LIFECYCLE_ACTION_TIME_SETTING.get(newIndexSettings));
        } else {
            assertEquals(now, (long) LifecycleSettings.LIFECYCLE_ACTION_TIME_SETTING.get(newIndexSettings));
        }
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
        public void performAction(IndexMetaData indexMetaData, Listener listener) {
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

        MockAsyncWaitStep(StepKey key, StepKey nextStepKey) {
            super(key, nextStepKey, null);
        }

        void setException(Exception exception) {
            this.exception = exception;
        }

        void setWillComplete(boolean willComplete) {
            this.willComplete = willComplete;
        }

        long getExecuteCount() {
            return executeCount;
        }

        @Override
        public void evaluateCondition(Index index, Listener listener) {
            executeCount++;
            if (exception == null) {
                listener.onResponse(willComplete);
            } else {
                listener.onFailure(exception);
            }
        }

    }

    static class MockInitializePolicyContextStep extends InitializePolicyContextStep {

        private RuntimeException exception;
        private long executeCount = 0;

        MockInitializePolicyContextStep(StepKey key, StepKey nextStepKey) {
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

        MockClusterStateWaitStep(StepKey key, StepKey nextStepKey) {
            super(key, nextStepKey);
        }

        public void setException(RuntimeException exception) {
            this.exception = exception;
        }

        public void setWillComplete(boolean willComplete) {
            this.willComplete = willComplete;
        }

        public long getExecuteCount() {
            return executeCount;
        }

        @Override
        public boolean isConditionMet(Index index, ClusterState clusterState) {
            executeCount++;
            if (exception != null) {
                throw exception;
            }
            return willComplete;
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
