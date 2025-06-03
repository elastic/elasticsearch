/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ilm.AbstractStepTestCase;
import org.elasticsearch.xpack.core.ilm.DataTierMigrationRoutedStep;
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
import org.elasticsearch.xpack.core.ilm.RolloverAction;
import org.elasticsearch.xpack.core.ilm.RolloverStep;
import org.elasticsearch.xpack.core.ilm.SetPriorityAction;
import org.elasticsearch.xpack.core.ilm.Step;
import org.elasticsearch.xpack.core.ilm.WaitForRolloverReadyStep;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY;
import static org.elasticsearch.xpack.core.ilm.PhaseCacheManagement.eligibleToCheckForRefresh;
import static org.elasticsearch.xpack.core.ilm.PhaseCacheManagementTests.refreshPhaseDefinition;
import static org.elasticsearch.xpack.ilm.IndexLifecycleRunnerTests.createOneStepPolicyStepRegistry;
import static org.elasticsearch.xpack.ilm.IndexLifecycleTransition.moveStateToNextActionAndUpdateCachedPhase;
import static org.elasticsearch.xpack.ilm.LifecyclePolicyTestsUtils.newTestLifecyclePolicy;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class IndexLifecycleTransitionTests extends ESTestCase {

    public void testMoveClusterStateToNextStep() {
        String indexName = "my_index";
        LifecyclePolicy policy = randomValueOtherThanMany(
            p -> p.getPhases().isEmpty(),
            () -> LifecyclePolicyTests.randomTestLifecyclePolicy("policy")
        );
        Phase nextPhase = policy.getPhases()
            .values()
            .stream()
            .findFirst()
            .orElseThrow(() -> new AssertionError("expected next phase to be present"));
        List<LifecyclePolicyMetadata> policyMetadatas = List.of(
            new LifecyclePolicyMetadata(policy, Map.of(), randomNonNegativeLong(), randomNonNegativeLong())
        );
        Step.StepKey currentStep = new Step.StepKey("current_phase", "current_action", "current_step");
        Step.StepKey nextStep = new Step.StepKey(nextPhase.getName(), "next_action", "next_step");
        long now = randomNonNegativeLong();

        // test going from null lifecycle settings to next step
        ProjectMetadata project = buildProject(
            indexName,
            Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, policy.getName()),
            LifecycleExecutionState.builder().build(),
            policyMetadatas
        );
        Index index = project.index(indexName).getIndex();
        PolicyStepsRegistry stepsRegistry = createOneStepPolicyStepRegistry(policy.getName(), new MockStep(nextStep, nextStep));
        ProjectMetadata newProject = IndexLifecycleTransition.moveProjectToStep(index, project, nextStep, () -> now, stepsRegistry, false);
        assertProjectOnNextStep(project, index, currentStep, nextStep, newProject, now);

        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhase(currentStep.phase());
        lifecycleState.setAction(currentStep.action());
        lifecycleState.setStep(currentStep.name());
        // test going from set currentStep settings to nextStep
        Settings.Builder indexSettingsBuilder = Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, policy.getName());
        if (randomBoolean()) {
            lifecycleState.setStepInfo(randomAlphaOfLength(20));
        }

        project = buildProject(indexName, indexSettingsBuilder, lifecycleState.build(), policyMetadatas);
        index = project.index(indexName).getIndex();
        newProject = IndexLifecycleTransition.moveProjectToStep(index, project, nextStep, () -> now, stepsRegistry, false);
        assertProjectOnNextStep(project, index, currentStep, nextStep, newProject, now);
    }

    public void testMoveClusterStateToNextStepSamePhase() {
        String indexName = "my_index";
        LifecyclePolicy policy = randomValueOtherThanMany(
            p -> p.getPhases().isEmpty(),
            () -> LifecyclePolicyTests.randomTestLifecyclePolicy("policy")
        );
        List<LifecyclePolicyMetadata> policyMetadatas = List.of(
            new LifecyclePolicyMetadata(policy, Map.of(), randomNonNegativeLong(), randomNonNegativeLong())
        );
        Step.StepKey currentStep = new Step.StepKey("current_phase", "current_action", "current_step");
        Step.StepKey nextStep = new Step.StepKey("current_phase", "next_action", "next_step");
        long now = randomNonNegativeLong();

        ProjectMetadata project = buildProject(
            indexName,
            Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, policy.getName()),
            LifecycleExecutionState.builder()
                .setPhase(currentStep.phase())
                .setAction(currentStep.action())
                .setStep(currentStep.name())
                .build(),
            policyMetadatas
        );
        Index index = project.index(indexName).getIndex();
        PolicyStepsRegistry stepsRegistry = createOneStepPolicyStepRegistry(policy.getName(), new MockStep(nextStep, nextStep));
        ProjectMetadata newProject = IndexLifecycleTransition.moveProjectToStep(index, project, nextStep, () -> now, stepsRegistry, false);
        assertProjectOnNextStep(project, index, currentStep, nextStep, newProject, now);

        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhase(currentStep.phase());
        lifecycleState.setAction(currentStep.action());
        lifecycleState.setStep(currentStep.name());
        if (randomBoolean()) {
            lifecycleState.setStepInfo(randomAlphaOfLength(20));
        }

        Settings.Builder indexSettingsBuilder = Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, policy.getName());

        project = buildProject(indexName, indexSettingsBuilder, lifecycleState.build(), policyMetadatas);
        index = project.index(indexName).getIndex();
        newProject = IndexLifecycleTransition.moveProjectToStep(index, project, nextStep, () -> now, stepsRegistry, false);
        assertProjectOnNextStep(project, index, currentStep, nextStep, newProject, now);
    }

    public void testMoveClusterStateToNextStepSameAction() {
        String indexName = "my_index";
        LifecyclePolicy policy = randomValueOtherThanMany(
            p -> p.getPhases().isEmpty(),
            () -> LifecyclePolicyTests.randomTestLifecyclePolicy("policy")
        );
        List<LifecyclePolicyMetadata> policyMetadatas = List.of(
            new LifecyclePolicyMetadata(policy, Map.of(), randomNonNegativeLong(), randomNonNegativeLong())
        );
        Step.StepKey currentStep = new Step.StepKey("current_phase", "current_action", "current_step");
        Step.StepKey nextStep = new Step.StepKey("current_phase", "current_action", "next_step");
        long now = randomNonNegativeLong();

        ProjectMetadata project = buildProject(
            indexName,
            Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, policy.getName()),
            LifecycleExecutionState.builder()
                .setPhase(currentStep.phase())
                .setAction(currentStep.action())
                .setStep(currentStep.name())
                .build(),
            policyMetadatas
        );
        Index index = project.index(indexName).getIndex();
        PolicyStepsRegistry stepsRegistry = createOneStepPolicyStepRegistry(policy.getName(), new MockStep(nextStep, nextStep));
        ProjectMetadata newProject = IndexLifecycleTransition.moveProjectToStep(index, project, nextStep, () -> now, stepsRegistry, false);
        assertProjectOnNextStep(project, index, currentStep, nextStep, newProject, now);

        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhase(currentStep.phase());
        lifecycleState.setAction(currentStep.action());
        lifecycleState.setStep(currentStep.name());
        if (randomBoolean()) {
            lifecycleState.setStepInfo(randomAlphaOfLength(20));
        }

        Settings.Builder indexSettingsBuilder = Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, policy.getName());

        project = buildProject(indexName, indexSettingsBuilder, lifecycleState.build(), policyMetadatas);
        index = project.index(indexName).getIndex();
        newProject = IndexLifecycleTransition.moveProjectToStep(index, project, nextStep, () -> now, stepsRegistry, false);
        assertProjectOnNextStep(project, index, currentStep, nextStep, newProject, now);
    }

    public void testSuccessfulValidatedMoveClusterStateToNextStep() {
        String indexName = "my_index";
        String policyName = "my_policy";
        LifecyclePolicy policy = randomValueOtherThanMany(
            p -> p.getPhases().isEmpty(),
            () -> LifecyclePolicyTests.randomTestLifecyclePolicy(policyName)
        );
        Phase nextPhase = policy.getPhases()
            .values()
            .stream()
            .findFirst()
            .orElseThrow(() -> new AssertionError("expected next phase to be present"));
        List<LifecyclePolicyMetadata> policyMetadatas = List.of(
            new LifecyclePolicyMetadata(policy, Map.of(), randomNonNegativeLong(), randomNonNegativeLong())
        );
        Step.StepKey currentStepKey = new Step.StepKey("current_phase", "current_action", "current_step");
        Step.StepKey nextStepKey = new Step.StepKey(nextPhase.getName(), "next_action", "next_step");
        long now = randomNonNegativeLong();
        Step step = new MockStep(nextStepKey, nextStepKey);
        PolicyStepsRegistry stepRegistry = createOneStepPolicyStepRegistry(policyName, step);

        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhase(currentStepKey.phase());
        lifecycleState.setAction(currentStepKey.action());
        lifecycleState.setStep(currentStepKey.name());

        Settings.Builder indexSettingsBuilder = Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, policyName);
        ProjectMetadata project = buildProject(indexName, indexSettingsBuilder, lifecycleState.build(), policyMetadatas);
        Index index = project.index(indexName).getIndex();
        ProjectMetadata newProject = IndexLifecycleTransition.moveProjectToStep(index, project, nextStepKey, () -> now, stepRegistry, true);
        assertProjectOnNextStep(project, index, currentStepKey, nextStepKey, newProject, now);
    }

    public void testValidatedMoveClusterStateToNextStepWithoutPolicy() {
        String indexName = "my_index";
        String policyName = "policy";
        Step.StepKey currentStepKey = new Step.StepKey("current_phase", "current_action", "current_step");
        Step.StepKey nextStepKey = new Step.StepKey("next_phase", "next_action", "next_step");
        long now = randomNonNegativeLong();
        Step step = new MockStep(nextStepKey, nextStepKey);
        PolicyStepsRegistry stepRegistry = createOneStepPolicyStepRegistry(policyName, step);

        Settings.Builder indexSettingsBuilder = Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, randomBoolean() ? "" : null);
        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhase(currentStepKey.phase());
        lifecycleState.setAction(currentStepKey.action());
        lifecycleState.setStep(currentStepKey.name());

        ProjectMetadata project = buildProject(indexName, indexSettingsBuilder, lifecycleState.build(), List.of());
        Index index = project.index(indexName).getIndex();
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> IndexLifecycleTransition.moveProjectToStep(index, project, nextStepKey, () -> now, stepRegistry, true)
        );
        assertThat(exception.getMessage(), equalTo("index [my_index] is not associated with an Index Lifecycle Policy"));
    }

    public void testValidatedMoveClusterStateToNextStepInvalidNextStep() {
        String indexName = "my_index";
        String policyName = "my_policy";
        Step.StepKey currentStepKey = new Step.StepKey("current_phase", "current_action", "current_step");
        Step.StepKey nextStepKey = new Step.StepKey("next_phase", "next_action", "next_step");
        long now = randomNonNegativeLong();
        Step step = new MockStep(currentStepKey, nextStepKey);
        PolicyStepsRegistry stepRegistry = createOneStepPolicyStepRegistry(policyName, step);

        Settings.Builder indexSettingsBuilder = Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, policyName);
        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhase(currentStepKey.phase());
        lifecycleState.setAction(currentStepKey.action());
        lifecycleState.setStep(currentStepKey.name());

        ProjectMetadata project = buildProject(indexName, indexSettingsBuilder, lifecycleState.build(), List.of());
        Index index = project.index(indexName).getIndex();
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> IndexLifecycleTransition.moveProjectToStep(index, project, nextStepKey, () -> now, stepRegistry, true)
        );
        assertThat(exception.getMessage(), equalTo("""
            step [{"phase":"next_phase","action":"next_action","name":"next_step"}] \
            for index [my_index] with policy [my_policy] does not exist"""));
    }

    public void testMoveClusterStateToErrorStep() throws IOException {
        String indexName = "my_index";
        Step.StepKey currentStep = new Step.StepKey("current_phase", "current_action", "current_step");
        Step.StepKey nextStepKey = new Step.StepKey("next_phase", "next_action", "next_step");
        long now = randomNonNegativeLong();
        Exception cause = new ElasticsearchException("THIS IS AN EXPECTED CAUSE");

        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhase(currentStep.phase());
        lifecycleState.setAction(currentStep.action());
        lifecycleState.setStep(currentStep.name());
        ClusterState clusterState = buildClusterState(indexName, Settings.builder(), lifecycleState.build(), List.of());
        Index index = clusterState.metadata().getProject().index(indexName).getIndex();

        ClusterState newClusterState = IndexLifecycleTransition.moveClusterStateToErrorStep(
            index,
            clusterState,
            cause,
            () -> now,
            (idxMeta, stepKey) -> new MockStep(stepKey, nextStepKey)
        );
        assertClusterStateOnErrorStep(clusterState, index, currentStep, newClusterState, now, """
            {"type":"exception","reason":"THIS IS AN EXPECTED CAUSE\"""");

        cause = new IllegalArgumentException("non elasticsearch-exception");
        newClusterState = IndexLifecycleTransition.moveClusterStateToErrorStep(
            index,
            clusterState,
            cause,
            () -> now,
            (idxMeta, stepKey) -> new MockStep(stepKey, nextStepKey)
        );
        assertClusterStateOnErrorStep(clusterState, index, currentStep, newClusterState, now, """
            {"type":"illegal_argument_exception","reason":"non elasticsearch-exception\"""");
    }

    public void testAddStepInfoToClusterState() throws IOException {
        String indexName = "my_index";
        Step.StepKey currentStep = new Step.StepKey("current_phase", "current_action", "current_step");
        RandomStepInfo stepInfo = new RandomStepInfo(() -> randomAlphaOfLength(10));

        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhase(currentStep.phase());
        lifecycleState.setAction(currentStep.action());
        lifecycleState.setStep(currentStep.name());
        ClusterState clusterState = buildClusterState(indexName, Settings.builder(), lifecycleState.build(), List.of());
        Index index = clusterState.metadata().getProject().index(indexName).getIndex();
        ClusterState newClusterState = IndexLifecycleTransition.addStepInfoToClusterState(index, clusterState, stepInfo);
        assertClusterStateStepInfo(clusterState, index, currentStep, newClusterState, stepInfo);
        ClusterState runAgainClusterState = IndexLifecycleTransition.addStepInfoToClusterState(index, newClusterState, stepInfo);
        assertSame(newClusterState, runAgainClusterState);
    }

    public void testRemovePolicyForIndex() {
        String indexName = randomAlphaOfLength(10);
        String oldPolicyName = "old_policy";
        Step.StepKey currentStep = new Step.StepKey(randomAlphaOfLength(10), MockAction.NAME, randomAlphaOfLength(10));
        LifecyclePolicy oldPolicy = createPolicy(oldPolicyName, currentStep, null);
        Settings.Builder indexSettingsBuilder = Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, oldPolicyName);
        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhase(currentStep.phase());
        lifecycleState.setAction(currentStep.action());
        lifecycleState.setStep(currentStep.name());
        List<LifecyclePolicyMetadata> policyMetadatas = new ArrayList<>();
        policyMetadatas.add(new LifecyclePolicyMetadata(oldPolicy, Map.of(), randomNonNegativeLong(), randomNonNegativeLong()));
        ClusterState clusterState = buildClusterState(indexName, indexSettingsBuilder, lifecycleState.build(), policyMetadatas);
        Index index = clusterState.metadata().getProject().index(indexName).getIndex();
        Index[] indices = new Index[] { index };
        List<String> failedIndexes = new ArrayList<>();

        ClusterState newClusterState = IndexLifecycleTransition.removePolicyForIndexes(indices, clusterState, failedIndexes);

        assertTrue(failedIndexes.isEmpty());
        assertIndexNotManagedByILM(newClusterState, index);
    }

    public void testRemovePolicyForIndexNoCurrentPolicy() {
        String indexName = randomAlphaOfLength(10);
        Settings.Builder indexSettingsBuilder = Settings.builder();
        ClusterState clusterState = buildClusterState(
            indexName,
            indexSettingsBuilder,
            LifecycleExecutionState.builder().build(),
            List.of()
        );
        Index index = clusterState.metadata().getProject().index(indexName).getIndex();
        Index[] indices = new Index[] { index };
        List<String> failedIndexes = new ArrayList<>();

        ClusterState newClusterState = IndexLifecycleTransition.removePolicyForIndexes(indices, clusterState, failedIndexes);

        assertTrue(failedIndexes.isEmpty());
        assertIndexNotManagedByILM(newClusterState, index);
    }

    public void testRemovePolicyForIndexIndexDoesntExist() {
        String indexName = randomAlphaOfLength(10);
        String oldPolicyName = "old_policy";
        LifecyclePolicy oldPolicy = newTestLifecyclePolicy(oldPolicyName, Map.of());
        Step.StepKey currentStep = AbstractStepTestCase.randomStepKey();
        Settings.Builder indexSettingsBuilder = Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, oldPolicyName);
        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhase(currentStep.phase());
        lifecycleState.setAction(currentStep.action());
        lifecycleState.setStep(currentStep.name());
        List<LifecyclePolicyMetadata> policyMetadatas = new ArrayList<>();
        policyMetadatas.add(new LifecyclePolicyMetadata(oldPolicy, Map.of(), randomNonNegativeLong(), randomNonNegativeLong()));
        ClusterState clusterState = buildClusterState(indexName, indexSettingsBuilder, lifecycleState.build(), policyMetadatas);
        Index index = new Index("doesnt_exist", "im_not_here");
        Index[] indices = new Index[] { index };
        List<String> failedIndexes = new ArrayList<>();

        ClusterState newClusterState = IndexLifecycleTransition.removePolicyForIndexes(indices, clusterState, failedIndexes);

        assertEquals(1, failedIndexes.size());
        assertEquals("doesnt_exist", failedIndexes.get(0));
        assertSame(clusterState, newClusterState);
    }

    public void testRemovePolicyForIndexIndexInUnsafe() {
        String indexName = randomAlphaOfLength(10);
        String oldPolicyName = "old_policy";
        Step.StepKey currentStep = new Step.StepKey(randomAlphaOfLength(10), MockAction.NAME, randomAlphaOfLength(10));
        LifecyclePolicy oldPolicy = createPolicy(oldPolicyName, null, currentStep);
        Settings.Builder indexSettingsBuilder = Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, oldPolicyName);
        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhase(currentStep.phase());
        lifecycleState.setAction(currentStep.action());
        lifecycleState.setStep(currentStep.name());
        List<LifecyclePolicyMetadata> policyMetadatas = new ArrayList<>();
        policyMetadatas.add(new LifecyclePolicyMetadata(oldPolicy, Map.of(), randomNonNegativeLong(), randomNonNegativeLong()));
        ClusterState clusterState = buildClusterState(indexName, indexSettingsBuilder, lifecycleState.build(), policyMetadatas);
        Index index = clusterState.metadata().getProject().index(indexName).getIndex();
        Index[] indices = new Index[] { index };
        List<String> failedIndexes = new ArrayList<>();

        ClusterState newClusterState = IndexLifecycleTransition.removePolicyForIndexes(indices, clusterState, failedIndexes);

        assertTrue(failedIndexes.isEmpty());
        assertIndexNotManagedByILM(newClusterState, index);
    }

    public void testRemovePolicyWithIndexingComplete() {
        String indexName = randomAlphaOfLength(10);
        String oldPolicyName = "old_policy";
        Step.StepKey currentStep = new Step.StepKey(randomAlphaOfLength(10), MockAction.NAME, randomAlphaOfLength(10));
        LifecyclePolicy oldPolicy = createPolicy(oldPolicyName, null, currentStep);
        Settings.Builder indexSettingsBuilder = Settings.builder()
            .put(LifecycleSettings.LIFECYCLE_NAME, oldPolicyName)
            .put(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE, true);
        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhase(currentStep.phase());
        lifecycleState.setAction(currentStep.action());
        lifecycleState.setStep(currentStep.name());
        List<LifecyclePolicyMetadata> policyMetadatas = new ArrayList<>();
        policyMetadatas.add(new LifecyclePolicyMetadata(oldPolicy, Map.of(), randomNonNegativeLong(), randomNonNegativeLong()));
        ClusterState clusterState = buildClusterState(indexName, indexSettingsBuilder, lifecycleState.build(), policyMetadatas);
        Index index = clusterState.metadata().getProject().index(indexName).getIndex();
        Index[] indices = new Index[] { index };
        List<String> failedIndexes = new ArrayList<>();

        ClusterState newClusterState = IndexLifecycleTransition.removePolicyForIndexes(indices, clusterState, failedIndexes);

        assertTrue(failedIndexes.isEmpty());
        assertIndexNotManagedByILM(newClusterState, index);
    }

    public void testValidateTransitionThrowsExceptionForMissingIndexPolicy() {
        IndexMetadata indexMetadata = IndexMetadata.builder("index")
            .settings(settings(IndexVersion.current()))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();

        Step.StepKey currentStepKey = new Step.StepKey("hot", "action", "firstStep");
        Step.StepKey nextStepKey = new Step.StepKey("hot", "action", "secondStep");
        Step currentStep = new MockStep(currentStepKey, nextStepKey);
        PolicyStepsRegistry policyRegistry = createOneStepPolicyStepRegistry("policy", currentStep);

        expectThrows(
            IllegalArgumentException.class,
            () -> IndexLifecycleTransition.validateTransition(indexMetadata, currentStepKey, nextStepKey, policyRegistry)
        );
    }

    public void testValidateTransitionThrowsExceptionIfTheCurrentStepIsIncorrect() {
        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhase("hot");
        lifecycleState.setAction("action");
        lifecycleState.setStep("another_step");
        String policy = "policy";
        IndexMetadata indexMetadata = buildIndexMetadata(policy, lifecycleState);

        Step.StepKey currentStepKey = new Step.StepKey("hot", "action", "firstStep");
        Step.StepKey nextStepKey = new Step.StepKey("hot", "action", "secondStep");
        Step currentStep = new MockStep(currentStepKey, nextStepKey);
        PolicyStepsRegistry policyRegistry = createOneStepPolicyStepRegistry(policy, currentStep);

        expectThrows(
            IllegalArgumentException.class,
            () -> IndexLifecycleTransition.validateTransition(indexMetadata, currentStepKey, nextStepKey, policyRegistry)
        );
    }

    public void testValidateTransitionThrowsExceptionIfNextStepDoesNotExist() {
        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhase("hot");
        lifecycleState.setAction("action");
        lifecycleState.setStep("firstStep");
        String policy = "policy";
        IndexMetadata indexMetadata = buildIndexMetadata(policy, lifecycleState);

        Step.StepKey currentStepKey = new Step.StepKey("hot", "action", "firstStep");
        Step.StepKey nextStepKey = new Step.StepKey("hot", "action", "secondStep");
        Step currentStep = new MockStep(currentStepKey, nextStepKey);
        PolicyStepsRegistry policyRegistry = createOneStepPolicyStepRegistry(policy, currentStep);

        expectThrows(
            IllegalArgumentException.class,
            () -> IndexLifecycleTransition.validateTransition(indexMetadata, currentStepKey, nextStepKey, policyRegistry)
        );
    }

    public void testValidateValidTransition() {
        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhase("hot");
        lifecycleState.setAction("action");
        lifecycleState.setStep("firstStep");
        String policy = "policy";
        IndexMetadata indexMetadata = buildIndexMetadata(policy, lifecycleState);

        Step.StepKey currentStepKey = new Step.StepKey("hot", "action", "firstStep");
        Step.StepKey nextStepKey = new Step.StepKey("hot", "action", "secondStep");
        Step finalStep = new MockStep(nextStepKey, new Step.StepKey("hot", "action", "completed"));
        PolicyStepsRegistry policyRegistry = createOneStepPolicyStepRegistry(policy, finalStep);

        try {
            IndexLifecycleTransition.validateTransition(indexMetadata, currentStepKey, nextStepKey, policyRegistry);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            fail("validateTransition should not throw exception on valid transitions");
        }
    }

    public void testValidateTransitionToCachedStepMissingFromPolicy() {
        LifecycleExecutionState.Builder executionState = LifecycleExecutionState.builder()
            .setPhase("hot")
            .setAction("rollover")
            .setStep("check-rollover-ready")
            .setPhaseDefinition("""
                {
                  "policy" : "my-policy",
                  "phase_definition" : {
                    "min_age" : "20m",
                    "actions" : {
                      "rollover" : {
                        "max_age" : "5s"
                      },
                      "set_priority" : {
                        "priority" : 150
                      }
                    }
                  },
                  "version" : 1,
                  "modified_date_in_millis" : 1578521007076
                }""");

        IndexMetadata meta = buildIndexMetadata("my-policy", executionState);

        try (var threadPool = createThreadPool()) {
            final var client = new NoOpClient(threadPool);
            Step.StepKey currentStepKey = new Step.StepKey("hot", RolloverAction.NAME, WaitForRolloverReadyStep.NAME);
            Step.StepKey nextStepKey = new Step.StepKey("hot", RolloverAction.NAME, RolloverStep.NAME);
            Step currentStep = new WaitForRolloverReadyStep(
                currentStepKey,
                nextStepKey,
                client,
                null,
                null,
                null,
                1L,
                null,
                null,
                null,
                null,
                null,
                null
            );
            try {
                IndexLifecycleTransition.validateTransition(
                    meta,
                    currentStepKey,
                    nextStepKey,
                    createOneStepPolicyStepRegistry("my-policy", currentStep)
                );
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                fail("validateTransition should not throw exception on valid transitions");
            }
        }
    }

    public void testValidateTransitionToCachedStepWhenMissingPhaseFromPolicy() {
        // we'll test the case when the warm phase was deleted and the next step is the phase complete one

        LifecycleExecutionState.Builder executionState = LifecycleExecutionState.builder()
            .setPhase("warm")
            .setAction("migrate")
            .setStep("check-migration")
            .setPhaseDefinition("""
                {
                  "policy" : "my-policy",
                  "phase_definition" : {
                    "min_age" : "20m",
                    "actions" : {
                      "set_priority" : {
                        "priority" : 150
                      }
                    }
                  },
                  "version" : 1,
                  "modified_date_in_millis" : 1578521007076
                }""");

        IndexMetadata meta = buildIndexMetadata("my-policy", executionState);

        try (var threadPool = createThreadPool()) {
            final var client = new NoOpClient(threadPool);
            Step.StepKey currentStepKey = new Step.StepKey("warm", MigrateAction.NAME, DataTierMigrationRoutedStep.NAME);
            Step.StepKey nextStepKey = new Step.StepKey("warm", PhaseCompleteStep.NAME, PhaseCompleteStep.NAME);

            Step.StepKey waitForRolloverStepKey = new Step.StepKey("hot", RolloverAction.NAME, WaitForRolloverReadyStep.NAME);
            Step.StepKey rolloverStepKey = new Step.StepKey("hot", RolloverAction.NAME, RolloverStep.NAME);
            Step waitForRolloverReadyStep = new WaitForRolloverReadyStep(
                waitForRolloverStepKey,
                rolloverStepKey,
                client,
                null,
                null,
                null,
                1L,
                null,
                null,
                null,
                null,
                null,
                null
            );

            try {
                IndexLifecycleTransition.validateTransition(
                    meta,
                    currentStepKey,
                    nextStepKey,
                    createOneStepPolicyStepRegistry("my-policy", waitForRolloverReadyStep)
                );
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                fail("validateTransition should not throw exception on valid transitions");
            }
        }
    }

    public void testValidateTransitionToInjectedMissingStep() {
        // we'll test the case when the warm phase was deleted and the next step is an injected one

        LifecycleExecutionState.Builder executionState = LifecycleExecutionState.builder()
            .setPhase("warm")
            .setAction("migrate")
            .setStep("migrate")
            .setPhaseDefinition("""
                {
                  "policy" : "my-policy",
                  "phase_definition" : {
                    "min_age" : "20m",
                    "actions" : {
                      "set_priority" : {
                        "priority" : 150
                      }
                    }
                  },
                  "version" : 1,
                  "modified_date_in_millis" : 1578521007076
                }""");

        IndexMetadata meta = buildIndexMetadata("my-policy", executionState);

        try (var threadPool = createThreadPool()) {
            final var client = new NoOpClient(threadPool);
            Step.StepKey currentStepKey = new Step.StepKey("warm", MigrateAction.NAME, MigrateAction.NAME);
            Step.StepKey nextStepKey = new Step.StepKey("warm", MigrateAction.NAME, DataTierMigrationRoutedStep.NAME);

            Step.StepKey waitForRolloverStepKey = new Step.StepKey("hot", RolloverAction.NAME, WaitForRolloverReadyStep.NAME);
            Step.StepKey rolloverStepKey = new Step.StepKey("hot", RolloverAction.NAME, RolloverStep.NAME);
            Step waitForRolloverReadyStep = new WaitForRolloverReadyStep(
                waitForRolloverStepKey,
                rolloverStepKey,
                client,
                null,
                null,
                null,
                1L,
                null,
                null,
                null,
                null,
                null,
                null
            );

            try {
                IndexLifecycleTransition.validateTransition(
                    meta,
                    currentStepKey,
                    nextStepKey,
                    createOneStepPolicyStepRegistry("my-policy", waitForRolloverReadyStep)
                );
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                fail("validateTransition should not throw exception on valid transitions");
            }
        }
    }

    public void testMoveClusterStateToFailedStep() {
        String indexName = "my_index";
        String policyName = "my_policy";
        long now = randomNonNegativeLong();
        Step.StepKey failedStepKey = new Step.StepKey("current_phase", MockAction.NAME, "current_step");
        Step.StepKey errorStepKey = new Step.StepKey(failedStepKey.phase(), failedStepKey.action(), ErrorStep.NAME);
        Step step = new MockStep(failedStepKey, null);
        LifecyclePolicy policy = createPolicy(policyName, failedStepKey, null);
        LifecyclePolicyMetadata policyMetadata = new LifecyclePolicyMetadata(
            policy,
            Map.of(),
            randomNonNegativeLong(),
            randomNonNegativeLong()
        );

        PolicyStepsRegistry policyRegistry = createOneStepPolicyStepRegistry(policyName, step);
        Settings.Builder indexSettingsBuilder = Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, policyName);
        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhase(errorStepKey.phase());
        lifecycleState.setPhaseTime(now);
        lifecycleState.setAction(errorStepKey.action());
        lifecycleState.setActionTime(now);
        lifecycleState.setStep(errorStepKey.name());
        lifecycleState.setStepTime(now);
        lifecycleState.setFailedStep(failedStepKey.name());
        ClusterState clusterState = buildClusterState(indexName, indexSettingsBuilder, lifecycleState.build(), List.of(policyMetadata));
        Index index = clusterState.metadata().getProject().index(indexName).getIndex();
        ClusterState nextClusterState = IndexLifecycleTransition.moveClusterStateToPreviouslyFailedStep(
            clusterState,
            indexName,
            () -> now,
            policyRegistry,
            false
        );
        IndexLifecycleRunnerTests.assertClusterStateOnNextStep(clusterState, index, errorStepKey, failedStepKey, nextClusterState, now);
        LifecycleExecutionState executionState = nextClusterState.metadata().getProject().index(indexName).getLifecycleExecutionState();
        assertThat("manual move to failed step should not count as a retry", executionState.failedStepRetryCount(), is(nullValue()));
    }

    public void testMoveClusterStateToFailedStepWithUnknownStep() {
        String indexName = "my_index";
        String policyName = "my_policy";
        long now = randomNonNegativeLong();
        Step.StepKey failedStepKey = new Step.StepKey("current_phase", MockAction.NAME, "current_step");
        Step.StepKey errorStepKey = new Step.StepKey(failedStepKey.phase(), failedStepKey.action(), ErrorStep.NAME);

        Step.StepKey registeredStepKey = new Step.StepKey(randomFrom(failedStepKey.phase(), "other"), MockAction.NAME, "different_step");
        Step step = new MockStep(registeredStepKey, null);
        LifecyclePolicy policy = createPolicy(policyName, failedStepKey, null);
        LifecyclePolicyMetadata policyMetadata = new LifecyclePolicyMetadata(
            policy,
            Map.of(),
            randomNonNegativeLong(),
            randomNonNegativeLong()
        );

        PolicyStepsRegistry policyRegistry = createOneStepPolicyStepRegistry(policyName, step);
        Settings.Builder indexSettingsBuilder = Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, policyName);
        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhase(errorStepKey.phase());
        lifecycleState.setPhaseTime(now);
        lifecycleState.setAction(errorStepKey.action());
        lifecycleState.setActionTime(now);
        lifecycleState.setStep(errorStepKey.name());
        lifecycleState.setStepTime(now);
        lifecycleState.setFailedStep(failedStepKey.name());
        ClusterState clusterState = buildClusterState(indexName, indexSettingsBuilder, lifecycleState.build(), List.of(policyMetadata));
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> IndexLifecycleTransition.moveClusterStateToPreviouslyFailedStep(clusterState, indexName, () -> now, policyRegistry, false)
        );
        assertThat(
            exception.getMessage(),
            equalTo("step [" + failedStepKey + "] for index [my_index] with policy [my_policy] does not exist")
        );
    }

    public void testMoveClusterStateToFailedStepIndexNotFound() {
        String existingIndexName = "my_index";
        String invalidIndexName = "does_not_exist";
        ClusterState clusterState = buildClusterState(
            existingIndexName,
            Settings.builder(),
            LifecycleExecutionState.builder().build(),
            List.of()
        );
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> IndexLifecycleTransition.moveClusterStateToPreviouslyFailedStep(clusterState, invalidIndexName, () -> 0L, null, false)
        );
        assertThat(exception.getMessage(), equalTo("index [" + invalidIndexName + "] does not exist"));
    }

    public void testMoveClusterStateToFailedStepInvalidPolicySetting() {
        String indexName = "my_index";
        String policyName = "my_policy";
        long now = randomNonNegativeLong();
        Step.StepKey failedStepKey = new Step.StepKey("current_phase", "current_action", "current_step");
        Step.StepKey errorStepKey = new Step.StepKey(failedStepKey.phase(), failedStepKey.action(), ErrorStep.NAME);
        Step step = new MockStep(failedStepKey, null);
        PolicyStepsRegistry policyRegistry = createOneStepPolicyStepRegistry(policyName, step);
        Settings.Builder indexSettingsBuilder = Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, (String) null);
        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhase(errorStepKey.phase());
        lifecycleState.setAction(errorStepKey.action());
        lifecycleState.setStep(errorStepKey.name());
        lifecycleState.setFailedStep(failedStepKey.name());
        ClusterState clusterState = buildClusterState(indexName, indexSettingsBuilder, lifecycleState.build(), List.of());
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> IndexLifecycleTransition.moveClusterStateToPreviouslyFailedStep(clusterState, indexName, () -> now, policyRegistry, false)
        );
        assertThat(exception.getMessage(), equalTo("index [" + indexName + "] is not associated with an Index Lifecycle Policy"));
    }

    public void testMoveClusterStateToFailedNotOnError() {
        String indexName = "my_index";
        String policyName = "my_policy";
        long now = randomNonNegativeLong();
        Step.StepKey failedStepKey = new Step.StepKey("current_phase", "current_action", "current_step");
        Step step = new MockStep(failedStepKey, null);
        PolicyStepsRegistry policyRegistry = createOneStepPolicyStepRegistry(policyName, step);
        Settings.Builder indexSettingsBuilder = Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, (String) null);
        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhase(failedStepKey.phase());
        lifecycleState.setAction(failedStepKey.action());
        lifecycleState.setStep(failedStepKey.name());
        ClusterState clusterState = buildClusterState(indexName, indexSettingsBuilder, lifecycleState.build(), List.of());
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> IndexLifecycleTransition.moveClusterStateToPreviouslyFailedStep(clusterState, indexName, () -> now, policyRegistry, false)
        );
        assertThat(
            exception.getMessage(),
            equalTo(
                "cannot retry an action for an index [" + indexName + "] that has not encountered an error when running a Lifecycle Policy"
            )
        );
    }

    public void testMoveClusterStateToPreviouslyFailedStepAsAutomaticRetryAndSetsPreviousStepInfo() {
        String indexName = "my_index";
        String policyName = "my_policy";
        long now = randomNonNegativeLong();
        Step.StepKey failedStepKey = new Step.StepKey("current_phase", MockAction.NAME, "current_step");
        Step.StepKey errorStepKey = new Step.StepKey(failedStepKey.phase(), failedStepKey.action(), ErrorStep.NAME);
        Step retryableStep = new IndexLifecycleRunnerTests.RetryableMockStep(failedStepKey, null);
        LifecyclePolicy policy = createPolicy(policyName, failedStepKey, null);
        LifecyclePolicyMetadata policyMetadata = new LifecyclePolicyMetadata(
            policy,
            Map.of(),
            randomNonNegativeLong(),
            randomNonNegativeLong()
        );

        PolicyStepsRegistry policyRegistry = createOneStepPolicyStepRegistry(policyName, retryableStep);
        Settings.Builder indexSettingsBuilder = Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, policyName);
        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhase(errorStepKey.phase());
        lifecycleState.setPhaseTime(now);
        lifecycleState.setAction(errorStepKey.action());
        lifecycleState.setActionTime(now);
        lifecycleState.setStep(errorStepKey.name());
        lifecycleState.setStepTime(now);
        lifecycleState.setFailedStep(failedStepKey.name());
        String initialStepInfo = randomAlphaOfLengthBetween(10, 50);
        lifecycleState.setStepInfo(initialStepInfo);
        ClusterState clusterState = buildClusterState(indexName, indexSettingsBuilder, lifecycleState.build(), List.of(policyMetadata));
        Index index = clusterState.metadata().getProject().index(indexName).getIndex();
        ClusterState nextClusterState = IndexLifecycleTransition.moveClusterStateToPreviouslyFailedStep(
            clusterState,
            indexName,
            () -> now,
            policyRegistry,
            true
        );
        IndexLifecycleRunnerTests.assertClusterStateOnNextStep(clusterState, index, errorStepKey, failedStepKey, nextClusterState, now);
        LifecycleExecutionState executionState = nextClusterState.metadata().getProject().index(indexName).getLifecycleExecutionState();
        assertThat(executionState.failedStepRetryCount(), is(1));
        assertThat(executionState.previousStepInfo(), is(initialStepInfo));
    }

    public void testMoveToFailedStepDoesntRefreshCachedPhaseWhenUnsafe() {
        String initialPhaseDefinition = """
            {
              "policy" : "my-policy",
              "phase_definition" : {
                "min_age" : "20m",
                "actions" : {
                  "rollover" : {
                    "max_age" : "5s"
                  },
                  "set_priority" : {
                    "priority" : 150
                  }
                }
              },
              "version" : 1,
              "modified_date_in_millis" : 1578521007076
            }""";
        String failedStep = "check-rollover-ready";
        LifecycleExecutionState.Builder currentExecutionState = LifecycleExecutionState.builder()
            .setPhase("hot")
            .setAction("rollover")
            .setStep(ErrorStep.NAME)
            .setFailedStep(failedStep)
            // the phase definition contains the rollover action, but the actual policy does not contain rollover anymore
            .setPhaseDefinition(initialPhaseDefinition);

        IndexMetadata meta = buildIndexMetadata("my-policy", currentExecutionState);
        String indexName = meta.getIndex().getName();

        Map<String, LifecycleAction> actions = new HashMap<>();
        actions.put("set_priority", new SetPriorityAction(100));
        Phase hotPhase = new Phase("hot", TimeValue.ZERO, actions);
        Map<String, Phase> phases = Map.of("hot", hotPhase);
        LifecyclePolicy currentPolicy = new LifecyclePolicy("my-policy", phases);

        List<LifecyclePolicyMetadata> policyMetadatas = new ArrayList<>();
        policyMetadatas.add(new LifecyclePolicyMetadata(currentPolicy, Map.of(), randomNonNegativeLong(), randomNonNegativeLong()));

        Step.StepKey errorStepKey = new Step.StepKey("hot", RolloverAction.NAME, ErrorStep.NAME);
        PolicyStepsRegistry stepsRegistry = createOneStepPolicyStepRegistry("my-policy", new ErrorStep(errorStepKey));

        ClusterState clusterState = buildClusterState(
            indexName,
            Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, "my-policy"),
            currentExecutionState.build(),
            policyMetadatas
        );
        ClusterState newState = IndexLifecycleTransition.moveClusterStateToPreviouslyFailedStep(
            clusterState,
            indexName,
            ESTestCase::randomNonNegativeLong,
            stepsRegistry,
            false
        );

        LifecycleExecutionState nextLifecycleExecutionState = newState.metadata()
            .getProject()
            .index(indexName)
            .getLifecycleExecutionState();
        assertThat(
            "we musn't refresh the cache definition if the failed step is not part of the real policy anymore",
            nextLifecycleExecutionState.phaseDefinition(),
            is(initialPhaseDefinition)
        );
        assertThat(nextLifecycleExecutionState.step(), is(failedStep));
    }

    public void testRefreshPhaseJson() throws IOException {
        LifecycleExecutionState.Builder exState = LifecycleExecutionState.builder()
            .setPhase("hot")
            .setAction("rollover")
            .setStep("check-rollover-ready")
            .setPhaseDefinition("""
                {
                  "policy" : "my-policy",
                  "phase_definition" : {
                    "min_age" : "20m",
                    "actions" : {
                      "rollover" : {
                        "max_age" : "5s"
                      },
                      "set_priority" : {
                        "priority" : 150
                      }
                    }
                  },
                  "version" : 1,
                  "modified_date_in_millis" : 1578521007076
                }""");

        IndexMetadata meta = buildIndexMetadata("my-policy", exState);
        String index = meta.getIndex().getName();

        Map<String, LifecycleAction> actions = new HashMap<>();
        actions.put("rollover", new RolloverAction(null, null, null, 1L, null, null, null, null, null, null));
        actions.put("set_priority", new SetPriorityAction(100));
        Phase hotPhase = new Phase("hot", TimeValue.ZERO, actions);
        Map<String, Phase> phases = Map.of("hot", hotPhase);
        LifecyclePolicy newPolicy = new LifecyclePolicy("my-policy", phases);
        LifecyclePolicyMetadata policyMetadata = new LifecyclePolicyMetadata(newPolicy, Map.of(), 2L, 2L);

        ClusterState existingState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(Metadata.builder(Metadata.EMPTY_METADATA).put(meta, false).build())
            .build();

        ClusterState changedState = refreshPhaseDefinition(existingState, index, policyMetadata);

        IndexMetadata newIdxMeta = changedState.metadata().getProject().index(index);
        LifecycleExecutionState afterExState = newIdxMeta.getLifecycleExecutionState();
        Map<String, String> beforeState = new HashMap<>(exState.build().asMap());
        beforeState.remove("phase_definition");
        Map<String, String> afterState = new HashMap<>(afterExState.asMap());
        afterState.remove("phase_definition");
        // Check that no other execution state changes have been made
        assertThat(beforeState, equalTo(afterState));

        // Check that the phase definition has been refreshed
        assertThat(afterExState.phaseDefinition(), equalTo(XContentHelper.stripWhitespace("""
            {
              "policy": "my-policy",
              "phase_definition": {
                "min_age": "0ms",
                "actions": {
                  "rollover": {
                    "max_docs": 1
                  },
                  "set_priority": {
                    "priority": 100
                  }
                }
              },
              "version": 2,
              "modified_date_in_millis": 2
            }""")));
    }

    public void testEligibleForRefresh() {
        IndexMetadata meta = IndexMetadata.builder("index")
            .settings(
                indexSettings(IndexVersion.current(), randomIntBetween(1, 10), randomIntBetween(0, 5)).put(
                    IndexMetadata.SETTING_INDEX_UUID,
                    randomAlphaOfLength(5)
                )
            )
            .build();
        assertFalse(eligibleToCheckForRefresh(meta));

        LifecycleExecutionState state = LifecycleExecutionState.builder().build();
        meta = IndexMetadata.builder("index")
            .settings(
                indexSettings(IndexVersion.current(), randomIntBetween(1, 10), randomIntBetween(0, 5)).put(
                    IndexMetadata.SETTING_INDEX_UUID,
                    randomAlphaOfLength(5)
                )
            )
            .putCustom(ILM_CUSTOM_METADATA_KEY, state.asMap())
            .build();
        assertFalse(eligibleToCheckForRefresh(meta));

        state = LifecycleExecutionState.builder().setPhase("phase").setAction("action").setStep("step").build();
        meta = IndexMetadata.builder("index")
            .settings(
                indexSettings(IndexVersion.current(), randomIntBetween(1, 10), randomIntBetween(0, 5)).put(
                    IndexMetadata.SETTING_INDEX_UUID,
                    randomAlphaOfLength(5)
                )
            )
            .putCustom(ILM_CUSTOM_METADATA_KEY, state.asMap())
            .build();
        assertFalse(eligibleToCheckForRefresh(meta));

        state = LifecycleExecutionState.builder().setPhaseDefinition("{}").build();
        meta = IndexMetadata.builder("index")
            .settings(
                indexSettings(IndexVersion.current(), randomIntBetween(1, 10), randomIntBetween(0, 5)).put(
                    IndexMetadata.SETTING_INDEX_UUID,
                    randomAlphaOfLength(5)
                )
            )
            .putCustom(ILM_CUSTOM_METADATA_KEY, state.asMap())
            .build();
        assertFalse(eligibleToCheckForRefresh(meta));

        state = LifecycleExecutionState.builder()
            .setPhase("phase")
            .setAction("action")
            .setStep(ErrorStep.NAME)
            .setPhaseDefinition("{}")
            .build();
        meta = IndexMetadata.builder("index")
            .settings(
                indexSettings(IndexVersion.current(), randomIntBetween(1, 10), randomIntBetween(0, 5)).put(
                    IndexMetadata.SETTING_INDEX_UUID,
                    randomAlphaOfLength(5)
                )
            )
            .putCustom(ILM_CUSTOM_METADATA_KEY, state.asMap())
            .build();
        assertFalse(eligibleToCheckForRefresh(meta));

        state = LifecycleExecutionState.builder().setPhase("phase").setAction("action").setStep("step").setPhaseDefinition("{}").build();
        meta = IndexMetadata.builder("index")
            .settings(
                indexSettings(IndexVersion.current(), randomIntBetween(1, 10), randomIntBetween(0, 5)).put(
                    IndexMetadata.SETTING_INDEX_UUID,
                    randomAlphaOfLength(5)
                )
            )
            .putCustom(ILM_CUSTOM_METADATA_KEY, state.asMap())
            .build();
        assertTrue(eligibleToCheckForRefresh(meta));
    }

    public void testMoveStateToNextActionAndUpdateCachedPhase() {
        LifecycleExecutionState.Builder currentExecutionState = LifecycleExecutionState.builder()
            .setPhase("hot")
            .setAction("rollover")
            .setStep("check-rollover-ready")
            .setPhaseDefinition("""
                {
                  "policy" : "my-policy",
                  "phase_definition" : {
                    "min_age" : "20m",
                    "actions" : {
                      "rollover" : {
                        "max_age" : "5s"
                      },
                      "set_priority" : {
                        "priority" : 150
                      }
                    }
                  },
                  "version" : 1,
                  "modified_date_in_millis" : 1578521007076
                }""");

        IndexMetadata meta = buildIndexMetadata("my-policy", currentExecutionState);

        Map<String, LifecycleAction> actions = new HashMap<>();
        actions.put("rollover", new RolloverAction(null, null, null, 1L, null, null, null, null, null, null));
        actions.put("set_priority", new SetPriorityAction(100));
        Phase hotPhase = new Phase("hot", TimeValue.ZERO, actions);
        Map<String, Phase> phases = Map.of("hot", hotPhase);
        LifecyclePolicy currentPolicy = new LifecyclePolicy("my-policy", phases);

        {
            // test index is in step for action that was removed in the updated policy
            // the expected new state is that the index was moved into the next *action* and its cached phase definition updated to reflect
            // the updated policy
            Map<String, LifecycleAction> actionsWithoutRollover = new HashMap<>();
            actionsWithoutRollover.put("set_priority", new SetPriorityAction(100));
            Phase hotPhaseNoRollover = new Phase("hot", TimeValue.ZERO, actionsWithoutRollover);
            Map<String, Phase> phasesNoRollover = Map.of("hot", hotPhaseNoRollover);
            LifecyclePolicyMetadata updatedPolicyMetadata = new LifecyclePolicyMetadata(
                new LifecyclePolicy("my-policy", phasesNoRollover),
                Map.of(),
                2L,
                2L
            );

            try (var threadPool = createThreadPool()) {
                final var client = new NoOpClient(threadPool);
                LifecycleExecutionState newState = moveStateToNextActionAndUpdateCachedPhase(
                    meta,
                    meta.getLifecycleExecutionState(),
                    System::currentTimeMillis,
                    currentPolicy,
                    updatedPolicyMetadata,
                    client,
                    null
                );

                Step.StepKey hotPhaseCompleteStepKey = PhaseCompleteStep.finalStep("hot").getKey();
                assertThat(newState.action(), is(hotPhaseCompleteStepKey.action()));
                assertThat(newState.step(), is(hotPhaseCompleteStepKey.name()));
                assertThat(
                    "the cached phase should not contain rollover anymore",
                    newState.phaseDefinition(),
                    not(containsString(RolloverAction.NAME))
                );
            }
        }

        {
            // test that the index is in an action that still exists in the update policy
            // the expected new state is that the index is moved into the next action (could be the complete one) and the cached phase
            // definition is updated
            Map<String, LifecycleAction> actionsWitoutSetPriority = new HashMap<>();
            actionsWitoutSetPriority.put("rollover", new RolloverAction(null, null, null, 1L, null, null, null, null, null, null));
            Phase hotPhaseNoSetPriority = new Phase("hot", TimeValue.ZERO, actionsWitoutSetPriority);
            Map<String, Phase> phasesWithoutSetPriority = Map.of("hot", hotPhaseNoSetPriority);
            LifecyclePolicyMetadata updatedPolicyMetadata = new LifecyclePolicyMetadata(
                new LifecyclePolicy("my-policy", phasesWithoutSetPriority),
                Map.of(),
                2L,
                2L
            );

            try (var threadPool = createThreadPool()) {
                final var client = new NoOpClient(threadPool);
                LifecycleExecutionState newState = moveStateToNextActionAndUpdateCachedPhase(
                    meta,
                    meta.getLifecycleExecutionState(),
                    System::currentTimeMillis,
                    currentPolicy,
                    updatedPolicyMetadata,
                    client,
                    null
                );

                Step.StepKey hotPhaseCompleteStepKey = PhaseCompleteStep.finalStep("hot").getKey();
                // the state was still moved into the next action, even if the updated policy still contained the action the index was
                // currently executing
                assertThat(newState.action(), is(hotPhaseCompleteStepKey.action()));
                assertThat(newState.step(), is(hotPhaseCompleteStepKey.name()));
                assertThat(newState.phaseDefinition(), containsString(RolloverAction.NAME));
                assertThat(
                    "the cached phase should not contain set_priority anymore",
                    newState.phaseDefinition(),
                    not(containsString(SetPriorityAction.NAME))
                );
            }
        }
    }

    private static LifecyclePolicy createPolicy(String policyName, Step.StepKey safeStep, Step.StepKey unsafeStep) {
        Map<String, Phase> phases = new HashMap<>();
        if (safeStep != null) {
            assert MockAction.NAME.equals(safeStep.action()) : "The safe action needs to be MockAction.NAME";
            assert unsafeStep == null || safeStep.phase().equals(unsafeStep.phase()) == false
                : "safe and unsafe actions must be in different phases";
            Map<String, LifecycleAction> actions = new HashMap<>();
            List<Step> steps = List.of(new MockStep(safeStep, null));
            MockAction safeAction = new MockAction(steps, true);
            actions.put(safeAction.getWriteableName(), safeAction);
            Phase phase = new Phase(safeStep.phase(), TimeValue.timeValueMillis(0), actions);
            phases.put(phase.getName(), phase);
        }
        if (unsafeStep != null) {
            assert MockAction.NAME.equals(unsafeStep.action()) : "The unsafe action needs to be MockAction.NAME";
            Map<String, LifecycleAction> actions = new HashMap<>();
            List<Step> steps = List.of(new MockStep(unsafeStep, null));
            MockAction unsafeAction = new MockAction(steps, false);
            actions.put(unsafeAction.getWriteableName(), unsafeAction);
            Phase phase = new Phase(unsafeStep.phase(), TimeValue.timeValueMillis(0), actions);
            phases.put(phase.getName(), phase);
        }
        return newTestLifecyclePolicy(policyName, phases);
    }

    private ClusterState buildClusterState(
        String indexName,
        Settings.Builder indexSettingsBuilder,
        LifecycleExecutionState lifecycleState,
        List<LifecyclePolicyMetadata> lifecyclePolicyMetadatas
    ) {
        return ClusterState.builder(ClusterName.DEFAULT)
            .putProjectMetadata(buildProject(indexName, indexSettingsBuilder, lifecycleState, lifecyclePolicyMetadatas))
            .build();
    }

    private ProjectMetadata buildProject(
        String indexName,
        Settings.Builder indexSettingsBuilder,
        LifecycleExecutionState lifecycleState,
        List<LifecyclePolicyMetadata> lifecyclePolicyMetadatas
    ) {
        Settings indexSettings = indexSettingsBuilder.put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .build();
        IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
            .settings(indexSettings)
            .putCustom(ILM_CUSTOM_METADATA_KEY, lifecycleState.asMap())
            .build();

        Map<String, LifecyclePolicyMetadata> lifecyclePolicyMetadatasMap = lifecyclePolicyMetadatas.stream()
            .collect(Collectors.toMap(LifecyclePolicyMetadata::getName, Function.identity()));
        IndexLifecycleMetadata indexLifecycleMetadata = new IndexLifecycleMetadata(lifecyclePolicyMetadatasMap, OperationMode.RUNNING);

        @FixForMultiProject // Use non-default ID when the remainder of IndexLifecycleTransition is project-aware.
        final var projectId = ProjectId.DEFAULT;
        return ProjectMetadata.builder(projectId)
            .put(indexMetadata, true)
            .putCustom(IndexLifecycleMetadata.TYPE, indexLifecycleMetadata)
            .build();
    }

    public static void assertIndexNotManagedByILM(ClusterState clusterState, Index index) {
        Metadata metadata = clusterState.metadata();
        assertNotNull(metadata);

        IndexMetadata indexMetadata = metadata.getProject().getIndexSafe(index);
        assertNotNull(indexMetadata);

        assertNull(indexMetadata.getLifecyclePolicyName());

        Settings indexSettings = indexMetadata.getSettings();
        assertNotNull(indexSettings);
        assertFalse(LifecycleSettings.LIFECYCLE_NAME_SETTING.exists(indexSettings));
        assertFalse(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS_SETTING.exists(indexSettings));
        assertFalse(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE_SETTING.exists(indexSettings));
    }

    public static void assertProjectOnNextStep(
        ProjectMetadata oldProject,
        Index index,
        Step.StepKey currentStep,
        Step.StepKey nextStep,
        ProjectMetadata newProject,
        long now
    ) {
        assertNotSame(oldProject, newProject);
        IndexMetadata newIndexMetadata = newProject.getIndexSafe(index);
        assertNotSame(oldProject.index(index), newIndexMetadata);
        LifecycleExecutionState newLifecycleState = newProject.index(index).getLifecycleExecutionState();
        LifecycleExecutionState oldLifecycleState = oldProject.index(index).getLifecycleExecutionState();
        assertNotSame(oldLifecycleState, newLifecycleState);
        assertEquals(nextStep.phase(), newLifecycleState.phase());
        assertEquals(nextStep.action(), newLifecycleState.action());
        assertEquals(nextStep.name(), newLifecycleState.step());
        if (currentStep.phase().equals(nextStep.phase())) {
            assertEquals(
                "expected phase times to be the same but they were different",
                oldLifecycleState.phaseTime(),
                newLifecycleState.phaseTime()
            );
        } else {
            assertEquals(now, newLifecycleState.phaseTime().longValue());
        }
        if (currentStep.action().equals(nextStep.action())) {
            assertEquals(
                "expected action times to be the same but they were different",
                oldLifecycleState.actionTime(),
                newLifecycleState.actionTime()
            );
        } else {
            assertEquals(now, newLifecycleState.actionTime().longValue());
        }
        assertEquals(now, newLifecycleState.stepTime().longValue());
        assertEquals(null, newLifecycleState.failedStep());
        assertEquals(null, newLifecycleState.stepInfo());
    }

    private IndexMetadata buildIndexMetadata(String policy, LifecycleExecutionState.Builder lifecycleState) {
        return IndexMetadata.builder("index")
            .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_NAME, policy))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .putCustom(ILM_CUSTOM_METADATA_KEY, lifecycleState.build().asMap())
            .build();
    }

    private void assertClusterStateOnErrorStep(
        ClusterState oldClusterState,
        Index index,
        Step.StepKey currentStep,
        ClusterState newClusterState,
        long now,
        String expectedCauseValue
    ) {
        assertNotSame(oldClusterState, newClusterState);
        Metadata newMetadata = newClusterState.metadata();
        assertNotSame(oldClusterState.metadata(), newMetadata);
        IndexMetadata newIndexMetadata = newMetadata.getProject().getIndexSafe(index);
        assertNotSame(oldClusterState.metadata().getProject().index(index), newIndexMetadata);
        LifecycleExecutionState newLifecycleState = newClusterState.metadata().getProject().index(index).getLifecycleExecutionState();
        LifecycleExecutionState oldLifecycleState = oldClusterState.metadata().getProject().index(index).getLifecycleExecutionState();
        assertNotSame(oldLifecycleState, newLifecycleState);
        assertEquals(currentStep.phase(), newLifecycleState.phase());
        assertEquals(currentStep.action(), newLifecycleState.action());
        assertEquals(ErrorStep.NAME, newLifecycleState.step());
        assertEquals(currentStep.name(), newLifecycleState.failedStep());
        assertThat(newLifecycleState.stepInfo(), containsString(expectedCauseValue));
        assertEquals(oldLifecycleState.phaseTime(), newLifecycleState.phaseTime());
        assertEquals(oldLifecycleState.actionTime(), newLifecycleState.actionTime());
        assertEquals(now, newLifecycleState.stepTime().longValue());
    }

    private void assertClusterStateStepInfo(
        ClusterState oldClusterState,
        Index index,
        Step.StepKey currentStep,
        ClusterState newClusterState,
        ToXContentObject stepInfo
    ) throws IOException {
        XContentBuilder stepInfoXContentBuilder = JsonXContent.contentBuilder();
        stepInfo.toXContent(stepInfoXContentBuilder, ToXContent.EMPTY_PARAMS);
        String expectedstepInfoValue = BytesReference.bytes(stepInfoXContentBuilder).utf8ToString();
        assertNotSame(oldClusterState, newClusterState);
        Metadata newMetadata = newClusterState.metadata();
        assertNotSame(oldClusterState.metadata(), newMetadata);
        IndexMetadata newIndexMetadata = newMetadata.getProject().getIndexSafe(index);
        assertNotSame(oldClusterState.metadata().getProject().index(index), newIndexMetadata);
        LifecycleExecutionState newLifecycleState = newClusterState.metadata().getProject().index(index).getLifecycleExecutionState();
        LifecycleExecutionState oldLifecycleState = oldClusterState.metadata().getProject().index(index).getLifecycleExecutionState();
        assertNotSame(oldLifecycleState, newLifecycleState);
        assertEquals(currentStep.phase(), newLifecycleState.phase());
        assertEquals(currentStep.action(), newLifecycleState.action());
        assertEquals(currentStep.name(), newLifecycleState.step());
        assertEquals(expectedstepInfoValue, newLifecycleState.stepInfo());
        assertEquals(oldLifecycleState.phaseTime(), newLifecycleState.phaseTime());
        assertEquals(oldLifecycleState.actionTime(), newLifecycleState.actionTime());
        assertEquals(oldLifecycleState.stepTime(), newLifecycleState.stepTime());
    }
}
