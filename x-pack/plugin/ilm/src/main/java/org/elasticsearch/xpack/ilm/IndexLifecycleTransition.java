/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.Index;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xpack.core.ilm.ErrorStep;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.InitializePolicyContextStep;
import org.elasticsearch.xpack.core.ilm.InitializePolicyException;
import org.elasticsearch.xpack.core.ilm.LifecycleExecutionStateUtils;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.PhaseCompleteStep;
import org.elasticsearch.xpack.core.ilm.PhaseExecutionInfo;
import org.elasticsearch.xpack.core.ilm.RolloverAction;
import org.elasticsearch.xpack.core.ilm.Step;
import org.elasticsearch.xpack.core.ilm.TerminalPolicyStep;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.LongSupplier;

import static org.elasticsearch.cluster.metadata.LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY;
import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;

/**
 * The {@link IndexLifecycleTransition} class handles cluster state transitions
 * related to ILM operations. These operations are all at the index level
 * (inside of {@link IndexMetadata}) for the index in question.
 *
 * Each method is static and only changes a given state, no actions are
 * performed by methods in this class.
 */
public final class IndexLifecycleTransition {
    private static final Logger logger = LogManager.getLogger(IndexLifecycleTransition.class);

    /**
     * Validates that the given transition from {@code currentStepKey} to {@code newStepKey} can be accomplished
     * @throws IllegalArgumentException when the transition is not valid
     */
    public static void validateTransition(
        IndexMetadata idxMeta,
        Step.StepKey currentStepKey,
        Step.StepKey newStepKey,
        PolicyStepsRegistry stepRegistry
    ) {
        String indexName = idxMeta.getIndex().getName();
        String policyName = idxMeta.getLifecyclePolicyName();

        // policy could be updated in-between execution
        if (Strings.isNullOrEmpty(policyName)) {
            throw new IllegalArgumentException("index [" + indexName + "] is not associated with an Index Lifecycle Policy");
        }

        LifecycleExecutionState lifecycleState = idxMeta.getLifecycleExecutionState();
        Step.StepKey realKey = Step.getCurrentStepKey(lifecycleState);
        if (currentStepKey != null && currentStepKey.equals(realKey) == false) {
            throw new IllegalArgumentException(
                "index [" + indexName + "] is not on current step [" + currentStepKey + "], currently: [" + realKey + "]"
            );
        }

        final Set<Step.StepKey> cachedStepKeys = stepRegistry.parseStepKeysFromPhase(
            policyName,
            lifecycleState.phase(),
            lifecycleState.phaseDefinition()
        );
        boolean isNewStepCached = cachedStepKeys != null && cachedStepKeys.contains(newStepKey);

        // Always allow moving to the terminal step or to a step that's present in the cached phase, even if it doesn't exist in the policy
        if (isNewStepCached == false
            && (stepRegistry.stepExists(policyName, newStepKey) == false
                && newStepKey.equals(TerminalPolicyStep.KEY) == false
                && newStepKey.equals(PhaseCompleteStep.stepKey(lifecycleState.phase())) == false)) {
            throw new IllegalArgumentException(
                "step [" + newStepKey + "] for index [" + indexName + "] with policy [" + policyName + "] does not exist"
            );
        }
    }

    /**
     * This method is intended for handling moving to different steps from {@link TransportAction} executions.
     * For this reason, it is reasonable to throw {@link IllegalArgumentException} when state is not as expected.
     *
     * @param index          The index whose step is to change
     * @param state          The current {@link ClusterState}
     * @param newStepKey     The new step to move the index into
     * @param nowSupplier    The current-time supplier for updating when steps changed
     * @param stepRegistry   The steps registry to check a step-key's existence in the index's current policy
     * @param forcePhaseDefinitionRefresh Whether to force the phase JSON to be reread or not
     * @return The updated cluster state where the index moved to <code>newStepKey</code>
     */
    static ClusterState moveClusterStateToStep(
        Index index,
        ClusterState state,
        Step.StepKey newStepKey,
        LongSupplier nowSupplier,
        PolicyStepsRegistry stepRegistry,
        boolean forcePhaseDefinitionRefresh
    ) {
        IndexMetadata idxMeta = state.getMetadata().getProject().index(index);
        Step.StepKey currentStepKey = Step.getCurrentStepKey(idxMeta.getLifecycleExecutionState());
        validateTransition(idxMeta, currentStepKey, newStepKey, stepRegistry);

        String policyName = idxMeta.getLifecyclePolicyName();
        logger.info("moving index [{}] from [{}] to [{}] in policy [{}]", index.getName(), currentStepKey, newStepKey, policyName);

        IndexLifecycleMetadata ilmMeta = state.metadata().getProject().custom(IndexLifecycleMetadata.TYPE);
        LifecyclePolicyMetadata policyMetadata = ilmMeta.getPolicyMetadatas().get(idxMeta.getLifecyclePolicyName());
        LifecycleExecutionState lifecycleState = idxMeta.getLifecycleExecutionState();
        LifecycleExecutionState newLifecycleState = updateExecutionStateToStep(
            policyMetadata,
            lifecycleState,
            newStepKey,
            nowSupplier,
            forcePhaseDefinitionRefresh,
            true
        );

        return LifecycleExecutionStateUtils.newClusterStateWithLifecycleState(state, idxMeta.getIndex(), newLifecycleState);
    }

    /**
     * Moves the given index into the ERROR step. The ERROR step will have the same phase and
     * action, but use the {@link ErrorStep#NAME} as the name in the lifecycle execution state.
     */
    static ClusterState moveClusterStateToErrorStep(
        Index index,
        ClusterState clusterState,
        Exception cause,
        LongSupplier nowSupplier,
        BiFunction<IndexMetadata, Step.StepKey, Step> stepLookupFunction
    ) {
        IndexMetadata idxMeta = clusterState.getMetadata().getProject().index(index);
        IndexLifecycleMetadata ilmMeta = clusterState.metadata().getProject().custom(IndexLifecycleMetadata.TYPE);
        LifecyclePolicyMetadata policyMetadata = ilmMeta.getPolicyMetadatas().get(idxMeta.getLifecyclePolicyName());
        LifecycleExecutionState currentState = idxMeta.getLifecycleExecutionState();
        Step.StepKey currentStep;
        // if an error is encountered while initialising the policy the lifecycle execution state will not yet contain any step information
        // as we haven't yet initialised the policy, so we'll manually set the current step to be the "initialize policy" step so we can
        // record the error (and later retry the init policy step)
        if (cause instanceof InitializePolicyException) {
            currentStep = InitializePolicyContextStep.KEY;
        } else {
            currentStep = Objects.requireNonNull(
                Step.getCurrentStepKey(currentState),
                "unable to move to an error step where there is no current step, state: " + currentState
            );
        }
        LifecycleExecutionState nextStepState = updateExecutionStateToStep(
            policyMetadata,
            currentState,
            new Step.StepKey(currentStep.phase(), currentStep.action(), ErrorStep.NAME),
            nowSupplier,
            false,
            false
        );

        LifecycleExecutionState.Builder failedState = LifecycleExecutionState.builder(nextStepState);
        failedState.setFailedStep(currentStep.name());
        failedState.setStepInfo(Strings.toString(((builder, params) -> {
            ElasticsearchException.generateThrowableXContent(builder, EMPTY_PARAMS, cause);
            return builder;
        })));
        Step failedStep = stepLookupFunction.apply(idxMeta, currentStep);

        if (failedStep != null) {
            // as an initial step we'll mark the failed step as auto retryable without actually looking at the cause to determine
            // if the error is transient/recoverable from
            failedState.setIsAutoRetryableError(failedStep.isRetryable());
            // maintain the retry count of the failed step as it will be cleared after a successful execution
            failedState.setFailedStepRetryCount(currentState.failedStepRetryCount());
        } else {
            logger.warn(
                "failed step [{}] for index [{}] is not part of policy [{}] anymore, or it is invalid",
                currentStep.name(),
                index,
                policyMetadata.getName()
            );
        }

        return LifecycleExecutionStateUtils.newClusterStateWithLifecycleState(clusterState, idxMeta.getIndex(), failedState.build());
    }

    /**
     * Move the given index's execution state back to a step that had previously failed. If this is
     * an automatic retry ({@code isAutomaticRetry}), the retry count is incremented.
     */
    static ClusterState moveClusterStateToPreviouslyFailedStep(
        ClusterState currentState,
        String index,
        LongSupplier nowSupplier,
        PolicyStepsRegistry stepRegistry,
        boolean isAutomaticRetry
    ) {
        ClusterState newState;
        IndexMetadata indexMetadata = currentState.metadata().getProject().index(index);
        if (indexMetadata == null) {
            throw new IllegalArgumentException("index [" + index + "] does not exist");
        }
        LifecycleExecutionState lifecycleState = indexMetadata.getLifecycleExecutionState();
        Step.StepKey currentStepKey = Step.getCurrentStepKey(lifecycleState);
        String failedStep = lifecycleState.failedStep();
        if (currentStepKey != null && ErrorStep.NAME.equals(currentStepKey.name()) && Strings.isNullOrEmpty(failedStep) == false) {
            Step.StepKey nextStepKey = new Step.StepKey(currentStepKey.phase(), currentStepKey.action(), failedStep);
            validateTransition(indexMetadata, currentStepKey, nextStepKey, stepRegistry);
            IndexLifecycleMetadata ilmMeta = currentState.metadata().getProject().custom(IndexLifecycleMetadata.TYPE);

            LifecyclePolicyMetadata policyMetadata = ilmMeta.getPolicyMetadatas().get(indexMetadata.getLifecyclePolicyName());

            Map<String, Phase> policyPhases = policyMetadata.getPolicy().getPhases();

            // we only refresh the cached phase if the failed step's action is still present in the underlying policy
            // as otherwise ILM would block due to not recognizing the next step as part of the policy.
            // if the policy was updated to not contain the action or even phase, we honour the cached phase as it is and do not refresh it
            boolean forcePhaseDefinitionRefresh = policyPhases.get(nextStepKey.phase()) != null
                && policyPhases.get(nextStepKey.phase()).getActions().get(nextStepKey.action()) != null;

            final LifecycleExecutionState nextStepState = IndexLifecycleTransition.updateExecutionStateToStep(
                policyMetadata,
                lifecycleState,
                nextStepKey,
                nowSupplier,
                forcePhaseDefinitionRefresh,
                false
            );

            LifecycleExecutionState.Builder retryStepState = LifecycleExecutionState.builder(nextStepState);
            retryStepState.setIsAutoRetryableError(lifecycleState.isAutoRetryableError());
            Integer currentRetryCount = lifecycleState.failedStepRetryCount();
            if (isAutomaticRetry) {
                retryStepState.setFailedStepRetryCount(currentRetryCount == null ? 1 : ++currentRetryCount);
            } else {
                // manual retries don't update the retry count
                retryStepState.setFailedStepRetryCount(lifecycleState.failedStepRetryCount());
            }
            newState = LifecycleExecutionStateUtils.newClusterStateWithLifecycleState(
                currentState,
                indexMetadata.getIndex(),
                retryStepState.build()
            );
        } else {
            throw new IllegalArgumentException(
                "cannot retry an action for an index [" + index + "] that has not encountered an error when running a Lifecycle Policy"
            );
        }
        return newState;
    }

    /**
     * Given the existing execution state for an index, this updates pieces of the state with new
     * timings and optionally the phase JSON (when transitioning to a different phase).
     */
    private static LifecycleExecutionState updateExecutionStateToStep(
        LifecyclePolicyMetadata policyMetadata,
        LifecycleExecutionState existingState,
        Step.StepKey newStep,
        LongSupplier nowSupplier,
        boolean forcePhaseDefinitionRefresh,
        boolean allowNullPreviousStepInfo
    ) {
        Step.StepKey currentStep = Step.getCurrentStepKey(existingState);
        long nowAsMillis = nowSupplier.getAsLong();
        LifecycleExecutionState.Builder updatedState = LifecycleExecutionState.builder(existingState);
        updatedState.setPhase(newStep.phase());
        updatedState.setAction(newStep.action());
        updatedState.setStep(newStep.name());
        updatedState.setStepTime(nowAsMillis);

        // clear any step info or error-related settings from the current step
        updatedState.setFailedStep(null);
        if (allowNullPreviousStepInfo || existingState.stepInfo() != null) {
            updatedState.setPreviousStepInfo(existingState.stepInfo());
        }
        updatedState.setStepInfo(null);
        updatedState.setIsAutoRetryableError(null);
        updatedState.setFailedStepRetryCount(null);

        if (currentStep == null || currentStep.phase().equals(newStep.phase()) == false || forcePhaseDefinitionRefresh) {
            final String newPhaseDefinition;
            final Phase nextPhase;
            if ("new".equals(newStep.phase()) || TerminalPolicyStep.KEY.equals(newStep)) {
                nextPhase = null;
            } else {
                nextPhase = policyMetadata.getPolicy().getPhases().get(newStep.phase());
            }
            PhaseExecutionInfo phaseExecutionInfo = new PhaseExecutionInfo(
                policyMetadata.getName(),
                nextPhase,
                policyMetadata.getVersion(),
                policyMetadata.getModifiedDate()
            );
            newPhaseDefinition = Strings.toString(phaseExecutionInfo, false, false);
            updatedState.setPhaseDefinition(newPhaseDefinition);
            updatedState.setPhaseTime(nowAsMillis);
        } else if (currentStep.phase().equals(InitializePolicyContextStep.INITIALIZATION_PHASE)) {
            // The "new" phase is the initialization phase, usually the phase
            // time would be set on phase transition, but since there is no
            // transition into the "new" phase, we set it any time in the "new"
            // phase
            updatedState.setPhaseTime(nowAsMillis);
        }

        if (currentStep == null || currentStep.action().equals(newStep.action()) == false) {
            updatedState.setActionTime(nowAsMillis);
        }
        return updatedState.build();
    }

    /**
     * Transition the managed index to the first step of the next action in the current phase and update the cached phase definition for
     * the index to reflect the new phase definition.
     *
     * The intended purpose of this method is to help with the situations where a policy is updated and we need to update the cached
     * phase for the indices that are currently executing an action that was potentially removed in the new policy.
     *
     * Returns the same {@link LifecycleExecutionState} if the transition is not possible or the new execution state otherwise.
     */
    public static LifecycleExecutionState moveStateToNextActionAndUpdateCachedPhase(
        IndexMetadata indexMetadata,
        LifecycleExecutionState existingState,
        LongSupplier nowSupplier,
        LifecyclePolicy oldPolicy,
        LifecyclePolicyMetadata newPolicyMetadata,
        Client client,
        XPackLicenseState licenseState
    ) {
        String indexName = indexMetadata.getIndex().getName();
        String policyName = indexMetadata.getLifecyclePolicyName();
        Step.StepKey currentStepKey = Step.getCurrentStepKey(existingState);
        if (currentStepKey == null) {
            logger.warn(
                "unable to identify what the current step is for index [{}] as part of policy [{}]. the "
                    + "cached phase definition will not be updated for this index",
                indexName,
                policyName
            );
            return existingState;
        }

        List<Step> policySteps = oldPolicy.toSteps(client, licenseState);
        Optional<Step> currentStep = policySteps.stream().filter(step -> step.getKey().equals(currentStepKey)).findFirst();

        if (currentStep.isPresent() == false) {
            logger.warn(
                "unable to find current step [{}] for index [{}] as part of policy [{}]. the cached phase definition will not be "
                    + "updated for this index",
                currentStepKey,
                indexName,
                policyName
            );
            return existingState;
        }

        int indexOfCurrentStep = policySteps.indexOf(currentStep.get());
        assert indexOfCurrentStep != -1 : "the current step must be part of the old policy";

        Optional<Step> nextStepInActionAfterCurrent = policySteps.stream()
            .skip(indexOfCurrentStep)
            .filter(step -> step.getKey().action().equals(currentStepKey.action()) == false)
            .findFirst();

        assert nextStepInActionAfterCurrent.isPresent() : "there should always be a complete step at the end of every phase";
        Step.StepKey nextStep = nextStepInActionAfterCurrent.get().getKey();
        logger.debug("moving index [{}] in policy [{}] out of step [{}] to new step [{}]", indexName, policyName, currentStepKey, nextStep);

        long nowAsMillis = nowSupplier.getAsLong();
        LifecycleExecutionState.Builder updatedState = LifecycleExecutionState.builder(existingState);
        updatedState.setPhase(nextStep.phase());
        updatedState.setAction(nextStep.action());
        updatedState.setActionTime(nowAsMillis);
        updatedState.setStep(nextStep.name());
        updatedState.setStepTime(nowAsMillis);
        updatedState.setFailedStep(null);
        updatedState.setPreviousStepInfo(null);
        updatedState.setStepInfo(null);
        updatedState.setIsAutoRetryableError(null);
        updatedState.setFailedStepRetryCount(null);

        PhaseExecutionInfo phaseExecutionInfo = new PhaseExecutionInfo(
            newPolicyMetadata.getPolicy().getName(),
            newPolicyMetadata.getPolicy().getPhases().get(currentStepKey.phase()),
            newPolicyMetadata.getVersion(),
            newPolicyMetadata.getModifiedDate()
        );
        updatedState.setPhaseDefinition(Strings.toString(phaseExecutionInfo, false, false));
        return updatedState.build();
    }

    /**
     * Conditionally updates cluster state with new step info. The new cluster state is only
     * built if the step info has changed, otherwise the same old <code>clusterState</code> is
     * returned
     *
     * @param index        the index to modify
     * @param clusterState the cluster state to modify
     * @param stepInfo     the new step info to update
     * @return Updated cluster state with <code>stepInfo</code> if changed, otherwise the same cluster state
     * if no changes to step info exist
     */
    static ClusterState addStepInfoToClusterState(Index index, ClusterState clusterState, ToXContentObject stepInfo) {
        IndexMetadata indexMetadata = clusterState.getMetadata().getProject().index(index);
        if (indexMetadata == null) {
            // This index doesn't exist anymore, we can't do anything
            return clusterState;
        }
        LifecycleExecutionState lifecycleState = indexMetadata.getLifecycleExecutionState();
        final String stepInfoString = Strings.toString(stepInfo);
        if (stepInfoString.equals(lifecycleState.stepInfo())) {
            return clusterState;
        }
        LifecycleExecutionState.Builder newState = LifecycleExecutionState.builder(lifecycleState);
        newState.setStepInfo(stepInfoString);
        return LifecycleExecutionStateUtils.newClusterStateWithLifecycleState(clusterState, indexMetadata.getIndex(), newState.build());
    }

    /**
     * Remove the ILM policy from the given indices, this removes the lifecycle setting as well as
     * any lifecycle execution state that may be present in the index metadata
     */
    public static ClusterState removePolicyForIndexes(final Index[] indices, ClusterState currentState, List<String> failedIndexes) {
        final ProjectMetadata currentProject = currentState.metadata().getProject();
        final ProjectMetadata.Builder updatedProject = removePolicyForIndexes(indices, currentProject, failedIndexes);

        if (updatedProject == null) {
            return currentState;
        } else {
            return ClusterState.builder(currentState).putProjectMetadata(updatedProject).build();
        }
    }

    /**
     * @return If one or more policies were removed, then a new builder representing the changed project state.
     *         Otherwise {@code null} (if no changes were made)
     */
    @Nullable
    private static ProjectMetadata.Builder removePolicyForIndexes(
        final Index[] indices,
        ProjectMetadata currentProject,
        List<String> failedIndexes
    ) {
        ProjectMetadata.Builder newProject = null;
        for (Index index : indices) {
            IndexMetadata indexMetadata = currentProject.index(index);
            if (indexMetadata == null) {
                // Index doesn't exist so fail it
                failedIndexes.add(index.getName());
            } else {
                IndexMetadata.Builder newIdxMetadata = removePolicyForIndex(indexMetadata);
                if (newIdxMetadata != null) {
                    if (newProject == null) {
                        newProject = ProjectMetadata.builder(currentProject);
                    }
                    newProject.put(newIdxMetadata);
                }
            }
        }

        return newProject;
    }

    /**
     * Remove ILM-related metadata from an index's {@link IndexMetadata}
     */
    private static IndexMetadata.Builder removePolicyForIndex(IndexMetadata indexMetadata) {
        Settings idxSettings = indexMetadata.getSettings();
        Settings.Builder newSettings = Settings.builder().put(idxSettings);
        boolean notChanged = true;

        notChanged &= Strings.isNullOrEmpty(newSettings.remove(LifecycleSettings.LIFECYCLE_NAME_SETTING.getKey()));
        notChanged &= Strings.isNullOrEmpty(newSettings.remove(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE_SETTING.getKey()));
        notChanged &= Strings.isNullOrEmpty(newSettings.remove(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS_SETTING.getKey()));
        long newSettingsVersion = notChanged ? indexMetadata.getSettingsVersion() : 1 + indexMetadata.getSettingsVersion();

        IndexMetadata.Builder builder = IndexMetadata.builder(indexMetadata);
        builder.removeCustom(ILM_CUSTOM_METADATA_KEY);
        return builder.settings(newSettings).settingsVersion(newSettingsVersion);
    }
}
