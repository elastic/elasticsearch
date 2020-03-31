/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.Index;
import org.elasticsearch.xpack.core.ilm.ErrorStep;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.InitializePolicyContextStep;
import org.elasticsearch.xpack.core.ilm.InitializePolicyException;
import org.elasticsearch.xpack.core.ilm.LifecycleExecutionState;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.PhaseExecutionInfo;
import org.elasticsearch.xpack.core.ilm.RolloverAction;
import org.elasticsearch.xpack.core.ilm.Step;
import org.elasticsearch.xpack.core.ilm.TerminalPolicyStep;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.LongSupplier;

import static org.elasticsearch.ElasticsearchException.REST_EXCEPTION_SKIP_STACK_TRACE;
import static org.elasticsearch.xpack.core.ilm.LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY;

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
    private static final ToXContent.Params STACKTRACE_PARAMS =
        new ToXContent.MapParams(Collections.singletonMap(REST_EXCEPTION_SKIP_STACK_TRACE, "false"));

    /**
     * Validates that the given transition from {@code currentStepKey} to {@code newStepKey} can be accomplished
     * @throws IllegalArgumentException when the transition is not valid
     */
    public static void validateTransition(IndexMetadata idxMeta, Step.StepKey currentStepKey,
                                          Step.StepKey newStepKey, PolicyStepsRegistry stepRegistry) {
        String indexName = idxMeta.getIndex().getName();
        Settings indexSettings = idxMeta.getSettings();
        String indexPolicySetting = LifecycleSettings.LIFECYCLE_NAME_SETTING.get(indexSettings);

        // policy could be updated in-between execution
        if (Strings.isNullOrEmpty(indexPolicySetting)) {
            throw new IllegalArgumentException("index [" + indexName + "] is not associated with an Index Lifecycle Policy");
        }

        LifecycleExecutionState lifecycleState = LifecycleExecutionState.fromIndexMetadata(idxMeta);
        Step.StepKey realKey = LifecycleExecutionState.getCurrentStepKey(lifecycleState);
        if (currentStepKey != null && currentStepKey.equals(realKey) == false) {
            throw new IllegalArgumentException("index [" + indexName + "] is not on current step [" + currentStepKey +
                "], currently: [" + realKey + "]");
        }

        // Always allow moving to the terminal step, even if it doesn't exist in the policy
        if (stepRegistry.stepExists(indexPolicySetting, newStepKey) == false && newStepKey.equals(TerminalPolicyStep.KEY) == false) {
            throw new IllegalArgumentException("step [" + newStepKey + "] for index [" + idxMeta.getIndex().getName() +
                "] with policy [" + indexPolicySetting + "] does not exist");
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
    static ClusterState moveClusterStateToStep(Index index, ClusterState state, Step.StepKey newStepKey, LongSupplier nowSupplier,
                                               PolicyStepsRegistry stepRegistry, boolean forcePhaseDefinitionRefresh) {
        IndexMetadata idxMeta = state.getMetadata().index(index);
        Step.StepKey currentStepKey = LifecycleExecutionState.getCurrentStepKey(LifecycleExecutionState.fromIndexMetadata(idxMeta));
        validateTransition(idxMeta, currentStepKey, newStepKey, stepRegistry);

        Settings indexSettings = idxMeta.getSettings();
        String policy = LifecycleSettings.LIFECYCLE_NAME_SETTING.get(indexSettings);
        logger.info("moving index [{}] from [{}] to [{}] in policy [{}]", index.getName(), currentStepKey, newStepKey, policy);

        IndexLifecycleMetadata ilmMeta = state.metadata().custom(IndexLifecycleMetadata.TYPE);
        LifecyclePolicyMetadata policyMetadata = ilmMeta.getPolicyMetadatas()
            .get(LifecycleSettings.LIFECYCLE_NAME_SETTING.get(idxMeta.getSettings()));
        LifecycleExecutionState lifecycleState = LifecycleExecutionState.fromIndexMetadata(idxMeta);
        LifecycleExecutionState newLifecycleState = updateExecutionStateToStep(policyMetadata,
            lifecycleState, newStepKey, nowSupplier, forcePhaseDefinitionRefresh);
        ClusterState.Builder newClusterStateBuilder = newClusterStateWithLifecycleState(index, state, newLifecycleState);

        return newClusterStateBuilder.build();
    }

    /**
     * Moves the given index into the ERROR step. The ERROR step will have the same phase and
     * action, but use the {@link ErrorStep#NAME} as the name in the lifecycle execution state.
     */
    static ClusterState moveClusterStateToErrorStep(Index index, ClusterState clusterState, Exception cause, LongSupplier nowSupplier,
                                                    BiFunction<IndexMetadata, Step.StepKey, Step> stepLookupFunction) throws IOException {
        IndexMetadata idxMeta = clusterState.getMetadata().index(index);
        IndexLifecycleMetadata ilmMeta = clusterState.metadata().custom(IndexLifecycleMetadata.TYPE);
        LifecyclePolicyMetadata policyMetadata = ilmMeta.getPolicyMetadatas()
            .get(LifecycleSettings.LIFECYCLE_NAME_SETTING.get(idxMeta.getSettings()));
        XContentBuilder causeXContentBuilder = JsonXContent.contentBuilder();
        causeXContentBuilder.startObject();
        ElasticsearchException.generateThrowableXContent(causeXContentBuilder, STACKTRACE_PARAMS, cause);
        causeXContentBuilder.endObject();
        LifecycleExecutionState currentState = LifecycleExecutionState.fromIndexMetadata(idxMeta);
        Step.StepKey currentStep;
        // if an error is encountered while initialising the policy the lifecycle execution state will not yet contain any step information
        // as we haven't yet initialised the policy, so we'll manually set the current step to be the "initialize policy" step so we can
        // record the error (and later retry the init policy step)
        if (cause instanceof InitializePolicyException) {
            currentStep = InitializePolicyContextStep.KEY;
        } else {
            currentStep = Objects.requireNonNull(LifecycleExecutionState.getCurrentStepKey(currentState),
                "unable to move to an error step where there is no current step, state: " + currentState);
        }
        LifecycleExecutionState nextStepState = updateExecutionStateToStep(policyMetadata, currentState,
            new Step.StepKey(currentStep.getPhase(), currentStep.getAction(), ErrorStep.NAME), nowSupplier, false);

        LifecycleExecutionState.Builder failedState = LifecycleExecutionState.builder(nextStepState);
        failedState.setFailedStep(currentStep.getName());
        failedState.setStepInfo(BytesReference.bytes(causeXContentBuilder).utf8ToString());
        Step failedStep = stepLookupFunction.apply(idxMeta, currentStep);

        if (failedStep != null) {
            // as an initial step we'll mark the failed step as auto retryable without actually looking at the cause to determine
            // if the error is transient/recoverable from
            failedState.setIsAutoRetryableError(failedStep.isRetryable());
            // maintain the retry count of the failed step as it will be cleared after a successful execution
            failedState.setFailedStepRetryCount(currentState.getFailedStepRetryCount());
        } else {
            logger.warn("failed step [{}] for index [{}] is not part of policy [{}] anymore, or it is invalid",
                currentStep.getName(), index, policyMetadata.getName());
        }

        ClusterState.Builder newClusterStateBuilder = newClusterStateWithLifecycleState(index, clusterState, failedState.build());
        return newClusterStateBuilder.build();
    }

    /**
     * Move the given index's execution state back to a step that had previously failed. If this is
     * an automatic retry ({@code isAutomaticRetry}), the retry count is incremented.
     */
    static ClusterState moveClusterStateToPreviouslyFailedStep(ClusterState currentState, String index, LongSupplier nowSupplier,
                                                               PolicyStepsRegistry stepRegistry, boolean isAutomaticRetry) {
        ClusterState newState;
        IndexMetadata indexMetadata = currentState.metadata().index(index);
        if (indexMetadata == null) {
            throw new IllegalArgumentException("index [" + index + "] does not exist");
        }
        LifecycleExecutionState lifecycleState = LifecycleExecutionState.fromIndexMetadata(indexMetadata);
        Step.StepKey currentStepKey = LifecycleExecutionState.getCurrentStepKey(lifecycleState);
        String failedStep = lifecycleState.getFailedStep();
        if (currentStepKey != null && ErrorStep.NAME.equals(currentStepKey.getName()) && Strings.isNullOrEmpty(failedStep) == false) {
            Step.StepKey nextStepKey = new Step.StepKey(currentStepKey.getPhase(), currentStepKey.getAction(), failedStep);
            IndexLifecycleTransition.validateTransition(indexMetadata, currentStepKey, nextStepKey, stepRegistry);
            IndexLifecycleMetadata ilmMeta = currentState.metadata().custom(IndexLifecycleMetadata.TYPE);

            LifecyclePolicyMetadata policyMetadata = ilmMeta.getPolicyMetadatas()
                .get(LifecycleSettings.LIFECYCLE_NAME_SETTING.get(indexMetadata.getSettings()));
            LifecycleExecutionState nextStepState = IndexLifecycleTransition.updateExecutionStateToStep(policyMetadata,
                lifecycleState, nextStepKey, nowSupplier, true);
            LifecycleExecutionState.Builder retryStepState = LifecycleExecutionState.builder(nextStepState);
            retryStepState.setIsAutoRetryableError(lifecycleState.isAutoRetryableError());
            Integer currentRetryCount = lifecycleState.getFailedStepRetryCount();
            if (isAutomaticRetry) {
                retryStepState.setFailedStepRetryCount(currentRetryCount == null ? 1 : ++currentRetryCount);
            } else {
                // manual retries don't update the retry count
                retryStepState.setFailedStepRetryCount(lifecycleState.getFailedStepRetryCount());
            }
            newState = IndexLifecycleTransition.newClusterStateWithLifecycleState(indexMetadata.getIndex(),
                currentState, retryStepState.build()).build();
        } else {
            throw new IllegalArgumentException("cannot retry an action for an index ["
                + index + "] that has not encountered an error when running a Lifecycle Policy");
        }
        return newState;
    }

    /**
     * Given the existing execution state for an index, this updates pieces of the state with new
     * timings and optionally the phase JSON (when transitioning to a different phase).
     */
    private static LifecycleExecutionState updateExecutionStateToStep(LifecyclePolicyMetadata policyMetadata,
                                                                      LifecycleExecutionState existingState,
                                                                      Step.StepKey newStep,
                                                                      LongSupplier nowSupplier,
                                                                      boolean forcePhaseDefinitionRefresh) {
        Step.StepKey currentStep = LifecycleExecutionState.getCurrentStepKey(existingState);
        long nowAsMillis = nowSupplier.getAsLong();
        LifecycleExecutionState.Builder updatedState = LifecycleExecutionState.builder(existingState);
        updatedState.setPhase(newStep.getPhase());
        updatedState.setAction(newStep.getAction());
        updatedState.setStep(newStep.getName());
        updatedState.setStepTime(nowAsMillis);

        // clear any step info or error-related settings from the current step
        updatedState.setFailedStep(null);
        updatedState.setStepInfo(null);
        updatedState.setIsAutoRetryableError(null);
        updatedState.setFailedStepRetryCount(null);

        if (currentStep == null ||
                currentStep.getPhase().equals(newStep.getPhase()) == false ||
                forcePhaseDefinitionRefresh) {
            final String newPhaseDefinition;
            final Phase nextPhase;
            if ("new".equals(newStep.getPhase()) || TerminalPolicyStep.KEY.equals(newStep)) {
                nextPhase = null;
            } else {
                nextPhase = policyMetadata.getPolicy().getPhases().get(newStep.getPhase());
            }
            PhaseExecutionInfo phaseExecutionInfo = new PhaseExecutionInfo(policyMetadata.getName(), nextPhase,
                policyMetadata.getVersion(), policyMetadata.getModifiedDate());
            newPhaseDefinition = Strings.toString(phaseExecutionInfo, false, false);
            updatedState.setPhaseDefinition(newPhaseDefinition);
            updatedState.setPhaseTime(nowAsMillis);
        } else if (currentStep.getPhase().equals(InitializePolicyContextStep.INITIALIZATION_PHASE)) {
            // The "new" phase is the initialization phase, usually the phase
            // time would be set on phase transition, but since there is no
            // transition into the "new" phase, we set it any time in the "new"
            // phase
            updatedState.setPhaseTime(nowAsMillis);
        }

        if (currentStep == null || currentStep.getAction().equals(newStep.getAction()) == false) {
            updatedState.setActionTime(nowAsMillis);
        }
        return updatedState.build();
    }

    /**
     * Given a cluster state and lifecycle state, return a new state using the new lifecycle state for the given index.
     */
    public static ClusterState.Builder newClusterStateWithLifecycleState(Index index, ClusterState clusterState,
                                                                         LifecycleExecutionState lifecycleState) {
        ClusterState.Builder newClusterStateBuilder = ClusterState.builder(clusterState);
        newClusterStateBuilder.metadata(Metadata.builder(clusterState.getMetadata())
            .put(IndexMetadata.builder(clusterState.getMetadata().index(index))
                .putCustom(ILM_CUSTOM_METADATA_KEY, lifecycleState.asMap())));
        return newClusterStateBuilder;
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
     * @throws IOException if parsing step info fails
     */
    static ClusterState addStepInfoToClusterState(Index index, ClusterState clusterState, ToXContentObject stepInfo) throws IOException {
        IndexMetadata indexMetadata = clusterState.getMetadata().index(index);
        if (indexMetadata == null) {
            // This index doesn't exist anymore, we can't do anything
            return clusterState;
        }
        LifecycleExecutionState lifecycleState = LifecycleExecutionState.fromIndexMetadata(indexMetadata);
        final String stepInfoString;
        try (XContentBuilder infoXContentBuilder = JsonXContent.contentBuilder()) {
            stepInfo.toXContent(infoXContentBuilder, ToXContent.EMPTY_PARAMS);
            stepInfoString = BytesReference.bytes(infoXContentBuilder).utf8ToString();
        }
        if (stepInfoString.equals(lifecycleState.getStepInfo())) {
            return clusterState;
        }
        LifecycleExecutionState.Builder newState = LifecycleExecutionState.builder(lifecycleState);
        newState.setStepInfo(stepInfoString);
        ClusterState.Builder newClusterStateBuilder = newClusterStateWithLifecycleState(index, clusterState, newState.build());
        return newClusterStateBuilder.build();
    }

    /**
     * Remove the ILM policy from the given indices, this removes the lifecycle setting as well as
     * any lifecycle execution state that may be present in the index metadata
     */
    public static ClusterState removePolicyForIndexes(final Index[] indices, ClusterState currentState, List<String> failedIndexes) {
        Metadata.Builder newMetadata = Metadata.builder(currentState.getMetadata());
        boolean clusterStateChanged = false;
        for (Index index : indices) {
            IndexMetadata indexMetadata = currentState.getMetadata().index(index);
            if (indexMetadata == null) {
                // Index doesn't exist so fail it
                failedIndexes.add(index.getName());
            } else {
                IndexMetadata.Builder newIdxMetadata = removePolicyForIndex(indexMetadata);
                if (newIdxMetadata != null) {
                    newMetadata.put(newIdxMetadata);
                    clusterStateChanged = true;
                }
            }
        }
        if (clusterStateChanged) {
            ClusterState.Builder newClusterState = ClusterState.builder(currentState);
            newClusterState.metadata(newMetadata);
            return newClusterState.build();
        } else {
            return currentState;
        }
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
