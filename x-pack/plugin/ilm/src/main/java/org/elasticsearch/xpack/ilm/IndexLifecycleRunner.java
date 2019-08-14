/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.Index;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ilm.AsyncActionStep;
import org.elasticsearch.xpack.core.ilm.AsyncWaitStep;
import org.elasticsearch.xpack.core.ilm.ClusterStateActionStep;
import org.elasticsearch.xpack.core.ilm.ClusterStateWaitStep;
import org.elasticsearch.xpack.core.ilm.ErrorStep;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.InitializePolicyContextStep;
import org.elasticsearch.xpack.core.ilm.LifecycleExecutionState;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.PhaseCompleteStep;
import org.elasticsearch.xpack.core.ilm.PhaseExecutionInfo;
import org.elasticsearch.xpack.core.ilm.RolloverAction;
import org.elasticsearch.xpack.core.ilm.Step;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;
import org.elasticsearch.xpack.core.ilm.TerminalPolicyStep;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.LongSupplier;

import static org.elasticsearch.ElasticsearchException.REST_EXCEPTION_SKIP_STACK_TRACE;
import static org.elasticsearch.xpack.core.ilm.LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY;

public class IndexLifecycleRunner {
    private static final Logger logger = LogManager.getLogger(IndexLifecycleRunner.class);
    private static final ToXContent.Params STACKTRACE_PARAMS =
        new ToXContent.MapParams(Collections.singletonMap(REST_EXCEPTION_SKIP_STACK_TRACE, "false"));
    private final ThreadPool threadPool;
    private PolicyStepsRegistry stepRegistry;
    private ClusterService clusterService;
    private LongSupplier nowSupplier;

    public IndexLifecycleRunner(PolicyStepsRegistry stepRegistry, ClusterService clusterService,
                                ThreadPool threadPool, LongSupplier nowSupplier) {
        this.stepRegistry = stepRegistry;
        this.clusterService = clusterService;
        this.nowSupplier = nowSupplier;
        this.threadPool = threadPool;
    }

    /**
     * Return true or false depending on whether the index is ready to be in {@code phase}
     */
    boolean isReadyToTransitionToThisPhase(final String policy, final IndexMetaData indexMetaData, final String phase) {
        LifecycleExecutionState lifecycleState = LifecycleExecutionState.fromIndexMetadata(indexMetaData);
        if (lifecycleState.getLifecycleDate() == null) {
            logger.trace("no index creation date has been set yet");
            return true;
        }
        final Long lifecycleDate = lifecycleState.getLifecycleDate();
        assert lifecycleDate != null && lifecycleDate >= 0 : "expected index to have a lifecycle date but it did not";
        final TimeValue after = stepRegistry.getIndexAgeForPhase(policy, phase);
        final long now = nowSupplier.getAsLong();
        final TimeValue age = new TimeValue(now - lifecycleDate);
        if (logger.isTraceEnabled()) {
            logger.trace("[{}] checking for index age to be at least [{}] before performing actions in " +
                    "the \"{}\" phase. Now: {}, lifecycle date: {}, age: [{}/{}s]",
                indexMetaData.getIndex().getName(), after, phase,
                new TimeValue(now).seconds(),
                new TimeValue(lifecycleDate).seconds(),
                age, age.seconds());
        }
        return now >= lifecycleDate + after.getMillis();
    }

    /**
     * Run the current step, only if it is an asynchronous wait step. These
     * wait criteria are checked periodically from the ILM scheduler
     */
    public void runPeriodicStep(String policy, IndexMetaData indexMetaData) {
        String index = indexMetaData.getIndex().getName();
        LifecycleExecutionState lifecycleState = LifecycleExecutionState.fromIndexMetadata(indexMetaData);
        Step currentStep = getCurrentStep(stepRegistry, policy, indexMetaData, lifecycleState);
        if (currentStep == null) {
            if (stepRegistry.policyExists(policy) == false) {
                markPolicyDoesNotExist(policy, indexMetaData.getIndex(), lifecycleState);
                return;
            } else {
                logger.error("current step [{}] for index [{}] with policy [{}] is not recognized",
                    getCurrentStepKey(lifecycleState), index, policy);
                return;
            }
        }

        if (currentStep instanceof TerminalPolicyStep) {
            logger.debug("policy [{}] for index [{}] complete, skipping execution", policy, index);
            return;
        } else if (currentStep instanceof ErrorStep) {
            logger.debug("policy [{}] for index [{}] on an error step, skipping execution", policy, index);
            return;
        }

        logger.trace("[{}] maybe running periodic step ({}) with current step {}",
            index, currentStep.getClass().getSimpleName(), currentStep.getKey());
        // Only phase changing and async wait steps should be run through periodic polling
        if (currentStep instanceof PhaseCompleteStep) {
            // Only proceed to the next step if enough time has elapsed to go into the next phase
            if (isReadyToTransitionToThisPhase(policy, indexMetaData, currentStep.getNextStepKey().getPhase())) {
                moveToStep(indexMetaData.getIndex(), policy, currentStep.getKey(), currentStep.getNextStepKey());
            }
        } else if (currentStep instanceof AsyncWaitStep) {
            logger.debug("[{}] running periodic policy with current-step [{}]", index, currentStep.getKey());
            ((AsyncWaitStep) currentStep).evaluateCondition(indexMetaData, new AsyncWaitStep.Listener() {

                @Override
                public void onResponse(boolean conditionMet, ToXContentObject stepInfo) {
                    logger.trace("cs-change-async-wait-callback, [{}] current-step: {}", index, currentStep.getKey());
                    if (conditionMet) {
                        moveToStep(indexMetaData.getIndex(), policy, currentStep.getKey(), currentStep.getNextStepKey());
                    } else if (stepInfo != null) {
                        setStepInfo(indexMetaData.getIndex(), policy, currentStep.getKey(), stepInfo);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    moveToErrorStep(indexMetaData.getIndex(), policy, currentStep.getKey(), e);
                }
            });
        } else {
            logger.trace("[{}] ignoring non periodic step execution from step transition [{}]", index, currentStep.getKey());
        }
    }

    /**
     * If the current step (matching the expected step key) is an asynchronous action step, run it
     */
    public void maybeRunAsyncAction(ClusterState currentState, IndexMetaData indexMetaData, String policy, StepKey expectedStepKey) {
        String index = indexMetaData.getIndex().getName();
        LifecycleExecutionState lifecycleState = LifecycleExecutionState.fromIndexMetadata(indexMetaData);
        Step currentStep = getCurrentStep(stepRegistry, policy, indexMetaData, lifecycleState);
        if (currentStep == null) {
            logger.warn("current step [{}] for index [{}] with policy [{}] is not recognized",
                getCurrentStepKey(lifecycleState), index, policy);
            return;
        }

        logger.trace("[{}] maybe running async action step ({}) with current step {}",
            index, currentStep.getClass().getSimpleName(), currentStep.getKey());
        if (currentStep.getKey().equals(expectedStepKey) == false) {
            throw new IllegalStateException("expected index [" + indexMetaData.getIndex().getName() + "] with policy [" + policy +
                "] to have current step consistent with provided step key (" + expectedStepKey + ") but it was " + currentStep.getKey());
        }
        if (currentStep instanceof AsyncActionStep) {
            logger.debug("[{}] running policy with async action step [{}]", index, currentStep.getKey());
            ((AsyncActionStep) currentStep).performAction(indexMetaData, currentState,
                new ClusterStateObserver(clusterService, null, logger, threadPool.getThreadContext()), new AsyncActionStep.Listener() {

                @Override
                public void onResponse(boolean complete) {
                    logger.trace("cs-change-async-action-callback, [{}], current-step: {}", index, currentStep.getKey());
                    if (complete && ((AsyncActionStep) currentStep).indexSurvives()) {
                        moveToStep(indexMetaData.getIndex(), policy, currentStep.getKey(), currentStep.getNextStepKey());
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    moveToErrorStep(indexMetaData.getIndex(), policy, currentStep.getKey(), e);
                }
            });
        } else {
            logger.trace("[{}] ignoring non async action step execution from step transition [{}]", index, currentStep.getKey());
        }
    }

    /**
     * Run the current step that either waits for index age, or updates/waits-on cluster state.
     * Invoked after the cluster state has been changed
     */
    public void runPolicyAfterStateChange(String policy, IndexMetaData indexMetaData) {
        String index = indexMetaData.getIndex().getName();
        LifecycleExecutionState lifecycleState = LifecycleExecutionState.fromIndexMetadata(indexMetaData);
        Step currentStep = getCurrentStep(stepRegistry, policy, indexMetaData, lifecycleState);
        if (currentStep == null) {
            if (stepRegistry.policyExists(policy) == false) {
                markPolicyDoesNotExist(policy, indexMetaData.getIndex(), lifecycleState);
                return;
            } else {
                logger.error("current step [{}] for index [{}] with policy [{}] is not recognized",
                    getCurrentStepKey(lifecycleState), index, policy);
                return;
            }
        }

        if (currentStep instanceof TerminalPolicyStep) {
            logger.debug("policy [{}] for index [{}] complete, skipping execution", policy, index);
            return;
        } else if (currentStep instanceof ErrorStep) {
            logger.debug("policy [{}] for index [{}] on an error step, skipping execution", policy, index);
            return;
        }

        logger.trace("[{}] maybe running step ({}) after state change: {}",
            index, currentStep.getClass().getSimpleName(), currentStep.getKey());
        if (currentStep instanceof PhaseCompleteStep) {
            // Only proceed to the next step if enough time has elapsed to go into the next phase
            if (isReadyToTransitionToThisPhase(policy, indexMetaData, currentStep.getNextStepKey().getPhase())) {
                moveToStep(indexMetaData.getIndex(), policy, currentStep.getKey(), currentStep.getNextStepKey());
            }
        } else if (currentStep instanceof ClusterStateActionStep || currentStep instanceof ClusterStateWaitStep) {
            logger.debug("[{}] running policy with current-step [{}]", indexMetaData.getIndex().getName(), currentStep.getKey());
            clusterService.submitStateUpdateTask("ilm-execute-cluster-state-steps",
                new ExecuteStepsUpdateTask(policy, indexMetaData.getIndex(), currentStep, stepRegistry, this, nowSupplier));
        } else {
            logger.trace("[{}] ignoring step execution from cluster state change event [{}]", index, currentStep.getKey());
        }
    }

    /**
     * Retrieves the current {@link StepKey} from the index settings. Note that
     * it is illegal for the step to be set with the phase and/or action unset,
     * or for the step to be unset with the phase and/or action set. All three
     * settings must be either present or missing.
     *
     * @param lifecycleState the index custom data to extract the {@link StepKey} from.
     */
    public static StepKey getCurrentStepKey(LifecycleExecutionState lifecycleState) {
        String currentPhase = lifecycleState.getPhase();
        String currentAction = lifecycleState.getAction();
        String currentStep = lifecycleState.getStep();
        if (Strings.isNullOrEmpty(currentStep)) {
            assert Strings.isNullOrEmpty(currentPhase) : "Current phase is not empty: " + currentPhase;
            assert Strings.isNullOrEmpty(currentAction) : "Current action is not empty: " + currentAction;
            return null;
        } else {
            assert Strings.isNullOrEmpty(currentPhase) == false;
            assert Strings.isNullOrEmpty(currentAction) == false;
            return new StepKey(currentPhase, currentAction, currentStep);
        }
    }

    static Step getCurrentStep(PolicyStepsRegistry stepRegistry, String policy, IndexMetaData indexMetaData,
                               LifecycleExecutionState lifecycleState) {
        StepKey currentStepKey = getCurrentStepKey(lifecycleState);
        logger.trace("[{}] retrieved current step key: {}", indexMetaData.getIndex().getName(), currentStepKey);
        if (currentStepKey == null) {
            return stepRegistry.getFirstStep(policy);
        } else {
            return stepRegistry.getStep(indexMetaData, currentStepKey);
        }
    }

    /**
     * This method is intended for handling moving to different steps from {@link TransportAction} executions.
     * For this reason, it is reasonable to throw {@link IllegalArgumentException} when state is not as expected.
     *
     * @param indexName      The index whose step is to change
     * @param currentState   The current {@link ClusterState}
     * @param currentStepKey The current {@link StepKey} found for the index in the current cluster state
     * @param nextStepKey    The next step to move the index into
     * @param nowSupplier    The current-time supplier for updating when steps changed
     * @param stepRegistry   The steps registry to check a step-key's existence in the index's current policy
     * @param forcePhaseDefinitionRefresh When true, step information will be recompiled from the latest version of the
     *                                    policy. Otherwise, existing phase definition is used.
     * @return The updated cluster state where the index moved to <code>nextStepKey</code>
     */
    static ClusterState moveClusterStateToStep(String indexName, ClusterState currentState, StepKey currentStepKey,
                                               StepKey nextStepKey, LongSupplier nowSupplier,
                                               PolicyStepsRegistry stepRegistry, boolean forcePhaseDefinitionRefresh) {
        IndexMetaData idxMeta = currentState.getMetaData().index(indexName);
        Settings indexSettings = idxMeta.getSettings();
        String indexPolicySetting = LifecycleSettings.LIFECYCLE_NAME_SETTING.get(indexSettings);

        // policy could be updated in-between execution
        if (Strings.isNullOrEmpty(indexPolicySetting)) {
            throw new IllegalArgumentException("index [" + indexName + "] is not associated with an Index Lifecycle Policy");
        }

        LifecycleExecutionState lifecycleState = LifecycleExecutionState.fromIndexMetadata(idxMeta);
        if (currentStepKey.equals(IndexLifecycleRunner.getCurrentStepKey(lifecycleState)) == false) {
            throw new IllegalArgumentException("index [" + indexName + "] is not on current step [" + currentStepKey + "]");
        }

        if (stepRegistry.stepExists(indexPolicySetting, nextStepKey) == false) {
            throw new IllegalArgumentException("step [" + nextStepKey + "] for index [" + idxMeta.getIndex().getName() +
                "] with policy [" + indexPolicySetting + "] does not exist");
        }

        logger.info("moving index [{}] from [{}] to [{}] in policy [{}]",
            indexName, currentStepKey, nextStepKey, indexPolicySetting);

        return IndexLifecycleRunner.moveClusterStateToNextStep(idxMeta.getIndex(), currentState, currentStepKey,
            nextStepKey, nowSupplier, forcePhaseDefinitionRefresh);
    }

    static ClusterState moveClusterStateToNextStep(Index index, ClusterState clusterState, StepKey currentStep, StepKey nextStep,
                                                   LongSupplier nowSupplier, boolean forcePhaseDefinitionRefresh) {
        IndexMetaData idxMeta = clusterState.getMetaData().index(index);
        IndexLifecycleMetadata ilmMeta = clusterState.metaData().custom(IndexLifecycleMetadata.TYPE);
        LifecyclePolicyMetadata policyMetadata = ilmMeta.getPolicyMetadatas()
            .get(LifecycleSettings.LIFECYCLE_NAME_SETTING.get(idxMeta.getSettings()));
        LifecycleExecutionState lifecycleState = LifecycleExecutionState.fromIndexMetadata(idxMeta);
        LifecycleExecutionState newLifecycleState = moveExecutionStateToNextStep(policyMetadata,
            lifecycleState, currentStep, nextStep, nowSupplier, forcePhaseDefinitionRefresh);
        ClusterState.Builder newClusterStateBuilder = newClusterStateWithLifecycleState(index, clusterState, newLifecycleState);

        return newClusterStateBuilder.build();
    }

    static ClusterState moveClusterStateToErrorStep(Index index, ClusterState clusterState, StepKey currentStep, Exception cause,
                                                    LongSupplier nowSupplier) throws IOException {
        IndexMetaData idxMeta = clusterState.getMetaData().index(index);
        IndexLifecycleMetadata ilmMeta = clusterState.metaData().custom(IndexLifecycleMetadata.TYPE);
        LifecyclePolicyMetadata policyMetadata = ilmMeta.getPolicyMetadatas()
            .get(LifecycleSettings.LIFECYCLE_NAME_SETTING.get(idxMeta.getSettings()));
        XContentBuilder causeXContentBuilder = JsonXContent.contentBuilder();
        causeXContentBuilder.startObject();
        ElasticsearchException.generateThrowableXContent(causeXContentBuilder, STACKTRACE_PARAMS, cause);
        causeXContentBuilder.endObject();
        LifecycleExecutionState nextStepState = moveExecutionStateToNextStep(policyMetadata,
            LifecycleExecutionState.fromIndexMetadata(idxMeta), currentStep, new StepKey(currentStep.getPhase(),
                currentStep.getAction(), ErrorStep.NAME), nowSupplier, false);
        LifecycleExecutionState.Builder failedState = LifecycleExecutionState.builder(nextStepState);
        failedState.setFailedStep(currentStep.getName());
        failedState.setStepInfo(BytesReference.bytes(causeXContentBuilder).utf8ToString());
        ClusterState.Builder newClusterStateBuilder = newClusterStateWithLifecycleState(index, clusterState, failedState.build());
        return newClusterStateBuilder.build();
    }

    ClusterState moveClusterStateToFailedStep(ClusterState currentState, String[] indices) {
        ClusterState newState = currentState;
        for (String index : indices) {
            IndexMetaData indexMetaData = currentState.metaData().index(index);
            if (indexMetaData == null) {
                throw new IllegalArgumentException("index [" + index + "] does not exist");
            }
            LifecycleExecutionState lifecycleState = LifecycleExecutionState.fromIndexMetadata(indexMetaData);
            StepKey currentStepKey = IndexLifecycleRunner.getCurrentStepKey(lifecycleState);
            String failedStep = lifecycleState.getFailedStep();
            if (currentStepKey != null && ErrorStep.NAME.equals(currentStepKey.getName())
                    && Strings.isNullOrEmpty(failedStep) == false) {
                StepKey nextStepKey = new StepKey(currentStepKey.getPhase(), currentStepKey.getAction(), failedStep);
                newState = moveClusterStateToStep(index, currentState, currentStepKey, nextStepKey, nowSupplier, stepRegistry, true);
            } else {
                throw new IllegalArgumentException("cannot retry an action for an index ["
                    + index + "] that has not encountered an error when running a Lifecycle Policy");
            }
        }
        return newState;
    }

    private static LifecycleExecutionState moveExecutionStateToNextStep(LifecyclePolicyMetadata policyMetadata,
                                                                        LifecycleExecutionState existingState,
                                                                        StepKey currentStep, StepKey nextStep,
                                                                        LongSupplier nowSupplier,
                                                                        boolean forcePhaseDefinitionRefresh) {
        long nowAsMillis = nowSupplier.getAsLong();
        LifecycleExecutionState.Builder updatedState = LifecycleExecutionState.builder(existingState);
        updatedState.setPhase(nextStep.getPhase());
        updatedState.setAction(nextStep.getAction());
        updatedState.setStep(nextStep.getName());
        updatedState.setStepTime(nowAsMillis);

        // clear any step info or error-related settings from the current step
        updatedState.setFailedStep(null);
        updatedState.setStepInfo(null);

        if (currentStep.getPhase().equals(nextStep.getPhase()) == false || forcePhaseDefinitionRefresh) {
            final String newPhaseDefinition;
            final Phase nextPhase;
            if ("new".equals(nextStep.getPhase()) || TerminalPolicyStep.KEY.equals(nextStep)) {
                nextPhase = null;
            } else {
                nextPhase = policyMetadata.getPolicy().getPhases().get(nextStep.getPhase());
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

        if (currentStep.getAction().equals(nextStep.getAction()) == false) {
            updatedState.setActionTime(nowAsMillis);
        }
        return updatedState.build();
    }

    static ClusterState.Builder newClusterStateWithLifecycleState(Index index, ClusterState clusterState,
                                                                  LifecycleExecutionState lifecycleState) {
        ClusterState.Builder newClusterStateBuilder = ClusterState.builder(clusterState);
        newClusterStateBuilder.metaData(MetaData.builder(clusterState.getMetaData())
            .put(IndexMetaData.builder(clusterState.getMetaData().index(index))
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
        IndexMetaData indexMetaData = clusterState.getMetaData().index(index);
        if (indexMetaData == null) {
            // This index doesn't exist anymore, we can't do anything
            return clusterState;
        }
        LifecycleExecutionState lifecycleState = LifecycleExecutionState.fromIndexMetadata(indexMetaData);
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

    private void moveToStep(Index index, String policy, StepKey currentStepKey, StepKey nextStepKey) {
        logger.debug("[{}] moving to step [{}] {} -> {}", index.getName(), policy, currentStepKey, nextStepKey);
        clusterService.submitStateUpdateTask("ilm-move-to-step",
            new MoveToNextStepUpdateTask(index, policy, currentStepKey, nextStepKey, nowSupplier, clusterState ->
            {
                IndexMetaData indexMetaData = clusterState.metaData().index(index);
                if (nextStepKey != null && nextStepKey != TerminalPolicyStep.KEY && indexMetaData != null) {
                    maybeRunAsyncAction(clusterState, indexMetaData, policy, nextStepKey);
                }
            }));
    }

    private void moveToErrorStep(Index index, String policy, StepKey currentStepKey, Exception e) {
        logger.error(new ParameterizedMessage("policy [{}] for index [{}] failed on step [{}]. Moving to ERROR step",
                policy, index.getName(), currentStepKey), e);
        clusterService.submitStateUpdateTask("ilm-move-to-error-step",
            new MoveToErrorStepUpdateTask(index, policy, currentStepKey, e, nowSupplier));
    }

    private void setStepInfo(Index index, String policy, StepKey currentStepKey, ToXContentObject stepInfo) {
        clusterService.submitStateUpdateTask("ilm-set-step-info", new SetStepInfoUpdateTask(index, policy, currentStepKey, stepInfo));
    }

    public static ClusterState removePolicyForIndexes(final Index[] indices, ClusterState currentState, List<String> failedIndexes) {
        MetaData.Builder newMetadata = MetaData.builder(currentState.getMetaData());
        boolean clusterStateChanged = false;
        for (Index index : indices) {
            IndexMetaData indexMetadata = currentState.getMetaData().index(index);
            if (indexMetadata == null) {
                // Index doesn't exist so fail it
                failedIndexes.add(index.getName());
            } else {
                IndexMetaData.Builder newIdxMetadata = IndexLifecycleRunner.removePolicyForIndex(indexMetadata);
                if (newIdxMetadata != null) {
                    newMetadata.put(newIdxMetadata);
                    clusterStateChanged = true;
                }
            }
        }
        if (clusterStateChanged) {
            ClusterState.Builder newClusterState = ClusterState.builder(currentState);
            newClusterState.metaData(newMetadata);
            return newClusterState.build();
        } else {
            return currentState;
        }
    }

    private static IndexMetaData.Builder removePolicyForIndex(IndexMetaData indexMetadata) {
        Settings idxSettings = indexMetadata.getSettings();
        Settings.Builder newSettings = Settings.builder().put(idxSettings);
        boolean notChanged = true;

        notChanged &= Strings.isNullOrEmpty(newSettings.remove(LifecycleSettings.LIFECYCLE_NAME_SETTING.getKey()));
        notChanged &= Strings.isNullOrEmpty(newSettings.remove(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE_SETTING.getKey()));
        notChanged &= Strings.isNullOrEmpty(newSettings.remove(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS_SETTING.getKey()));
        long newSettingsVersion = notChanged ? indexMetadata.getSettingsVersion() : 1 + indexMetadata.getSettingsVersion();

        IndexMetaData.Builder builder = IndexMetaData.builder(indexMetadata);
        builder.removeCustom(ILM_CUSTOM_METADATA_KEY);
        return builder.settings(newSettings).settingsVersion(newSettingsVersion);
    }

    private void markPolicyDoesNotExist(String policyName, Index index, LifecycleExecutionState executionState) {
        logger.debug("policy [{}] for index [{}] does not exist, recording this in step_info for this index",
            policyName, index.getName());
        setStepInfo(index, policyName, getCurrentStepKey(executionState),
            new SetStepInfoUpdateTask.ExceptionWrapper(
                new IllegalArgumentException("policy [" + policyName + "] does not exist")));
    }
}
