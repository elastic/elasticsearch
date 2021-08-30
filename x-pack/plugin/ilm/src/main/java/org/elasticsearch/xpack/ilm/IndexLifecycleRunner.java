/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ilm.AsyncActionStep;
import org.elasticsearch.xpack.core.ilm.AsyncWaitStep;
import org.elasticsearch.xpack.core.ilm.ClusterStateActionStep;
import org.elasticsearch.xpack.core.ilm.ClusterStateWaitStep;
import org.elasticsearch.xpack.core.ilm.ErrorStep;
import org.elasticsearch.xpack.core.ilm.LifecycleExecutionState;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.PhaseCompleteStep;
import org.elasticsearch.xpack.core.ilm.Step;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;
import org.elasticsearch.xpack.core.ilm.TerminalPolicyStep;
import org.elasticsearch.xpack.ilm.history.ILMHistoryItem;
import org.elasticsearch.xpack.ilm.history.ILMHistoryStore;

import java.util.Locale;
import java.util.function.LongSupplier;

import static org.elasticsearch.xpack.core.ilm.LifecycleSettings.LIFECYCLE_ORIGINATION_DATE;

class IndexLifecycleRunner {
    private static final Logger logger = LogManager.getLogger(IndexLifecycleRunner.class);
    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final PolicyStepsRegistry stepRegistry;
    private final ILMHistoryStore ilmHistoryStore;
    private final LongSupplier nowSupplier;

    IndexLifecycleRunner(PolicyStepsRegistry stepRegistry, ILMHistoryStore ilmHistoryStore, ClusterService clusterService,
                         ThreadPool threadPool, LongSupplier nowSupplier) {
        this.stepRegistry = stepRegistry;
        this.ilmHistoryStore = ilmHistoryStore;
        this.clusterService = clusterService;
        this.nowSupplier = nowSupplier;
        this.threadPool = threadPool;
    }

    /**
     * Retrieve the index's current step.
     */
    static Step getCurrentStep(PolicyStepsRegistry stepRegistry, String policy, IndexMetadata indexMetadata) {
        LifecycleExecutionState lifecycleState = LifecycleExecutionState.fromIndexMetadata(indexMetadata);
        StepKey currentStepKey = LifecycleExecutionState.getCurrentStepKey(lifecycleState);
        logger.trace("[{}] retrieved current step key: {}", indexMetadata.getIndex().getName(), currentStepKey);
        if (currentStepKey == null) {
            return stepRegistry.getFirstStep(policy);
        } else {
            return stepRegistry.getStep(indexMetadata, currentStepKey);
        }
    }

    /**
     * Calculate the index's origination time (in milliseconds) based on its
     * metadata. Returns null if there is no lifecycle date and the origination
     * date is not set.
     */
    @Nullable
    private static Long calculateOriginationMillis(final IndexMetadata indexMetadata) {
        LifecycleExecutionState lifecycleState = LifecycleExecutionState.fromIndexMetadata(indexMetadata);
        Long originationDate = indexMetadata.getSettings().getAsLong(LIFECYCLE_ORIGINATION_DATE, -1L);
        if (lifecycleState.getLifecycleDate() == null && originationDate == -1L) {
            return null;
        }
        return originationDate == -1L ? lifecycleState.getLifecycleDate() : originationDate;
    }

    /**
     * Return true or false depending on whether the index is ready to be in {@code phase}
     */
    boolean isReadyToTransitionToThisPhase(final String policy, final IndexMetadata indexMetadata, final String phase) {
        final Long lifecycleDate = calculateOriginationMillis(indexMetadata);
        if (lifecycleDate == null) {
            logger.trace("[{}] no index creation or origination date has been set yet", indexMetadata.getIndex().getName());
            return true;
        }
        final TimeValue after = stepRegistry.getIndexAgeForPhase(policy, phase);
        final long now = nowSupplier.getAsLong();
        final long ageMillis = now - lifecycleDate;
        final TimeValue age;
        if (ageMillis >= 0) {
            age = new TimeValue(ageMillis);
        } else if (ageMillis == Long.MIN_VALUE) {
            age = new TimeValue(Long.MAX_VALUE);
        } else {
            age = new TimeValue(-ageMillis);
        }
        if (logger.isTraceEnabled()) {
            logger.trace("[{}] checking for index age to be at least [{}] before performing actions in " +
                    "the \"{}\" phase. Now: {}, lifecycle date: {}, age: [{}{}/{}s]",
                indexMetadata.getIndex().getName(), after, phase,
                new TimeValue(now).seconds(),
                new TimeValue(lifecycleDate).seconds(),
                ageMillis < 0 ? "-" : "", age, age.seconds());
        }
        return now >= lifecycleDate + after.getMillis();
    }

    /**
     * Run the current step, only if it is an asynchronous wait step. These
     * wait criteria are checked periodically from the ILM scheduler
     */
    void runPeriodicStep(String policy, Metadata metadata, IndexMetadata indexMetadata) {
        String index = indexMetadata.getIndex().getName();
        LifecycleExecutionState lifecycleState = LifecycleExecutionState.fromIndexMetadata(indexMetadata);
        final Step currentStep;
        try {
            currentStep = getCurrentStep(stepRegistry, policy, indexMetadata);
        } catch (Exception e) {
            markPolicyRetrievalError(policy, indexMetadata.getIndex(), lifecycleState, e);
            return;
        }

        if (currentStep == null) {
            if (stepRegistry.policyExists(policy) == false) {
                markPolicyDoesNotExist(policy, indexMetadata.getIndex(), lifecycleState);
                return;
            } else {
                Step.StepKey currentStepKey = LifecycleExecutionState.getCurrentStepKey(lifecycleState);
                if (TerminalPolicyStep.KEY.equals(currentStepKey)) {
                    // This index is a leftover from before we halted execution on the final phase
                    // instead of going to the completed phase, so it's okay to ignore this index
                    // for now
                    return;
                }
                logger.error("current step [{}] for index [{}] with policy [{}] is not recognized",
                    currentStepKey, index, policy);
                return;
            }
        }

        if (currentStep instanceof TerminalPolicyStep) {
            logger.debug("policy [{}] for index [{}] complete, skipping execution", policy, index);
            return;
        } else if (currentStep instanceof ErrorStep) {
            onErrorMaybeRetryFailedStep(policy, indexMetadata);
            return;
        }

        logger.trace("[{}] maybe running periodic step ({}) with current step {}",
            index, currentStep.getClass().getSimpleName(), currentStep.getKey());
        // Only phase changing and async wait steps should be run through periodic polling
        if (currentStep instanceof PhaseCompleteStep) {
            if (currentStep.getNextStepKey() == null) {
                logger.debug("[{}] stopping in the current phase ({}) as there are no more steps in the policy",
                    index, currentStep.getKey().getPhase());
                return;
            }
            // Only proceed to the next step if enough time has elapsed to go into the next phase
            if (isReadyToTransitionToThisPhase(policy, indexMetadata, currentStep.getNextStepKey().getPhase())) {
                moveToStep(indexMetadata.getIndex(), policy, currentStep.getKey(), currentStep.getNextStepKey());
            }
        } else if (currentStep instanceof AsyncWaitStep) {
            logger.debug("[{}] running periodic policy with current-step [{}]", index, currentStep.getKey());
            ((AsyncWaitStep) currentStep).evaluateCondition(metadata, indexMetadata.getIndex(), new AsyncWaitStep.Listener() {

                @Override
                public void onResponse(boolean conditionMet, ToXContentObject stepInfo) {
                    logger.trace("cs-change-async-wait-callback, [{}] current-step: {}", index, currentStep.getKey());
                    if (conditionMet) {
                        moveToStep(indexMetadata.getIndex(), policy, currentStep.getKey(), currentStep.getNextStepKey());
                    } else if (stepInfo != null) {
                        setStepInfo(indexMetadata.getIndex(), policy, currentStep.getKey(), stepInfo);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    moveToErrorStep(indexMetadata.getIndex(), policy, currentStep.getKey(), e);
                }
            }, TimeValue.MAX_VALUE);
        } else {
            logger.trace("[{}] ignoring non periodic step execution from step transition [{}]", index, currentStep.getKey());
        }
    }

    /**
     * Given the policy and index metadata for an index, this moves the index's
     * execution state to the previously failed step, incrementing the retry
     * counter.
     */
    private void onErrorMaybeRetryFailedStep(String policy, IndexMetadata indexMetadata) {
        String index = indexMetadata.getIndex().getName();
        LifecycleExecutionState lifecycleState = LifecycleExecutionState.fromIndexMetadata(indexMetadata);
        Step failedStep = stepRegistry.getStep(indexMetadata, new StepKey(lifecycleState.getPhase(), lifecycleState.getAction(),
            lifecycleState.getFailedStep()));
        if (failedStep == null) {
            logger.warn("failed step [{}] for index [{}] is not part of policy [{}] anymore, or it is invalid. skipping execution",
                lifecycleState.getFailedStep(), index, policy);
            return;
        }

        if (lifecycleState.isAutoRetryableError() != null && lifecycleState.isAutoRetryableError()) {
            int currentRetryAttempt = lifecycleState.getFailedStepRetryCount() == null ? 1 : 1 + lifecycleState.getFailedStepRetryCount();
            logger.info("policy [{}] for index [{}] on an error step due to a transient error, moving back to the failed " +
                "step [{}] for execution. retry attempt [{}]", policy, index, lifecycleState.getFailedStep(), currentRetryAttempt);
            // we can afford to drop these requests if they timeout as on the next {@link
            // IndexLifecycleRunner#runPeriodicStep} run the policy will still be in the ERROR step, as we haven't been able
            // to move it back into the failed step, so we'll try again
            clusterService.submitStateUpdateTask(
                String.format(Locale.ROOT, "ilm-retry-failed-step {policy [%s], index [%s], failedStep [%s]}", policy, index,
                    failedStep.getKey()), new ClusterStateUpdateTask(TimeValue.MAX_VALUE) {

                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        return IndexLifecycleTransition.moveClusterStateToPreviouslyFailedStep(currentState, index,
                            nowSupplier, stepRegistry, true);
                    }

                    @Override
                    public void onFailure(String source, Exception e) {
                        logger.error(new ParameterizedMessage("retry execution of step [{}] for index [{}] failed",
                            failedStep.getKey().getName(), index), e);
                    }

                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        if (oldState.equals(newState) == false) {
                            IndexMetadata newIndexMeta = newState.metadata().index(index);
                            Step indexMetaCurrentStep = getCurrentStep(stepRegistry, policy, newIndexMeta);
                            StepKey stepKey = indexMetaCurrentStep.getKey();
                            if (stepKey != null && stepKey != TerminalPolicyStep.KEY && newIndexMeta != null) {
                                logger.trace("policy [{}] for index [{}] was moved back on the failed step for as part of an automatic " +
                                    "retry. Attempting to execute the failed step [{}] if it's an async action", policy, index, stepKey);
                                maybeRunAsyncAction(newState, newIndexMeta, policy, stepKey);
                            }
                        }
                    }
                });
        } else {
            logger.debug("policy [{}] for index [{}] on an error step after a terminal error, skipping execution", policy, index);
        }
    }

    /**
     * If the current step (matching the expected step key) is an asynchronous action step, run it
     */
    void maybeRunAsyncAction(ClusterState currentState, IndexMetadata indexMetadata, String policy, StepKey expectedStepKey) {
        String index = indexMetadata.getIndex().getName();
        LifecycleExecutionState lifecycleState = LifecycleExecutionState.fromIndexMetadata(indexMetadata);
        final Step currentStep;
        try {
            currentStep = getCurrentStep(stepRegistry, policy, indexMetadata);
        } catch (Exception e) {
            markPolicyRetrievalError(policy, indexMetadata.getIndex(), lifecycleState, e);
            return;
        }
        if (currentStep == null) {
            Step.StepKey currentStepKey = LifecycleExecutionState.getCurrentStepKey(lifecycleState);
            if (TerminalPolicyStep.KEY.equals(currentStepKey)) {
                // This index is a leftover from before we halted execution on the final phase
                // instead of going to the completed phase, so it's okay to ignore this index
                // for now
                return;
            }
            logger.warn("current step [{}] for index [{}] with policy [{}] is not recognized",
                currentStepKey, index, policy);
            return;
        }

        logger.trace("[{}] maybe running async action step ({}) with current step {}",
            index, currentStep.getClass().getSimpleName(), currentStep.getKey());
        if (currentStep.getKey().equals(expectedStepKey) == false) {
            throw new IllegalStateException("expected index [" + indexMetadata.getIndex().getName() + "] with policy [" + policy +
                "] to have current step consistent with provided step key (" + expectedStepKey + ") but it was " + currentStep.getKey());
        }
        if (currentStep instanceof AsyncActionStep) {
            logger.debug("[{}] running policy with async action step [{}]", index, currentStep.getKey());
            ((AsyncActionStep) currentStep).performAction(indexMetadata, currentState,
                new ClusterStateObserver(clusterService, null, logger, threadPool.getThreadContext()), new ActionListener<Void>() {

                    @Override
                    public void onResponse(Void unused) {
                        logger.trace("cs-change-async-action-callback, [{}], current-step: {}", index, currentStep.getKey());
                        if (((AsyncActionStep) currentStep).indexSurvives()) {
                            moveToStep(indexMetadata.getIndex(), policy, currentStep.getKey(), currentStep.getNextStepKey());
                        } else {
                            // Delete needs special handling, because after this step we
                            // will no longer have access to any information about the
                            // index since it will be... deleted.
                            registerDeleteOperation(indexMetadata);
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        moveToErrorStep(indexMetadata.getIndex(), policy, currentStep.getKey(), e);
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
    void runPolicyAfterStateChange(String policy, IndexMetadata indexMetadata) {
        String index = indexMetadata.getIndex().getName();
        LifecycleExecutionState lifecycleState = LifecycleExecutionState.fromIndexMetadata(indexMetadata);
        final Step currentStep;
        try {
            currentStep = getCurrentStep(stepRegistry, policy, indexMetadata);
        } catch (Exception e) {
            markPolicyRetrievalError(policy, indexMetadata.getIndex(), lifecycleState, e);
            return;
        }
        if (currentStep == null) {
            if (stepRegistry.policyExists(policy) == false) {
                markPolicyDoesNotExist(policy, indexMetadata.getIndex(), lifecycleState);
                return;
            } else {
                Step.StepKey currentStepKey = LifecycleExecutionState.getCurrentStepKey(lifecycleState);
                if (TerminalPolicyStep.KEY.equals(currentStepKey)) {
                    // This index is a leftover from before we halted execution on the final phase
                    // instead of going to the completed phase, so it's okay to ignore this index
                    // for now
                    return;
                }
                logger.error("current step [{}] for index [{}] with policy [{}] is not recognized",
                    currentStepKey, index, policy);
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
            if (currentStep.getNextStepKey() == null) {
                logger.debug("[{}] stopping in the current phase ({}) as there are no more steps in the policy",
                    index, currentStep.getKey().getPhase());
                return;
            }
            // Only proceed to the next step if enough time has elapsed to go into the next phase
            if (isReadyToTransitionToThisPhase(policy, indexMetadata, currentStep.getNextStepKey().getPhase())) {
                moveToStep(indexMetadata.getIndex(), policy, currentStep.getKey(), currentStep.getNextStepKey());
            }
        } else if (currentStep instanceof ClusterStateActionStep || currentStep instanceof ClusterStateWaitStep) {
            logger.debug("[{}] running policy with current-step [{}]", indexMetadata.getIndex().getName(), currentStep.getKey());
            clusterService.submitStateUpdateTask(String.format(Locale.ROOT, "ilm-execute-cluster-state-steps [%s]", currentStep),
                new ExecuteStepsUpdateTask(policy, indexMetadata.getIndex(), currentStep, stepRegistry, this, nowSupplier));
        } else {
            logger.trace("[{}] ignoring step execution from cluster state change event [{}]", index, currentStep.getKey());
        }
    }

    /**
     * Move the index to the given {@code newStepKey}, always checks to ensure that the index's
     * current step matches the {@code currentStepKey} prior to changing the state.
     */
    private void moveToStep(Index index, String policy, Step.StepKey currentStepKey, Step.StepKey newStepKey) {
        logger.debug("[{}] moving to step [{}] {} -> {}", index.getName(), policy, currentStepKey, newStepKey);
        clusterService.submitStateUpdateTask(
            String.format(Locale.ROOT, "ilm-move-to-step {policy [%s], index [%s], currentStep [%s], nextStep [%s]}", policy,
                index.getName(), currentStepKey, newStepKey),
            new MoveToNextStepUpdateTask(index, policy, currentStepKey, newStepKey, nowSupplier, stepRegistry, clusterState ->
            {
                IndexMetadata indexMetadata = clusterState.metadata().index(index);
                registerSuccessfulOperation(indexMetadata);
                if (newStepKey != null && newStepKey != TerminalPolicyStep.KEY && indexMetadata != null) {
                    maybeRunAsyncAction(clusterState, indexMetadata, policy, newStepKey);
                }
            }));
    }

    /**
     * Move the index to the ERROR step.
     */
    private void moveToErrorStep(Index index, String policy, Step.StepKey currentStepKey, Exception e) {
        logger.error(new ParameterizedMessage("policy [{}] for index [{}] failed on step [{}]. Moving to ERROR step",
            policy, index.getName(), currentStepKey), e);
        clusterService.submitStateUpdateTask(
            String.format(Locale.ROOT, "ilm-move-to-error-step {policy [%s], index [%s], currentStep [%s]}", policy, index.getName(),
                currentStepKey),
            new MoveToErrorStepUpdateTask(index, policy, currentStepKey, e, nowSupplier, stepRegistry::getStep, clusterState -> {
                IndexMetadata indexMetadata = clusterState.metadata().index(index);
                registerFailedOperation(indexMetadata, e);
            }));
    }

    /**
     * Set step info for the given index inside of its {@link LifecycleExecutionState} without
     * changing other execution state.
     */
    private void setStepInfo(Index index, String policy, @Nullable Step.StepKey currentStepKey, ToXContentObject stepInfo) {
        clusterService.submitStateUpdateTask(
            String.format(Locale.ROOT, "ilm-set-step-info {policy [%s], index [%s], currentStep [%s]}", policy, index.getName(),
                currentStepKey),
            new SetStepInfoUpdateTask(index, policy, currentStepKey, stepInfo));
    }

    /**
     * Mark the index with step info explaining that the policy doesn't exist.
     */
    private void markPolicyDoesNotExist(String policyName, Index index, LifecycleExecutionState executionState) {
        markPolicyRetrievalError(policyName, index, executionState,
            new IllegalArgumentException("policy [" + policyName + "] does not exist"));
    }

    /**
     * Mark the index with step info for a given error encountered while retrieving policy
     * information. This is opposed to lifecycle execution errors, which would cause a transition to
     * the ERROR step, however, the policy may be unparseable in which case there is no way to move
     * to the ERROR step, so this is the best effort at capturing the error retrieving the policy.
     */
    private void markPolicyRetrievalError(String policyName, Index index, LifecycleExecutionState executionState, Exception e) {
        logger.debug(
            new ParameterizedMessage("unable to retrieve policy [{}] for index [{}], recording this in step_info for this index",
                policyName, index.getName()), e);
        setStepInfo(index, policyName, LifecycleExecutionState.getCurrentStepKey(executionState),
            new SetStepInfoUpdateTask.ExceptionWrapper(e));
    }

    /**
     * For the given index metadata, register (index a document) that the index has transitioned
     * successfully into this new state using the {@link ILMHistoryStore}
     */
    void registerSuccessfulOperation(IndexMetadata indexMetadata) {
        if (indexMetadata == null) {
            // This index may have been deleted and has no metadata, so ignore it
            return;
        }
        Long origination = calculateOriginationMillis(indexMetadata);
        ilmHistoryStore.putAsync(
            ILMHistoryItem.success(indexMetadata.getIndex().getName(),
                LifecycleSettings.LIFECYCLE_NAME_SETTING.get(indexMetadata.getSettings()),
                nowSupplier.getAsLong(),
                origination == null ? null : (nowSupplier.getAsLong() - origination),
                LifecycleExecutionState.fromIndexMetadata(indexMetadata)));
    }

    /**
     * For the given index metadata, register (index a document) that the index
     * has been deleted by ILM using the {@link ILMHistoryStore}
     */
    void registerDeleteOperation(IndexMetadata metadataBeforeDeletion) {
        if (metadataBeforeDeletion == null) {
            throw new IllegalStateException("cannot register deletion of an index that did not previously exist");
        }
        Long origination = calculateOriginationMillis(metadataBeforeDeletion);
        ilmHistoryStore.putAsync(
            ILMHistoryItem.success(metadataBeforeDeletion.getIndex().getName(),
                LifecycleSettings.LIFECYCLE_NAME_SETTING.get(metadataBeforeDeletion.getSettings()),
                nowSupplier.getAsLong(),
                origination == null ? null : (nowSupplier.getAsLong() - origination),
                LifecycleExecutionState.builder(LifecycleExecutionState.fromIndexMetadata(metadataBeforeDeletion))
                    // Register that the delete phase is now "complete"
                    .setStep(PhaseCompleteStep.NAME)
                    .build()));
    }

    /**
     * For the given index metadata, register (index a document) that the index has transitioned
     * into the ERROR state using the {@link ILMHistoryStore}
     */
    void registerFailedOperation(IndexMetadata indexMetadata, Exception failure) {
        if (indexMetadata == null) {
            // This index may have been deleted and has no metadata, so ignore it
            return;
        }
        Long origination = calculateOriginationMillis(indexMetadata);
        ilmHistoryStore.putAsync(
            ILMHistoryItem.failure(indexMetadata.getIndex().getName(),
                LifecycleSettings.LIFECYCLE_NAME_SETTING.get(indexMetadata.getSettings()),
                nowSupplier.getAsLong(),
                origination == null ? null : (nowSupplier.getAsLong() - origination),
                LifecycleExecutionState.fromIndexMetadata(indexMetadata),
                failure));
    }
}
