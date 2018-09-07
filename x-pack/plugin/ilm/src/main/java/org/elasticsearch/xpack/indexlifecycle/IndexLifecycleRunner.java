/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.ClusterState;
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
import org.elasticsearch.xpack.core.indexlifecycle.AsyncActionStep;
import org.elasticsearch.xpack.core.indexlifecycle.AsyncWaitStep;
import org.elasticsearch.xpack.core.indexlifecycle.ClusterStateActionStep;
import org.elasticsearch.xpack.core.indexlifecycle.ClusterStateWaitStep;
import org.elasticsearch.xpack.core.indexlifecycle.ErrorStep;
import org.elasticsearch.xpack.core.indexlifecycle.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.indexlifecycle.LifecyclePolicy;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleSettings;
import org.elasticsearch.xpack.core.indexlifecycle.Phase;
import org.elasticsearch.xpack.core.indexlifecycle.PhaseCompleteStep;
import org.elasticsearch.xpack.core.indexlifecycle.RolloverAction;
import org.elasticsearch.xpack.core.indexlifecycle.Step;
import org.elasticsearch.xpack.core.indexlifecycle.Step.StepKey;
import org.elasticsearch.xpack.core.indexlifecycle.TerminalPolicyStep;

import java.io.IOException;
import java.util.List;
import java.util.function.LongSupplier;

public class IndexLifecycleRunner {
    private static final Logger logger = LogManager.getLogger(IndexLifecycleRunner.class);
    private PolicyStepsRegistry stepRegistry;
    private ClusterService clusterService;
    private LongSupplier nowSupplier;

    public IndexLifecycleRunner(PolicyStepsRegistry stepRegistry, ClusterService clusterService, LongSupplier nowSupplier) {
        this.stepRegistry = stepRegistry;
        this.clusterService = clusterService;
        this.nowSupplier = nowSupplier;
    }

    /**
     * Return true or false depending on whether the index is ready to be in {@code phase}
     */
    boolean isReadyToTransitionToThisPhase(final String policy, final IndexMetaData indexMetaData, final String phase) {
        final Settings indexSettings = indexMetaData.getSettings();
        if (indexSettings.hasValue(LifecycleSettings.LIFECYCLE_INDEX_CREATION_DATE) == false) {
            logger.trace("no index creation date has been set yet");
            return true;
        }
        final long lifecycleDate = indexSettings.getAsLong(LifecycleSettings.LIFECYCLE_INDEX_CREATION_DATE, -1L);
        assert lifecycleDate >= 0 : "expected index to have a lifecycle date but it did not";
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

    public void runPolicy(String policy, IndexMetaData indexMetaData, ClusterState currentState,
            boolean fromClusterStateChange) {
        Settings indexSettings = indexMetaData.getSettings();
        if (LifecycleSettings.LIFECYCLE_SKIP_SETTING.get(indexSettings)) {
            logger.info("skipping policy [" + policy + "] for index [" + indexMetaData.getIndex().getName() + "]."
                + LifecycleSettings.LIFECYCLE_SKIP + "== true");
            return;
        }
        Step currentStep = getCurrentStep(stepRegistry, policy, indexMetaData.getIndex(), indexSettings);
        if (currentStep == null) {
            // This may happen in the case that there is invalid ilm-step index settings or the stepRegistry is out of
            // sync with the current cluster state
            logger.warn("current step [" + getCurrentStepKey(indexSettings) + "] for index [" + indexMetaData.getIndex().getName()
                + "] with policy [" + policy + "] is not recognized");
            return;
        }
        logger.debug("running policy with current-step [" + currentStep.getKey() + "]");
        if (currentStep instanceof TerminalPolicyStep) {
            logger.debug("policy [" + policy + "] for index [" + indexMetaData.getIndex().getName() + "] complete, skipping execution");
            return;
        } else if (currentStep instanceof ErrorStep) {
            logger.debug(
                "policy [" + policy + "] for index [" + indexMetaData.getIndex().getName() + "] on an error step, skipping execution");
            return;
        } else if (currentStep instanceof PhaseCompleteStep) {
            // Only proceed to the next step if enough time has elapsed to go into the next phase
            if (isReadyToTransitionToThisPhase(policy, indexMetaData, currentStep.getNextStepKey().getPhase())) {
                moveToStep(indexMetaData.getIndex(), policy, currentStep.getKey(), currentStep.getNextStepKey());
            }
            return;
        }

        if (currentStep instanceof ClusterStateActionStep || currentStep instanceof ClusterStateWaitStep) {
            executeClusterStateSteps(indexMetaData.getIndex(), policy, currentStep);
        } else if (currentStep instanceof AsyncWaitStep) {
            if (fromClusterStateChange == false) {
                ((AsyncWaitStep) currentStep).evaluateCondition(indexMetaData.getIndex(), new AsyncWaitStep.Listener() {

                    @Override
                    public void onResponse(boolean conditionMet, ToXContentObject stepInfo) {
                        logger.debug("cs-change-async-wait-callback. current-step:" + currentStep.getKey());
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
            }
        } else if (currentStep instanceof AsyncActionStep) {
            if (fromClusterStateChange == false) {
                ((AsyncActionStep) currentStep).performAction(indexMetaData, currentState, new AsyncActionStep.Listener() {

                    @Override
                    public void onResponse(boolean complete) {
                        logger.debug("cs-change-async-action-callback. current-step:" + currentStep.getKey());
                        if (complete && ((AsyncActionStep) currentStep).indexSurvives()) {
                            moveToStep(indexMetaData.getIndex(), policy, currentStep.getKey(), currentStep.getNextStepKey());
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        moveToErrorStep(indexMetaData.getIndex(), policy, currentStep.getKey(), e);
                    }
                });
            }
        } else {
            throw new IllegalStateException(
                    "Step with key [" + currentStep.getKey() + "] is not a recognised type: [" + currentStep.getClass().getName() + "]");
        }
    }

    private void runPolicy(IndexMetaData indexMetaData, ClusterState currentState) {
        if (indexMetaData == null) {
            // This index doesn't exist any more, there's nothing to execute
            return;
        }
        Settings indexSettings = indexMetaData.getSettings();
        String policy = LifecycleSettings.LIFECYCLE_NAME_SETTING.get(indexSettings);
        runPolicy(policy, indexMetaData, currentState, false);
    }

    private void executeClusterStateSteps(Index index, String policy, Step step) {
        assert step instanceof ClusterStateActionStep || step instanceof ClusterStateWaitStep;
        clusterService.submitStateUpdateTask("ILM", new ExecuteStepsUpdateTask(policy, index, step, stepRegistry, nowSupplier));
    }

    /**
     * Retrieves the current {@link StepKey} from the index settings. Note that
     * it is illegal for the step to be set with the phase and/or action unset,
     * or for the step to be unset with the phase and/or action set. All three
     * settings must be either present or missing.
     *
     * @param indexSettings
     *            the index settings to extract the {@link StepKey} from.
     */
    public static StepKey getCurrentStepKey(Settings indexSettings) {
        String currentPhase = LifecycleSettings.LIFECYCLE_PHASE_SETTING.get(indexSettings);
        String currentAction = LifecycleSettings.LIFECYCLE_ACTION_SETTING.get(indexSettings);
        String currentStep = LifecycleSettings.LIFECYCLE_STEP_SETTING.get(indexSettings);
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

    static Step getCurrentStep(PolicyStepsRegistry stepRegistry, String policy, Index index, Settings indexSettings) {
        StepKey currentStepKey = getCurrentStepKey(indexSettings);
        if (currentStepKey == null) {
            return stepRegistry.getFirstStep(policy);
        } else {
            return stepRegistry.getStep(index, currentStepKey);
        }
    }

    /**
     * This method is intended for handling moving to different steps from {@link TransportAction} executions.
     * For this reason, it is reasonable to throw {@link IllegalArgumentException} when state is not as expected.
     * @param indexName
     *          The index whose step is to change
     * @param currentState
     *          The current {@link ClusterState}
     * @param currentStepKey
     *          The current {@link StepKey} found for the index in the current cluster state
     * @param nextStepKey
     *          The next step to move the index into
     * @param nowSupplier
     *          The current-time supplier for updating when steps changed
     * @param stepRegistry
     *          The steps registry to check a step-key's existence in the index's current policy
     * @return The updated cluster state where the index moved to <code>nextStepKey</code>
     */
    static ClusterState moveClusterStateToStep(String indexName, ClusterState currentState, StepKey currentStepKey,
                                               StepKey nextStepKey, LongSupplier nowSupplier,
                                               PolicyStepsRegistry stepRegistry) {
        IndexMetaData idxMeta = currentState.getMetaData().index(indexName);
        Settings indexSettings = idxMeta.getSettings();
        String indexPolicySetting = LifecycleSettings.LIFECYCLE_NAME_SETTING.get(indexSettings);

        // policy could be updated in-between execution
        if (Strings.isNullOrEmpty(indexPolicySetting)) {
            throw new IllegalArgumentException("index [" + indexName + "] is not associated with an Index Lifecycle Policy");
        }

        if (currentStepKey.equals(IndexLifecycleRunner.getCurrentStepKey(indexSettings)) == false) {
            throw new IllegalArgumentException("index [" + indexName + "] is not on current step [" + currentStepKey + "]");
        }

        if (stepRegistry.stepExists(indexPolicySetting, nextStepKey) == false) {
            throw new IllegalArgumentException("step [" + nextStepKey + "] for index [" + idxMeta.getIndex().getName() +
                "] with policy [" + indexPolicySetting + "] does not exist");
        }

        return IndexLifecycleRunner.moveClusterStateToNextStep(idxMeta.getIndex(), currentState, currentStepKey, nextStepKey, nowSupplier);
    }

    static ClusterState moveClusterStateToNextStep(Index index, ClusterState clusterState, StepKey currentStep, StepKey nextStep,
            LongSupplier nowSupplier) {
        IndexMetaData idxMeta = clusterState.getMetaData().index(index);
        IndexLifecycleMetadata ilmMeta = clusterState.metaData().custom(IndexLifecycleMetadata.TYPE);
        LifecyclePolicy policy = ilmMeta.getPolicies().get(LifecycleSettings.LIFECYCLE_NAME_SETTING.get(idxMeta.getSettings()));
        Settings.Builder indexSettings = moveIndexSettingsToNextStep(policy, idxMeta.getSettings(), currentStep, nextStep, nowSupplier);
        ClusterState.Builder newClusterStateBuilder = newClusterStateWithIndexSettings(index, clusterState, indexSettings);
        return newClusterStateBuilder.build();
    }

    static ClusterState moveClusterStateToErrorStep(Index index, ClusterState clusterState, StepKey currentStep, Exception cause,
            LongSupplier nowSupplier) throws IOException {
        IndexMetaData idxMeta = clusterState.getMetaData().index(index);
        IndexLifecycleMetadata ilmMeta = clusterState.metaData().custom(IndexLifecycleMetadata.TYPE);
        LifecyclePolicy policy = ilmMeta.getPolicies().get(LifecycleSettings.LIFECYCLE_NAME_SETTING.get(idxMeta.getSettings()));
        XContentBuilder causeXContentBuilder = JsonXContent.contentBuilder();
        causeXContentBuilder.startObject();
        ElasticsearchException.generateThrowableXContent(causeXContentBuilder, ToXContent.EMPTY_PARAMS, cause);
        causeXContentBuilder.endObject();
        Settings.Builder indexSettings = moveIndexSettingsToNextStep(policy, idxMeta.getSettings(), currentStep,
                new StepKey(currentStep.getPhase(), currentStep.getAction(), ErrorStep.NAME), nowSupplier)
                        .put(LifecycleSettings.LIFECYCLE_FAILED_STEP, currentStep.getName())
                        .put(LifecycleSettings.LIFECYCLE_STEP_INFO, BytesReference.bytes(causeXContentBuilder).utf8ToString());
        ClusterState.Builder newClusterStateBuilder = newClusterStateWithIndexSettings(index, clusterState, indexSettings);
        return newClusterStateBuilder.build();
    }

    ClusterState moveClusterStateToFailedStep(ClusterState currentState, String[] indices) {
        ClusterState newState = currentState;
        for (String index : indices) {
            IndexMetaData indexMetaData = currentState.metaData().index(index);
            if (indexMetaData == null) {
                throw new IllegalArgumentException("index [" + index + "] does not exist");
            }
            StepKey currentStepKey = IndexLifecycleRunner.getCurrentStepKey(indexMetaData.getSettings());
            String failedStep = LifecycleSettings.LIFECYCLE_FAILED_STEP_SETTING.get(indexMetaData.getSettings());
            if (currentStepKey != null && ErrorStep.NAME.equals(currentStepKey.getName())
                && Strings.isNullOrEmpty(failedStep) == false) {
                StepKey nextStepKey = new StepKey(currentStepKey.getPhase(), currentStepKey.getAction(), failedStep);
                newState = moveClusterStateToStep(index, currentState, currentStepKey, nextStepKey, nowSupplier, stepRegistry);
            } else {
                throw new IllegalArgumentException("cannot retry an action for an index ["
                    + index + "] that has not encountered an error when running a Lifecycle Policy");
            }
        }
        return newState;
    }

    private static Settings.Builder moveIndexSettingsToNextStep(LifecyclePolicy policy, Settings existingSettings,
                                                                StepKey currentStep, StepKey nextStep, LongSupplier nowSupplier) {
        long nowAsMillis = nowSupplier.getAsLong();
        Settings.Builder newSettings = Settings.builder().put(existingSettings).put(LifecycleSettings.LIFECYCLE_PHASE, nextStep.getPhase())
                .put(LifecycleSettings.LIFECYCLE_ACTION, nextStep.getAction()).put(LifecycleSettings.LIFECYCLE_STEP, nextStep.getName())
                .put(LifecycleSettings.LIFECYCLE_STEP_TIME, nowAsMillis)
                // clear any step info or error-related settings from the current step
                .put(LifecycleSettings.LIFECYCLE_FAILED_STEP, (String) null)
                .put(LifecycleSettings.LIFECYCLE_STEP_INFO, (String) null);
        if (currentStep.getPhase().equals(nextStep.getPhase()) == false) {
            final String newPhaseDefinition;
            if ("new".equals(nextStep.getPhase()) || TerminalPolicyStep.KEY.equals(nextStep)) {
                newPhaseDefinition = nextStep.getPhase();
            } else {
                Phase nextPhase = policy.getPhases().get(nextStep.getPhase());
                if (nextPhase == null) {
                    newPhaseDefinition = null;
                } else {
                    newPhaseDefinition = Strings.toString(nextPhase, false, false);
                }
            }
            newSettings.put(LifecycleSettings.LIFECYCLE_PHASE_DEFINITION, newPhaseDefinition);
            newSettings.put(LifecycleSettings.LIFECYCLE_PHASE_TIME, nowAsMillis);
        }
        if (currentStep.getAction().equals(nextStep.getAction()) == false) {
            newSettings.put(LifecycleSettings.LIFECYCLE_ACTION_TIME, nowAsMillis);
        }
        return newSettings;
    }

    static ClusterState.Builder newClusterStateWithIndexSettings(Index index, ClusterState clusterState,
            Settings.Builder newSettings) {
        ClusterState.Builder newClusterStateBuilder = ClusterState.builder(clusterState);
        newClusterStateBuilder.metaData(MetaData.builder(clusterState.getMetaData())
                .put(IndexMetaData.builder(clusterState.getMetaData().index(index)).settings(newSettings)));
        return newClusterStateBuilder;
    }

    /**
     * Conditionally updates cluster state with new step info. The new cluster state is only
     * built if the step info has changed, otherwise the same old <code>clusterState</code> is
     * returned
     *
     * @param index the index to modify
     * @param clusterState the cluster state to modify
     * @param stepInfo the new step info to update
     * @return Updated cluster state with <code>stepInfo</code> if changed, otherwise the same cluster state
     *         if no changes to step info exist
     * @throws IOException if parsing step info fails
     */
    static ClusterState addStepInfoToClusterState(Index index, ClusterState clusterState, ToXContentObject stepInfo) throws IOException {
        IndexMetaData idxMeta = clusterState.getMetaData().index(index);
        final String stepInfoString;
        try (XContentBuilder infoXContentBuilder = JsonXContent.contentBuilder()) {
            stepInfo.toXContent(infoXContentBuilder, ToXContent.EMPTY_PARAMS);
            stepInfoString = BytesReference.bytes(infoXContentBuilder).utf8ToString();
        }
        if (stepInfoString.equals(LifecycleSettings.LIFECYCLE_STEP_INFO_SETTING.get(idxMeta.getSettings()))) {
            return clusterState;
        }
        Settings.Builder indexSettings = Settings.builder().put(idxMeta.getSettings())
            .put(LifecycleSettings.LIFECYCLE_STEP_INFO_SETTING.getKey(), stepInfoString);
        ClusterState.Builder newClusterStateBuilder = newClusterStateWithIndexSettings(index, clusterState, indexSettings);
        return newClusterStateBuilder.build();
    }

    private void moveToStep(Index index, String policy, StepKey currentStepKey, StepKey nextStepKey) {
        logger.debug("moveToStep[" + policy + "] [" + index.getName() + "]" + currentStepKey + " -> "
                + nextStepKey);
        clusterService.submitStateUpdateTask("ILM", new MoveToNextStepUpdateTask(index, policy, currentStepKey,
                nextStepKey, nowSupplier, newState -> runPolicy(newState.getMetaData().index(index), newState)));
    }

    private void moveToErrorStep(Index index, String policy, StepKey currentStepKey, Exception e) {
        logger.error("policy [" + policy + "] for index [" + index.getName() + "] failed on step [" + currentStepKey
                + "]. Moving to ERROR step.", e);
        clusterService.submitStateUpdateTask("ILM", new MoveToErrorStepUpdateTask(index, policy, currentStepKey, e, nowSupplier));
    }

    private void setStepInfo(Index index, String policy, StepKey currentStepKey, ToXContentObject stepInfo) {
        clusterService.submitStateUpdateTask("ILM", new SetStepInfoUpdateTask(index, policy, currentStepKey, stepInfo));
    }

    public static ClusterState setPolicyForIndexes(final String newPolicyName, final Index[] indices, ClusterState currentState,
            LifecyclePolicy newPolicy, List<String> failedIndexes, LongSupplier nowSupplier) {
        MetaData.Builder newMetadata = MetaData.builder(currentState.getMetaData());
        boolean clusterStateChanged = false;
        for (Index index : indices) {
            IndexMetaData indexMetadata = currentState.getMetaData().index(index);
            if (indexMetadata == null) {
                // Index doesn't exist so fail it
                failedIndexes.add(index.getName());
            } else {
                IndexMetaData.Builder newIdxMetadata = IndexLifecycleRunner.setPolicyForIndex(newPolicyName,
                    newPolicy, indexMetadata, nowSupplier);
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

    private static IndexMetaData.Builder setPolicyForIndex(final String newPolicyName, LifecyclePolicy newPolicy,
                                                           IndexMetaData indexMetadata, LongSupplier nowSupplier) {
        Settings idxSettings = indexMetadata.getSettings();
        StepKey currentStepKey = IndexLifecycleRunner.getCurrentStepKey(idxSettings);

        Settings.Builder newSettings = Settings.builder().put(idxSettings);
        if (currentStepKey != null) {
            // Check if current step exists in new policy and if not move to
            // next available step
            StepKey nextValidStepKey = newPolicy.getNextValidStep(currentStepKey);
            if (nextValidStepKey.equals(currentStepKey) == false) {
                newSettings = moveIndexSettingsToNextStep(newPolicy, idxSettings, currentStepKey, nextValidStepKey, nowSupplier);
            }
        }
        newSettings.put(LifecycleSettings.LIFECYCLE_NAME_SETTING.getKey(), newPolicyName);
        return IndexMetaData.builder(indexMetadata).settings(newSettings);
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

        newSettings.remove(LifecycleSettings.LIFECYCLE_NAME_SETTING.getKey());
        newSettings.remove(LifecycleSettings.LIFECYCLE_PHASE_SETTING.getKey());
        newSettings.remove(LifecycleSettings.LIFECYCLE_PHASE_TIME_SETTING.getKey());
        newSettings.remove(LifecycleSettings.LIFECYCLE_ACTION_SETTING.getKey());
        newSettings.remove(LifecycleSettings.LIFECYCLE_ACTION_TIME_SETTING.getKey());
        newSettings.remove(LifecycleSettings.LIFECYCLE_STEP_SETTING.getKey());
        newSettings.remove(LifecycleSettings.LIFECYCLE_STEP_TIME_SETTING.getKey());
        newSettings.remove(LifecycleSettings.LIFECYCLE_STEP_INFO_SETTING.getKey());
        newSettings.remove(LifecycleSettings.LIFECYCLE_FAILED_STEP_SETTING.getKey());
        newSettings.remove(LifecycleSettings.LIFECYCLE_INDEX_CREATION_DATE_SETTING.getKey());
        newSettings.remove(LifecycleSettings.LIFECYCLE_SKIP_SETTING.getKey());
        newSettings.remove(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS_SETTING.getKey());
        return IndexMetaData.builder(indexMetadata).settings(newSettings);
    }
}
