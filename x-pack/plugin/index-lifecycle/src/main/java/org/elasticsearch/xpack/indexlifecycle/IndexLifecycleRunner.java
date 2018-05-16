/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.Index;
import org.elasticsearch.xpack.core.indexlifecycle.AsyncActionStep;
import org.elasticsearch.xpack.core.indexlifecycle.AsyncWaitStep;
import org.elasticsearch.xpack.core.indexlifecycle.ClusterStateWaitStep;
import org.elasticsearch.xpack.core.indexlifecycle.ErrorStep;
import org.elasticsearch.xpack.core.indexlifecycle.InitializePolicyContextStep;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleSettings;
import org.elasticsearch.xpack.core.indexlifecycle.Step;
import org.elasticsearch.xpack.core.indexlifecycle.Step.StepKey;
import org.elasticsearch.xpack.core.indexlifecycle.TerminalPolicyStep;

import java.io.IOException;
import java.util.function.LongSupplier;

public class IndexLifecycleRunner {
    private static final Logger logger = ESLoggerFactory.getLogger(IndexLifecycleRunner.class);
    private PolicyStepsRegistry stepRegistry;
    private ClusterService clusterService;
    private LongSupplier nowSupplier;

    public IndexLifecycleRunner(PolicyStepsRegistry stepRegistry, ClusterService clusterService, LongSupplier nowSupplier) {
        this.stepRegistry = stepRegistry;
        this.clusterService = clusterService;
        this.nowSupplier = nowSupplier;
    }

    public void runPolicy(String policy, IndexMetaData indexMetaData, ClusterState currentState,
            boolean fromClusterStateChange) {
        Settings indexSettings = indexMetaData.getSettings();
        Step currentStep = getCurrentStep(stepRegistry, policy, indexSettings);
        logger.debug("running policy with current-step[" + currentStep.getKey() + "]");
        if (currentStep instanceof TerminalPolicyStep) {
            logger.debug("policy [" + policy + "] for index [" + indexMetaData.getIndex().getName() + "] complete, skipping execution");
        } else if (currentStep instanceof ErrorStep) {
            logger.debug(
                    "policy [" + policy + "] for index [" + indexMetaData.getIndex().getName() + "] on an error step, skipping execution");
        } else if (currentStep instanceof InitializePolicyContextStep || currentStep instanceof ClusterStateWaitStep) {
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
        Settings indexSettings = indexMetaData.getSettings();
        String policy = LifecycleSettings.LIFECYCLE_NAME_SETTING.get(indexSettings);
        runPolicy(policy, indexMetaData, currentState, false);
    }

    private void executeClusterStateSteps(Index index, String policy, Step step) {
        assert step instanceof InitializePolicyContextStep || step instanceof ClusterStateWaitStep;
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
    static StepKey getCurrentStepKey(Settings indexSettings) {
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

    static Step getCurrentStep(PolicyStepsRegistry stepRegistry, String policy, Settings indexSettings) {
        StepKey currentStepKey = getCurrentStepKey(indexSettings);
        if (currentStepKey == null) {
            return stepRegistry.getFirstStep(policy);
        } else {
            return stepRegistry.getStep(policy, currentStepKey);
        }
    }

    static ClusterState moveClusterStateToNextStep(Index index, ClusterState clusterState, StepKey currentStep, StepKey nextStep,
            LongSupplier nowSupplier) {
        IndexMetaData idxMeta = clusterState.getMetaData().index(index);
        Settings.Builder indexSettings = moveIndexSettingsToNextStep(idxMeta.getSettings(), currentStep, nextStep, nowSupplier);
        ClusterState.Builder newClusterStateBuilder = newClusterStateWithIndexSettings(index, clusterState, indexSettings);
        return newClusterStateBuilder.build();
    }

    static ClusterState moveClusterStateToErrorStep(Index index, ClusterState clusterState, StepKey currentStep, Exception cause,
            LongSupplier nowSupplier) throws IOException {
        IndexMetaData idxMeta = clusterState.getMetaData().index(index);
        XContentBuilder causeXContentBuilder = JsonXContent.contentBuilder();
        causeXContentBuilder.startObject();
        ElasticsearchException.generateFailureXContent(causeXContentBuilder, ToXContent.EMPTY_PARAMS, cause, false);
        causeXContentBuilder.endObject();
        Settings.Builder indexSettings = moveIndexSettingsToNextStep(idxMeta.getSettings(), currentStep,
                new StepKey(currentStep.getPhase(), currentStep.getAction(), ErrorStep.NAME), nowSupplier)
                        .put(LifecycleSettings.LIFECYCLE_FAILED_STEP, currentStep.getName())
                        .put(LifecycleSettings.LIFECYCLE_STEP_INFO, BytesReference.bytes(causeXContentBuilder).utf8ToString());
        ClusterState.Builder newClusterStateBuilder = newClusterStateWithIndexSettings(index, clusterState, indexSettings);
        return newClusterStateBuilder.build();
    }

    private static Settings.Builder moveIndexSettingsToNextStep(Settings existingSettings, StepKey currentStep, StepKey nextStep,
            LongSupplier nowSupplier) {
        long nowAsMillis = nowSupplier.getAsLong();
        Settings.Builder newSettings = Settings.builder().put(existingSettings).put(LifecycleSettings.LIFECYCLE_PHASE, nextStep.getPhase())
                .put(LifecycleSettings.LIFECYCLE_ACTION, nextStep.getAction()).put(LifecycleSettings.LIFECYCLE_STEP, nextStep.getName())
                .put(LifecycleSettings.LIFECYCLE_STEP_TIME, nowAsMillis)
                // clear any step info from the current step
                .put(LifecycleSettings.LIFECYCLE_STEP_INFO, (String) null);
        if (currentStep.getPhase().equals(nextStep.getPhase()) == false) {
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

    static ClusterState addStepInfoToClusterState(Index index, ClusterState clusterState, ToXContentObject stepInfo) throws IOException {
        IndexMetaData idxMeta = clusterState.getMetaData().index(index);
        XContentBuilder infoXContentBuilder = JsonXContent.contentBuilder();
        stepInfo.toXContent(infoXContentBuilder, ToXContent.EMPTY_PARAMS);
        String stepInfoString = BytesReference.bytes(infoXContentBuilder).utf8ToString();
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
        logger.debug("policy [" + policy + "] for index [" + index.getName() + "] failed on step [" + currentStepKey
                + "]. Moving to ERROR step.", e);
        clusterService.submitStateUpdateTask("ILM", new MoveToErrorStepUpdateTask(index, policy, currentStepKey, e, nowSupplier));
    }

    private void setStepInfo(Index index, String policy, StepKey currentStepKey, ToXContentObject stepInfo) {
        clusterService.submitStateUpdateTask("ILM", new SetStepInfoUpdateTask(index, policy, currentStepKey, stepInfo));
    }
}
