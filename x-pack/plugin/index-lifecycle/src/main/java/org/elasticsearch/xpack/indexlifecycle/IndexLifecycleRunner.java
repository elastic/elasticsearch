/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.index.Index;
import org.elasticsearch.xpack.core.indexlifecycle.AsyncActionStep;
import org.elasticsearch.xpack.core.indexlifecycle.AsyncWaitStep;
import org.elasticsearch.xpack.core.indexlifecycle.ClusterStateActionStep;
import org.elasticsearch.xpack.core.indexlifecycle.ClusterStateWaitStep;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleSettings;
import org.elasticsearch.xpack.core.indexlifecycle.Step;
import org.elasticsearch.xpack.core.indexlifecycle.Step.StepKey;

public class IndexLifecycleRunner {

    private PolicyStepsRegistry stepRegistry;
    private ClusterService clusterService;

    public IndexLifecycleRunner(PolicyStepsRegistry stepRegistry, ClusterService clusterService) {
        this.stepRegistry = stepRegistry;
        this.clusterService = clusterService;
    }

    public void runPolicy(String policy, Index index, Settings indexSettings, Cause cause) {
        Step currentStep = getCurrentStep(policy, indexSettings);
        if (currentStep instanceof ClusterStateActionStep || currentStep instanceof ClusterStateWaitStep) {
            if (cause != Cause.SCHEDULE_TRIGGER) {
                executeClusterStateSteps(index, policy, currentStep);
            }
        } else if (currentStep instanceof AsyncWaitStep) {
            if (cause != Cause.CLUSTER_STATE_CHANGE) {
                ((AsyncWaitStep) currentStep).evaluateCondition(index, new AsyncWaitStep.Listener() {
    
                    @Override
                    public void onResponse(boolean conditionMet) {
                        if (conditionMet) {
                            moveToStep(index, policy, new StepKey(currentStep.getPhase(), currentStep.getAction(), currentStep.getName()),
                                    currentStep.getNextStepKey(), Cause.CALLBACK);
                        }
                    }
    
                    @Override
                    public void onFailure(Exception e) {
                        throw new RuntimeException(e); // NORELEASE implement error handling
                    }
                    
                });
            }
        } else if (currentStep instanceof AsyncActionStep) {
            if (cause != Cause.CLUSTER_STATE_CHANGE) {
                ((AsyncActionStep) currentStep).performAction(index, new AsyncActionStep.Listener() {
    
                    @Override
                    public void onResponse(boolean complete) {
                        if (complete) {
                            moveToStep(index, policy, new StepKey(currentStep.getPhase(), currentStep.getAction(), currentStep.getName()),
                                    currentStep.getNextStepKey(), Cause.CALLBACK);
                        }
                    }
    
                    @Override
                    public void onFailure(Exception e) {
                        throw new RuntimeException(e); // NORELEASE implement error handling
                    }
                });
            }
        } else {
            throw new IllegalStateException(
                    "Step with key [" + currentStep.getName() + "] is not a recognised type: [" + currentStep.getClass().getName() + "]");
        }
    }

    private void runPolicy(Index index, ClusterState clusterState, Cause cause) {
        IndexMetaData indexMetaData = clusterState.getMetaData().index(index);
        Settings indexSettings = indexMetaData.getSettings();
        String policy = LifecycleSettings.LIFECYCLE_NAME_SETTING.get(indexSettings);
        runPolicy(policy, index, indexSettings, cause);
    }

    private void executeClusterStateSteps(Index index, String policy, Step step) {
        assert step instanceof ClusterStateActionStep || step instanceof ClusterStateWaitStep;
        clusterService.submitStateUpdateTask("ILM", new ClusterStateUpdateTask() {

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                Step currentStep = step;
                if (currentStep.equals(getCurrentStep(policy, currentState.getMetaData().index(index).getSettings()))) {
                    // We can do cluster state steps all together until we
                    // either get to a step that isn't a cluster state step or a
                    // cluster state wait step returns not completed
                    while (currentStep instanceof ClusterStateActionStep || currentStep instanceof ClusterStateWaitStep) {
                        if (currentStep instanceof ClusterStateActionStep) {
                            // cluster state action step so do the action and
                            // move
                            // the cluster state to the next step
                            currentState = ((ClusterStateActionStep) currentStep).performAction(index, currentState);
                            currentState = moveClusterStateToNextStep(index, currentState, currentStep.getNextStepKey());
                        } else {
                            // cluster state wait step so evaluate the
                            // condition, if the condition is met move to the
                            // next step, if its not met return the current
                            // cluster state so it can be applied and we will
                            // wait for the next trigger to evaluate the
                            // condition again
                            boolean complete = ((ClusterStateWaitStep) currentStep).isConditionMet(index, currentState);
                            if (complete) {
                                currentState = moveClusterStateToNextStep(index, currentState, currentStep.getNextStepKey());
                            } else {
                                return currentState;
                            }
                        }
                        currentStep = stepRegistry.getStep(policy, currentStep.getNextStepKey());
                    }
                    return currentState;
                } else {
                    // either we are no longer the master or the step is now
                    // not the same as when we submitted the update task. In
                    // either case we don't want to do anything now
                    return currentState;
                }
            }

            @Override
            public void onFailure(String source, Exception e) {
                throw new RuntimeException(e); // NORELEASE implement error handling
            }

        });
    }

    private StepKey getCurrentStepKey(Settings indexSettings) {
        String currentPhase = LifecycleSettings.LIFECYCLE_PHASE_SETTING.get(indexSettings);
        String currentAction = LifecycleSettings.LIFECYCLE_ACTION_SETTING.get(indexSettings);
        String currentStep = LifecycleSettings.LIFECYCLE_STEP_SETTING.get(indexSettings);
        if (currentStep == null) {
            assert currentPhase == null : "Current phase is not null: " + currentPhase;
            assert currentAction == null : "Current action is not null: " + currentAction;
            return null;
        } else {
            assert currentPhase != null;
            assert currentAction != null;
            return new StepKey(currentPhase, currentAction, currentStep);
        }
    }

    private Step getCurrentStep(String policy, Settings indexSettings) {
        StepKey currentStepKey = getCurrentStepKey(indexSettings);
        if (currentStepKey == null) {
            return stepRegistry.getFirstStep(policy);
        } else {
            return stepRegistry.getStep(policy, currentStepKey);
        }
    }

    private ClusterState moveClusterStateToNextStep(Index index, ClusterState clusterState, StepKey nextStep) {
        ClusterState.Builder newClusterStateBuilder = ClusterState.builder(clusterState);
        IndexMetaData idxMeta = clusterState.getMetaData().index(index);
        Builder indexSettings = Settings.builder().put(idxMeta.getSettings()).put(LifecycleSettings.LIFECYCLE_PHASE, nextStep.getPhase())
                .put(LifecycleSettings.LIFECYCLE_ACTION, nextStep.getAction()).put(LifecycleSettings.LIFECYCLE_STEP, nextStep.getName());
        if (indexSettings.keys().contains(LifecycleSettings.LIFECYCLE_INDEX_CREATION_DATE) == false) {
            indexSettings.put(LifecycleSettings.LIFECYCLE_INDEX_CREATION_DATE_SETTING.getKey(), idxMeta.getCreationDate());
        }
        newClusterStateBuilder.metaData(MetaData.builder(clusterState.getMetaData()).put(IndexMetaData
                .builder(clusterState.getMetaData().index(index))
                .settings(indexSettings)));
        return newClusterStateBuilder.build();
    }

    private void moveToStep(Index index, String policy, StepKey currentStepKey, StepKey nextStepKey, Cause cause) {
        clusterService.submitStateUpdateTask("ILM", new ClusterStateUpdateTask() {

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                Settings indexSettings = currentState.getMetaData().index(index).getSettings();
                if (policy.equals(LifecycleSettings.LIFECYCLE_NAME_SETTING.get(indexSettings))
                        && currentStepKey.equals(getCurrentStepKey(indexSettings))) {
                    return moveClusterStateToNextStep(index, currentState, nextStepKey);
                } else {
                    // either the policy has changed or the step is now
                    // not the same as when we submitted the update task. In
                    // either case we don't want to do anything now
                    return currentState;
                }
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                // if the new cluster state is different from the old one then
                // we moved to the new step in the execute method so we should
                // execute the next step
                if (oldState != newState) {
                    runPolicy(index, newState, cause);
                }
            }

            @Override
            public void onFailure(String source, Exception e) {
                throw new RuntimeException(e); // NORELEASE implement error handling
            }
        });
    }

    public static enum Cause {
        CLUSTER_STATE_CHANGE, SCHEDULE_TRIGGER, CALLBACK;
    }
}
