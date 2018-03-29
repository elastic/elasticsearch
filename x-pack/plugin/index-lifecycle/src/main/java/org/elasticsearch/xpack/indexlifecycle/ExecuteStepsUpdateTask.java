/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.index.Index;
import org.elasticsearch.xpack.core.indexlifecycle.ClusterStateActionStep;
import org.elasticsearch.xpack.core.indexlifecycle.ClusterStateWaitStep;
import org.elasticsearch.xpack.core.indexlifecycle.Step;

import java.util.function.BiFunction;
import java.util.function.Function;

public class ExecuteStepsUpdateTask extends ClusterStateUpdateTask {
    private static final Logger logger = ESLoggerFactory.getLogger(ExecuteStepsUpdateTask.class);
    private final Index index;
    private final Step startStep;
    private final Function<ClusterState, Step> getCurrentStepInClusterState;
    private final Function<Step.StepKey, Step> getStepFromRegistry;
    private final BiFunction<ClusterState, Step, ClusterState> moveClusterStateToNextStep;

    public ExecuteStepsUpdateTask(Index index, Step startStep, Function<ClusterState, Step> getCurrentStepInClusterState,
                                  BiFunction<ClusterState, Step, ClusterState> moveClusterStateToNextStep,
                                  Function<Step.StepKey, Step> getStepFromRegistry) {
        this.index = index;
        this.startStep = startStep;
        this.getCurrentStepInClusterState = getCurrentStepInClusterState;
        this.moveClusterStateToNextStep = moveClusterStateToNextStep;
        this.getStepFromRegistry = getStepFromRegistry;
    }


    @Override
    public ClusterState execute(ClusterState currentState) throws Exception {
        Step currentStep = startStep;
        if (currentStep.equals(getCurrentStepInClusterState.apply(currentState))) {
            // We can do cluster state steps all together until we
            // either get to a step that isn't a cluster state step or a
            // cluster state wait step returns not completed
            while (currentStep instanceof ClusterStateActionStep || currentStep instanceof ClusterStateWaitStep) {
                if (currentStep instanceof ClusterStateActionStep) {
                    // cluster state action step so do the action and
                    // move
                    // the cluster state to the next step
                    currentState = ((ClusterStateActionStep) currentStep).performAction(index, currentState);
                    currentState = moveClusterStateToNextStep.apply(currentState, currentStep);
                } else {
                    // cluster state wait step so evaluate the
                    // condition, if the condition is met move to the
                    // next step, if its not met return the current
                    // cluster state so it can be applied and we will
                    // wait for the next trigger to evaluate the
                    // condition again
                    boolean complete = ((ClusterStateWaitStep) currentStep).isConditionMet(index, currentState);
                    if (complete) {
                        currentState = moveClusterStateToNextStep.apply(currentState, currentStep);
                    } else {
                        logger.warn("condition not met, returning existing state");
                        return currentState;
                    }
                }
                currentStep = getStepFromRegistry.apply(currentStep.getNextStepKey());
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
}
