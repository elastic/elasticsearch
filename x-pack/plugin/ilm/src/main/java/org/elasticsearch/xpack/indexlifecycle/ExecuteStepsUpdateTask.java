/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.indexlifecycle;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.index.Index;
import org.elasticsearch.xpack.core.indexlifecycle.ClusterStateActionStep;
import org.elasticsearch.xpack.core.indexlifecycle.ClusterStateWaitStep;
import org.elasticsearch.xpack.core.indexlifecycle.Step;

import java.io.IOException;
import java.util.function.LongSupplier;

public class ExecuteStepsUpdateTask extends ClusterStateUpdateTask {
    private static final Logger logger = ESLoggerFactory.getLogger(ExecuteStepsUpdateTask.class);
    private final String policy;
    private final Index index;
    private final Step startStep;
    private final PolicyStepsRegistry policyStepsRegistry;
    private LongSupplier nowSupplier;

    public ExecuteStepsUpdateTask(String policy, Index index, Step startStep, PolicyStepsRegistry policyStepsRegistry,
            LongSupplier nowSupplier) {
        this.policy = policy;
        this.index = index;
        this.startStep = startStep;
        this.policyStepsRegistry = policyStepsRegistry;
        this.nowSupplier = nowSupplier;
    }

    String getPolicy() {
        return policy;
    }

    Index getIndex() {
        return index;
    }

    Step getStartStep() {
        return startStep;
    }


    /**
     * {@link Step}s for the current index and policy are executed in succession until the next step to be
     * executed is not a {@link ClusterStateActionStep}, or not a {@link ClusterStateWaitStep}, or does not
     * belong to the same phase as the executed step. All other types of steps are executed outside of this
     * {@link ClusterStateUpdateTask}, so they are of no concern here.
     *
     * @param currentState The current state to execute the <code>startStep</code> with
     * @return the new cluster state after cluster-state operations and step transitions are applied
     * @throws IOException if any exceptions occur
     */
    @Override
    public ClusterState execute(ClusterState currentState) throws IOException {
        Step currentStep = startStep;
        Step registeredCurrentStep = IndexLifecycleRunner.getCurrentStep(policyStepsRegistry, policy,
            currentState.metaData().index(index).getSettings());
        if (currentStep.equals(registeredCurrentStep)) {
            // We can do cluster state steps all together until we
            // either get to a step that isn't a cluster state step or a
            // cluster state wait step returns not completed
            while (currentStep instanceof ClusterStateActionStep || currentStep instanceof ClusterStateWaitStep) {
                if (currentStep instanceof ClusterStateActionStep) {
                    // cluster state action step so do the action and
                    // move
                    // the cluster state to the next step
                    currentState = ((ClusterStateActionStep) currentStep).performAction(index, currentState);
                    if (currentStep.getNextStepKey() == null) {
                        return currentState;
                    }
                    currentState = IndexLifecycleRunner.moveClusterStateToNextStep(index, currentState, currentStep.getKey(),
                            currentStep.getNextStepKey(), nowSupplier);
                } else {
                    // cluster state wait step so evaluate the
                    // condition, if the condition is met move to the
                    // next step, if its not met return the current
                    // cluster state so it can be applied and we will
                    // wait for the next trigger to evaluate the
                    // condition again
                    ClusterStateWaitStep.Result result = ((ClusterStateWaitStep) currentStep).isConditionMet(index, currentState);
                    if (result.isComplete()) {
                        if (currentStep.getNextStepKey() == null) {
                            return currentState;
                        }
                        currentState = IndexLifecycleRunner.moveClusterStateToNextStep(index, currentState, currentStep.getKey(),
                                currentStep.getNextStepKey(), nowSupplier);
                    } else {
                        logger.debug("condition not met, returning existing state");
                        ToXContentObject stepInfo = result.getInfomationContext();
                        if (stepInfo == null) {
                            return currentState;
                        } else {
                            return IndexLifecycleRunner.addStepInfoToClusterState(index, currentState, stepInfo);
                        }
                    }
                }
                if (currentStep.getKey().getPhase().equals(currentStep.getNextStepKey().getPhase()) == false) {
                    return currentState;
                }
                currentStep = policyStepsRegistry.getStep(policy, currentStep.getNextStepKey());
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
        throw new ElasticsearchException(
                "policy [" + policy + "] for index [" + index.getName() + "] failed on step [" + startStep.getKey() + "].", e);
    }
}
