/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.index.Index;
import org.elasticsearch.xpack.core.ilm.ClusterStateActionStep;
import org.elasticsearch.xpack.core.ilm.ClusterStateWaitStep;
import org.elasticsearch.xpack.core.ilm.ErrorStep;
import org.elasticsearch.xpack.core.ilm.LifecycleExecutionState;
import org.elasticsearch.xpack.core.ilm.Step;
import org.elasticsearch.xpack.core.ilm.TerminalPolicyStep;

import java.io.IOException;
import java.util.function.LongSupplier;

public class ExecuteStepsUpdateTask extends ClusterStateUpdateTask {
    private static final Logger logger = LogManager.getLogger(ExecuteStepsUpdateTask.class);
    private final String policy;
    private final Index index;
    private final Step startStep;
    private final PolicyStepsRegistry policyStepsRegistry;
    private final IndexLifecycleRunner lifecycleRunner;
    private final LongSupplier nowSupplier;
    private Step.StepKey nextStepKey = null;
    private Exception failure = null;

    public ExecuteStepsUpdateTask(String policy, Index index, Step startStep, PolicyStepsRegistry policyStepsRegistry,
                                  IndexLifecycleRunner lifecycleRunner, LongSupplier nowSupplier) {
        this.policy = policy;
        this.index = index;
        this.startStep = startStep;
        this.policyStepsRegistry = policyStepsRegistry;
        this.nowSupplier = nowSupplier;
        this.lifecycleRunner = lifecycleRunner;
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

    Step.StepKey getNextStepKey() {
        return nextStepKey;
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
    public ClusterState execute(final ClusterState currentState) throws IOException {
        Step currentStep = startStep;
        IndexMetadata indexMetadata = currentState.metadata().index(index);
        if (indexMetadata == null) {
            logger.debug("lifecycle for index [{}] executed but index no longer exists", index.getName());
            // This index doesn't exist any more, there's nothing to execute currently
            return currentState;
        }
        Step registeredCurrentStep = IndexLifecycleRunner.getCurrentStep(policyStepsRegistry, policy, indexMetadata);
        if (currentStep.equals(registeredCurrentStep)) {
            ClusterState state = currentState;
            // We can do cluster state steps all together until we
            // either get to a step that isn't a cluster state step or a
            // cluster state wait step returns not completed
            while (currentStep instanceof ClusterStateActionStep || currentStep instanceof ClusterStateWaitStep) {
                if (currentStep instanceof ClusterStateActionStep) {
                    // cluster state action step so do the action and
                    // move the cluster state to the next step
                    logger.trace("[{}] performing cluster state action ({}) [{}]",
                        index.getName(), currentStep.getClass().getSimpleName(), currentStep.getKey());
                    try {
                        state = ((ClusterStateActionStep) currentStep).performAction(index, state);
                    } catch (Exception exception) {
                        return moveToErrorStep(state, currentStep.getKey(), exception);
                    }
                    // set here to make sure that the clusterProcessed knows to execute the
                    // correct step if it an async action
                    nextStepKey = currentStep.getNextStepKey();
                    if (nextStepKey == null) {
                        return state;
                    } else {
                        logger.trace("[{}] moving cluster state to next step [{}]", index.getName(), nextStepKey);
                        state = IndexLifecycleTransition.moveClusterStateToStep(index, state, nextStepKey, nowSupplier,
                            policyStepsRegistry, false);
                    }
                } else {
                    // set here to make sure that the clusterProcessed knows to execute the
                    // correct step if it an async action
                    nextStepKey = currentStep.getNextStepKey();
                    // cluster state wait step so evaluate the
                    // condition, if the condition is met move to the
                    // next step, if its not met return the current
                    // cluster state so it can be applied and we will
                    // wait for the next trigger to evaluate the
                    // condition again
                    logger.trace("[{}] waiting for cluster state step condition ({}) [{}], next: [{}]",
                        index.getName(), currentStep.getClass().getSimpleName(), currentStep.getKey(), nextStepKey);
                    ClusterStateWaitStep.Result result;
                    try {
                        result = ((ClusterStateWaitStep) currentStep).isConditionMet(index, state);
                    } catch (Exception exception) {
                        return moveToErrorStep(state, currentStep.getKey(), exception);
                    }
                    if (result.isComplete()) {
                        logger.trace("[{}] cluster state step condition met successfully ({}) [{}], moving to next step {}",
                            index.getName(), currentStep.getClass().getSimpleName(), currentStep.getKey(), nextStepKey);
                        if (nextStepKey == null) {
                            return state;
                        } else {
                            state = IndexLifecycleTransition.moveClusterStateToStep(index, state,
                                nextStepKey, nowSupplier, policyStepsRegistry,false);
                        }
                    } else {
                        final ToXContentObject stepInfo = result.getInfomationContext();
                        if (logger.isTraceEnabled()) {
                            logger.trace("[{}] condition not met ({}) [{}], returning existing state (info: {})",
                                index.getName(), currentStep.getClass().getSimpleName(), currentStep.getKey(),
                                stepInfo == null ? "null" : Strings.toString(stepInfo));
                        }
                        // We may have executed a step and set "nextStepKey" to
                        // a value, but in this case, since the condition was
                        // not met, we can't advance any way, so don't attempt
                        // to run the current step
                        nextStepKey = null;
                        if (stepInfo == null) {
                            return state;
                        } else {
                            return IndexLifecycleTransition.addStepInfoToClusterState(index, state, stepInfo);
                        }
                    }
                }
                // There are actions we need to take in the event a phase
                // transition happens, so even if we would continue in the while
                // loop, if we are about to go into a new phase, return so that
                // other processing can occur
                if (currentStep.getKey().getPhase().equals(currentStep.getNextStepKey().getPhase()) == false) {
                    return state;
                }
                currentStep = policyStepsRegistry.getStep(indexMetadata, currentStep.getNextStepKey());
            }
            return state;
        } else {
            // either we are no longer the master or the step is now
            // not the same as when we submitted the update task. In
            // either case we don't want to do anything now
            return currentState;
        }
    }

    @Override
    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
        if (oldState.equals(newState) == false) {
            IndexMetadata indexMetadata = newState.metadata().index(index);
            if (indexMetadata != null) {

                LifecycleExecutionState exState = LifecycleExecutionState.fromIndexMetadata(indexMetadata);
                if (ErrorStep.NAME.equals(exState.getStep()) && this.failure != null) {
                    lifecycleRunner.registerFailedOperation(indexMetadata, failure);
                } else {
                    lifecycleRunner.registerSuccessfulOperation(indexMetadata);
                }

                if (nextStepKey != null && nextStepKey != TerminalPolicyStep.KEY) {
                    logger.trace("[{}] step sequence starting with {} has completed, running next step {} if it is an async action",
                        index.getName(), startStep.getKey(), nextStepKey);
                    // After the cluster state has been processed and we have moved
                    // to a new step, we need to conditionally execute the step iff
                    // it is an `AsyncAction` so that it is executed exactly once.
                    lifecycleRunner.maybeRunAsyncAction(newState, indexMetadata, policy, nextStepKey);
                }
            }
        }
    }

    @Override
    public void onFailure(String source, Exception e) {
        throw new ElasticsearchException(
                "policy [" + policy + "] for index [" + index.getName() + "] failed on step [" + startStep.getKey() + "].", e);
    }

    private ClusterState moveToErrorStep(final ClusterState state, Step.StepKey currentStepKey, Exception cause) throws IOException {
        this.failure = cause;
        logger.error("policy [{}] for index [{}] failed on cluster state step [{}]. Moving to ERROR step", policy, index.getName(),
            currentStepKey);
        return IndexLifecycleTransition.moveClusterStateToErrorStep(index, state, cause, nowSupplier, policyStepsRegistry::getStep);
    }
}
