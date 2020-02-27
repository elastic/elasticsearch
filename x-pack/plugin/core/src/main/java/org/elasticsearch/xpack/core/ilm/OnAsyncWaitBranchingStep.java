/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ilm;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContentObject;

import java.util.Objects;

/**
 * This step changes its {@link #getNextStepKey()} depending on the result of an {@link AsyncWaitStep}.
 * <p>
 * The next step key will point towards the "wait action fulfilled" step when the condition of the async action is met, and towards the
 * "wait action unfulfilled" step when the client instructs they'd like to stop waiting for the condition using the
 * {@link BranchingStepListener#onStopWaitingAndMoveToNextKey(ToXContentObject)}.
 * <p>
 * If the async action that's branching the execution results in a failure, the {@link #getNextStepKey()} is not set (as ILM would move
 * into the {@link ErrorStep} step).
 */
public class OnAsyncWaitBranchingStep extends AsyncWaitStep {
    public static final String NAME = "async-branch";

    private StepKey nextStepKeyUnfulfilledWaitAction;
    private StepKey nextStepKeyFulfilledWaitAction;
    private TriConsumer<Client, IndexMetaData, BranchingStepListener> asyncWaitAction;
    private SetOnce<Boolean> onCompleteConditionMet;

    /**
     * {@link BranchingStep} is a step whose next step is based on the {@link #asyncWaitAction} condition being met or not.
     *
     * @param key                                the step's key
     * @param nextStepKeyUnfulfilledWaitAction the key of the step to run if the client decides to stop waiting for the condition to be
     *                                           via {@link BranchingStepListener#onStopWaitingAndMoveToNextKey(ToXContentObject)}
     * @param nextStepKeyFulfilledWaitAction   the key of the step to run if the {@link #asyncWaitAction} condition is met
     * @param asyncWaitAction                    the action to execute, would usually be similar to an instance of {@link AsyncWaitStep}
     *                                           but the user has the option to decide to stop waiting for the condition to be fulfilled
     */
    public OnAsyncWaitBranchingStep(StepKey key, StepKey nextStepKeyUnfulfilledWaitAction, StepKey nextStepKeyFulfilledWaitAction,
                                    Client client, TriConsumer<Client, IndexMetaData, BranchingStepListener> asyncWaitAction) {
        // super.nextStepKey is set to null since it is not used by this step
        super(key, null, client);
        this.nextStepKeyUnfulfilledWaitAction = nextStepKeyUnfulfilledWaitAction;
        this.nextStepKeyFulfilledWaitAction = nextStepKeyFulfilledWaitAction;
        this.asyncWaitAction = asyncWaitAction;
        this.onCompleteConditionMet = new SetOnce<>();
    }

    @Override
    public boolean isRetryable() {
        return true;
    }

    @Override
    public void evaluateCondition(IndexMetaData indexMetaData, Listener listener, TimeValue masterTimeout) {
        asyncWaitAction.apply(getClient(), indexMetaData, new BranchingStepListener() {
            @Override
            public void onResponse(boolean conditionMet, ToXContentObject informationContext) {
                onCompleteConditionMet.set(true);
                listener.onResponse(conditionMet, informationContext);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }

            @Override
            public void onStopWaitingAndMoveToNextKey(ToXContentObject informationContext) {
                onCompleteConditionMet.set(false);
                // to the "outside" world the condition was met (as the user decided so) and we're ready to move to the next step.
                // {@link #getNextStepKey} will now point towards {@link #nextStepKeyUnfulfilledWaitAction)
                listener.onResponse(true, informationContext);
            }
        });
    }

    /**
     * {@link AsyncWaitStep} listener that allows the client to instruct ILM to stop waiting for the condition to be met, without moving
     * into the ERROR step (which would happen if onFailure would be triggered).
     * This is designed to be used in conjunction with {@link OnAsyncWaitBranchingStep} where we can configure a step to move into when
     * the client decides not to wait for the condition to be met anymore.
     */
    interface BranchingStepListener extends AsyncWaitStep.Listener {
        void onStopWaitingAndMoveToNextKey(ToXContentObject informationContext);
    }

    /**
     * Returns the next step to be executed based on the result of the configured {@link #asyncWaitAction}
     *
     * @return the next step to execute, the "condition fulfilled" step if the wait condition was met or the "condition unfulfilled" step
     * if the condition was not met.
     */
    @Override
    public final StepKey getNextStepKey() {
        if (onCompleteConditionMet.get() == null) {
            throw new IllegalStateException("Cannot call getNextStepKey before evaluateCondition");
        }
        return onCompleteConditionMet.get() ? nextStepKeyFulfilledWaitAction : nextStepKeyUnfulfilledWaitAction;
    }

    final StepKey getNextStepKeyUnfulfilledWaitAction() {
        return nextStepKeyUnfulfilledWaitAction;
    }

    final StepKey getNextStepKeyFulfilledWaitAction() {
        return nextStepKeyFulfilledWaitAction;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        OnAsyncWaitBranchingStep that = (OnAsyncWaitBranchingStep) o;
        return super.equals(o)
            && Objects.equals(nextStepKeyUnfulfilledWaitAction, that.nextStepKeyUnfulfilledWaitAction)
            && Objects.equals(nextStepKeyFulfilledWaitAction, that.nextStepKeyFulfilledWaitAction);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), nextStepKeyUnfulfilledWaitAction, nextStepKeyFulfilledWaitAction);
    }
}
