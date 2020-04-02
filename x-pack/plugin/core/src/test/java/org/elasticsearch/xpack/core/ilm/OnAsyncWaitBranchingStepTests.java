/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContentObject;

import static org.hamcrest.Matchers.is;

public class OnAsyncWaitBranchingStepTests extends AbstractStepTestCase<OnAsyncWaitBranchingStep> {

    public static final TriConsumer<Client, IndexMetadata, OnAsyncWaitBranchingStep.BranchingStepListener> NO_OP_ASYNC_ACTION =
        (client, indexMetadata, branchingStepListener) -> {
        };

    @Override
    protected OnAsyncWaitBranchingStep createRandomInstance() {
        return new OnAsyncWaitBranchingStep(randomStepKey(), randomStepKey(), randomStepKey(), client,
            NO_OP_ASYNC_ACTION);
    }

    @Override
    protected OnAsyncWaitBranchingStep mutateInstance(OnAsyncWaitBranchingStep instance) {
        Step.StepKey key = instance.getKey();
        Step.StepKey nextKeyOnUnfulfilledAction = instance.getNextStepKeyUnfulfilledWaitAction();
        Step.StepKey nextKeyOnFulfilledAction = instance.getNextStepKeyFulfilledWaitAction();

        switch (between(0, 2)) {
            case 0:
                key = randomStepKey();
                break;
            case 1:
                nextKeyOnUnfulfilledAction = randomStepKey();
                break;
            case 2:
                nextKeyOnFulfilledAction = randomStepKey();
                break;
            default:
                throw new AssertionError("Illegal randomisation branch");
        }
        return new OnAsyncWaitBranchingStep(key, nextKeyOnUnfulfilledAction, nextKeyOnFulfilledAction, instance.getClient(),
            NO_OP_ASYNC_ACTION);
    }

    @Override
    protected OnAsyncWaitBranchingStep copyInstance(OnAsyncWaitBranchingStep instance) {
        return new OnAsyncWaitBranchingStep(instance.getKey(), instance.getNextStepKeyUnfulfilledWaitAction(),
            instance.getNextStepKeyFulfilledWaitAction(), instance.getClient(), instance.getAsyncWaitAction());
    }

    public void testBranchToFulfilledKeyWhenTheActionReportsAsCompleted() {
        Step.StepKey nextKeyOnUnfulfilledAction = randomStepKey();
        Step.StepKey nextKeyOnFulfilledAction = randomStepKey();

        OnAsyncWaitBranchingStep onAsyncWaitBranchingStep = new OnAsyncWaitBranchingStep(randomStepKey(), nextKeyOnUnfulfilledAction,
            nextKeyOnFulfilledAction, client, (client, indexMetaData, branchingStepListener) -> {
            branchingStepListener.onResponse(true, null);
        });

        onAsyncWaitBranchingStep.evaluateCondition(null, new AsyncWaitStep.Listener() {
            @Override
            public void onResponse(boolean conditionMet, ToXContentObject informationContext) {
            }

            @Override
            public void onFailure(Exception e) {
            }
        }, TimeValue.timeValueSeconds(10));

        assertThat(onAsyncWaitBranchingStep.getNextStepKey(), is(nextKeyOnFulfilledAction));
    }

    public void testBranchToTheUnfulFilledKeyWhenTheWaitIsStopped() {
        Step.StepKey nextKeyOnUnfulfilledAction = randomStepKey();
        Step.StepKey nextKeyOnFulfilledAction = randomStepKey();

        OnAsyncWaitBranchingStep onAsyncWaitBranchingStep = new OnAsyncWaitBranchingStep(randomStepKey(), nextKeyOnUnfulfilledAction,
            nextKeyOnFulfilledAction, client, (client, indexMetadata, branchingStepListener) -> {
            branchingStepListener.onStopWaitingAndMoveToNextKey(null);
        });

        onAsyncWaitBranchingStep.evaluateCondition(null, new AsyncWaitStep.Listener() {
            @Override
            public void onResponse(boolean conditionMet, ToXContentObject informationContext) {
                assertThat("when we want to stop waiting the condition must be reported as met in order to be able to move to the next " +
                    "step", conditionMet, is(true));
            }

            @Override
            public void onFailure(Exception e) {
                fail("signaling no more waiting for the condition to be met should not trigger a failure");
            }
        }, TimeValue.timeValueSeconds(10));

        assertThat(onAsyncWaitBranchingStep.getNextStepKey(), is(nextKeyOnUnfulfilledAction));
    }

    public void testStepReportsFailure() {
        IllegalStateException failure = new IllegalStateException("failure");
        OnAsyncWaitBranchingStep onAsyncWaitBranchingStep = new OnAsyncWaitBranchingStep(randomStepKey(), randomStepKey(), randomStepKey(),
            client, (client, indexMetadata, branchingStepListener) -> {
            branchingStepListener.onFailure(failure);
        });

        onAsyncWaitBranchingStep.evaluateCondition(null, new AsyncWaitStep.Listener() {
            @Override
            public void onResponse(boolean conditionMet, ToXContentObject informationContext) {
                fail("step should propagate the failure using the onFailure callback");
            }

            @Override
            public void onFailure(Exception e) {
                assertThat(e, is(failure));
            }
        }, TimeValue.timeValueSeconds(10));
    }
}
