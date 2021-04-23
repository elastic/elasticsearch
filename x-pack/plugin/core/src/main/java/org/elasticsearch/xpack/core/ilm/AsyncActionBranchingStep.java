/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.metadata.IndexMetadata;

import java.util.Objects;

/**
 * This step wraps an {@link AsyncActionStep} in order to be able to manipulate what the next step will be, depending on the result of the
 * wrapped {@link AsyncActionStep}.
 * <p>
 * If the action response is complete, the {@link AsyncActionBranchingStep}'s nextStepKey will be the nextStepKey of the wrapped action. If
 * the response is incomplete the nextStepKey will be the provided {@link AsyncActionBranchingStep#nextKeyOnIncompleteResponse}.
 * Failures encountered whilst executing the wrapped action will be propagated directly.
 */
public class AsyncActionBranchingStep extends AsyncActionStep {
    private final AsyncActionStep stepToExecute;

    private StepKey nextKeyOnIncompleteResponse;
    private SetOnce<Boolean> onResponseResult;

    public AsyncActionBranchingStep(AsyncActionStep stepToExecute, StepKey nextKeyOnIncompleteResponse, Client client) {
        // super.nextStepKey is set to null since it is not used by this step
        super(stepToExecute.getKey(), null, client);
        this.stepToExecute = stepToExecute;
        this.nextKeyOnIncompleteResponse = nextKeyOnIncompleteResponse;
        this.onResponseResult = new SetOnce<>();
    }

    @Override
    public boolean isRetryable() {
        return true;
    }

    @Override
    public void performAction(IndexMetadata indexMetadata, ClusterState currentClusterState, ClusterStateObserver observer,
                              ActionListener<Boolean> listener) {
        stepToExecute.performAction(indexMetadata, currentClusterState, observer, new ActionListener<>() {
            @Override
            public void onResponse(Boolean complete) {
                onResponseResult.set(complete);
                listener.onResponse(complete);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    @Override
    public final StepKey getNextStepKey() {
        if (onResponseResult.get() == null) {
            throw new IllegalStateException("cannot call getNextStepKey before performAction");
        }
        return onResponseResult.get() ? stepToExecute.getNextStepKey() : nextKeyOnIncompleteResponse;
    }

    /**
     * Represents the {@link AsyncActionStep} that's wrapped by this branching step.
     */
    AsyncActionStep getStepToExecute() {
        return stepToExecute;
    }

    /**
     * The step key to be reported as the {@link AsyncActionBranchingStep#getNextStepKey()} if the response of the wrapped
     * {@link AsyncActionBranchingStep#getStepToExecute()} is incomplete.
     */
    StepKey getNextKeyOnIncompleteResponse() {
        return nextKeyOnIncompleteResponse;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (super.equals(o) == false) {
            return false;
        }
        AsyncActionBranchingStep that = (AsyncActionBranchingStep) o;
        return super.equals(o)
            && Objects.equals(stepToExecute, that.stepToExecute)
            && Objects.equals(nextKeyOnIncompleteResponse, that.nextKeyOnIncompleteResponse);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), stepToExecute, nextKeyOnIncompleteResponse);
    }
}
