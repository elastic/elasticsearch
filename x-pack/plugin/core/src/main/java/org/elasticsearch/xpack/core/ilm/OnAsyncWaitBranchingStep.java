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

public class OnAsyncWaitBranchingStep extends AsyncWaitStep {
    public static final String NAME = "async-branch";

    private StepKey nextStepKeyOnActionFailure;
    private StepKey nextStepKeyOnActionSuccess;
    private TriConsumer<Client, IndexMetaData, Listener> asyncWaitAction;
    private SetOnce<Boolean> onCompleteConditionMet;

    public OnAsyncWaitBranchingStep(StepKey key, StepKey nextStepKeyOnActionFailure, StepKey nextStepKeyOnActionSuccess,
                                    Client client, TriConsumer<Client, IndexMetaData, Listener> asyncWaitAction) {
        // super.nextStepKey is set to null since it is not used by this step
        super(key, null, client);
        this.nextStepKeyOnActionFailure = nextStepKeyOnActionFailure;
        this.nextStepKeyOnActionSuccess = nextStepKeyOnActionSuccess;
        this.asyncWaitAction = asyncWaitAction;
        this.onCompleteConditionMet = new SetOnce<>();
    }

    @Override
    public void evaluateCondition(IndexMetaData indexMetaData, Listener listener, TimeValue masterTimeout) {
        asyncWaitAction.apply(getClient(), indexMetaData, new Listener() {
            @Override
            public void onResponse(boolean conditionMet, ToXContentObject informationContext) {
                if (conditionMet) {
                    onCompleteConditionMet.set(true);
                } else {
                    onCompleteConditionMet.set(false);
                }
                listener.onResponse(conditionMet, informationContext);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    @Override
    public final StepKey getNextStepKey() {
        if (onCompleteConditionMet.get() == null) {
            throw new IllegalStateException("Cannot call getNextStepKey before performAction");
        }
        return onCompleteConditionMet.get() ? nextStepKeyOnActionSuccess : nextStepKeyOnActionFailure;
    }

    final StepKey getNextStepKeyOnActionFailure() {
        return nextStepKeyOnActionFailure;
    }

    final StepKey getNextStepKeyOnActionSuccess() {
        return nextStepKeyOnActionSuccess;
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
            && Objects.equals(nextStepKeyOnActionFailure, that.nextStepKeyOnActionFailure)
            && Objects.equals(nextStepKeyOnActionSuccess, that.nextStepKeyOnActionSuccess);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), nextStepKeyOnActionFailure, nextStepKeyOnActionSuccess);
    }
}
