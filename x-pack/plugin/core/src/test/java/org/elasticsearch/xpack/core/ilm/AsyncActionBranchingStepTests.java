/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;

import static org.hamcrest.Matchers.is;

public class AsyncActionBranchingStepTests extends AbstractStepTestCase<AsyncActionBranchingStep> {

    @Override
    protected AsyncActionBranchingStep createRandomInstance() {
        return new AsyncActionBranchingStep(new UpdateSettingsStep(randomStepKey(), randomStepKey(), client, Settings.EMPTY),
            randomStepKey(), client);
    }

    @Override
    protected AsyncActionBranchingStep mutateInstance(AsyncActionBranchingStep instance) {
        AsyncActionStep wrappedStep = instance.getStepToExecute();
        Step.StepKey nextKeyOnIncompleteResponse = instance.getNextKeyOnIncompleteResponse();

        switch (between(0, 1)) {
            case 0:
                wrappedStep = new UpdateSettingsStep(randomStepKey(), randomStepKey(), client, Settings.EMPTY);
                break;
            case 1:
                nextKeyOnIncompleteResponse = randomStepKey();
                break;
            default:
                throw new AssertionError("Illegal randomisation branch");
        }
        return new AsyncActionBranchingStep(wrappedStep, nextKeyOnIncompleteResponse, client);
    }

    @Override
    protected AsyncActionBranchingStep copyInstance(AsyncActionBranchingStep instance) {
        return new AsyncActionBranchingStep(instance.getStepToExecute(), instance.getNextKeyOnIncompleteResponse(), instance.getClient());
    }

    protected IndexMetadata getIndexMetadata() {
        return IndexMetadata.builder(randomAlphaOfLength(10)).settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
    }

    public void testBranchStepKeyIsTheWrappedStepKey() {
        AsyncActionStep stepToExecute = new AsyncActionStep(randomStepKey(), randomStepKey(), client) {
            @Override
            public boolean isRetryable() {
                return true;
            }

            @Override
            public void performAction(IndexMetadata indexMetadata, ClusterState currentClusterState, ClusterStateObserver observer,
                                      ActionListener<Boolean> listener) {
            }
        };

        AsyncActionBranchingStep asyncActionBranchingStep = new AsyncActionBranchingStep(stepToExecute, randomStepKey(), client);
        assertThat(asyncActionBranchingStep.getKey(), is(stepToExecute.getKey()));
    }

    public void testBranchStepNextKeyOnCompleteResponse() {
        AsyncActionStep stepToExecute = new AsyncActionStep(randomStepKey(), randomStepKey(), client) {
            @Override
            public boolean isRetryable() {
                return true;
            }

            @Override
            public void performAction(IndexMetadata indexMetadata, ClusterState currentClusterState, ClusterStateObserver observer,
                                      ActionListener<Boolean> listener) {
                listener.onResponse(true);
            }
        };

        AsyncActionBranchingStep asyncActionBranchingStep = new AsyncActionBranchingStep(stepToExecute, randomStepKey(), client);

        assertTrue(PlainActionFuture.get(f -> asyncActionBranchingStep.performAction(getIndexMetadata(), emptyClusterState(), null, f)));
        assertThat(asyncActionBranchingStep.getNextStepKey(), is(stepToExecute.getNextStepKey()));
    }

    public void testBranchStepNextKeyOnInCompleteResponse() {
        AsyncActionStep stepToExecute = new AsyncActionStep(randomStepKey(), randomStepKey(), client) {
            @Override
            public boolean isRetryable() {
                return true;
            }

            @Override
            public void performAction(IndexMetadata indexMetadata, ClusterState currentClusterState, ClusterStateObserver observer,
                                      ActionListener<Boolean> listener) {
                listener.onResponse(false);
            }
        };

        Step.StepKey nextKeyOnIncompleteResponse = randomStepKey();
        AsyncActionBranchingStep asyncActionBranchingStep = new AsyncActionBranchingStep(stepToExecute, nextKeyOnIncompleteResponse,
            client);

        assertFalse(PlainActionFuture.get(f -> asyncActionBranchingStep.performAction(getIndexMetadata(), emptyClusterState(), null, f)));
        assertThat(asyncActionBranchingStep.getNextStepKey(), is(nextKeyOnIncompleteResponse));
    }

    public void testBranchStepPropagatesFailure() {
        NullPointerException failException = new NullPointerException("fail");
        AsyncActionStep stepToExecute = new AsyncActionStep(randomStepKey(), randomStepKey(), client) {
            @Override
            public boolean isRetryable() {
                return true;
            }

            @Override
            public void performAction(IndexMetadata indexMetadata, ClusterState currentClusterState, ClusterStateObserver observer,
                                      ActionListener<Boolean> listener) {
                listener.onFailure(failException);
            }
        };

        AsyncActionBranchingStep asyncActionBranchingStep = new AsyncActionBranchingStep(stepToExecute, randomStepKey(), client);

        asyncActionBranchingStep.performAction(getIndexMetadata(), emptyClusterState(), null, new ActionListener<>() {

            @Override
            public void onResponse(Boolean complete) {
                fail("expecting a failure as the wrapped step failed");
            }

            @Override
            public void onFailure(Exception e) {
                assertThat(e, is(failException));
            }
        });
        expectThrows(IllegalStateException.class, () -> asyncActionBranchingStep.getNextStepKey());
    }
}
