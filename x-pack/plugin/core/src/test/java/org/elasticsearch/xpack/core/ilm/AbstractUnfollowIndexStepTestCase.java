/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.Version;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.mockito.Mockito;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public abstract class AbstractUnfollowIndexStepTestCase<T extends AbstractUnfollowIndexStep> extends AbstractStepTestCase<T> {

    @Override
    protected final T createRandomInstance() {
        Step.StepKey stepKey = randomStepKey();
        Step.StepKey nextStepKey = randomStepKey();
        return newInstance(stepKey, nextStepKey, Mockito.mock(Client.class));
    }

    @Override
    protected final T mutateInstance(T instance) {
        Step.StepKey key = instance.getKey();
        Step.StepKey nextKey = instance.getNextStepKey();

        if (randomBoolean()) {
            key = new Step.StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
        } else {
            nextKey = new Step.StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
        }

        return newInstance(key, nextKey, instance.getClient());
    }

    @Override
    protected final T copyInstance(T instance) {
        return newInstance(instance.getKey(), instance.getNextStepKey(), instance.getClient());
    }

    public final void testNotAFollowerIndex() {
        IndexMetaData indexMetadata = IndexMetaData.builder("follower-index")
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE, "true"))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        Client client = Mockito.mock(Client.class);
        T step = newInstance(randomStepKey(), randomStepKey(), client);

        Boolean[] completed = new Boolean[1];
        Exception[] failure = new Exception[1];
        step.performAction(indexMetadata, null, null, new AsyncActionStep.Listener() {
            @Override
            public void onResponse(boolean complete) {
                completed[0] = complete;
            }

            @Override
            public void onFailure(Exception e) {
                failure[0] = e;
            }
        });
        assertThat(completed[0], is(true));
        assertThat(failure[0], nullValue());
        Mockito.verifyZeroInteractions(client);
    }

    protected abstract T newInstance(Step.StepKey key, Step.StepKey nextKey, Client client);
}
