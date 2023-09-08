/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.index.IndexVersion;
import org.mockito.Mockito;

public abstract class AbstractUnfollowIndexStepTestCase<T extends AbstractUnfollowIndexStep> extends AbstractStepTestCase<T> {

    @Override
    protected final T createRandomInstance() {
        Step.StepKey stepKey = randomStepKey();
        Step.StepKey nextStepKey = randomStepKey();
        return newInstance(stepKey, nextStepKey);
    }

    @Override
    protected final T mutateInstance(T instance) {
        Step.StepKey key = instance.getKey();
        Step.StepKey nextKey = instance.getNextStepKey();

        if (randomBoolean()) {
            key = new Step.StepKey(key.phase(), key.action(), key.name() + randomAlphaOfLength(5));
        } else {
            nextKey = new Step.StepKey(nextKey.phase(), nextKey.action(), nextKey.name() + randomAlphaOfLength(5));
        }

        return newInstance(key, nextKey);
    }

    @Override
    protected final T copyInstance(T instance) {
        return newInstance(instance.getKey(), instance.getNextStepKey());
    }

    public final void testNotAFollowerIndex() throws Exception {
        IndexMetadata indexMetadata = IndexMetadata.builder("follower-index")
            .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE, "true"))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        T step = newInstance(randomStepKey(), randomStepKey());

        PlainActionFuture.<Void, Exception>get(f -> step.performAction(indexMetadata, null, null, f));
        Mockito.verifyNoMoreInteractions(client);
    }

    protected abstract T newInstance(Step.StepKey key, Step.StepKey nextKey);
}
