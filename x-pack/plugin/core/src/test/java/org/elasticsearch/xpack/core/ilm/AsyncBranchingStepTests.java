/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;

public class AsyncBranchingStepTests extends AbstractStepTestCase<AsyncBranchingStep> {

    public void testPredicateNextStepChange() throws InterruptedException {
        String indexName = randomAlphaOfLength(5);
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(
                Metadata.builder()
                    .put(IndexMetadata.builder(indexName).settings(settings(IndexVersion.current())).numberOfShards(1).numberOfReplicas(0))
            )
            .build();
        StepKey stepKey = new StepKey(randomAlphaOfLength(5), randomAlphaOfLength(5), BranchingStep.NAME);
        StepKey nextStepKey = new StepKey(randomAlphaOfLength(6), randomAlphaOfLength(6), BranchingStep.NAME);
        StepKey nextSkipKey = new StepKey(randomAlphaOfLength(7), randomAlphaOfLength(7), BranchingStep.NAME);
        {
            AsyncBranchingStep step = new AsyncBranchingStep(stepKey, nextStepKey, nextSkipKey, (i, c, l) -> l.onResponse(true), client);
            expectThrows(IllegalStateException.class, step::getNextStepKey);
            CountDownLatch latch = new CountDownLatch(1);
            step.performAction(state.metadata().getProject().index(indexName), state, null, new Listener(latch));
            assertTrue(latch.await(5, TimeUnit.SECONDS));
            assertThat(step.getNextStepKey(), equalTo(step.getNextStepKeyOnTrue()));
        }
        {
            AsyncBranchingStep step = new AsyncBranchingStep(stepKey, nextStepKey, nextSkipKey, (i, c, l) -> l.onResponse(false), client);
            expectThrows(IllegalStateException.class, step::getNextStepKey);
            CountDownLatch latch = new CountDownLatch(1);
            step.performAction(state.metadata().getProject().index(indexName), state, null, new Listener(latch));
            assertTrue(latch.await(5, TimeUnit.SECONDS));
            assertThat(step.getNextStepKey(), equalTo(step.getNextStepKeyOnFalse()));
        }
    }

    @Override
    public AsyncBranchingStep createRandomInstance() {
        StepKey stepKey = new StepKey(randomAlphaOfLength(5), randomAlphaOfLength(5), BranchingStep.NAME);
        StepKey nextStepKey = new StepKey(randomAlphaOfLength(6), randomAlphaOfLength(6), BranchingStep.NAME);
        StepKey nextSkipKey = new StepKey(randomAlphaOfLength(7), randomAlphaOfLength(7), BranchingStep.NAME);
        return new AsyncBranchingStep(stepKey, nextStepKey, nextSkipKey, (i, c, l) -> l.onResponse(randomBoolean()), client);
    }

    @Override
    public AsyncBranchingStep mutateInstance(AsyncBranchingStep instance) {
        StepKey key = instance.getKey();
        StepKey nextStepKey = instance.getNextStepKeyOnFalse();
        StepKey nextSkipStepKey = instance.getNextStepKeyOnTrue();
        TriConsumer<IndexMetadata, ClusterState, ActionListener<Boolean>> asyncPredicate = instance.getAsyncPredicate();

        switch (between(0, 2)) {
            case 0 -> key = new StepKey(key.phase(), key.action(), key.name() + randomAlphaOfLength(5));
            case 1 -> nextStepKey = new StepKey(nextStepKey.phase(), nextStepKey.action(), nextStepKey.name() + randomAlphaOfLength(5));
            case 2 -> nextSkipStepKey = new StepKey(
                nextSkipStepKey.phase(),
                nextSkipStepKey.action(),
                nextSkipStepKey.name() + randomAlphaOfLength(5)
            );
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new AsyncBranchingStep(key, nextStepKey, nextSkipStepKey, asyncPredicate, client);
    }

    @Override
    public AsyncBranchingStep copyInstance(AsyncBranchingStep instance) {
        return new AsyncBranchingStep(
            instance.getKey(),
            instance.getNextStepKeyOnFalse(),
            instance.getNextStepKeyOnTrue(),
            instance.getAsyncPredicate(),
            client
        );
    }

    class Listener implements ActionListener<Void> {
        private final CountDownLatch latch;

        Listener(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void onResponse(Void unused) {
            latch.countDown();
        }

        @Override
        public void onFailure(Exception e) {
            fail("Unexpected failure: " + e.getMessage());
        }
    }
}
