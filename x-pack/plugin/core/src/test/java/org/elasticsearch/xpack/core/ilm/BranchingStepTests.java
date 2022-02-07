/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.index.Index;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import java.util.function.BiPredicate;

import static org.hamcrest.Matchers.equalTo;

public class BranchingStepTests extends AbstractStepTestCase<BranchingStep> {

    public void testPredicateNextStepChange() {
        String indexName = randomAlphaOfLength(5);
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(
                Metadata.builder()
                    .put(IndexMetadata.builder(indexName).settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0))
            )
            .build();
        StepKey stepKey = new StepKey(randomAlphaOfLength(5), randomAlphaOfLength(5), BranchingStep.NAME);
        StepKey nextStepKey = new StepKey(randomAlphaOfLength(6), randomAlphaOfLength(6), BranchingStep.NAME);
        StepKey nextSkipKey = new StepKey(randomAlphaOfLength(7), randomAlphaOfLength(7), BranchingStep.NAME);
        {
            BranchingStep step = new BranchingStep(stepKey, nextStepKey, nextSkipKey, (i, c) -> true);
            expectThrows(IllegalStateException.class, step::getNextStepKey);
            step.performAction(state.metadata().index(indexName).getIndex(), state);
            assertThat(step.getNextStepKey(), equalTo(step.getNextStepKeyOnTrue()));
            expectThrows(SetOnce.AlreadySetException.class, () -> step.performAction(state.metadata().index(indexName).getIndex(), state));
        }
        {
            BranchingStep step = new BranchingStep(stepKey, nextStepKey, nextSkipKey, (i, c) -> false);
            expectThrows(IllegalStateException.class, step::getNextStepKey);
            step.performAction(state.metadata().index(indexName).getIndex(), state);
            assertThat(step.getNextStepKey(), equalTo(step.getNextStepKeyOnFalse()));
            expectThrows(SetOnce.AlreadySetException.class, () -> step.performAction(state.metadata().index(indexName).getIndex(), state));
        }
    }

    @Override
    public BranchingStep createRandomInstance() {
        StepKey stepKey = new StepKey(randomAlphaOfLength(5), randomAlphaOfLength(5), BranchingStep.NAME);
        StepKey nextStepKey = new StepKey(randomAlphaOfLength(6), randomAlphaOfLength(6), BranchingStep.NAME);
        StepKey nextSkipKey = new StepKey(randomAlphaOfLength(7), randomAlphaOfLength(7), BranchingStep.NAME);
        return new BranchingStep(stepKey, nextStepKey, nextSkipKey, (i, c) -> randomBoolean());
    }

    @Override
    public BranchingStep mutateInstance(BranchingStep instance) {
        StepKey key = instance.getKey();
        StepKey nextStepKey = instance.getNextStepKeyOnFalse();
        StepKey nextSkipStepKey = instance.getNextStepKeyOnTrue();
        BiPredicate<Index, ClusterState> predicate = instance.getPredicate();

        switch (between(0, 2)) {
            case 0 -> key = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
            case 1 -> nextStepKey = new StepKey(
                nextStepKey.getPhase(),
                nextStepKey.getAction(),
                nextStepKey.getName() + randomAlphaOfLength(5)
            );
            case 2 -> nextSkipStepKey = new StepKey(
                nextSkipStepKey.getPhase(),
                nextSkipStepKey.getAction(),
                nextSkipStepKey.getName() + randomAlphaOfLength(5)
            );
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new BranchingStep(key, nextStepKey, nextSkipStepKey, predicate);
    }

    @Override
    public BranchingStep copyInstance(BranchingStep instance) {
        return new BranchingStep(
            instance.getKey(),
            instance.getNextStepKeyOnFalse(),
            instance.getNextStepKeyOnTrue(),
            instance.getPredicate()
        );
    }
}
