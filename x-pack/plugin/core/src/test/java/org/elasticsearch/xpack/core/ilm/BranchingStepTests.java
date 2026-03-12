/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import static org.hamcrest.Matchers.equalTo;

public class BranchingStepTests extends AbstractStepTestCase<BranchingStep> {

    public void testPredicateNextStepChange() {
        String indexName = randomAlphaOfLength(5);
        ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault())
            .put(IndexMetadata.builder(indexName).settings(settings(IndexVersion.current())).numberOfShards(1).numberOfReplicas(0))
            .build();
        ProjectState state = ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(project).build().projectState(project.id());
        StepKey stepKey = new StepKey(randomAlphaOfLength(5), randomAlphaOfLength(5), BranchingStep.NAME);
        StepKey nextStepKey = new StepKey(randomAlphaOfLength(6), randomAlphaOfLength(6), BranchingStep.NAME);
        StepKey nextSkipKey = new StepKey(randomAlphaOfLength(7), randomAlphaOfLength(7), BranchingStep.NAME);
        {
            BranchingStep step = new BranchingStep(stepKey, nextStepKey, nextSkipKey, (i, c) -> true);
            expectThrows(IllegalStateException.class, step::getNextStepKey);
            step.performAction(project.index(indexName).getIndex(), state);
            assertThat(step.getNextStepKey(), equalTo(step.getNextStepKeyOnTrue()));
            expectThrows(SetOnce.AlreadySetException.class, () -> step.performAction(project.index(indexName).getIndex(), state));
        }
        {
            BranchingStep step = new BranchingStep(stepKey, nextStepKey, nextSkipKey, (i, c) -> false);
            expectThrows(IllegalStateException.class, step::getNextStepKey);
            step.performAction(project.index(indexName).getIndex(), state);
            assertThat(step.getNextStepKey(), equalTo(step.getNextStepKeyOnFalse()));
            expectThrows(SetOnce.AlreadySetException.class, () -> step.performAction(project.index(indexName).getIndex(), state));
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

        return new BranchingStep(key, nextStepKey, nextSkipStepKey, instance.getPredicate());
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
