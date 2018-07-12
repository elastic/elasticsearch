/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.indexlifecycle.Step.StepKey;

import java.io.IOException;
import java.util.List;

public class ReplicasActionTests extends AbstractActionTestCase<ReplicasAction> {

    @Override
    protected ReplicasAction doParseInstance(XContentParser parser) {
        return ReplicasAction.parse(parser);
    }

    @Override
    protected ReplicasAction createTestInstance() {
        return new ReplicasAction(randomIntBetween(0, 10));
    }

    @Override
    protected Reader<ReplicasAction> instanceReader() {
        return ReplicasAction::new;
    }

    @Override
    protected ReplicasAction mutateInstance(ReplicasAction instance) throws IOException {
        return new ReplicasAction(instance.getNumberOfReplicas() + randomIntBetween(1, 5));
    }

    public void testInvalidNumReplicas() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
                () -> new ReplicasAction(randomIntBetween(-1000, -1)));
        assertEquals("[" + ReplicasAction.NUMBER_OF_REPLICAS_FIELD.getPreferredName() + "] must be >= 0", exception.getMessage());
    }

    public void testToSteps() {
        ReplicasAction action = createTestInstance();
        String phase = randomAlphaOfLengthBetween(1, 10);
        StepKey nextStepKey = new StepKey(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10),
                randomAlphaOfLengthBetween(1, 10));
        List<Step> steps = action.toSteps(null, phase, nextStepKey);
        assertNotNull(steps);
        assertEquals(2, steps.size());
        StepKey expectedFirstStepKey = new StepKey(phase, ReplicasAction.NAME, UpdateSettingsStep.NAME);
        StepKey expectedSecondStepKey = new StepKey(phase, ReplicasAction.NAME, ReplicasAllocatedStep.NAME);
        UpdateSettingsStep firstStep = (UpdateSettingsStep) steps.get(0);
        assertEquals(expectedFirstStepKey, firstStep.getKey());
        assertEquals(expectedSecondStepKey, firstStep.getNextStepKey());
        assertEquals(1, firstStep.getSettings().size());
        assertEquals(action.getNumberOfReplicas(), IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.get(firstStep.getSettings()).intValue());
        ReplicasAllocatedStep secondStep = (ReplicasAllocatedStep) steps.get(1);
        assertEquals(expectedSecondStepKey, secondStep.getKey());
        assertEquals(nextStepKey, secondStep.getNextStepKey());
    }
}
