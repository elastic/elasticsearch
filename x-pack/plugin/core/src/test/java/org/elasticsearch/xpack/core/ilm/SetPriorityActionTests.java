/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class SetPriorityActionTests extends AbstractActionTestCase<SetPriorityAction> {

    private final int priority = randomIntBetween(0, Integer.MAX_VALUE);

    static SetPriorityAction randomInstance() {
        return new SetPriorityAction(randomIntBetween(2, Integer.MAX_VALUE - 1));
    }

    @Override
    protected SetPriorityAction doParseInstance(XContentParser parser) {
        return SetPriorityAction.parse(parser);
    }

    @Override
    protected SetPriorityAction createTestInstance() {
        return new SetPriorityAction(priority);
    }

    @Override
    protected Reader<SetPriorityAction> instanceReader() {
        return SetPriorityAction::new;
    }

    public void testNonPositivePriority() {
        Exception e = expectThrows(Exception.class, () -> new SetPriorityAction(randomIntBetween(-100, -1)));
        assertThat(e.getMessage(), equalTo("[priority] must be 0 or greater"));
    }

    public void testNullPriorityAllowed() {
        SetPriorityAction nullPriority = new SetPriorityAction((Integer) null);
        assertNull(nullPriority.recoveryPriority);
    }

    public void testToSteps() {
        SetPriorityAction action = createTestInstance();
        String phase = randomAlphaOfLengthBetween(1, 10);
        StepKey nextStepKey = new StepKey(
            randomAlphaOfLengthBetween(1, 10),
            randomAlphaOfLengthBetween(1, 10),
            randomAlphaOfLengthBetween(1, 10)
        );
        List<Step> steps = action.toSteps(null, phase, nextStepKey);
        assertNotNull(steps);
        assertEquals(1, steps.size());
        StepKey expectedFirstStepKey = new StepKey(phase, SetPriorityAction.NAME, SetPriorityAction.NAME);
        UpdateSettingsStep firstStep = (UpdateSettingsStep) steps.get(0);
        assertThat(firstStep.getKey(), equalTo(expectedFirstStepKey));
        assertThat(firstStep.getNextStepKey(), equalTo(nextStepKey));
        assertThat(firstStep.getSettings().size(), equalTo(1));
        assertEquals(priority, (long) IndexMetadata.INDEX_PRIORITY_SETTING.get(firstStep.getSettings()));
    }

    public void testNullPriorityStep() {
        SetPriorityAction action = new SetPriorityAction((Integer) null);
        String phase = randomAlphaOfLengthBetween(1, 10);
        StepKey nextStepKey = new StepKey(
            randomAlphaOfLengthBetween(1, 10),
            randomAlphaOfLengthBetween(1, 10),
            randomAlphaOfLengthBetween(1, 10)
        );
        List<Step> steps = action.toSteps(null, phase, nextStepKey);
        assertNotNull(steps);
        assertEquals(1, steps.size());
        StepKey expectedFirstStepKey = new StepKey(phase, SetPriorityAction.NAME, SetPriorityAction.NAME);
        UpdateSettingsStep firstStep = (UpdateSettingsStep) steps.get(0);
        assertThat(firstStep.getKey(), equalTo(expectedFirstStepKey));
        assertThat(firstStep.getNextStepKey(), equalTo(nextStepKey));
        assertThat(firstStep.getSettings().size(), equalTo(1));
        assertThat(
            IndexMetadata.INDEX_PRIORITY_SETTING.get(firstStep.getSettings()),
            equalTo(IndexMetadata.INDEX_PRIORITY_SETTING.getDefault(firstStep.getSettings()))
        );
    }

    public void testEqualsAndHashCode() {
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(createTestInstance(), this::copy, this::notCopy);
    }

    SetPriorityAction copy(SetPriorityAction setPriorityAction) {
        return new SetPriorityAction(setPriorityAction.recoveryPriority);
    }

    SetPriorityAction notCopy(SetPriorityAction setPriorityAction) {
        return new SetPriorityAction(setPriorityAction.recoveryPriority + 1);
    }
}
