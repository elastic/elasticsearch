/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;
import org.elasticsearch.xpack.core.rollup.v2.RollupActionConfig;
import org.elasticsearch.xpack.core.rollup.v2.RollupActionConfigTests;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class RollupILMActionTests extends AbstractActionTestCase<RollupILMAction> {

    static RollupILMAction randomInstance() {
        return new RollupILMAction(RollupActionConfigTests.randomConfig(random()), randomBoolean(),
            randomBoolean() ? randomAlphaOfLength(5) : null);
    }

    @Override
    protected RollupILMAction doParseInstance(XContentParser parser) {
        return RollupILMAction.parse(parser);
    }

    @Override
    protected RollupILMAction createTestInstance() {
        return randomInstance();
    }

    @Override
    protected Reader<RollupILMAction> instanceReader() {
        return RollupILMAction::new;
    }

    @Override
    public boolean isSafeAction() {
        return false;
    }

    @Override
    public void testToSteps() {
        RollupILMAction action = new RollupILMAction(RollupActionConfigTests.randomConfig(random()), false, null);
        String phase = randomAlphaOfLengthBetween(1, 10);
        StepKey nextStepKey = new StepKey(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10),
            randomAlphaOfLengthBetween(1, 10));
        List<Step> steps = action.toSteps(null, phase, nextStepKey);
        assertNotNull(steps);
        assertEquals(3, steps.size());
        assertThat(steps.get(0).getKey().getName(), equalTo(CheckNotDataStreamWriteIndexStep.NAME));
        assertThat(steps.get(0).getNextStepKey().getName(), equalTo(ReadOnlyStep.NAME));
        assertThat(steps.get(1).getKey().getName(), equalTo(ReadOnlyStep.NAME));
        assertThat(steps.get(1).getNextStepKey().getName(), equalTo(RollupStep.NAME));
        assertThat(steps.get(2).getKey().getName(), equalTo(RollupStep.NAME));
        assertThat(steps.get(2).getNextStepKey(), equalTo(nextStepKey));
    }

    public void testToStepsWithDelete() {
        RollupILMAction action = new RollupILMAction(RollupActionConfigTests.randomConfig(random()), true, null);
        String phase = randomAlphaOfLengthBetween(1, 10);
        StepKey nextStepKey = new StepKey(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10),
            randomAlphaOfLengthBetween(1, 10));
        List<Step> steps = action.toSteps(null, phase, nextStepKey);
        assertNotNull(steps);
        assertEquals(5, steps.size());
        assertThat(steps.get(0).getKey().getName(), equalTo(CheckNotDataStreamWriteIndexStep.NAME));
        assertThat(steps.get(0).getNextStepKey().getName(), equalTo(ReadOnlyStep.NAME));
        assertThat(steps.get(1).getKey().getName(), equalTo(ReadOnlyStep.NAME));
        assertThat(steps.get(1).getNextStepKey().getName(), equalTo(RollupStep.NAME));
        assertThat(steps.get(2).getKey().getName(), equalTo(RollupStep.NAME));
        assertThat(steps.get(2).getNextStepKey().getName(), equalTo(WaitForNoFollowersStep.NAME));
        assertThat(steps.get(3).getKey().getName(), equalTo(WaitForNoFollowersStep.NAME));
        assertThat(steps.get(3).getNextStepKey().getName(), equalTo(DeleteStep.NAME));
        assertThat(steps.get(4).getKey().getName(), equalTo(DeleteStep.NAME));
        assertThat(steps.get(4).getNextStepKey(), equalTo(nextStepKey));
    }

    public void testEqualsAndHashCode() {
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(createTestInstance(), this::copy, this::notCopy);
    }

    RollupILMAction copy(RollupILMAction rollupILMAction) {
        return new RollupILMAction(rollupILMAction.config(), rollupILMAction.shouldDeleteOriginalIndex(), rollupILMAction.rollupPolicy());
    }

    RollupILMAction notCopy(RollupILMAction rollupILMAction) {
        RollupActionConfig newConfig = rollupILMAction.config();
        boolean newDeleteOriginalIndex = rollupILMAction.shouldDeleteOriginalIndex();
        String newRollupPolicy = rollupILMAction.rollupPolicy();
        switch (randomIntBetween(0, 2)) {
            case 0:
                newConfig = new RollupActionConfig(rollupILMAction.config().getGroupConfig(),
                    rollupILMAction.config().getMetricsConfig(), rollupILMAction.config().getTimeout(),
                    rollupILMAction.config().getRollupIndex() + "not");
                break;
            case 1:
                newDeleteOriginalIndex = !newDeleteOriginalIndex;
                break;
            case 2:
                newRollupPolicy = randomAlphaOfLength(3);
                break;
            default:
                throw new IllegalStateException("unreachable branch");
        }
        return new RollupILMAction(newConfig, newDeleteOriginalIndex, newRollupPolicy);
    }
}
