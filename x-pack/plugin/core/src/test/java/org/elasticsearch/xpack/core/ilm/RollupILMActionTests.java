/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;
import org.elasticsearch.xpack.core.rollup.ConfigTestHelpers;
import org.elasticsearch.xpack.core.rollup.RollupActionConfig;
import org.elasticsearch.xpack.core.rollup.RollupActionConfigTests;

import java.util.List;

import static org.elasticsearch.xpack.core.ilm.RollupILMAction.GENERATE_ROLLUP_STEP_NAME;
import static org.hamcrest.Matchers.equalTo;

public class RollupILMActionTests extends AbstractActionTestCase<RollupILMAction> {

    static RollupILMAction randomInstance() {
        return new RollupILMAction(RollupActionConfigTests.randomConfig());
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
        RollupILMAction action = new RollupILMAction(RollupActionConfigTests.randomConfig());
        String phase = randomAlphaOfLengthBetween(1, 10);
        StepKey nextStepKey = new StepKey(
            randomAlphaOfLengthBetween(1, 10),
            randomAlphaOfLengthBetween(1, 10),
            randomAlphaOfLengthBetween(1, 10)
        );
        List<Step> steps = action.toSteps(null, phase, nextStepKey);
        assertNotNull(steps);
        assertEquals(5, steps.size());
        assertThat(steps.get(0).getKey().getName(), equalTo(CheckNotDataStreamWriteIndexStep.NAME));
        assertThat(steps.get(0).getNextStepKey().getName(), equalTo(ReadOnlyStep.NAME));
        assertThat(steps.get(1).getKey().getName(), equalTo(WaitForNoFollowersStep.NAME));
        assertThat(steps.get(1).getNextStepKey().getName(), equalTo(ReadOnlyStep.NAME));

        assertThat(steps.get(2).getKey().getName(), equalTo(ReadOnlyStep.NAME));
        assertThat(steps.get(2).getNextStepKey().getName(), equalTo(GENERATE_ROLLUP_STEP_NAME));
        assertThat(steps.get(3).getKey().getName(), equalTo(GENERATE_ROLLUP_STEP_NAME));
        assertThat(steps.get(3).getNextStepKey().getName(), equalTo(RollupStep.NAME));
        assertThat(steps.get(4).getKey().getName(), equalTo(RollupStep.NAME));
        assertThat(steps.get(4).getNextStepKey(), equalTo(nextStepKey));
    }

    public void testEqualsAndHashCode() {
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(createTestInstance(), this::copy, this::notCopy);
    }

    RollupILMAction copy(RollupILMAction rollupILMAction) {
        return new RollupILMAction(rollupILMAction.config());
    }

    RollupILMAction notCopy(RollupILMAction rollupILMAction) {
        DateHistogramInterval fixedInterval = randomValueOtherThan(
            rollupILMAction.config().getFixedInterval(),
            ConfigTestHelpers::randomInterval
        );
        RollupActionConfig newConfig = new RollupActionConfig(fixedInterval);
        return new RollupILMAction(newConfig);
    }
}
