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
import org.elasticsearch.xpack.core.rollup.v2.RollupV2Config;
import org.elasticsearch.xpack.core.rollup.v2.RollupV2ConfigTests;

import java.util.List;

public class RollupActionTests extends AbstractActionTestCase<RollupAction> {

    static RollupAction randomInstance() {
        return new RollupAction(RollupV2ConfigTests.randomConfig(random()));
    }

    @Override
    protected RollupAction doParseInstance(XContentParser parser) {
        return RollupAction.parse(parser);
    }

    @Override
    protected RollupAction createTestInstance() {
        return randomInstance();
    }

    @Override
    protected Reader<RollupAction> instanceReader() {
        return RollupAction::new;
    }

    @Override
    public boolean isSafeAction() {
        return false;
    }

    public void testToSteps() {
        RollupAction action = createTestInstance();
        String phase = randomAlphaOfLengthBetween(1, 10);
        StepKey nextStepKey = new StepKey(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10),
            randomAlphaOfLengthBetween(1, 10));
        List<Step> steps = action.toSteps(null, phase, nextStepKey);
        assertNotNull(steps);
        assertEquals(3, steps.size());
    }

    public void testEqualsAndHashCode() {
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(createTestInstance(), this::copy, this::notCopy);
    }

    RollupAction copy(RollupAction rollupAction) {
        return new RollupAction(rollupAction.config);
    }

    RollupAction notCopy(RollupAction rollupAction) {
        RollupV2Config newConfig = new RollupV2Config(rollupAction.config.getSourceIndex() + "not",
            rollupAction.config.getGroupConfig(), rollupAction.config.getMetricsConfig(), rollupAction.config.getTimeout(),
            rollupAction.config.getRollupIndex());
        return new RollupAction(newConfig);
    }
}
