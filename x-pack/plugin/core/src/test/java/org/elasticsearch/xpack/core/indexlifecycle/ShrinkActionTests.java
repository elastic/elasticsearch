/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.indexlifecycle.Step.StepKey;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class ShrinkActionTests extends AbstractSerializingTestCase<ShrinkAction> {

    @Override
    protected ShrinkAction doParseInstance(XContentParser parser) throws IOException {
        return ShrinkAction.parse(parser);
    }

    @Override
    protected ShrinkAction createTestInstance() {
        return new ShrinkAction(randomIntBetween(1, 100));
    }

    @Override
    protected ShrinkAction mutateInstance(ShrinkAction action) {
        return new ShrinkAction(action.getNumberOfShards() + randomIntBetween(1, 2));
    }

    @Override
    protected Reader<ShrinkAction> instanceReader() {
        return ShrinkAction::new;
    }

    public void testNonPositiveShardNumber() {
        Exception e = expectThrows(Exception.class, () -> new ShrinkAction(randomIntBetween(-100, 0)));
        assertThat(e.getMessage(), equalTo("[number_of_shards] must be greater than 0"));
    }

    public void testToSteps() {
        ShrinkAction action = createTestInstance();
        String phase = randomAlphaOfLengthBetween(1, 10);
        StepKey nextStepKey = new StepKey(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10),
            randomAlphaOfLengthBetween(1, 10));
        List<Step> steps = action.toSteps(null, phase, nextStepKey);
        assertThat(steps.size(), equalTo(4));
        StepKey expectedFirstKey = new StepKey(phase, ShrinkAction.NAME, ShrinkStep.NAME);
        StepKey expectedSecondKey = new StepKey(phase, ShrinkAction.NAME, EnoughShardsWaitStep.NAME);
        StepKey expectedThirdKey = new StepKey(phase, ShrinkAction.NAME, AliasStep.NAME);
        StepKey expectedFourthKey = new StepKey(phase, ShrinkAction.NAME, ShrunkenIndexCheckStep.NAME);
        assertTrue(steps.get(0) instanceof ShrinkStep);
        assertThat(steps.get(0).getKey(), equalTo(expectedFirstKey));
        assertThat(((ShrinkStep) steps.get(0)).getNumberOfShards(), equalTo(action.getNumberOfShards()));
        assertTrue(steps.get(1) instanceof EnoughShardsWaitStep);
        assertThat(steps.get(1).getKey(), equalTo(expectedSecondKey));
        assertThat(((EnoughShardsWaitStep) steps.get(1)).getNumberOfShards(), equalTo(action.getNumberOfShards()));
        assertTrue(steps.get(2) instanceof AliasStep);
        assertThat(steps.get(2).getKey(), equalTo(expectedThirdKey));
        assertTrue(steps.get(3) instanceof ShrunkenIndexCheckStep);
        assertThat(steps.get(3).getKey(), equalTo(expectedFourthKey));
    }
}
