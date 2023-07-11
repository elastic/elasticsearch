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

import java.util.List;

import static org.elasticsearch.xpack.core.ilm.DownsampleAction.CONDITIONAL_DATASTREAM_CHECK_KEY;
import static org.elasticsearch.xpack.core.ilm.DownsampleAction.CONDITIONAL_TIME_SERIES_CHECK_KEY;
import static org.elasticsearch.xpack.core.ilm.DownsampleAction.GENERATE_DOWNSAMPLE_STEP_NAME;
import static org.hamcrest.Matchers.equalTo;

public class DownsampleActionTests extends AbstractActionTestCase<DownsampleAction> {

    static DownsampleAction randomInstance() {
        return new DownsampleAction(ConfigTestHelpers.randomInterval());
    }

    @Override
    protected DownsampleAction doParseInstance(XContentParser parser) {
        return DownsampleAction.parse(parser);
    }

    @Override
    protected DownsampleAction createTestInstance() {
        return randomInstance();
    }

    @Override
    protected DownsampleAction mutateInstance(DownsampleAction instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Reader<DownsampleAction> instanceReader() {
        return DownsampleAction::new;
    }

    @Override
    public boolean isSafeAction() {
        return false;
    }

    @Override
    public void testToSteps() {
        DownsampleAction action = new DownsampleAction(ConfigTestHelpers.randomInterval());
        String phase = randomAlphaOfLengthBetween(1, 10);
        StepKey nextStepKey = new StepKey(
            randomAlphaOfLengthBetween(1, 10),
            randomAlphaOfLengthBetween(1, 10),
            randomAlphaOfLengthBetween(1, 10)
        );
        List<Step> steps = action.toSteps(null, phase, nextStepKey);
        assertNotNull(steps);
        assertEquals(13, steps.size());

        assertTrue(steps.get(0) instanceof BranchingStep);
        assertThat(steps.get(0).getKey().name(), equalTo(CONDITIONAL_TIME_SERIES_CHECK_KEY));
        expectThrows(IllegalStateException.class, () -> steps.get(0).getNextStepKey());
        assertThat(((BranchingStep) steps.get(0)).getNextStepKeyOnFalse(), equalTo(nextStepKey));
        assertThat(((BranchingStep) steps.get(0)).getNextStepKeyOnTrue().name(), equalTo(CheckNotDataStreamWriteIndexStep.NAME));

        assertTrue(steps.get(1) instanceof CheckNotDataStreamWriteIndexStep);
        assertThat(steps.get(1).getKey().name(), equalTo(CheckNotDataStreamWriteIndexStep.NAME));
        assertThat(steps.get(1).getNextStepKey().name(), equalTo(WaitForNoFollowersStep.NAME));

        assertTrue(steps.get(2) instanceof WaitForNoFollowersStep);
        assertThat(steps.get(2).getKey().name(), equalTo(WaitForNoFollowersStep.NAME));
        assertThat(steps.get(2).getNextStepKey().name(), equalTo(ReadOnlyStep.NAME));

        assertTrue(steps.get(3) instanceof ReadOnlyStep);
        assertThat(steps.get(3).getKey().name(), equalTo(ReadOnlyStep.NAME));
        assertThat(steps.get(3).getNextStepKey().name(), equalTo(CleanupTargetIndexStep.NAME));

        assertTrue(steps.get(4) instanceof CleanupTargetIndexStep);
        assertThat(steps.get(4).getKey().name(), equalTo(CleanupTargetIndexStep.NAME));
        assertThat(steps.get(4).getNextStepKey().name(), equalTo(GENERATE_DOWNSAMPLE_STEP_NAME));

        assertTrue(steps.get(5) instanceof GenerateUniqueIndexNameStep);
        assertThat(steps.get(5).getKey().name(), equalTo(GENERATE_DOWNSAMPLE_STEP_NAME));
        assertThat(steps.get(5).getNextStepKey().name(), equalTo(DownsampleStep.NAME));

        assertTrue(steps.get(6) instanceof DownsampleStep);
        assertThat(steps.get(6).getKey().name(), equalTo(DownsampleStep.NAME));
        assertThat(steps.get(6).getNextStepKey().name(), equalTo(WaitForIndexColorStep.NAME));

        assertTrue(steps.get(7) instanceof ClusterStateWaitUntilThresholdStep);
        assertThat(steps.get(7).getKey().name(), equalTo(WaitForIndexColorStep.NAME));
        assertThat(steps.get(7).getNextStepKey().name(), equalTo(CopyExecutionStateStep.NAME));

        assertTrue(steps.get(8) instanceof CopyExecutionStateStep);
        assertThat(steps.get(8).getKey().name(), equalTo(CopyExecutionStateStep.NAME));
        assertThat(steps.get(8).getNextStepKey().name(), equalTo(CONDITIONAL_DATASTREAM_CHECK_KEY));

        assertTrue(steps.get(9) instanceof BranchingStep);
        assertThat(steps.get(9).getKey().name(), equalTo(CONDITIONAL_DATASTREAM_CHECK_KEY));
        expectThrows(IllegalStateException.class, () -> steps.get(9).getNextStepKey());
        assertThat(((BranchingStep) steps.get(9)).getNextStepKeyOnFalse().name(), equalTo(SwapAliasesAndDeleteSourceIndexStep.NAME));
        assertThat(((BranchingStep) steps.get(9)).getNextStepKeyOnTrue().name(), equalTo(ReplaceDataStreamBackingIndexStep.NAME));

        assertTrue(steps.get(10) instanceof ReplaceDataStreamBackingIndexStep);
        assertThat(steps.get(10).getKey().name(), equalTo(ReplaceDataStreamBackingIndexStep.NAME));
        assertThat(steps.get(10).getNextStepKey().name(), equalTo(DeleteStep.NAME));

        assertTrue(steps.get(11) instanceof DeleteStep);
        assertThat(steps.get(11).getKey().name(), equalTo(DeleteStep.NAME));
        assertThat(steps.get(11).getNextStepKey(), equalTo(nextStepKey));

        assertTrue(steps.get(12) instanceof SwapAliasesAndDeleteSourceIndexStep);
        assertThat(steps.get(12).getKey().name(), equalTo(SwapAliasesAndDeleteSourceIndexStep.NAME));
        assertThat(steps.get(12).getNextStepKey(), equalTo(nextStepKey));
    }

    public void testEqualsAndHashCode() {
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(createTestInstance(), this::copy, this::notCopy);
    }

    DownsampleAction copy(DownsampleAction downsampleAction) {
        return new DownsampleAction(downsampleAction.fixedInterval());
    }

    DownsampleAction notCopy(DownsampleAction downsampleAction) {
        DateHistogramInterval fixedInterval = randomValueOtherThan(downsampleAction.fixedInterval(), ConfigTestHelpers::randomInterval);
        return new DownsampleAction(fixedInterval);
    }
}
