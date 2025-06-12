/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;
import org.elasticsearch.xpack.core.rollup.ConfigTestHelpers;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.core.ilm.DownsampleAction.CONDITIONAL_DATASTREAM_CHECK_KEY;
import static org.elasticsearch.xpack.core.ilm.DownsampleAction.CONDITIONAL_TIME_SERIES_CHECK_KEY;
import static org.elasticsearch.xpack.core.ilm.DownsampleAction.DOWNSAMPLED_INDEX_PREFIX;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class DownsampleActionTests extends AbstractActionTestCase<DownsampleAction> {

    public static final TimeValue WAIT_TIMEOUT = new TimeValue(1, TimeUnit.MINUTES);

    static DownsampleAction randomInstance() {
        return new DownsampleAction(ConfigTestHelpers.randomInterval(), WAIT_TIMEOUT);
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
        DownsampleAction action = new DownsampleAction(ConfigTestHelpers.randomInterval(), WAIT_TIMEOUT);
        String phase = randomAlphaOfLengthBetween(1, 10);
        StepKey nextStepKey = new StepKey(
            randomAlphaOfLengthBetween(1, 10),
            randomAlphaOfLengthBetween(1, 10),
            randomAlphaOfLengthBetween(1, 10)
        );
        List<Step> steps = action.toSteps(null, phase, nextStepKey);
        assertNotNull(steps);
        assertEquals(15, steps.size());

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
        assertThat(steps.get(2).getNextStepKey().name(), equalTo(WaitUntilTimeSeriesEndTimePassesStep.NAME));

        assertTrue(steps.get(3) instanceof WaitUntilTimeSeriesEndTimePassesStep);
        assertThat(steps.get(3).getKey().name(), equalTo(WaitUntilTimeSeriesEndTimePassesStep.NAME));
        assertThat(steps.get(3).getNextStepKey().name(), equalTo(ReadOnlyStep.NAME));

        assertTrue(steps.get(4) instanceof ReadOnlyStep);
        assertThat(steps.get(4).getKey().name(), equalTo(ReadOnlyStep.NAME));
        assertThat(steps.get(4).getNextStepKey().name(), equalTo(DownsamplePrepareLifeCycleStateStep.NAME));

        assertTrue(steps.get(5) instanceof NoopStep);
        assertThat(steps.get(5).getKey().name(), equalTo(CleanupTargetIndexStep.NAME));
        assertThat(steps.get(5).getNextStepKey().name(), equalTo(DownsampleStep.NAME));

        assertTrue(steps.get(6) instanceof DownsamplePrepareLifeCycleStateStep);
        assertThat(steps.get(6).getKey().name(), equalTo(DownsamplePrepareLifeCycleStateStep.NAME));
        assertThat(steps.get(6).getNextStepKey().name(), equalTo(DownsampleStep.NAME));

        assertTrue(steps.get(7) instanceof DownsampleStep);
        assertThat(steps.get(7).getKey().name(), equalTo(DownsampleStep.NAME));
        assertThat(steps.get(7).getNextStepKey().name(), equalTo(WaitForIndexColorStep.NAME));

        assertTrue(steps.get(8) instanceof ClusterStateWaitUntilThresholdStep);
        assertThat(steps.get(8).getKey().name(), equalTo(WaitForIndexColorStep.NAME));
        assertThat(steps.get(8).getNextStepKey().name(), equalTo(CopyExecutionStateStep.NAME));

        assertTrue(steps.get(9) instanceof CopyExecutionStateStep);
        assertThat(steps.get(9).getKey().name(), equalTo(CopyExecutionStateStep.NAME));
        assertThat(steps.get(9).getNextStepKey().name(), equalTo(CopySettingsStep.NAME));

        assertTrue(steps.get(10) instanceof CopySettingsStep);
        assertThat(steps.get(10).getKey().name(), equalTo(CopySettingsStep.NAME));
        assertThat(steps.get(10).getNextStepKey().name(), equalTo(CONDITIONAL_DATASTREAM_CHECK_KEY));

        assertTrue(steps.get(11) instanceof BranchingStep);
        assertThat(steps.get(11).getKey().name(), equalTo(CONDITIONAL_DATASTREAM_CHECK_KEY));
        expectThrows(IllegalStateException.class, () -> steps.get(11).getNextStepKey());
        assertThat(((BranchingStep) steps.get(11)).getNextStepKeyOnFalse().name(), equalTo(SwapAliasesAndDeleteSourceIndexStep.NAME));
        assertThat(((BranchingStep) steps.get(11)).getNextStepKeyOnTrue().name(), equalTo(ReplaceDataStreamBackingIndexStep.NAME));

        assertTrue(steps.get(12) instanceof ReplaceDataStreamBackingIndexStep);
        assertThat(steps.get(12).getKey().name(), equalTo(ReplaceDataStreamBackingIndexStep.NAME));
        assertThat(steps.get(12).getNextStepKey().name(), equalTo(DeleteStep.NAME));

        assertTrue(steps.get(13) instanceof DeleteStep);
        assertThat(steps.get(13).getKey().name(), equalTo(DeleteStep.NAME));
        assertThat(steps.get(13).getNextStepKey(), equalTo(nextStepKey));

        assertTrue(steps.get(14) instanceof SwapAliasesAndDeleteSourceIndexStep);
        assertThat(steps.get(14).getKey().name(), equalTo(SwapAliasesAndDeleteSourceIndexStep.NAME));
        assertThat(steps.get(14).getNextStepKey(), equalTo(nextStepKey));
    }

    public void testDownsamplingPrerequisitesStep() {
        DateHistogramInterval fixedInterval = ConfigTestHelpers.randomInterval();
        DownsampleAction action = new DownsampleAction(fixedInterval, WAIT_TIMEOUT);
        String phase = randomAlphaOfLengthBetween(1, 10);
        StepKey nextStepKey = new StepKey(
            randomAlphaOfLengthBetween(1, 10),
            randomAlphaOfLengthBetween(1, 10),
            randomAlphaOfLengthBetween(1, 10)
        );
        {
            // non time series indices skip the action
            BranchingStep branchingStep = getFirstBranchingStep(action, phase, nextStepKey);
            IndexMetadata indexMetadata = newIndexMeta("test", Settings.EMPTY);

            ProjectState state = projectStateFromProject(ProjectMetadata.builder(randomProjectIdOrDefault()).put(indexMetadata, true));

            branchingStep.performAction(indexMetadata.getIndex(), state);
            assertThat(branchingStep.getNextStepKey(), is(nextStepKey));
        }
        {
            // time series indices execute the action
            BranchingStep branchingStep = getFirstBranchingStep(action, phase, nextStepKey);
            Settings settings = Settings.builder()
                .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
                .put("index.routing_path", "uid")
                .build();
            IndexMetadata indexMetadata = newIndexMeta("test", settings);

            ProjectState state = projectStateFromProject(ProjectMetadata.builder(randomProjectIdOrDefault()).put(indexMetadata, true));

            branchingStep.performAction(indexMetadata.getIndex(), state);
            assertThat(branchingStep.getNextStepKey().name(), is(CheckNotDataStreamWriteIndexStep.NAME));
        }
        {
            // already downsampled indices for the interval skip the action
            BranchingStep branchingStep = getFirstBranchingStep(action, phase, nextStepKey);
            Settings settings = Settings.builder()
                .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
                .put("index.routing_path", "uid")
                .put(IndexMetadata.INDEX_DOWNSAMPLE_STATUS_KEY, IndexMetadata.DownsampleTaskStatus.SUCCESS)
                .put(IndexMetadata.INDEX_DOWNSAMPLE_ORIGIN_NAME.getKey(), "test")
                .build();
            String indexName = DOWNSAMPLED_INDEX_PREFIX + fixedInterval + "-test";
            IndexMetadata indexMetadata = newIndexMeta(indexName, settings);

            ProjectState state = projectStateFromProject(ProjectMetadata.builder(randomProjectIdOrDefault()).put(indexMetadata, true));

            branchingStep.performAction(indexMetadata.getIndex(), state);
            assertThat(branchingStep.getNextStepKey(), is(nextStepKey));
        }
        {
            // indices with the same name as the target downsample index that are NOT downsample indices skip the action
            BranchingStep branchingStep = getFirstBranchingStep(action, phase, nextStepKey);
            String indexName = DOWNSAMPLED_INDEX_PREFIX + fixedInterval + "-test";
            IndexMetadata indexMetadata = newIndexMeta(indexName, Settings.EMPTY);

            ProjectState state = projectStateFromProject(ProjectMetadata.builder(randomProjectIdOrDefault()).put(indexMetadata, true));

            branchingStep.performAction(indexMetadata.getIndex(), state);
            assertThat(branchingStep.getNextStepKey(), is(nextStepKey));
        }
    }

    private static BranchingStep getFirstBranchingStep(DownsampleAction action, String phase, StepKey nextStepKey) {
        List<Step> steps = action.toSteps(null, phase, nextStepKey);
        assertNotNull(steps);
        assertEquals(15, steps.size());

        assertTrue(steps.get(0) instanceof BranchingStep);
        assertThat(steps.get(0).getKey().name(), equalTo(CONDITIONAL_TIME_SERIES_CHECK_KEY));

        return (BranchingStep) steps.get(0);
    }

    public static IndexMetadata newIndexMeta(String name, Settings indexSettings) {
        return IndexMetadata.builder(name).settings(indexSettings(IndexVersion.current(), 1, 1).put(indexSettings)).build();
    }

    public void testEqualsAndHashCode() {
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(createTestInstance(), this::copy, this::notCopy);
    }

    DownsampleAction copy(DownsampleAction downsampleAction) {
        return new DownsampleAction(downsampleAction.fixedInterval(), downsampleAction.waitTimeout());
    }

    DownsampleAction notCopy(DownsampleAction downsampleAction) {
        DateHistogramInterval fixedInterval = randomValueOtherThan(downsampleAction.fixedInterval(), ConfigTestHelpers::randomInterval);
        return new DownsampleAction(fixedInterval, WAIT_TIMEOUT);
    }
}
