/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class ShrinkActionTests extends AbstractActionTestCase<ShrinkAction> {

    @Override
    protected ShrinkAction doParseInstance(XContentParser parser) throws IOException {
        return ShrinkAction.parse(parser);
    }

    @Override
    protected ShrinkAction createTestInstance() {
        return randomInstance();
    }

    static ShrinkAction randomInstance() {
        if (randomBoolean()) {
            return new ShrinkAction(randomIntBetween(1, 100), null);
        } else {
            return new ShrinkAction(null, new ByteSizeValue(randomIntBetween(1, 100)));
        }
    }

    @Override
    protected ShrinkAction mutateInstance(ShrinkAction action) {
        if (action.getNumberOfShards() != null) {
            return new ShrinkAction(action.getNumberOfShards() + randomIntBetween(1, 2), null);
        } else {
            return new ShrinkAction(null, new ByteSizeValue(action.getMaxPrimaryShardSize().getBytes() + 1));
        }
    }

    @Override
    protected Reader<ShrinkAction> instanceReader() {
        return ShrinkAction::new;
    }

    public void testNonPositiveShardNumber() {
        Exception e = expectThrows(Exception.class, () -> new ShrinkAction(randomIntBetween(-100, 0), null));
        assertThat(e.getMessage(), equalTo("[number_of_shards] must be greater than 0"));
    }

    public void testMaxPrimaryShardSize() {
        ByteSizeValue maxPrimaryShardSize1 = new ByteSizeValue(10);
        Exception e1 = expectThrows(Exception.class, () -> new ShrinkAction(randomIntBetween(1, 100), maxPrimaryShardSize1));
        assertThat(e1.getMessage(), equalTo("Cannot set both [number_of_shards] and [max_primary_shard_size]"));

        ByteSizeValue maxPrimaryShardSize2 = new ByteSizeValue(0);
        Exception e2 = expectThrows(Exception.class, () -> new ShrinkAction(null, maxPrimaryShardSize2));
        assertThat(e2.getMessage(), equalTo("[max_primary_shard_size] must be greater than 0"));
    }

    public void testPerformActionWithSkip() {
        String lifecycleName = randomAlphaOfLengthBetween(4, 10);
        int numberOfShards = randomIntBetween(1, 10);
        ShrinkAction action = new ShrinkAction(numberOfShards, null);
        String phase = randomAlphaOfLengthBetween(1, 10);
        StepKey nextStepKey = new StepKey(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10),
            randomAlphaOfLengthBetween(1, 10));
        List<Step> steps = action.toSteps(null, phase, nextStepKey);
        BranchingStep step = ((BranchingStep) steps.get(0));

        LifecyclePolicy policy = new LifecyclePolicy(lifecycleName, Collections.singletonMap("warm",
                new Phase("warm", TimeValue.ZERO, Collections.singletonMap(action.getWriteableName(), action))));
        LifecyclePolicyMetadata policyMetadata = new LifecyclePolicyMetadata(policy, Collections.emptyMap(),
            randomNonNegativeLong(), randomNonNegativeLong());
        String indexName = randomAlphaOfLength(5);
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metadata(Metadata.builder()
            .putCustom(IndexLifecycleMetadata.TYPE, new IndexLifecycleMetadata(
                Collections.singletonMap(policyMetadata.getName(), policyMetadata), OperationMode.RUNNING))
            .put(IndexMetadata.builder(indexName).settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, lifecycleName))
                .putCustom(LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY,
                    LifecycleExecutionState.builder()
                        .setPhase(step.getKey().getPhase())
                        .setPhaseTime(0L)
                        .setAction(step.getKey().getAction())
                        .setActionTime(0L)
                        .setStep(step.getKey().getName())
                        .setStepTime(0L)
                        .build().asMap())
                .numberOfShards(numberOfShards).numberOfReplicas(0))).build();
        step.performAction(state.metadata().index(indexName).getIndex(), state);
        assertThat(step.getNextStepKey(), equalTo(nextStepKey));
    }

    public void testPerformActionWithoutSkip() {
        int numShards = 6;
        int divisor = randomFrom(2, 3, 6);
        int expectedFinalShards = numShards / divisor;
        String lifecycleName = randomAlphaOfLengthBetween(4, 10);
        ShrinkAction action = new ShrinkAction(expectedFinalShards, null);
        String phase = randomAlphaOfLengthBetween(1, 10);
        StepKey nextStepKey = new StepKey(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10),
            randomAlphaOfLengthBetween(1, 10));
        List<Step> steps = action.toSteps(null, phase, nextStepKey);
        BranchingStep step = ((BranchingStep) steps.get(0));

        LifecyclePolicy policy = new LifecyclePolicy(lifecycleName, Collections.singletonMap("warm",
            new Phase("warm", TimeValue.ZERO, Collections.singletonMap(action.getWriteableName(), action))));
        LifecyclePolicyMetadata policyMetadata = new LifecyclePolicyMetadata(policy, Collections.emptyMap(),
            randomNonNegativeLong(), randomNonNegativeLong());
        String indexName = randomAlphaOfLength(5);
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metadata(Metadata.builder()
            .putCustom(IndexLifecycleMetadata.TYPE, new IndexLifecycleMetadata(
                Collections.singletonMap(policyMetadata.getName(), policyMetadata), OperationMode.RUNNING))
            .put(IndexMetadata.builder(indexName).settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, lifecycleName))
                .putCustom(LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY,
                    LifecycleExecutionState.builder()
                        .setPhase(step.getKey().getPhase())
                        .setPhaseTime(0L)
                        .setAction(step.getKey().getAction())
                        .setActionTime(0L)
                        .setStep(step.getKey().getName())
                        .setStepTime(0L)
                        .build().asMap())
                .numberOfShards(numShards).numberOfReplicas(0))).build();
        step.performAction(state.metadata().index(indexName).getIndex(), state);
        assertThat(step.getNextStepKey(), equalTo(steps.get(1).getKey()));
    }

    public void testToSteps() {
        ShrinkAction action = createTestInstance();
        String phase = randomAlphaOfLengthBetween(1, 10);
        StepKey nextStepKey = new StepKey(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10),
            randomAlphaOfLengthBetween(1, 10));
        List<Step> steps = action.toSteps(null, phase, nextStepKey);
        assertThat(steps.size(), equalTo(17));
        StepKey expectedFirstKey = new StepKey(phase, ShrinkAction.NAME, ShrinkAction.CONDITIONAL_SKIP_SHRINK_STEP);
        StepKey expectedSecondKey = new StepKey(phase, ShrinkAction.NAME, CheckNotDataStreamWriteIndexStep.NAME);
        StepKey expectedThirdKey = new StepKey(phase, ShrinkAction.NAME, WaitForNoFollowersStep.NAME);
        StepKey expectedFourthKey = new StepKey(phase, ShrinkAction.NAME, ReadOnlyAction.NAME);
        StepKey expectedFifthKey = new StepKey(phase, ShrinkAction.NAME, CheckTargetShardsCountStep.NAME);
        StepKey expectedSixthKey = new StepKey(phase, ShrinkAction.NAME, CleanupShrinkIndexStep.NAME);
        StepKey expectedSeventhKey = new StepKey(phase, ShrinkAction.NAME, GenerateUniqueIndexNameStep.NAME);
        StepKey expectedEighthKey = new StepKey(phase, ShrinkAction.NAME, SetSingleNodeAllocateStep.NAME);
        StepKey expectedNinthKey = new StepKey(phase, ShrinkAction.NAME, CheckShrinkReadyStep.NAME);
        StepKey expectedTenthKey = new StepKey(phase, ShrinkAction.NAME, ShrinkStep.NAME);
        StepKey expectedEleventhKey = new StepKey(phase, ShrinkAction.NAME, ShrunkShardsAllocatedStep.NAME);
        StepKey expectedTwelveKey = new StepKey(phase, ShrinkAction.NAME, CopyExecutionStateStep.NAME);
        StepKey expectedThirteenKey = new StepKey(phase, ShrinkAction.NAME, ShrinkAction.CONDITIONAL_DATASTREAM_CHECK_KEY);
        StepKey expectedFourteenKey = new StepKey(phase, ShrinkAction.NAME, ShrinkSetAliasStep.NAME);
        StepKey expectedFifteenKey = new StepKey(phase, ShrinkAction.NAME, ShrunkenIndexCheckStep.NAME);
        StepKey expectedSixteenKey = new StepKey(phase, ShrinkAction.NAME, ReplaceDataStreamBackingIndexStep.NAME);
        StepKey expectedSeventeenKey = new StepKey(phase, ShrinkAction.NAME, DeleteStep.NAME);

        assertTrue(steps.get(0) instanceof BranchingStep);
        assertThat(steps.get(0).getKey(), equalTo(expectedFirstKey));
        expectThrows(IllegalStateException.class, () -> steps.get(0).getNextStepKey());
        assertThat(((BranchingStep) steps.get(0)).getNextStepKeyOnFalse(), equalTo(expectedSecondKey));
        assertThat(((BranchingStep) steps.get(0)).getNextStepKeyOnTrue(), equalTo(nextStepKey));

        assertTrue(steps.get(1) instanceof CheckNotDataStreamWriteIndexStep);
        assertThat(steps.get(1).getKey(), equalTo(expectedSecondKey));
        assertThat(steps.get(1).getNextStepKey(), equalTo(expectedThirdKey));

        assertTrue(steps.get(2) instanceof WaitForNoFollowersStep);
        assertThat(steps.get(2).getKey(), equalTo(expectedThirdKey));
        assertThat(steps.get(2).getNextStepKey(), equalTo(expectedFourthKey));

        assertTrue(steps.get(3) instanceof ReadOnlyStep);
        assertThat(steps.get(3).getKey(), equalTo(expectedFourthKey));
        assertThat(steps.get(3).getNextStepKey(), equalTo(expectedFifthKey));

        assertTrue(steps.get(4) instanceof CheckTargetShardsCountStep);
        assertThat(steps.get(4).getKey(), equalTo(expectedFifthKey));
        assertThat(steps.get(4).getNextStepKey(), equalTo(expectedSixthKey));

        assertTrue(steps.get(5) instanceof CleanupShrinkIndexStep);
        assertThat(steps.get(5).getKey(), equalTo(expectedSixthKey));
        assertThat(steps.get(5).getNextStepKey(), equalTo(expectedSeventhKey));

        assertTrue(steps.get(6) instanceof GenerateUniqueIndexNameStep);
        assertThat(steps.get(6).getKey(), equalTo(expectedSeventhKey));
        assertThat(steps.get(6).getNextStepKey(), equalTo(expectedEighthKey));

        assertTrue(steps.get(7) instanceof SetSingleNodeAllocateStep);
        assertThat(steps.get(7).getKey(), equalTo(expectedEighthKey));
        assertThat(steps.get(7).getNextStepKey(), equalTo(expectedNinthKey));

        assertTrue(steps.get(8) instanceof ClusterStateWaitUntilThresholdStep);
        assertThat(((ClusterStateWaitUntilThresholdStep) steps.get(8)).getStepToExecute(), is(instanceOf(CheckShrinkReadyStep.class)));
        // assert in case the threshold is breached we go back to the "cleanup shrunk index" step
        assertThat(((ClusterStateWaitUntilThresholdStep) steps.get(8)).getNextKeyOnThreshold(), is(expectedEighthKey));
        assertThat(steps.get(8).getKey(), equalTo(expectedNinthKey));
        assertThat(steps.get(8).getNextStepKey(), equalTo(expectedTenthKey));

        assertTrue(steps.get(9) instanceof ShrinkStep);
        assertThat(steps.get(9).getKey(), equalTo(expectedTenthKey));
        assertThat(steps.get(9).getNextStepKey(), equalTo(expectedEleventhKey));

        assertTrue(steps.get(10) instanceof ClusterStateWaitUntilThresholdStep);
        assertThat(steps.get(10).getKey(), equalTo(expectedEleventhKey));
        assertThat(steps.get(10).getNextStepKey(), equalTo(expectedTwelveKey));
        assertThat(((ClusterStateWaitUntilThresholdStep) steps.get(10)).getStepToExecute(),
            is(instanceOf(ShrunkShardsAllocatedStep.class)));
        // assert in case the threshold is breached we go back to the "cleanup shrunk index" step
        assertThat(((ClusterStateWaitUntilThresholdStep) steps.get(10)).getNextKeyOnThreshold(), is(expectedSixthKey));

        assertTrue(steps.get(11) instanceof CopyExecutionStateStep);
        assertThat(steps.get(11).getKey(), equalTo(expectedTwelveKey));
        assertThat(steps.get(11).getNextStepKey(), equalTo(expectedThirteenKey));

        assertTrue(steps.get(12) instanceof BranchingStep);
        assertThat(steps.get(12).getKey(), equalTo(expectedThirteenKey));
        expectThrows(IllegalStateException.class, () -> steps.get(12).getNextStepKey());
        assertThat(((BranchingStep) steps.get(12)).getNextStepKeyOnFalse(), equalTo(expectedFourteenKey));
        assertThat(((BranchingStep) steps.get(12)).getNextStepKeyOnTrue(), equalTo(expectedSixteenKey));

        assertTrue(steps.get(13) instanceof ShrinkSetAliasStep);
        assertThat(steps.get(13).getKey(), equalTo(expectedFourteenKey));
        assertThat(steps.get(13).getNextStepKey(), equalTo(expectedFifteenKey));

        assertTrue(steps.get(14) instanceof ShrunkenIndexCheckStep);
        assertThat(steps.get(14).getKey(), equalTo(expectedFifteenKey));
        assertThat(steps.get(14).getNextStepKey(), equalTo(nextStepKey));

        assertTrue(steps.get(15) instanceof ReplaceDataStreamBackingIndexStep);
        assertThat(steps.get(15).getKey(), equalTo(expectedSixteenKey));
        assertThat(steps.get(15).getNextStepKey(), equalTo(expectedSeventeenKey));

        assertTrue(steps.get(16) instanceof DeleteStep);
        assertThat(steps.get(16).getKey(), equalTo(expectedSeventeenKey));
        assertThat(steps.get(16).getNextStepKey(), equalTo(expectedFifteenKey));
    }

    @Override
    protected boolean isSafeAction() {
        return false;
    }
}
