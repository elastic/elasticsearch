/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

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

    public void testPerformActionWithSkip() {
        String lifecycleName = randomAlphaOfLengthBetween(4, 10);
        int numberOfShards = randomIntBetween(1, 10);
        ShrinkAction action = new ShrinkAction(numberOfShards);
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
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metaData(MetaData.builder()
            .putCustom(IndexLifecycleMetadata.TYPE, new IndexLifecycleMetadata(
                Collections.singletonMap(policyMetadata.getName(), policyMetadata), OperationMode.RUNNING))
            .put(IndexMetaData.builder(indexName).settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, lifecycleName))
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
        step.performAction(state.metaData().index(indexName).getIndex(), state);
        assertThat(step.getNextStepKey(), equalTo(nextStepKey));
    }

    public void testPerformActionWithoutSkip() {
        int numShards = 6;
        int divisor = randomFrom(2, 3, 6);
        int expectedFinalShards = numShards / divisor;
        String lifecycleName = randomAlphaOfLengthBetween(4, 10);
        ShrinkAction action = new ShrinkAction(expectedFinalShards);
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
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metaData(MetaData.builder()
            .putCustom(IndexLifecycleMetadata.TYPE, new IndexLifecycleMetadata(
                Collections.singletonMap(policyMetadata.getName(), policyMetadata), OperationMode.RUNNING))
            .put(IndexMetaData.builder(indexName).settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, lifecycleName))
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
        ClusterState newState = step.performAction(state.metaData().index(indexName).getIndex(), state);
        assertThat(step.getNextStepKey(), equalTo(steps.get(1).getKey()));
    }

    public void testToSteps() {
        ShrinkAction action = createTestInstance();
        String phase = randomAlphaOfLengthBetween(1, 10);
        StepKey nextStepKey = new StepKey(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10),
            randomAlphaOfLengthBetween(1, 10));
        List<Step> steps = action.toSteps(null, phase, nextStepKey);
        assertThat(steps.size(), equalTo(10));
        StepKey expectedFirstKey = new StepKey(phase, ShrinkAction.NAME, BranchingStep.NAME);
        StepKey expectedSecondKey = new StepKey(phase, ShrinkAction.NAME, WaitForNoFollowersStep.NAME);
        StepKey expectedThirdKey = new StepKey(phase, ShrinkAction.NAME, ReadOnlyAction.NAME);
        StepKey expectedFourthKey = new StepKey(phase, ShrinkAction.NAME, SetSingleNodeAllocateStep.NAME);
        StepKey expectedFifthKey = new StepKey(phase, ShrinkAction.NAME, CheckShrinkReadyStep.NAME);
        StepKey expectedSixthKey = new StepKey(phase, ShrinkAction.NAME, ShrinkStep.NAME);
        StepKey expectedSeventhKey = new StepKey(phase, ShrinkAction.NAME, ShrunkShardsAllocatedStep.NAME);
        StepKey expectedEighthKey = new StepKey(phase, ShrinkAction.NAME, CopyExecutionStateStep.NAME);
        StepKey expectedNinthKey = new StepKey(phase, ShrinkAction.NAME, ShrinkSetAliasStep.NAME);
        StepKey expectedTenthKey = new StepKey(phase, ShrinkAction.NAME, ShrunkenIndexCheckStep.NAME);

        assertTrue(steps.get(0) instanceof BranchingStep);
        assertThat(steps.get(0).getKey(), equalTo(expectedFirstKey));
        expectThrows(IllegalStateException.class, () -> steps.get(0).getNextStepKey());
        assertThat(((BranchingStep) steps.get(0)).getNextStepKeyOnFalse(), equalTo(expectedSecondKey));
        assertThat(((BranchingStep) steps.get(0)).getNextStepKeyOnTrue(), equalTo(nextStepKey));

        assertTrue(steps.get(1) instanceof WaitForNoFollowersStep);
        assertThat(steps.get(1).getKey(), equalTo(expectedSecondKey));
        assertThat(steps.get(1).getNextStepKey(), equalTo(expectedThirdKey));

        assertTrue(steps.get(2) instanceof UpdateSettingsStep);
        assertThat(steps.get(2).getKey(), equalTo(expectedThirdKey));
        assertThat(steps.get(2).getNextStepKey(), equalTo(expectedFourthKey));
        assertTrue(IndexMetaData.INDEX_BLOCKS_WRITE_SETTING.get(((UpdateSettingsStep)steps.get(2)).getSettings()));

        assertTrue(steps.get(3) instanceof SetSingleNodeAllocateStep);
        assertThat(steps.get(3).getKey(), equalTo(expectedFourthKey));
        assertThat(steps.get(3).getNextStepKey(), equalTo(expectedFifthKey));

        assertTrue(steps.get(4) instanceof CheckShrinkReadyStep);
        assertThat(steps.get(4).getKey(), equalTo(expectedFifthKey));
        assertThat(steps.get(4).getNextStepKey(), equalTo(expectedSixthKey));

        assertTrue(steps.get(5) instanceof ShrinkStep);
        assertThat(steps.get(5).getKey(), equalTo(expectedSixthKey));
        assertThat(steps.get(5).getNextStepKey(), equalTo(expectedSeventhKey));
        assertThat(((ShrinkStep) steps.get(5)).getShrunkIndexPrefix(), equalTo(ShrinkAction.SHRUNKEN_INDEX_PREFIX));

        assertTrue(steps.get(6) instanceof ShrunkShardsAllocatedStep);
        assertThat(steps.get(6).getKey(), equalTo(expectedSeventhKey));
        assertThat(steps.get(6).getNextStepKey(), equalTo(expectedEighthKey));
        assertThat(((ShrunkShardsAllocatedStep) steps.get(6)).getShrunkIndexPrefix(), equalTo(ShrinkAction.SHRUNKEN_INDEX_PREFIX));

        assertTrue(steps.get(7) instanceof CopyExecutionStateStep);
        assertThat(steps.get(7).getKey(), equalTo(expectedEighthKey));
        assertThat(steps.get(7).getNextStepKey(), equalTo(expectedNinthKey));
        assertThat(((CopyExecutionStateStep) steps.get(7)).getShrunkIndexPrefix(), equalTo(ShrinkAction.SHRUNKEN_INDEX_PREFIX));

        assertTrue(steps.get(8) instanceof ShrinkSetAliasStep);
        assertThat(steps.get(8).getKey(), equalTo(expectedNinthKey));
        assertThat(steps.get(8).getNextStepKey(), equalTo(expectedTenthKey));
        assertThat(((ShrinkSetAliasStep) steps.get(8)).getShrunkIndexPrefix(), equalTo(ShrinkAction.SHRUNKEN_INDEX_PREFIX));

        assertTrue(steps.get(9) instanceof ShrunkenIndexCheckStep);
        assertThat(steps.get(9).getKey(), equalTo(expectedTenthKey));
        assertThat(steps.get(9).getNextStepKey(), equalTo(nextStepKey));
        assertThat(((ShrunkenIndexCheckStep) steps.get(9)).getShrunkIndexPrefix(), equalTo(ShrinkAction.SHRUNKEN_INDEX_PREFIX));
    }

    @Override
    protected boolean isSafeAction() {
        return false;
    }
}
