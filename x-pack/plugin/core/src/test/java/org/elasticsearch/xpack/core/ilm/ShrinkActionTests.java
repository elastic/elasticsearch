/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequestBuilder;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.client.internal.AdminClient;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.IndicesAdminClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;
import org.junit.Before;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class ShrinkActionTests extends AbstractActionTestCase<ShrinkAction> {

    private Client client;
    private IndicesAdminClient indicesClient;

    @Before
    public void setUpClient() throws Exception {
        super.setUp();
        client = Mockito.mock(Client.class);
        AdminClient adminClient = Mockito.mock(AdminClient.class);
        indicesClient = Mockito.mock(IndicesAdminClient.class);

        Mockito.when(client.admin()).thenReturn(adminClient);
        Mockito.when(adminClient.indices()).thenReturn(indicesClient);
    }

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
            return new ShrinkAction(randomIntBetween(1, 100), null, randomBoolean());
        } else {
            return new ShrinkAction(null, ByteSizeValue.ofBytes(randomIntBetween(1, 100)), randomBoolean());
        }
    }

    @Override
    protected ShrinkAction mutateInstance(ShrinkAction action) {
        Integer numberOfShards = action.getNumberOfShards();
        ByteSizeValue maxPrimaryShardSize = action.getMaxPrimaryShardSize();
        boolean allowWriteAfterShrink = action.getAllowWriteAfterShrink();

        switch (randomInt(2)) {
            case 0 -> {
                numberOfShards = randomValueOtherThan(numberOfShards, () -> randomIntBetween(1, 100));
                maxPrimaryShardSize = null;
            }
            case 1 -> {
                maxPrimaryShardSize = randomValueOtherThan(maxPrimaryShardSize, () -> ByteSizeValue.ofBytes(randomIntBetween(1, 100)));
                numberOfShards = null;
            }
            case 2 -> allowWriteAfterShrink = allowWriteAfterShrink == false;
        }
        return new ShrinkAction(numberOfShards, maxPrimaryShardSize, allowWriteAfterShrink);
    }

    @Override
    protected Reader<ShrinkAction> instanceReader() {
        return ShrinkAction::new;
    }

    public void testNonPositiveShardNumber() {
        Exception e = expectThrows(Exception.class, () -> new ShrinkAction(randomIntBetween(-100, 0), null, randomBoolean()));
        assertThat(e.getMessage(), equalTo("[number_of_shards] must be greater than 0"));
    }

    public void testMaxPrimaryShardSize() {
        ByteSizeValue maxPrimaryShardSize1 = ByteSizeValue.ofBytes(10);
        Exception e1 = expectThrows(
            Exception.class,
            () -> new ShrinkAction(randomIntBetween(1, 100), maxPrimaryShardSize1, randomBoolean())
        );
        assertThat(e1.getMessage(), equalTo("Cannot set both [number_of_shards] and [max_primary_shard_size]"));

        ByteSizeValue maxPrimaryShardSize2 = ByteSizeValue.ZERO;
        Exception e2 = expectThrows(Exception.class, () -> new ShrinkAction(null, maxPrimaryShardSize2, randomBoolean()));
        assertThat(e2.getMessage(), equalTo("[max_primary_shard_size] must be greater than 0"));
    }

    public void testPerformActionWithSkipBecauseOfShardNumber() throws InterruptedException {
        String lifecycleName = randomAlphaOfLengthBetween(4, 10);
        int numberOfShards = randomIntBetween(1, 10);
        ShrinkAction action = new ShrinkAction(numberOfShards, null, randomBoolean());
        StepKey nextStepKey = new StepKey(
            randomAlphaOfLengthBetween(1, 10),
            randomAlphaOfLengthBetween(1, 10),
            randomAlphaOfLengthBetween(1, 10)
        );
        String indexName = randomAlphaOfLength(5);
        IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexName)
            .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_NAME, lifecycleName))
            .numberOfShards(numberOfShards)
            .numberOfReplicas(0);
        assertPerformAction(lifecycleName, indexName, indexMetadataBuilder, action, nextStepKey, true, false);
    }

    public void testPerformActionWithSkipBecauseOfSearchableSnapshot() throws InterruptedException {
        String lifecycleName = randomAlphaOfLengthBetween(4, 10);
        int numberOfShards = randomIntBetween(1, 10);
        ShrinkAction action = new ShrinkAction(numberOfShards, null, randomBoolean());
        StepKey nextStepKey = new StepKey(
            randomAlphaOfLengthBetween(1, 10),
            randomAlphaOfLengthBetween(1, 10),
            randomAlphaOfLengthBetween(1, 10)
        );
        String indexName = randomAlphaOfLength(5);
        IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexName)
            .settings(
                settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_NAME, lifecycleName)
                    .put(LifecycleSettings.SNAPSHOT_INDEX_NAME, randomAlphaOfLength(10))
            )
            .numberOfShards(numberOfShards * 2)
            .numberOfReplicas(0);
        assertPerformAction(lifecycleName, indexName, indexMetadataBuilder, action, nextStepKey, true, false);
    }

    public void testPerformActionWithoutSkip() throws InterruptedException {
        int numShards = 6;
        int divisor = randomFrom(2, 3, 6);
        int expectedFinalShards = numShards / divisor;
        String lifecycleName = randomAlphaOfLengthBetween(4, 10);
        ShrinkAction action = new ShrinkAction(expectedFinalShards, null, randomBoolean());
        StepKey nextStepKey = new StepKey(
            randomAlphaOfLengthBetween(1, 10),
            randomAlphaOfLengthBetween(1, 10),
            randomAlphaOfLengthBetween(1, 10)
        );
        String indexName = randomAlphaOfLength(5);
        IndexMetadata.Builder indexMetadatBuilder = IndexMetadata.builder(indexName)
            .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_NAME, lifecycleName))
            .numberOfShards(numShards)
            .numberOfReplicas(0);
        assertPerformAction(lifecycleName, indexName, indexMetadatBuilder, action, nextStepKey, false, false);
    }

    public void testFailureIsPropagated() throws InterruptedException {
        String lifecycleName = randomAlphaOfLengthBetween(4, 10);
        int numberOfShards = randomIntBetween(1, 10);
        ShrinkAction action = new ShrinkAction(numberOfShards, null, randomBoolean());
        StepKey nextStepKey = new StepKey(
            randomAlphaOfLengthBetween(1, 10),
            randomAlphaOfLengthBetween(1, 10),
            randomAlphaOfLengthBetween(1, 10)
        );
        String indexName = randomAlphaOfLength(5);
        IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexName)
            .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_NAME, lifecycleName))
            .numberOfShards(numberOfShards)
            .numberOfReplicas(0);
        assertPerformAction(lifecycleName, indexName, indexMetadataBuilder, action, nextStepKey, true, true);
    }

    public void assertPerformAction(
        String lifecycleName,
        String indexName,
        IndexMetadata.Builder indexMetadataBuilder,
        ShrinkAction action,
        StepKey nextStepKey,
        boolean shouldSkip,
        boolean withError
    ) throws InterruptedException {
        String phase = randomAlphaOfLengthBetween(1, 10);
        List<Step> steps = action.toSteps(client, phase, nextStepKey);
        AsyncBranchingStep branchStep = ((AsyncBranchingStep) steps.get(0));

        LifecyclePolicy policy = new LifecyclePolicy(
            lifecycleName,
            Map.of("warm", new Phase("warm", TimeValue.ZERO, Map.of(action.getWriteableName(), action)))
        );
        LifecyclePolicyMetadata policyMetadata = new LifecyclePolicyMetadata(
            policy,
            Map.of(),
            randomNonNegativeLong(),
            randomNonNegativeLong()
        );
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(
                Metadata.builder()
                    .putCustom(
                        IndexLifecycleMetadata.TYPE,
                        new IndexLifecycleMetadata(Map.of(policyMetadata.getName(), policyMetadata), OperationMode.RUNNING)
                    )
                    .put(
                        indexMetadataBuilder.putCustom(
                            LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY,
                            LifecycleExecutionState.builder()
                                .setPhase(branchStep.getKey().phase())
                                .setPhaseTime(0L)
                                .setAction(branchStep.getKey().action())
                                .setActionTime(0L)
                                .setStep(branchStep.getKey().name())
                                .setStepTime(0L)
                                .build()
                                .asMap()
                        )
                    )
            )
            .build();
        setUpIndicesStatsRequestMock(indexName, withError);
        CountDownLatch countDownLatch = new CountDownLatch(1);
        AtomicBoolean failurePropagated = new AtomicBoolean(false);
        branchStep.performAction(state.metadata().getProject().index(indexName), state, null, new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {
                countDownLatch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                if (withError) {
                    assertThat(e.getMessage(), equalTo("simulated"));
                    failurePropagated.set(true);
                    countDownLatch.countDown();
                } else {
                    fail("Unexpected exception: " + e.getMessage());
                }
            }
        });
        assertTrue(countDownLatch.await(5, TimeUnit.SECONDS));

        if (withError) {
            assertTrue(failurePropagated.get());
        } else if (shouldSkip) {
            if (action.getAllowWriteAfterShrink()) {
                Step lastStep = steps.get(steps.size() - 1);
                assertThat(branchStep.getNextStepKey(), equalTo(lastStep.getKey()));
            } else {
                assertThat(branchStep.getNextStepKey(), equalTo(nextStepKey));
            }
        } else {
            assertThat(branchStep.getNextStepKey(), equalTo(steps.get(1).getKey()));
        }
    }

    public void testToSteps() {
        ShrinkAction action = createTestInstance();
        String phase = randomAlphaOfLengthBetween(1, 10);
        StepKey nextStepKey = new StepKey(
            randomAlphaOfLengthBetween(1, 10),
            randomAlphaOfLengthBetween(1, 10),
            randomAlphaOfLengthBetween(1, 10)
        );
        List<Step> steps = action.toSteps(client, phase, nextStepKey);
        assertThat(steps.size(), equalTo(action.getAllowWriteAfterShrink() ? 19 : 18));
        StepKey expectedFirstKey = new StepKey(phase, ShrinkAction.NAME, ShrinkAction.CONDITIONAL_SKIP_SHRINK_STEP);
        StepKey expectedSecondKey = new StepKey(phase, ShrinkAction.NAME, CheckNotDataStreamWriteIndexStep.NAME);
        StepKey expectedThirdKey = new StepKey(phase, ShrinkAction.NAME, WaitForNoFollowersStep.NAME);
        StepKey expectedFourthKey = new StepKey(phase, ShrinkAction.NAME, WaitUntilTimeSeriesEndTimePassesStep.NAME);
        StepKey expectedFifthKey = new StepKey(phase, ShrinkAction.NAME, ReadOnlyAction.NAME);
        StepKey expectedSixthKey = new StepKey(phase, ShrinkAction.NAME, CheckTargetShardsCountStep.NAME);
        StepKey expectedSeventhKey = new StepKey(phase, ShrinkAction.NAME, CleanupShrinkIndexStep.NAME);
        StepKey expectedEighthKey = new StepKey(phase, ShrinkAction.NAME, GenerateUniqueIndexNameStep.NAME);
        StepKey expectedNinthKey = new StepKey(phase, ShrinkAction.NAME, SetSingleNodeAllocateStep.NAME);
        StepKey expectedTenthKey = new StepKey(phase, ShrinkAction.NAME, CheckShrinkReadyStep.NAME);
        StepKey expectedEleventhKey = new StepKey(phase, ShrinkAction.NAME, ShrinkStep.NAME);
        StepKey expectedTwelveKey = new StepKey(phase, ShrinkAction.NAME, ShrunkShardsAllocatedStep.NAME);
        StepKey expectedThirteenKey = new StepKey(phase, ShrinkAction.NAME, CopyExecutionStateStep.NAME);
        StepKey expectedFourteenKey = new StepKey(phase, ShrinkAction.NAME, ShrinkAction.CONDITIONAL_DATASTREAM_CHECK_KEY);
        StepKey expectedFifteenKey = new StepKey(phase, ShrinkAction.NAME, ShrinkSetAliasStep.NAME);
        StepKey expectedSixteenKey = new StepKey(phase, ShrinkAction.NAME, ShrunkenIndexCheckStep.NAME);
        StepKey expectedSeventeenKey = new StepKey(phase, ShrinkAction.NAME, ReplaceDataStreamBackingIndexStep.NAME);
        StepKey expectedEighteenKey = new StepKey(phase, ShrinkAction.NAME, DeleteStep.NAME);
        StepKey expectedNineteenthKey = new StepKey(phase, ShrinkAction.NAME, UpdateSettingsStep.NAME);

        assertTrue(steps.get(0) instanceof AsyncBranchingStep);
        assertThat(steps.get(0).getKey(), equalTo(expectedFirstKey));
        expectThrows(IllegalStateException.class, () -> steps.get(0).getNextStepKey());
        assertThat(((AsyncBranchingStep) steps.get(0)).getNextStepKeyOnFalse(), equalTo(expectedSecondKey));
        assertThat(
            ((AsyncBranchingStep) steps.get(0)).getNextStepKeyOnTrue(),
            equalTo(action.getAllowWriteAfterShrink() ? expectedNineteenthKey : nextStepKey)
        );

        assertTrue(steps.get(1) instanceof CheckNotDataStreamWriteIndexStep);
        assertThat(steps.get(1).getKey(), equalTo(expectedSecondKey));
        assertThat(steps.get(1).getNextStepKey(), equalTo(expectedThirdKey));

        assertTrue(steps.get(2) instanceof WaitForNoFollowersStep);
        assertThat(steps.get(2).getKey(), equalTo(expectedThirdKey));
        assertThat(steps.get(2).getNextStepKey(), equalTo(expectedFourthKey));

        assertTrue(steps.get(3) instanceof WaitUntilTimeSeriesEndTimePassesStep);
        assertThat(steps.get(3).getKey(), equalTo(expectedFourthKey));
        assertThat(steps.get(3).getNextStepKey(), equalTo(expectedFifthKey));

        assertTrue(steps.get(4) instanceof ReadOnlyStep);
        assertThat(steps.get(4).getKey(), equalTo(expectedFifthKey));
        assertThat(steps.get(4).getNextStepKey(), equalTo(expectedSixthKey));

        assertTrue(steps.get(5) instanceof CheckTargetShardsCountStep);
        assertThat(steps.get(5).getKey(), equalTo(expectedSixthKey));
        assertThat(steps.get(5).getNextStepKey(), equalTo(expectedSeventhKey));

        assertTrue(steps.get(6) instanceof CleanupShrinkIndexStep);
        assertThat(steps.get(6).getKey(), equalTo(expectedSeventhKey));
        assertThat(steps.get(6).getNextStepKey(), equalTo(expectedEighthKey));

        assertTrue(steps.get(7) instanceof GenerateUniqueIndexNameStep);
        assertThat(steps.get(7).getKey(), equalTo(expectedEighthKey));
        assertThat(steps.get(7).getNextStepKey(), equalTo(expectedNinthKey));

        assertTrue(steps.get(8) instanceof SetSingleNodeAllocateStep);
        assertThat(steps.get(8).getKey(), equalTo(expectedNinthKey));
        assertThat(steps.get(8).getNextStepKey(), equalTo(expectedTenthKey));

        assertTrue(steps.get(9) instanceof ClusterStateWaitUntilThresholdStep);
        assertThat(((ClusterStateWaitUntilThresholdStep) steps.get(9)).getStepToExecute(), is(instanceOf(CheckShrinkReadyStep.class)));
        // assert in case the threshold is breached we go back to the "cleanup shrunk index" step
        assertThat(((ClusterStateWaitUntilThresholdStep) steps.get(9)).getNextKeyOnThreshold(), is(expectedNinthKey));
        assertThat(steps.get(9).getKey(), equalTo(expectedTenthKey));
        assertThat(steps.get(9).getNextStepKey(), equalTo(expectedEleventhKey));

        assertTrue(steps.get(10) instanceof ShrinkStep);
        assertThat(steps.get(10).getKey(), equalTo(expectedEleventhKey));
        assertThat(steps.get(10).getNextStepKey(), equalTo(expectedTwelveKey));

        assertTrue(steps.get(11) instanceof ClusterStateWaitUntilThresholdStep);
        assertThat(steps.get(11).getKey(), equalTo(expectedTwelveKey));
        assertThat(steps.get(11).getNextStepKey(), equalTo(expectedThirteenKey));
        assertThat(
            ((ClusterStateWaitUntilThresholdStep) steps.get(11)).getStepToExecute(),
            is(instanceOf(ShrunkShardsAllocatedStep.class))
        );
        // assert in case the threshold is breached we go back to the "cleanup shrunk index" step
        assertThat(((ClusterStateWaitUntilThresholdStep) steps.get(11)).getNextKeyOnThreshold(), is(expectedSeventhKey));

        assertTrue(steps.get(12) instanceof CopyExecutionStateStep);
        assertThat(steps.get(12).getKey(), equalTo(expectedThirteenKey));
        assertThat(steps.get(12).getNextStepKey(), equalTo(expectedFourteenKey));

        assertTrue(steps.get(13) instanceof BranchingStep);
        assertThat(steps.get(13).getKey(), equalTo(expectedFourteenKey));
        expectThrows(IllegalStateException.class, () -> steps.get(13).getNextStepKey());
        assertThat(((BranchingStep) steps.get(13)).getNextStepKeyOnFalse(), equalTo(expectedFifteenKey));
        assertThat(((BranchingStep) steps.get(13)).getNextStepKeyOnTrue(), equalTo(expectedSeventeenKey));

        assertTrue(steps.get(14) instanceof ShrinkSetAliasStep);
        assertThat(steps.get(14).getKey(), equalTo(expectedFifteenKey));
        assertThat(steps.get(14).getNextStepKey(), equalTo(expectedSixteenKey));

        assertTrue(steps.get(15) instanceof ShrunkenIndexCheckStep);
        assertThat(steps.get(15).getKey(), equalTo(expectedSixteenKey));
        assertThat(steps.get(15).getNextStepKey(), equalTo(action.getAllowWriteAfterShrink() ? expectedNineteenthKey : nextStepKey));

        assertTrue(steps.get(16) instanceof ReplaceDataStreamBackingIndexStep);
        assertThat(steps.get(16).getKey(), equalTo(expectedSeventeenKey));
        assertThat(steps.get(16).getNextStepKey(), equalTo(expectedEighteenKey));

        assertTrue(steps.get(17) instanceof DeleteStep);
        assertThat(steps.get(17).getKey(), equalTo(expectedEighteenKey));
        assertThat(steps.get(17).getNextStepKey(), equalTo(expectedSixteenKey));

        if (action.getAllowWriteAfterShrink()) {
            assertTrue(steps.get(18) instanceof UpdateSettingsStep);
            assertThat(steps.get(18).getKey(), equalTo(expectedNineteenthKey));
            assertThat(steps.get(18).getNextStepKey(), equalTo(nextStepKey));
        }
    }

    private void setUpIndicesStatsRequestMock(String index, boolean withError) {
        IndicesStatsRequestBuilder builder = Mockito.mock(IndicesStatsRequestBuilder.class);
        IndicesStatsResponse response = Mockito.mock(IndicesStatsResponse.class);
        CommonStats commonStats = Mockito.mock(CommonStats.class);
        Mockito.when(response.getPrimaries()).thenReturn(commonStats);
        Mockito.doAnswer(invocation -> {
            String request = (String) invocation.getArguments()[0];
            assertNotNull(request);
            assertEquals(index, request);
            return builder;
        }).when(indicesClient).prepareStats(Mockito.any());
        Mockito.when(builder.clear()).thenReturn(builder);
        Mockito.when(builder.setDocs(true)).thenReturn(builder);
        Mockito.when(builder.setStore(true)).thenReturn(builder);
        Mockito.doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<IndicesStatsResponse> listener = (ActionListener<IndicesStatsResponse>) invocation.getArguments()[0];
            assertNotNull(listener);
            if (withError) {
                listener.onFailure(new RuntimeException("simulated"));
            } else {
                listener.onResponse(response);
            }
            return null;
        }).when(builder).execute(Mockito.any());
    }

    @Override
    protected boolean isSafeAction() {
        return false;
    }
}
