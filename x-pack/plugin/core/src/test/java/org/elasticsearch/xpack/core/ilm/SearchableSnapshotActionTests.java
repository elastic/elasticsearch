/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.core.ilm.SearchableSnapshotAction.NAME;
import static org.elasticsearch.xpack.core.ilm.SearchableSnapshotAction.TOTAL_SHARDS_PER_NODE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class SearchableSnapshotActionTests extends AbstractActionTestCase<SearchableSnapshotAction> {

    @Override
    public void testToSteps() {
        String phase = randomBoolean() ? randomFrom(TimeseriesLifecycleType.ORDERED_VALID_PHASES) : randomAlphaOfLengthBetween(1, 10);
        SearchableSnapshotAction action = createTestInstance();
        StepKey nextStepKey = new StepKey(phase, randomAlphaOfLengthBetween(1, 5), randomAlphaOfLengthBetween(1, 5));

        List<Step> steps = action.toSteps(null, phase, nextStepKey, null);

        List<StepKey> expectedSteps = expectedStepKeys(
            phase,
            action.isForceMergeIndex(),
            action.getReplicateFor() != null,
            action.isForceMergeOnClone()
        );

        assertThat(steps.size(), is(expectedSteps.size()));
        for (int i = 0; i < expectedSteps.size(); i++) {
            assertThat("steps match expectation at index " + i, steps.get(i).getKey(), is(expectedSteps.get(i)));
        }

        int index = -1;
        for (int i = 0; i < expectedSteps.size(); i++) {
            if (expectedSteps.get(i).name().equals(CreateSnapshotStep.NAME)) {
                index = i;
                break;
            }
        }
        CreateSnapshotStep createSnapshotStep = (CreateSnapshotStep) steps.get(index);
        assertThat(createSnapshotStep.getNextKeyOnIncomplete(), is(expectedSteps.get(index - 1)));
        validateWaitForDataTierStep(phase, steps, index + 1, index + 2);
        validateForceMergeClone(action.isForceMergeIndex(), action.isForceMergeOnClone(), steps);
    }

    private void validateWaitForDataTierStep(String phase, List<Step> steps, int waitForDataTierStepIndex, int mountStepIndex) {
        WaitForDataTierStep waitForDataTierStep = (WaitForDataTierStep) steps.get(waitForDataTierStepIndex);
        if (phase.equals(TimeseriesLifecycleType.HOT_PHASE)) {
            assertThat(waitForDataTierStep.tierPreference(), equalTo(DataTier.DATA_HOT));
        } else {
            MountSnapshotStep mountStep = (MountSnapshotStep) steps.get(mountStepIndex);
            assertThat(waitForDataTierStep.tierPreference(), equalTo(mountStep.getStorage().defaultDataTiersPreference()));
        }
    }

    /**
     * Validate that the {@link ResizeIndexStep} used to clone the index for force merging configures the target index with 0 replicas.
     */
    private void validateForceMergeClone(boolean isForceMergeIndex, Boolean isForceMergeOnClone, List<Step> steps) {
        if (isForceMergeIndex == false || (isForceMergeOnClone != null && isForceMergeOnClone == false)) {
            return;
        }
        ResizeIndexStep cloneStep = (ResizeIndexStep) steps.stream()
            .filter(step -> step instanceof ResizeIndexStep)
            .findFirst()
            .orElseThrow();
        assertThat(cloneStep.getResizeType(), is(ResizeType.CLONE));
        var indexMetadata = IndexMetadata.builder(randomAlphaOfLength(5))
            .settings(indexSettings(IndexVersion.current(), randomIntBetween(1, 5), randomIntBetween(0, 5)))
            .build();
        Settings cloneIndexSettings = cloneStep.getTargetIndexSettingsSupplier().apply(indexMetadata);
        assertThat(cloneIndexSettings.getAsInt(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, -1), is(0));
    }

    public void testPrefixAndStorageTypeDefaults() {
        StepKey nonFrozenKey = new StepKey(randomFrom("hot", "warm", "cold", "delete"), randomAlphaOfLength(5), randomAlphaOfLength(5));
        StepKey frozenKey = new StepKey("frozen", randomAlphaOfLength(5), randomAlphaOfLength(5));

        assertThat(
            SearchableSnapshotAction.getConcreteStorageType(nonFrozenKey),
            equalTo(MountSearchableSnapshotRequest.Storage.FULL_COPY)
        );
        assertThat(
            SearchableSnapshotAction.getRestoredIndexPrefix(nonFrozenKey),
            equalTo(SearchableSnapshotAction.FULL_RESTORED_INDEX_PREFIX)
        );

        assertThat(
            SearchableSnapshotAction.getConcreteStorageType(frozenKey),
            equalTo(MountSearchableSnapshotRequest.Storage.SHARED_CACHE)
        );
        assertThat(
            SearchableSnapshotAction.getRestoredIndexPrefix(frozenKey),
            equalTo(SearchableSnapshotAction.PARTIAL_RESTORED_INDEX_PREFIX)
        );
    }

    public void testCreateWithInvalidTotalShardsPerNode() {
        int invalidTotalShardsPerNode = randomIntBetween(-100, 0);

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new SearchableSnapshotAction("test", true, invalidTotalShardsPerNode, null, null)
        );
        assertEquals("[" + TOTAL_SHARDS_PER_NODE.getPreferredName() + "] must be >= 1", exception.getMessage());
    }

    private List<StepKey> expectedStepKeys(String phase, boolean forceMergeIndex, boolean hasReplicateFor, Boolean forceMergeOnClone) {
        final var shouldForceMergeOnClone = forceMergeOnClone != null ? forceMergeOnClone : forceMergeIndex;
        return Stream.of(
            new StepKey(phase, NAME, SearchableSnapshotAction.CONDITIONAL_SKIP_ACTION_STEP),
            new StepKey(phase, NAME, CheckNotDataStreamWriteIndexStep.NAME),
            new StepKey(phase, NAME, WaitForNoFollowersStep.NAME),
            new StepKey(phase, NAME, WaitUntilTimeSeriesEndTimePassesStep.NAME),
            new StepKey(phase, NAME, SearchableSnapshotAction.CONDITIONAL_SKIP_GENERATE_AND_CLEAN),
            shouldForceMergeOnClone ? new StepKey(phase, NAME, SearchableSnapshotAction.CONDITIONAL_SKIP_CLONE_STEP) : null,
            shouldForceMergeOnClone ? new StepKey(phase, NAME, ReadOnlyStep.NAME) : null,
            shouldForceMergeOnClone ? new StepKey(phase, NAME, CleanupGeneratedIndexStep.NAME) : null,
            shouldForceMergeOnClone ? new StepKey(phase, NAME, GenerateUniqueIndexNameStep.NAME) : null,
            shouldForceMergeOnClone ? new StepKey(phase, NAME, ResizeIndexStep.CLONE) : null,
            shouldForceMergeOnClone ? new StepKey(phase, NAME, SearchableSnapshotAction.WAIT_FOR_CLONED_INDEX_GREEN) : null,
            forceMergeIndex ? new StepKey(phase, NAME, ForceMergeStep.NAME) : null,
            forceMergeIndex ? new StepKey(phase, NAME, SegmentCountStep.NAME) : null,
            new StepKey(phase, NAME, GenerateSnapshotNameStep.NAME),
            new StepKey(phase, NAME, CleanupSnapshotStep.NAME),
            new StepKey(phase, NAME, CreateSnapshotStep.NAME),
            new StepKey(phase, NAME, WaitForDataTierStep.NAME),
            new StepKey(phase, NAME, MountSnapshotStep.NAME),
            new StepKey(phase, NAME, WaitForIndexColorStep.NAME),
            new StepKey(phase, NAME, CopyExecutionStateStep.NAME),
            new StepKey(phase, NAME, CopySettingsStep.NAME),
            hasReplicateFor ? new StepKey(phase, NAME, WaitUntilReplicateForTimePassesStep.NAME) : null,
            hasReplicateFor ? new StepKey(phase, NAME, UpdateSettingsStep.NAME) : null,
            forceMergeIndex ? new StepKey(phase, NAME, SearchableSnapshotAction.CONDITIONAL_DELETE_FORCE_MERGED_INDEX_KEY) : null,
            forceMergeIndex ? new StepKey(phase, NAME, SearchableSnapshotAction.DELETE_FORCE_MERGED_INDEX_KEY) : null,
            new StepKey(phase, NAME, SearchableSnapshotAction.CONDITIONAL_DATASTREAM_CHECK_KEY),
            new StepKey(phase, NAME, ReplaceDataStreamBackingIndexStep.NAME),
            new StepKey(phase, NAME, DeleteStep.NAME),
            new StepKey(phase, NAME, SwapAliasesAndDeleteSourceIndexStep.NAME)
        ).filter(Objects::nonNull).toList();
    }

    @Override
    protected SearchableSnapshotAction doParseInstance(XContentParser parser) throws IOException {
        return SearchableSnapshotAction.parse(parser);
    }

    @Override
    protected SearchableSnapshotAction createTestInstance() {
        return randomInstance();
    }

    @Override
    protected Writeable.Reader<SearchableSnapshotAction> instanceReader() {
        return SearchableSnapshotAction::new;
    }

    @Override
    protected SearchableSnapshotAction mutateInstance(SearchableSnapshotAction instance) {
        var snapshotRepository = instance.getSnapshotRepository();
        var forceMergeIndex = instance.isForceMergeIndex();
        var totalShardsPerNode = instance.getTotalShardsPerNode();
        var replicateFor = instance.getReplicateFor();
        var forceMergeOnClone = instance.isForceMergeOnClone();
        switch (randomIntBetween(0, 4)) {
            case 0 -> snapshotRepository = randomAlphaOfLengthBetween(5, 10);
            case 1 -> {
                forceMergeIndex = forceMergeIndex == false;
                if (forceMergeIndex == false) {
                    forceMergeOnClone = null;
                }
            }
            case 2 -> totalShardsPerNode = totalShardsPerNode == null ? 1 : totalShardsPerNode + randomIntBetween(1, 100);
            case 3 -> replicateFor = replicateFor == null
                ? TimeValue.timeValueDays(1)
                : TimeValue.timeValueDays(replicateFor.getDays() + randomIntBetween(1, 10));
            case 4 -> {
                if (forceMergeOnClone == null) {
                    forceMergeOnClone = randomBoolean();
                } else {
                    forceMergeOnClone = randomBoolean() ? null : forceMergeOnClone == false;
                }
                if (forceMergeOnClone != null) {
                    forceMergeIndex = true;
                }
            }
            default -> throw new IllegalArgumentException("Invalid mutation branch");
        }
        return new SearchableSnapshotAction(snapshotRepository, forceMergeIndex, totalShardsPerNode, replicateFor, forceMergeOnClone);
    }

    static SearchableSnapshotAction randomInstance() {
        final var forceMergeIndex = randomBoolean();
        return new SearchableSnapshotAction(
            randomAlphaOfLengthBetween(5, 10),
            forceMergeIndex,
            (randomBoolean() ? null : randomIntBetween(1, 100)),
            (randomBoolean() ? null : TimeValue.timeValueDays(randomIntBetween(1, 10))),
            forceMergeIndex && randomBoolean() ? randomBoolean() : null
        );
    }
}
