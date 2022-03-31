/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.core.ilm.SearchableSnapshotAction.NAME;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class SearchableSnapshotActionTests extends AbstractActionTestCase<SearchableSnapshotAction> {

    @Override
    public void testToSteps() {
        String phase = randomBoolean() ? randomFrom(TimeseriesLifecycleType.ORDERED_VALID_PHASES) : randomAlphaOfLengthBetween(1, 10);
        SearchableSnapshotAction action = createTestInstance();
        StepKey nextStepKey = new StepKey(phase, randomAlphaOfLengthBetween(1, 5), randomAlphaOfLengthBetween(1, 5));

        List<Step> steps = action.toSteps(null, phase, nextStepKey, null);
        assertThat(steps.size(), is(action.isForceMergeIndex() ? 18 : 16));

        List<StepKey> expectedSteps = action.isForceMergeIndex()
            ? expectedStepKeysWithForceMerge(phase)
            : expectedStepKeysNoForceMerge(phase);

        assertThat(steps.get(0).getKey(), is(expectedSteps.get(0)));
        assertThat(steps.get(1).getKey(), is(expectedSteps.get(1)));
        assertThat(steps.get(2).getKey(), is(expectedSteps.get(2)));
        assertThat(steps.get(3).getKey(), is(expectedSteps.get(3)));
        assertThat(steps.get(4).getKey(), is(expectedSteps.get(4)));
        assertThat(steps.get(5).getKey(), is(expectedSteps.get(5)));
        assertThat(steps.get(6).getKey(), is(expectedSteps.get(6)));
        assertThat(steps.get(7).getKey(), is(expectedSteps.get(7)));
        assertThat(steps.get(8).getKey(), is(expectedSteps.get(8)));
        assertThat(steps.get(9).getKey(), is(expectedSteps.get(9)));
        assertThat(steps.get(10).getKey(), is(expectedSteps.get(10)));
        assertThat(steps.get(11).getKey(), is(expectedSteps.get(11)));
        assertThat(steps.get(12).getKey(), is(expectedSteps.get(12)));
        assertThat(steps.get(13).getKey(), is(expectedSteps.get(13)));
        assertThat(steps.get(14).getKey(), is(expectedSteps.get(14)));
        assertThat(steps.get(15).getKey(), is(expectedSteps.get(15)));

        if (action.isForceMergeIndex()) {
            assertThat(steps.get(16).getKey(), is(expectedSteps.get(16)));
            assertThat(steps.get(17).getKey(), is(expectedSteps.get(17)));
            CreateSnapshotStep createSnapshotStep = (CreateSnapshotStep) steps.get(8);
            assertThat(createSnapshotStep.getNextKeyOnIncomplete(), is(expectedSteps.get(7)));
            validateWaitForDataTierStep(phase, steps, 9, 10);
        } else {
            CreateSnapshotStep createSnapshotStep = (CreateSnapshotStep) steps.get(6);
            assertThat(createSnapshotStep.getNextKeyOnIncomplete(), is(expectedSteps.get(5)));
            validateWaitForDataTierStep(phase, steps, 7, 8);
        }
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

    public void testPrefixAndStorageTypeDefaults() {
        SearchableSnapshotAction action = new SearchableSnapshotAction("repo", randomBoolean());
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

    private List<StepKey> expectedStepKeysWithForceMerge(String phase) {
        return List.of(
            new StepKey(phase, NAME, SearchableSnapshotAction.CONDITIONAL_SKIP_ACTION_STEP),
            new StepKey(phase, NAME, CheckNotDataStreamWriteIndexStep.NAME),
            new StepKey(phase, NAME, WaitForNoFollowersStep.NAME),
            new StepKey(phase, NAME, SearchableSnapshotAction.CONDITIONAL_SKIP_GENERATE_AND_CLEAN),
            new StepKey(phase, NAME, ForceMergeStep.NAME),
            new StepKey(phase, NAME, SegmentCountStep.NAME),
            new StepKey(phase, NAME, GenerateSnapshotNameStep.NAME),
            new StepKey(phase, NAME, CleanupSnapshotStep.NAME),
            new StepKey(phase, NAME, CreateSnapshotStep.NAME),
            new StepKey(phase, NAME, WaitForDataTierStep.NAME),
            new StepKey(phase, NAME, MountSnapshotStep.NAME),
            new StepKey(phase, NAME, WaitForIndexColorStep.NAME),
            new StepKey(phase, NAME, CopyExecutionStateStep.NAME),
            new StepKey(phase, NAME, CopySettingsStep.NAME),
            new StepKey(phase, NAME, SearchableSnapshotAction.CONDITIONAL_DATASTREAM_CHECK_KEY),
            new StepKey(phase, NAME, ReplaceDataStreamBackingIndexStep.NAME),
            new StepKey(phase, NAME, DeleteStep.NAME),
            new StepKey(phase, NAME, SwapAliasesAndDeleteSourceIndexStep.NAME)
        );
    }

    private List<StepKey> expectedStepKeysNoForceMerge(String phase) {
        return List.of(
            new StepKey(phase, NAME, SearchableSnapshotAction.CONDITIONAL_SKIP_ACTION_STEP),
            new StepKey(phase, NAME, CheckNotDataStreamWriteIndexStep.NAME),
            new StepKey(phase, NAME, WaitForNoFollowersStep.NAME),
            new StepKey(phase, NAME, SearchableSnapshotAction.CONDITIONAL_SKIP_GENERATE_AND_CLEAN),
            new StepKey(phase, NAME, GenerateSnapshotNameStep.NAME),
            new StepKey(phase, NAME, CleanupSnapshotStep.NAME),
            new StepKey(phase, NAME, CreateSnapshotStep.NAME),
            new StepKey(phase, NAME, WaitForDataTierStep.NAME),
            new StepKey(phase, NAME, MountSnapshotStep.NAME),
            new StepKey(phase, NAME, WaitForIndexColorStep.NAME),
            new StepKey(phase, NAME, CopyExecutionStateStep.NAME),
            new StepKey(phase, NAME, CopySettingsStep.NAME),
            new StepKey(phase, NAME, SearchableSnapshotAction.CONDITIONAL_DATASTREAM_CHECK_KEY),
            new StepKey(phase, NAME, ReplaceDataStreamBackingIndexStep.NAME),
            new StepKey(phase, NAME, DeleteStep.NAME),
            new StepKey(phase, NAME, SwapAliasesAndDeleteSourceIndexStep.NAME)
        );
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
    protected SearchableSnapshotAction mutateInstance(SearchableSnapshotAction instance) throws IOException {
        return randomInstance();
    }

    static SearchableSnapshotAction randomInstance() {
        return new SearchableSnapshotAction(randomAlphaOfLengthBetween(5, 10), randomBoolean());
    }
}
