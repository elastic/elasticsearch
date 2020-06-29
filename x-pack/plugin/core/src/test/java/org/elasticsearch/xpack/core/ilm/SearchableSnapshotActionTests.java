/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.core.ilm.SearchableSnapshotAction.NAME;
import static org.hamcrest.Matchers.is;

public class SearchableSnapshotActionTests extends AbstractActionTestCase<SearchableSnapshotAction> {

    @Override
    public void testToSteps() {
        String phase = randomAlphaOfLengthBetween(1, 10);
        StepKey expectedFirstStep = new StepKey(phase, NAME, CheckNotDataStreamWriteIndexStep.NAME);
        StepKey expectedSecondStep = new StepKey(phase, NAME, WaitForNoFollowersStep.NAME);
        StepKey expectedThirdStep = new StepKey(phase, NAME, GenerateSnapshotNameStep.NAME);
        StepKey expectedFourthStep = new StepKey(phase, NAME, CleanupSnapshotStep.NAME);
        StepKey expectedFifthStep = new StepKey(phase, NAME, CreateSnapshotStep.NAME);
        StepKey expectedSixthStep = new StepKey(phase, NAME, MountSnapshotStep.NAME);
        StepKey expectedSeventhStep = new StepKey(phase, NAME, WaitForIndexColorStep.NAME);
        StepKey expectedEighthStep = new StepKey(phase, NAME, CopyExecutionStateStep.NAME);
        StepKey expectedNinthStep = new StepKey(phase, NAME, CopySettingsStep.NAME);
        StepKey expectedTenthStep = new StepKey(phase, NAME, SearchableSnapshotAction.CONDITIONAL_DATASTREAM_CHECK_KEY);
        StepKey expectedElevenStep = new StepKey(phase, NAME, ReplaceDataStreamBackingIndexStep.NAME);
        StepKey expectedTwelveStep = new StepKey(phase, NAME, DeleteStep.NAME);
        StepKey expectedThirteenStep = new StepKey(phase, NAME, SwapAliasesAndDeleteSourceIndexStep.NAME);

        SearchableSnapshotAction action = createTestInstance();
        StepKey nextStepKey = new StepKey(phase, randomAlphaOfLengthBetween(1, 5), randomAlphaOfLengthBetween(1, 5));

        List<Step> steps = action.toSteps(null, phase, nextStepKey);
        assertThat(steps.size(), is(13));

        assertThat(steps.get(0).getKey(), is(expectedFirstStep));
        assertThat(steps.get(1).getKey(), is(expectedSecondStep));
        assertThat(steps.get(2).getKey(), is(expectedThirdStep));
        assertThat(steps.get(3).getKey(), is(expectedFourthStep));
        assertThat(steps.get(4).getKey(), is(expectedFifthStep));
        assertThat(steps.get(5).getKey(), is(expectedSixthStep));
        assertThat(steps.get(6).getKey(), is(expectedSeventhStep));
        assertThat(steps.get(7).getKey(), is(expectedEighthStep));
        assertThat(steps.get(8).getKey(), is(expectedNinthStep));
        assertThat(steps.get(9).getKey(), is(expectedTenthStep));
        assertThat(steps.get(10).getKey(), is(expectedElevenStep));
        assertThat(steps.get(11).getKey(), is(expectedTwelveStep));
        assertThat(steps.get(12).getKey(), is(expectedThirteenStep));

        AsyncActionBranchingStep branchStep = (AsyncActionBranchingStep) steps.get(4);
        assertThat(branchStep.getNextKeyOnIncompleteResponse(), is(expectedFourthStep));
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
        return new SearchableSnapshotAction(randomAlphaOfLengthBetween(5, 10));
    }
}
