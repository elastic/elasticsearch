/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class ReadOnlyActionTests extends AbstractActionTestCase<ReadOnlyAction> {

    @Override
    protected ReadOnlyAction doParseInstance(XContentParser parser) {
        return ReadOnlyAction.parse(parser);
    }

    @Override
    protected ReadOnlyAction createTestInstance() {
        return new ReadOnlyAction();
    }

    @Override
    protected Reader<ReadOnlyAction> instanceReader() {
        return ReadOnlyAction::new;
    }

    public void testToSteps() {
        ReadOnlyAction action = createTestInstance();
        String phase = randomAlphaOfLengthBetween(1, 10);
        StepKey nextStepKey = new StepKey(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10),
                randomAlphaOfLengthBetween(1, 10));
        List<Step> steps = action.toSteps(null, phase, nextStepKey);
        assertNotNull(steps);
        assertEquals(1, steps.size());
        StepKey expectedFirstStepKey = new StepKey(phase, ReadOnlyAction.NAME, ReadOnlyAction.NAME);
        UpdateSettingsStep firstStep = (UpdateSettingsStep) steps.get(0);
        assertThat(firstStep.getKey(), equalTo(expectedFirstStepKey));
        assertThat(firstStep.getNextStepKey(), equalTo(nextStepKey));
        assertThat(firstStep.getSettings().size(), equalTo(1));
        assertTrue(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.get(firstStep.getSettings()));
    }

}
