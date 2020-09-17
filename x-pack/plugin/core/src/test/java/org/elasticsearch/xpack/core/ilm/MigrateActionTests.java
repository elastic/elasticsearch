/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType.DELETE_PHASE;

public class MigrateActionTests extends AbstractActionTestCase<MigrateAction> {

    @Override
    protected MigrateAction doParseInstance(XContentParser parser) throws IOException {
        return MigrateAction.parse(parser);
    }

    @Override
    protected MigrateAction createTestInstance() {
        return new MigrateAction();
    }

    @Override
    protected Reader<MigrateAction> instanceReader() {
        return MigrateAction::new;
    }

    public void testToSteps() {
        String phase = randomValueOtherThan(DELETE_PHASE, () -> randomFrom(TimeseriesLifecycleType.VALID_PHASES));
        StepKey nextStepKey = new StepKey(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10),
            randomAlphaOfLengthBetween(1, 10));
        {
            MigrateAction action = new MigrateAction();
            List<Step> steps = action.toSteps(null, phase, nextStepKey);
            assertNotNull(steps);
            assertEquals(2, steps.size());
            StepKey expectedFirstStepKey = new StepKey(phase, MigrateAction.NAME, MigrateAction.NAME);
            StepKey expectedSecondStepKey = new StepKey(phase, MigrateAction.NAME, DataTierMigrationRoutedStep.NAME);
            UpdateSettingsStep firstStep = (UpdateSettingsStep) steps.get(0);
            DataTierMigrationRoutedStep secondStep = (DataTierMigrationRoutedStep) steps.get(1);
            assertEquals(expectedFirstStepKey, firstStep.getKey());
            assertEquals(expectedSecondStepKey, firstStep.getNextStepKey());
            assertEquals(expectedSecondStepKey, secondStep.getKey());
            assertEquals(nextStepKey, secondStep.getNextStepKey());
        }

        {
            MigrateAction disabledMigrateAction = new MigrateAction(false);
            List<Step> steps = disabledMigrateAction.toSteps(null, phase, nextStepKey);
            assertEquals(0, steps.size());
        }
    }
}
