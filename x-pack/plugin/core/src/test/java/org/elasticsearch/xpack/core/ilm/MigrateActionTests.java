/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.cluster.routing.allocation.DataTierAllocationDecider;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED;
import static org.elasticsearch.xpack.core.DataTier.DATA_COLD;
import static org.elasticsearch.xpack.core.DataTier.DATA_HOT;
import static org.elasticsearch.xpack.core.DataTier.DATA_WARM;
import static org.elasticsearch.xpack.core.ilm.MigrateAction.getPreferredTiersConfiguration;
import static org.elasticsearch.xpack.core.ilm.MigrateAction.skipMigrateAction;
import static org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType.COLD_PHASE;
import static org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType.DELETE_PHASE;
import static org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType.FROZEN_PHASE;
import static org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType.HOT_PHASE;
import static org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType.WARM_PHASE;
import static org.hamcrest.CoreMatchers.is;

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
        String phase = randomValueOtherThan(DELETE_PHASE, () -> randomFrom(TimeseriesLifecycleType.ORDERED_VALID_PHASES));
        StepKey nextStepKey = new StepKey(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10),
            randomAlphaOfLengthBetween(1, 10));
        {
            MigrateAction action = new MigrateAction();
            List<Step> steps = action.toSteps(null, phase, nextStepKey);
            assertNotNull(steps);
            assertEquals(3, steps.size());
            StepKey expectedFirstStepKey = new StepKey(phase, MigrateAction.NAME, MigrateAction.CONDITIONAL_SKIP_MIGRATE_STEP);
            StepKey expectedSecondStepKey = new StepKey(phase, MigrateAction.NAME, MigrateAction.NAME);
            StepKey expectedThirdStepKey = new StepKey(phase, MigrateAction.NAME, DataTierMigrationRoutedStep.NAME);
            BranchingStep firstStep = (BranchingStep) steps.get(0);
            UpdateSettingsStep secondStep = (UpdateSettingsStep) steps.get(1);
            DataTierMigrationRoutedStep thirdStep = (DataTierMigrationRoutedStep) steps.get(2);
            assertEquals(expectedFirstStepKey, firstStep.getKey());
            assertEquals(expectedSecondStepKey, secondStep.getKey());
            assertEquals(expectedThirdStepKey, secondStep.getNextStepKey());
            assertEquals(expectedThirdStepKey, thirdStep.getKey());
            assertEquals(nextStepKey, thirdStep.getNextStepKey());
        }

        {
            MigrateAction disabledMigrateAction = new MigrateAction(false);
            List<Step> steps = disabledMigrateAction.toSteps(null, phase, nextStepKey);
            assertEquals(0, steps.size());
        }
    }

    public void testGetPreferredTiersConfiguration() {
        assertThat(getPreferredTiersConfiguration(DATA_HOT), is(DATA_HOT));
        assertThat(getPreferredTiersConfiguration(DATA_WARM), is(DATA_WARM + "," + DATA_HOT));
        assertThat(getPreferredTiersConfiguration(DATA_COLD), is(DATA_COLD + "," + DATA_WARM + "," + DATA_HOT));
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> getPreferredTiersConfiguration("no_tier"));
        assertThat(exception.getMessage(), is("invalid data tier [no_tier]"));
    }

    public void testMigrateActionsConfiguresTierPreference() {
        StepKey nextStepKey = new StepKey(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10),
            randomAlphaOfLengthBetween(1, 10));
        MigrateAction action = new MigrateAction();
        {
            List<Step> steps = action.toSteps(null, HOT_PHASE, nextStepKey);
            UpdateSettingsStep firstStep = (UpdateSettingsStep) steps.get(1);
            assertThat(DataTierAllocationDecider.INDEX_ROUTING_PREFER_SETTING.get(firstStep.getSettings()),
                is(DATA_HOT));
        }
        {
            List<Step> steps = action.toSteps(null, WARM_PHASE, nextStepKey);
            UpdateSettingsStep firstStep = (UpdateSettingsStep) steps.get(1);
            assertThat(DataTierAllocationDecider.INDEX_ROUTING_PREFER_SETTING.get(firstStep.getSettings()),
                is(DATA_WARM + "," + DATA_HOT));
        }
        {
            List<Step> steps = action.toSteps(null, COLD_PHASE, nextStepKey);
            UpdateSettingsStep firstStep = (UpdateSettingsStep) steps.get(1);
            assertThat(DataTierAllocationDecider.INDEX_ROUTING_PREFER_SETTING.get(firstStep.getSettings()),
                is(DATA_COLD + "," + DATA_WARM + "," + DATA_HOT));
        }
    }

    public void testSkipMigrateAction() {
        IndexMetadata snappedIndex = IndexMetadata.builder("snapped_index")
            .settings(
                Settings.builder()
                    .put(LifecycleSettings.SNAPSHOT_INDEX_NAME, "snapped")
                    .put(SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(SETTING_NUMBER_OF_SHARDS, 1)
                    .put(SETTING_NUMBER_OF_REPLICAS, 0)
            )
            .build();

        IndexMetadata regularIndex = IndexMetadata.builder("regular_index")
            .settings(
                Settings.builder()
                    .put(SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(SETTING_NUMBER_OF_SHARDS, 1)
                    .put(SETTING_NUMBER_OF_REPLICAS, 0)
            )
            .build();

        {
            // migrate action is not skipped if the index is not a searchable snapshot
            Arrays.asList(HOT_PHASE, WARM_PHASE, COLD_PHASE, FROZEN_PHASE)
                .forEach(phase -> assertThat(skipMigrateAction(phase, regularIndex), is(false)));
        }

        {
            // migrate action is skipped if the index is a searchable snapshot for phases hot -> cold
            Arrays.asList(HOT_PHASE, WARM_PHASE, COLD_PHASE)
                .forEach(phase -> assertThat(skipMigrateAction(phase, snappedIndex), is(true)));
        }

        {
            // migrate action is never skipped for the frozen phase
            assertThat(skipMigrateAction(FROZEN_PHASE, snappedIndex), is(false));
            assertThat(skipMigrateAction(FROZEN_PHASE, regularIndex), is(false));
        }
    }

}
