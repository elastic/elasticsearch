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
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.cluster.routing.allocation.DataTier.DATA_COLD;
import static org.elasticsearch.cluster.routing.allocation.DataTier.DATA_HOT;
import static org.elasticsearch.cluster.routing.allocation.DataTier.DATA_WARM;
import static org.elasticsearch.index.IndexModule.INDEX_STORE_TYPE_SETTING;
import static org.elasticsearch.snapshots.SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOT_STORE_TYPE;
import static org.elasticsearch.snapshots.SearchableSnapshotsSettings.SNAPSHOT_PARTIAL_SETTING;
import static org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType.COLD_PHASE;
import static org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType.DELETE_PHASE;
import static org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType.HOT_PHASE;
import static org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType.WARM_PHASE;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.equalTo;

public class MigrateActionTests extends AbstractActionTestCase<MigrateAction> {

    @Override
    protected MigrateAction doParseInstance(XContentParser parser) throws IOException {
        return MigrateAction.parse(parser);
    }

    @Override
    protected MigrateAction createTestInstance() {
        return randomBoolean() ? MigrateAction.ENABLED : MigrateAction.DISABLED;
    }

    @Override
    protected MigrateAction mutateInstance(MigrateAction instance) {
        return instance.isEnabled() == false ? MigrateAction.ENABLED : MigrateAction.DISABLED;
    }

    @Override
    protected Reader<MigrateAction> instanceReader() {
        return MigrateAction::readFrom;
    }

    public void testToSteps() {
        String phase = randomValueOtherThan(DELETE_PHASE, () -> randomFrom(TimeseriesLifecycleType.ORDERED_VALID_PHASES));
        StepKey nextStepKey = new StepKey(
            randomAlphaOfLengthBetween(1, 10),
            randomAlphaOfLengthBetween(1, 10),
            randomAlphaOfLengthBetween(1, 10)
        );
        {
            List<Step> steps = MigrateAction.ENABLED.toSteps(null, phase, nextStepKey);
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
            List<Step> steps = MigrateAction.DISABLED.toSteps(null, phase, nextStepKey);
            assertEquals(0, steps.size());
        }
    }

    public void testMigrateActionsConfiguresTierPreference() {
        StepKey nextStepKey = new StepKey(
            randomAlphaOfLengthBetween(1, 10),
            randomAlphaOfLengthBetween(1, 10),
            randomAlphaOfLengthBetween(1, 10)
        );
        {
            List<Step> steps = MigrateAction.ENABLED.toSteps(null, HOT_PHASE, nextStepKey);
            UpdateSettingsStep firstStep = (UpdateSettingsStep) steps.get(1);
            assertThat(DataTier.TIER_PREFERENCE_SETTING.get(firstStep.getSettings()), is(DATA_HOT));
        }
        {
            List<Step> steps = MigrateAction.ENABLED.toSteps(null, WARM_PHASE, nextStepKey);
            UpdateSettingsStep firstStep = (UpdateSettingsStep) steps.get(1);
            assertThat(DataTier.TIER_PREFERENCE_SETTING.get(firstStep.getSettings()), is(DATA_WARM + "," + DATA_HOT));
        }
        {
            List<Step> steps = MigrateAction.ENABLED.toSteps(null, COLD_PHASE, nextStepKey);
            UpdateSettingsStep firstStep = (UpdateSettingsStep) steps.get(1);
            assertThat(DataTier.TIER_PREFERENCE_SETTING.get(firstStep.getSettings()), is(DATA_COLD + "," + DATA_WARM + "," + DATA_HOT));
        }
    }

    public void testMigrateActionWillSkipAPartiallyMountedIndex() {
        StepKey nextStepKey = new StepKey(
            randomAlphaOfLengthBetween(1, 10),
            randomAlphaOfLengthBetween(1, 10),
            randomAlphaOfLengthBetween(1, 10)
        );

        // does not skip an ordinary index
        {
            IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(5))
                .settings(settings(Version.CURRENT))
                .numberOfShards(1)
                .numberOfReplicas(2)
                .build();

            ClusterState clusterState = ClusterState.builder(new ClusterName("_name"))
                .metadata(Metadata.builder().put(indexMetadata, true).build())
                .build();

            List<Step> steps = MigrateAction.ENABLED.toSteps(null, HOT_PHASE, nextStepKey);
            BranchingStep firstStep = (BranchingStep) steps.get(0);
            UpdateSettingsStep secondStep = (UpdateSettingsStep) steps.get(1);
            firstStep.performAction(indexMetadata.getIndex(), clusterState);

            assertEquals(secondStep.getKey(), firstStep.getNextStepKey());
        }

        // does skip a partially mounted
        {
            IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(5))
                .settings(
                    settings(Version.CURRENT).put(INDEX_STORE_TYPE_SETTING.getKey(), SEARCHABLE_SNAPSHOT_STORE_TYPE)
                        .put(SNAPSHOT_PARTIAL_SETTING.getKey(), true)
                )
                .numberOfShards(1)
                .numberOfReplicas(2)
                .build();

            ClusterState clusterState = ClusterState.builder(new ClusterName("_name"))
                .metadata(Metadata.builder().put(indexMetadata, true).build())
                .build();

            List<Step> steps = MigrateAction.ENABLED.toSteps(null, HOT_PHASE, nextStepKey);
            BranchingStep firstStep = (BranchingStep) steps.get(0);
            firstStep.performAction(indexMetadata.getIndex(), clusterState);

            assertEquals(nextStepKey, firstStep.getNextStepKey());
        }
    }

    @Override
    protected void assertEqualInstances(MigrateAction expectedInstance, MigrateAction newInstance) {
        assertThat(newInstance, equalTo(expectedInstance));
        assertThat(newInstance.hashCode(), equalTo(expectedInstance.hashCode()));
    }
}
