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
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.cluster.routing.allocation.DataTierAllocationDecider;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.index.IndexModule.INDEX_STORE_TYPE_SETTING;
import static org.elasticsearch.snapshots.SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOT_STORE_TYPE;
import static org.elasticsearch.xpack.core.DataTier.DATA_COLD;
import static org.elasticsearch.xpack.core.DataTier.DATA_HOT;
import static org.elasticsearch.xpack.core.DataTier.DATA_WARM;
import static org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType.COLD_PHASE;
import static org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType.DELETE_PHASE;
import static org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType.HOT_PHASE;
import static org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType.WARM_PHASE;
import static org.elasticsearch.xpack.core.searchablesnapshots.SearchableSnapshotsConstants.SNAPSHOT_PARTIAL_SETTING;
import static org.hamcrest.CoreMatchers.is;

public class MigrateActionTests extends AbstractActionTestCase<MigrateAction> {

    @Override
    protected MigrateAction doParseInstance(XContentParser parser) throws IOException {
        return MigrateAction.parse(parser);
    }

    @Override
    protected MigrateAction createTestInstance() {
        return new MigrateAction(randomBoolean());
    }

    @Override
    protected MigrateAction mutateInstance(MigrateAction instance) throws IOException {
        return new MigrateAction(instance.isEnabled() == false);
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

    public void testMigrateActionWillSkipAPartiallyMountedIndex() {
        StepKey nextStepKey = new StepKey(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10),
            randomAlphaOfLengthBetween(1, 10));
        MigrateAction action = new MigrateAction();

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

            List<Step> steps = action.toSteps(null, HOT_PHASE, nextStepKey);
            BranchingStep firstStep = (BranchingStep) steps.get(0);
            UpdateSettingsStep secondStep = (UpdateSettingsStep) steps.get(1);
            firstStep.performAction(indexMetadata.getIndex(), clusterState);

            assertEquals(secondStep.getKey(), firstStep.getNextStepKey());
        }

        // does skip a partially mounted
        {
            IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(5))
                .settings(settings(Version.CURRENT)
                    .put(INDEX_STORE_TYPE_SETTING.getKey(), SEARCHABLE_SNAPSHOT_STORE_TYPE)
                    .put(SNAPSHOT_PARTIAL_SETTING.getKey(), true))
                .numberOfShards(1)
                .numberOfReplicas(2)
                .build();

            ClusterState clusterState = ClusterState.builder(new ClusterName("_name"))
                .metadata(Metadata.builder().put(indexMetadata, true).build())
                .build();

            List<Step> steps = action.toSteps(null, HOT_PHASE, nextStepKey);
            BranchingStep firstStep = (BranchingStep) steps.get(0);
            firstStep.performAction(indexMetadata.getIndex(), clusterState);

            assertEquals(nextStepKey, firstStep.getNextStepKey());
        }
    }
}
