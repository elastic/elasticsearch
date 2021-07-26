/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.snapshots.SearchableSnapshotsSettings;
import org.elasticsearch.xpack.cluster.routing.allocation.DataTierAllocationDecider;
import org.elasticsearch.xpack.core.DataTier;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.core.DataTier.getPreferredTiersConfiguration;

/**
 * A {@link LifecycleAction} which enables or disables the automatic migration of data between
 * {@link org.elasticsearch.xpack.core.DataTier}s.
 */
public class MigrateAction implements LifecycleAction {
    public static final String NAME = "migrate";
    public static final ParseField ENABLED_FIELD = new ParseField("enabled");

    private static final Logger logger = LogManager.getLogger(MigrateAction.class);
    static final String CONDITIONAL_SKIP_MIGRATE_STEP = BranchingStep.NAME + "-check-skip-action";

    private static final ConstructingObjectParser<MigrateAction, Void> PARSER = new ConstructingObjectParser<>(NAME,
        a -> new MigrateAction(a[0] == null ? true : (boolean) a[0]));

    static {
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), ENABLED_FIELD);
    }

    private final boolean enabled;

    public static MigrateAction parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public MigrateAction() {
        this(true);
    }

    public MigrateAction(boolean enabled) {
        this.enabled = enabled;
    }

    public MigrateAction(StreamInput in) throws IOException {
        this(in.readBoolean());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(enabled);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    public boolean isEnabled() {
        return enabled;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ENABLED_FIELD.getPreferredName(), enabled);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean isSafeAction() {
        return true;
    }

    @Override
    public List<Step> toSteps(Client client, String phase, StepKey nextStepKey) {
        if (enabled) {
            StepKey preMigrateBranchingKey = new StepKey(phase, NAME, CONDITIONAL_SKIP_MIGRATE_STEP);
            StepKey migrationKey = new StepKey(phase, NAME, NAME);
            StepKey migrationRoutedKey = new StepKey(phase, NAME, DataTierMigrationRoutedStep.NAME);

            String targetTier = "data_" + phase;
            assert DataTier.validTierName(targetTier) : "invalid data tier name:" + targetTier;

            BranchingStep conditionalSkipActionStep = new BranchingStep(preMigrateBranchingKey, migrationKey, nextStepKey,
                (index, clusterState) -> {
                    Settings indexSettings = clusterState.metadata().index(index).getSettings();

                    // partially mounted indices will already have data_frozen, and we don't want to change that if they do
                    if (SearchableSnapshotsSettings.isPartialSearchableSnapshotIndex(indexSettings)) {
                        String policyName = LifecycleSettings.LIFECYCLE_NAME_SETTING.get(indexSettings);
                        logger.debug("[{}] action in policy [{}] is configured for index [{}] which is a partially mounted index. " +
                            "skipping this action", MigrateAction.NAME, policyName, index.getName());
                        return true;
                    }

                    return false;
                });
            UpdateSettingsStep updateMigrationSettingStep = new UpdateSettingsStep(migrationKey, migrationRoutedKey, client,
                Settings.builder()
                    .put(DataTierAllocationDecider.INDEX_ROUTING_PREFER, getPreferredTiersConfiguration(targetTier))
                    .build());
            DataTierMigrationRoutedStep migrationRoutedStep = new DataTierMigrationRoutedStep(migrationRoutedKey, nextStepKey);
            return org.elasticsearch.core.List.of(conditionalSkipActionStep, updateMigrationSettingStep, migrationRoutedStep);
        } else {
            return org.elasticsearch.core.List.of();
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(enabled);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        MigrateAction other = (MigrateAction) obj;
        return Objects.equals(enabled, other.enabled);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

}
