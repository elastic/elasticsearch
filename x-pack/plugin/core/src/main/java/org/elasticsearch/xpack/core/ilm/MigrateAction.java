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
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.cluster.routing.allocation.DataTierAllocationDecider;
import org.elasticsearch.xpack.core.DataTier;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType.FROZEN_PHASE;

/**
 * A {@link LifecycleAction} which enables or disables the automatic migration of data between
 * {@link org.elasticsearch.xpack.core.DataTier}s.
 */
public class MigrateAction implements LifecycleAction {
    public static final String NAME = "migrate";
    public static final ParseField ENABLED_FIELD = new ParseField("enabled");

    private static final Logger logger = LogManager.getLogger(MigrateAction.class);
    static final String CONDITIONAL_SKIP_MIGRATE_STEP = BranchingStep.NAME + "-check-skip-action";
    // Represents an ordered list of data tiers from frozen to hot (or slow to fast)
    private static final List<String> FROZEN_TO_HOT_TIERS =
        List.of(DataTier.DATA_FROZEN, DataTier.DATA_COLD, DataTier.DATA_WARM, DataTier.DATA_HOT);

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

            Settings.Builder migrationSettings = Settings.builder();
            String targetTier = "data_" + phase;
            assert DataTier.validTierName(targetTier) : "invalid data tier name:" + targetTier;

            BranchingStep conditionalSkipActionStep = new BranchingStep(preMigrateBranchingKey, migrationKey, nextStepKey,
                (index, clusterState) -> {
                    if (skipMigrateAction(phase, clusterState.metadata().index(index))) {
                        String policyName =
                            LifecycleSettings.LIFECYCLE_NAME_SETTING.get(clusterState.metadata().index(index).getSettings());
                        logger.debug("[{}] action is configured for index [{}] in policy [{}] which is already mounted as a searchable " +
                            "snapshot. skipping this action", MigrateAction.NAME, index.getName(), policyName);
                        return true;
                    }

                    // don't skip the migrate action as the index is not mounted as searchable snapshot or we're in the frozen phase
                    return false;
                });
            migrationSettings.put(DataTierAllocationDecider.INDEX_ROUTING_PREFER, getPreferredTiersConfiguration(targetTier));
            UpdateSettingsStep updateMigrationSettingStep = new UpdateSettingsStep(migrationKey, migrationRoutedKey, client,
                migrationSettings.build());
            DataTierMigrationRoutedStep migrationRoutedStep = new DataTierMigrationRoutedStep(migrationRoutedKey, nextStepKey);
            return Arrays.asList(conditionalSkipActionStep, updateMigrationSettingStep, migrationRoutedStep);
        } else {
            return List.of();
        }
    }

    static boolean skipMigrateAction(String phase, IndexMetadata indexMetadata) {
        // if the index is a searchable snapshot we skip the migrate action (as mounting an index as searchable snapshot
        // configures the tier allocation preference), unless we're in the frozen phase
        return (indexMetadata.getSettings().get(LifecycleSettings.SNAPSHOT_INDEX_NAME) != null)
            && (phase.equals(FROZEN_PHASE) == false);
    }

    /**
     * Based on the provided target tier it will return a comma separated list of preferred tiers.
     * ie. if `data_cold` is the target tier, it will return `data_cold,data_warm,data_hot`
     * This is usually used in conjunction with {@link DataTierAllocationDecider#INDEX_ROUTING_PREFER_SETTING}
     */
    static String getPreferredTiersConfiguration(String targetTier) {
        int indexOfTargetTier = FROZEN_TO_HOT_TIERS.indexOf(targetTier);
        if (indexOfTargetTier == -1) {
            throw new IllegalArgumentException("invalid data tier [" + targetTier + "]");
        }
        return FROZEN_TO_HOT_TIERS.stream().skip(indexOfTargetTier).collect(Collectors.joining(","));
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
