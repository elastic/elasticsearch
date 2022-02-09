/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.cluster.routing.allocation.DataTier.getPreferredTiersConfigurationSettings;

/**
 * A {@link LifecycleAction} which enables or disables the automatic migration of data between
 * {@link DataTier}s.
 */
public class MigrateAction implements LifecycleAction {
    public static final String NAME = "migrate";
    public static final ParseField ENABLED_FIELD = new ParseField("enabled");

    public static final MigrateAction ENABLED = new MigrateAction(true);
    public static final MigrateAction DISABLED = new MigrateAction(false);

    private static final Logger logger = LogManager.getLogger(MigrateAction.class);
    public static final String CONDITIONAL_SKIP_MIGRATE_STEP = BranchingStep.NAME + "-check-skip-action";

    private static final ConstructingObjectParser<MigrateAction, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        a -> a[0] == null || (boolean) a[0] ? ENABLED : DISABLED
    );

    static {
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), ENABLED_FIELD);
    }

    private final boolean enabled;

    public static MigrateAction parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private MigrateAction(boolean enabled) {
        this.enabled = enabled;
    }

    public static MigrateAction readFrom(StreamInput in) throws IOException {
        return in.readBoolean() ? ENABLED : DISABLED;
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

            BranchingStep conditionalSkipActionStep = new BranchingStep(
                preMigrateBranchingKey,
                migrationKey,
                nextStepKey,
                (index, clusterState) -> {
                    IndexMetadata indexMetadata = clusterState.metadata().index(index);

                    // partially mounted indices will already have data_frozen, and we don't want to change that if they do
                    if (indexMetadata.isPartialSearchableSnapshot()) {
                        String policyName = indexMetadata.getLifecyclePolicyName();
                        logger.debug(
                            "[{}] action in policy [{}] is configured for index [{}] which is a partially mounted index. "
                                + "skipping this action",
                            MigrateAction.NAME,
                            policyName,
                            index.getName()
                        );
                        return true;
                    }

                    return false;
                }
            );
            UpdateSettingsStep updateMigrationSettingStep = new UpdateSettingsStep(
                migrationKey,
                migrationRoutedKey,
                client,
                getPreferredTiersConfigurationSettings(targetTier)
            );
            DataTierMigrationRoutedStep migrationRoutedStep = new DataTierMigrationRoutedStep(migrationRoutedKey, nextStepKey);
            return List.of(conditionalSkipActionStep, updateMigrationSettingStep, migrationRoutedStep);
        } else {
            return List.of();
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
