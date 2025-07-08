/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.admin.indices.rollover.RolloverConditions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * A {@link LifecycleAction} which rolls over the index.
 */
public class RolloverAction implements LifecycleAction {
    public static final String NAME = "rollover";
    public static final String INDEXING_COMPLETE_STEP_NAME = "set-indexing-complete";
    public static final String LIFECYCLE_ROLLOVER_ALIAS = "index.lifecycle.rollover_alias";
    public static final Setting<String> LIFECYCLE_ROLLOVER_ALIAS_SETTING = Setting.simpleString(
        LIFECYCLE_ROLLOVER_ALIAS,
        Setting.Property.Dynamic,
        Setting.Property.IndexScope
    );

    private static final Settings INDEXING_COMPLETE = Settings.builder().put(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE, true).build();

    private final RolloverConditions conditions;

    public static RolloverAction parse(XContentParser parser) throws IOException {
        return new RolloverAction(RolloverConditions.fromXContent(parser));
    }

    public RolloverAction(RolloverConditions conditions) {
        if (conditions.hasMaxConditions() == false) {
            throw new IllegalArgumentException("At least one max_* rollover condition must be set.");
        }
        this.conditions = conditions;
    }

    public RolloverAction(
        @Nullable ByteSizeValue maxSize,
        @Nullable ByteSizeValue maxPrimaryShardSize,
        @Nullable TimeValue maxAge,
        @Nullable Long maxDocs,
        @Nullable Long maxPrimaryShardDocs,
        @Nullable ByteSizeValue minSize,
        @Nullable ByteSizeValue minPrimaryShardSize,
        @Nullable TimeValue minAge,
        @Nullable Long minDocs,
        @Nullable Long minPrimaryShardDocs
    ) {
        this(
            RolloverConditions.newBuilder()
                .addMaxIndexSizeCondition(maxSize)
                .addMaxPrimaryShardSizeCondition(maxPrimaryShardSize)
                .addMaxIndexAgeCondition(maxAge)
                .addMaxIndexDocsCondition(maxDocs)
                .addMaxPrimaryShardDocsCondition(maxPrimaryShardDocs)
                .addMinIndexSizeCondition(minSize)
                .addMinPrimaryShardSizeCondition(minPrimaryShardSize)
                .addMinIndexAgeCondition(minAge)
                .addMinIndexDocsCondition(minDocs)
                .addMinPrimaryShardDocsCondition(minPrimaryShardDocs)
                .build()
        );
    }

    public static RolloverAction read(StreamInput in) throws IOException {
        RolloverConditions.Builder builder = RolloverConditions.newBuilder();
        builder.addMaxIndexSizeCondition(in.readOptionalWriteable(ByteSizeValue::readFrom));
        builder.addMaxPrimaryShardSizeCondition(in.readOptionalWriteable(ByteSizeValue::readFrom));
        builder.addMaxIndexAgeCondition(in.readOptionalTimeValue());
        builder.addMaxIndexDocsCondition(in.readOptionalVLong());
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_2_0)) {
            builder.addMaxPrimaryShardDocsCondition(in.readOptionalVLong());
        }
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_4_0)) {
            builder.addMinIndexSizeCondition(in.readOptionalWriteable(ByteSizeValue::readFrom));
            builder.addMinPrimaryShardSizeCondition(in.readOptionalWriteable(ByteSizeValue::readFrom));
            builder.addMinIndexAgeCondition(in.readOptionalTimeValue());
            builder.addMinIndexDocsCondition(in.readOptionalVLong());
            builder.addMinPrimaryShardDocsCondition(in.readOptionalVLong());
        }
        return new RolloverAction(builder.build());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(conditions.getMaxSize());
        out.writeOptionalWriteable(conditions.getMaxPrimaryShardSize());
        out.writeOptionalTimeValue(conditions.getMaxAge());
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_2_0)) {
            out.writeOptionalVLong(conditions.getMaxDocs());
            out.writeOptionalVLong(conditions.getMaxPrimaryShardDocs());
        } else {
            // With an older version and if maxDocs is empty, we use maxPrimaryShardDocs in its place.
            if (conditions.getMaxDocs() == null) {
                out.writeOptionalVLong(conditions.getMaxPrimaryShardDocs());
            } else {
                out.writeOptionalVLong(conditions.getMaxDocs());
            }
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_4_0)) {
            out.writeOptionalWriteable(conditions.getMinSize());
            out.writeOptionalWriteable(conditions.getMinPrimaryShardSize());
            out.writeOptionalTimeValue(conditions.getMinAge());
            out.writeOptionalVLong(conditions.getMinDocs());
            out.writeOptionalVLong(conditions.getMinPrimaryShardDocs());
        }
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    public RolloverConditions getConditions() {
        return conditions;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return conditions.toXContent(builder, params);
    }

    @Override
    public boolean isSafeAction() {
        return true;
    }

    @Override
    public List<Step> toSteps(Client client, String phase, Step.StepKey nextStepKey) {
        StepKey waitForRolloverReadyStepKey = new StepKey(phase, NAME, WaitForRolloverReadyStep.NAME);
        StepKey rolloverStepKey = new StepKey(phase, NAME, RolloverStep.NAME);
        StepKey waitForActiveShardsKey = new StepKey(phase, NAME, WaitForActiveShardsStep.NAME);
        StepKey updateDateStepKey = new StepKey(phase, NAME, UpdateRolloverLifecycleDateStep.NAME);
        StepKey setIndexingCompleteStepKey = new StepKey(phase, NAME, INDEXING_COMPLETE_STEP_NAME);

        WaitForRolloverReadyStep waitForRolloverReadyStep = new WaitForRolloverReadyStep(
            waitForRolloverReadyStepKey,
            rolloverStepKey,
            client,
            conditions
        );
        RolloverStep rolloverStep = new RolloverStep(rolloverStepKey, waitForActiveShardsKey, client);
        WaitForActiveShardsStep waitForActiveShardsStep = new WaitForActiveShardsStep(waitForActiveShardsKey, updateDateStepKey);
        UpdateRolloverLifecycleDateStep updateDateStep = new UpdateRolloverLifecycleDateStep(
            updateDateStepKey,
            setIndexingCompleteStepKey,
            System::currentTimeMillis
        );
        UpdateSettingsStep setIndexingCompleteStep = new UpdateSettingsStep(
            setIndexingCompleteStepKey,
            nextStepKey,
            client,
            INDEXING_COMPLETE
        );
        return List.of(waitForRolloverReadyStep, rolloverStep, waitForActiveShardsStep, updateDateStep, setIndexingCompleteStep);
    }

    @Override
    public int hashCode() {
        return Objects.hash(conditions);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        RolloverAction other = (RolloverAction) obj;
        return Objects.equals(conditions, other.conditions);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

}
