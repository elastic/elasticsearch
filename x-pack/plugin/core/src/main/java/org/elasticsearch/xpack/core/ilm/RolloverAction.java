/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.RolloverConfiguration;
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
import java.util.Arrays;
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

    private final RolloverConfiguration configuration;

    public static RolloverAction parse(XContentParser parser) {
        return new RolloverAction(RolloverConfiguration.parse(parser));
    }

    public RolloverAction(RolloverConfiguration configuration) {
        this.configuration = configuration;
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
            new RolloverConfiguration(
                maxSize,
                maxPrimaryShardSize,
                maxAge,
                maxDocs,
                maxPrimaryShardDocs,
                minSize,
                minPrimaryShardSize,
                minAge,
                minDocs,
                minPrimaryShardDocs
            )
        );
    }

    public RolloverAction(StreamInput in) throws IOException {
        this(new RolloverConfiguration(in));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        configuration.writeTo(out);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    public RolloverConfiguration getConfiguration() {
        return configuration;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return configuration.toXContent(builder, params);
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
            configuration.getMaxSize(),
            configuration.getMaxPrimaryShardSize(),
            configuration.getMaxAge(),
            configuration.getMaxDocs(),
            configuration.getMaxPrimaryShardDocs(),
            configuration.getMinSize(),
            configuration.getMinPrimaryShardSize(),
            configuration.getMinAge(),
            configuration.getMinDocs(),
            configuration.getMinPrimaryShardDocs()
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
        return Arrays.asList(waitForRolloverReadyStep, rolloverStep, waitForActiveShardsStep, updateDateStep, setIndexingCompleteStep);
    }

    @Override
    public int hashCode() {
        return Objects.hash(configuration);
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
        return Objects.equals(configuration, other.configuration);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

}
