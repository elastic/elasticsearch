/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;
import org.elasticsearch.xpack.core.rollup.v2.RollupActionConfig;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * A {@link LifecycleAction} which calls {@link org.elasticsearch.xpack.core.rollup.v2.RollupAction} on an index
 */
public class RollupILMAction implements LifecycleAction {
    public static final String NAME = "rollup";

    private static final ParseField CONFIG_FIELD = new ParseField("config");
    private static final ParseField DELETE_FIELD = new ParseField("delete_original");
    private static final ParseField POLICY_FIELD = new ParseField("rollup_policy");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<RollupILMAction, Void> PARSER = new ConstructingObjectParser<>(NAME,
        a -> new RollupILMAction((RollupActionConfig) a[0], (boolean) a[1], (String) a[2]));

    private final RollupActionConfig config;
    private final boolean deleteOriginalIndex;
    private final String rollupPolicy;

    static {
        PARSER.declareField(ConstructingObjectParser.constructorArg(),
            (p, c) -> RollupActionConfig.fromXContent(p), CONFIG_FIELD, ObjectParser.ValueType.OBJECT);
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), DELETE_FIELD);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), POLICY_FIELD);
    }

    public static RollupILMAction parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public RollupILMAction(RollupActionConfig config, boolean deleteOriginalIndex, @Nullable String rollupPolicy) {
        this.config = config;
        this.deleteOriginalIndex = deleteOriginalIndex;
        this.rollupPolicy = rollupPolicy;
    }

    public RollupILMAction(StreamInput in) throws IOException {
        this(new RollupActionConfig(in), in.readBoolean(), in.readOptionalString());
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    RollupActionConfig config() {
        return config;
    }

    boolean shouldDeleteOriginalIndex() {
        return deleteOriginalIndex;
    }

    String rollupPolicy() {
        return rollupPolicy;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(CONFIG_FIELD.getPreferredName(), config);
        builder.field(DELETE_FIELD.getPreferredName(), deleteOriginalIndex);
        if (rollupPolicy != null) {
            builder.field(POLICY_FIELD.getPreferredName(), rollupPolicy);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        config.writeTo(out);
        out.writeBoolean(deleteOriginalIndex);
        out.writeOptionalString(rollupPolicy);
    }

    @Override
    public boolean isSafeAction() {
        return false;
    }

    @Override
    public List<Step> toSteps(Client client, String phase, StepKey nextStepKey) {
        StepKey checkNotWriteIndex = new StepKey(phase, NAME, CheckNotDataStreamWriteIndexStep.NAME);
        StepKey readOnlyKey = new StepKey(phase, NAME, ReadOnlyAction.NAME);
        StepKey rollupKey = new StepKey(phase, NAME, NAME);
        StepKey waitForNoFollowerStepKey = new StepKey(phase, NAME, WaitForNoFollowersStep.NAME);
        StepKey deleteStepKey = new StepKey(phase, NAME, DeleteStep.NAME);

        Settings readOnlySettings = Settings.builder().put(IndexMetadata.SETTING_BLOCKS_WRITE, true).build();

        CheckNotDataStreamWriteIndexStep checkNotWriteIndexStep = new CheckNotDataStreamWriteIndexStep(checkNotWriteIndex,
            readOnlyKey);
        UpdateSettingsStep readOnlyStep = new UpdateSettingsStep(readOnlyKey, rollupKey, client, readOnlySettings);
        WaitForNoFollowersStep waitForNoFollowersStep = new WaitForNoFollowersStep(waitForNoFollowerStepKey, deleteStepKey, client);

        if (deleteOriginalIndex) {
            RollupStep rollupStep = new RollupStep(rollupKey, waitForNoFollowerStepKey, client, config, rollupPolicy);
            DeleteStep deleteStep = new DeleteStep(deleteStepKey, nextStepKey, client);
            return List.of(checkNotWriteIndexStep, readOnlyStep, rollupStep, waitForNoFollowersStep, deleteStep);
        } else {
            RollupStep rollupStep = new RollupStep(rollupKey, nextStepKey, client, config, rollupPolicy);
            return List.of(checkNotWriteIndexStep, readOnlyStep, rollupStep);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RollupILMAction that = (RollupILMAction) o;

        return Objects.equals(this.config, that.config)
            && Objects.equals(this.deleteOriginalIndex, that.deleteOriginalIndex)
            && Objects.equals(this.rollupPolicy, that.rollupPolicy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(config, deleteOriginalIndex, rollupPolicy);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
