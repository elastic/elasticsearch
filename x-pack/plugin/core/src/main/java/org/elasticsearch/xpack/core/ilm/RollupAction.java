/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
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
import org.elasticsearch.xpack.core.rollup.v2.RollupV2Config;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A {@link LifecycleAction} which sets the index's priority. The higher the priority, the faster the recovery.
 */
public class RollupAction implements LifecycleAction {
    public static final String NAME = "rollup";

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<RollupAction, Void> PARSER = new ConstructingObjectParser<>(NAME,
        a -> new RollupAction((RollupV2Config) a[0]));

    //package private for testing
    final RollupV2Config config;

    static {
        PARSER.declareField(ConstructingObjectParser.constructorArg(),
            (p, c) -> RollupV2Config.fromXContent(p, "_source_index_todo"),
            new ParseField("config"), ObjectParser.ValueType.OBJECT);
    }

    public static RollupAction parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public RollupAction(RollupV2Config config) {
        this.config = config;
    }

    public RollupAction(StreamInput in) throws IOException {
        this(new RollupV2Config(in));
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("config", config);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        config.writeTo(out);
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
        CheckNotDataStreamWriteIndexStep checkNotWriteIndexStep = new CheckNotDataStreamWriteIndexStep(checkNotWriteIndex,
            readOnlyKey);
        Settings readOnlySettings = Settings.builder().put(IndexMetadata.SETTING_BLOCKS_WRITE, true).build();
        UpdateSettingsStep readOnlyStep = new UpdateSettingsStep(readOnlyKey, nextStepKey, client, readOnlySettings);
        RollupStep rollupStep = new RollupStep(rollupKey, nextStepKey, client, config);
        return List.of(checkNotWriteIndexStep, readOnlyStep, rollupStep);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RollupAction that = (RollupAction) o;

        return Objects.equals(this.config, that.config);
    }

    @Override
    public int hashCode() {
        return Objects.hash(config);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
