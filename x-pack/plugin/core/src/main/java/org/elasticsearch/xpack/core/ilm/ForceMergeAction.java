/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.apache.lucene.codecs.Codec;
import org.elasticsearch.Version;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * A {@link LifecycleAction} which force-merges the index.
 */
public class ForceMergeAction implements LifecycleAction {
    public static final String NAME = "forcemerge";
    public static final ParseField MAX_NUM_SEGMENTS_FIELD = new ParseField("max_num_segments");
    public static final ParseField CODEC = new ParseField("index_codec");

    private static final ConstructingObjectParser<ForceMergeAction, Void> PARSER = new ConstructingObjectParser<>(NAME,
        false, a -> {
        int maxNumSegments = (int) a[0];
        Codec codec = a[1] != null ? Codec.forName((String) a[1]) : Codec.getDefault();
        return new ForceMergeAction(maxNumSegments, codec);
    });

    static {
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), MAX_NUM_SEGMENTS_FIELD);
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), CODEC);
    }

    private final int maxNumSegments;
    private final Codec codec;

    public static ForceMergeAction parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public ForceMergeAction(int maxNumSegments, Codec codec) {
        if (maxNumSegments <= 0) {
            throw new IllegalArgumentException("[" + MAX_NUM_SEGMENTS_FIELD.getPreferredName()
                + "] must be a positive integer");
        }
        this.maxNumSegments = maxNumSegments;
        this.codec = codec;
    }

    public ForceMergeAction(StreamInput in) throws IOException {
        this.maxNumSegments = in.readVInt();
        if (in.getVersion().onOrAfter(Version.V_8_0_0)) {
            this.codec = Codec.forName(in.readString());
        } else {
            this.codec = Codec.getDefault();
        }
    }

    public int getMaxNumSegments() {
        return maxNumSegments;
    }

    public Codec getCodec() {
        return this.codec;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(maxNumSegments);
        if (out.getVersion().onOrAfter(Version.V_8_0_0)) {
            out.writeString(codec.getName());
        } else {
            out.writeString(Codec.getDefault().getName());
        }
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public boolean isSafeAction() {
        return true;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(MAX_NUM_SEGMENTS_FIELD.getPreferredName(), maxNumSegments);
        builder.field(CODEC.getPreferredName(), codec);
        builder.endObject();
        return builder;
    }

    @Override
    public List<Step> toSteps(Client client, String phase, Step.StepKey nextStepKey) {
        Settings readOnlySettings = Settings.builder().put(IndexMetaData.SETTING_BLOCKS_WRITE, true).build();
        Settings bestCompressionSettings = Settings.builder()
            .put(EngineConfig.INDEX_CODEC_SETTING.getKey(), CodecService.BEST_COMPRESSION_CODEC).build();

        StepKey readOnlyKey = new StepKey(phase, NAME, ReadOnlyAction.NAME);
        StepKey forceMergeKey = new StepKey(phase, NAME, ForceMergeStep.NAME);
        StepKey countKey = new StepKey(phase, NAME, SegmentCountStep.NAME);


        if (codec.getName().equals(CodecService.BEST_COMPRESSION_CODEC)) {
            StepKey closeKey = new StepKey(phase, NAME, CloseIndexStep.NAME);
            StepKey openKey = new StepKey(phase, NAME, OpenIndexStep.NAME);
            StepKey waitForGreenIndexKey = new StepKey(phase, NAME, WaitForIndexColorStep.NAME);
            StepKey updateCompressionKey = new StepKey(phase, NAME, UpdateSettingsStep.NAME);

            CloseIndexStep closeIndexStep = new CloseIndexStep(closeKey, updateCompressionKey, client);
            UpdateSettingsStep updateBestCompressionSettings = new UpdateSettingsStep(updateCompressionKey,
                openKey, client, bestCompressionSettings);
            OpenIndexStep openIndexStep = new OpenIndexStep(openKey, waitForGreenIndexKey, client);
            WaitForIndexColorStep waitForIndexGreenStep = new WaitForIndexColorStep(waitForGreenIndexKey,
                forceMergeKey, ClusterHealthStatus.GREEN);
            ForceMergeStep forceMergeStep = new ForceMergeStep(forceMergeKey, nextStepKey, client, maxNumSegments);
            return Arrays.asList(closeIndexStep, updateBestCompressionSettings,
                openIndexStep, waitForIndexGreenStep, forceMergeStep);
        }

        UpdateSettingsStep readOnlyStep = new UpdateSettingsStep(readOnlyKey, forceMergeKey, client, readOnlySettings);
        ForceMergeStep forceMergeStep = new ForceMergeStep(forceMergeKey, countKey, client, maxNumSegments);
        SegmentCountStep segmentCountStep = new SegmentCountStep(countKey, nextStepKey, client, maxNumSegments);
        return Arrays.asList(readOnlyStep, forceMergeStep, segmentCountStep);
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxNumSegments, codec);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        ForceMergeAction other = (ForceMergeAction) obj;
        return Objects.equals(maxNumSegments, other.maxNumSegments)
            && Objects.equals(codec, other.codec);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
