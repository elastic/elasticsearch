/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.client.Client;
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
import org.elasticsearch.xpack.core.indexlifecycle.Step.StepKey;

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
    public static final ParseField BEST_COMPRESSION_FIELD = new ParseField("best_compression");

    private static final ConstructingObjectParser<ForceMergeAction, Void> PARSER = new ConstructingObjectParser<>(NAME,
        false, a -> {
        int maxNumSegments = (int) a[0];
        boolean bestCompression = a[1] == null ? false : (boolean) a[1];
        return new ForceMergeAction(maxNumSegments, bestCompression);
    });

    static {
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), MAX_NUM_SEGMENTS_FIELD);
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), BEST_COMPRESSION_FIELD);
    }

    private final int maxNumSegments;
    private final boolean bestCompression;

    public static ForceMergeAction parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public ForceMergeAction(int maxNumSegments, boolean bestCompression) {
        if (maxNumSegments <= 0) {
            throw new IllegalArgumentException("[" + MAX_NUM_SEGMENTS_FIELD.getPreferredName()
                + "] must be a positive integer");
        }
        this.maxNumSegments = maxNumSegments;
        this.bestCompression = bestCompression;
    }

    public ForceMergeAction(StreamInput in) throws IOException {
        this.maxNumSegments = in.readVInt();
        this.bestCompression = in.readBoolean();
    }

    public int getMaxNumSegments() {
        return maxNumSegments;
    }

    public boolean isBestCompression() {
        return bestCompression;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(maxNumSegments);
        out.writeBoolean(bestCompression);
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
        builder.field(BEST_COMPRESSION_FIELD.getPreferredName(), bestCompression);
        builder.endObject();
        return builder;
    }

    @Override
    public List<Step> toSteps(Client client, String phase, Step.StepKey nextStepKey) {
        StepKey updateCompressionKey = new StepKey(phase, NAME, "best_compression");
        StepKey forceMergeKey = new StepKey(phase, NAME, ForceMergeStep.NAME);
        StepKey countKey = new StepKey(phase, NAME, SegmentCountStep.NAME);
        ForceMergeStep forceMergeStep = new ForceMergeStep(forceMergeKey, countKey, client, maxNumSegments);
        SegmentCountStep segmentCountStep = new SegmentCountStep(countKey, nextStepKey, client, maxNumSegments, bestCompression);
        if (bestCompression) {
            Settings compressionSettings = Settings.builder()
                .put(EngineConfig.INDEX_CODEC_SETTING.getKey(), CodecService.BEST_COMPRESSION_CODEC).build();
            UpdateSettingsStep updateBestCompression = new UpdateSettingsStep(updateCompressionKey,
                forceMergeKey, client, compressionSettings);
            return Arrays.asList(updateBestCompression, forceMergeStep, segmentCountStep);
        }
        return Arrays.asList(forceMergeStep, segmentCountStep);
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxNumSegments, bestCompression);
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
            && Objects.equals(bestCompression, other.bestCompression);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
