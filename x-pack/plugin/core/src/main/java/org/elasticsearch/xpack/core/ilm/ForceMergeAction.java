/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
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

    private static final ConstructingObjectParser<ForceMergeAction, Void> PARSER = new ConstructingObjectParser<>(NAME,
        false, a -> {
        int maxNumSegments = (int) a[0];
        return new ForceMergeAction(maxNumSegments);
    });

    static {
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), MAX_NUM_SEGMENTS_FIELD);
    }

    private final int maxNumSegments;

    public static ForceMergeAction parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public ForceMergeAction(int maxNumSegments) {
        if (maxNumSegments <= 0) {
            throw new IllegalArgumentException("[" + MAX_NUM_SEGMENTS_FIELD.getPreferredName()
                + "] must be a positive integer");
        }
        this.maxNumSegments = maxNumSegments;
    }

    public ForceMergeAction(StreamInput in) throws IOException {
        this.maxNumSegments = in.readVInt();
    }

    public int getMaxNumSegments() {
        return maxNumSegments;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(maxNumSegments);
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
        builder.endObject();
        return builder;
    }

    @Override
    public List<Step> toSteps(Client client, String phase, Step.StepKey nextStepKey) {
        Settings readOnlySettings = Settings.builder().put(IndexMetaData.SETTING_BLOCKS_WRITE, true).build();

        StepKey readOnlyKey = new StepKey(phase, NAME, ReadOnlyAction.NAME);
        StepKey forceMergeKey = new StepKey(phase, NAME, ForceMergeStep.NAME);
        StepKey countKey = new StepKey(phase, NAME, SegmentCountStep.NAME);

        UpdateSettingsStep readOnlyStep = new UpdateSettingsStep(readOnlyKey, forceMergeKey, client, readOnlySettings);
        ForceMergeStep forceMergeStep = new ForceMergeStep(forceMergeKey, countKey, client, maxNumSegments);
        SegmentCountStep segmentCountStep = new SegmentCountStep(countKey, nextStepKey, client, maxNumSegments);
        return Arrays.asList(readOnlyStep, forceMergeStep, segmentCountStep);
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxNumSegments);
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
        return Objects.equals(maxNumSegments, other.maxNumSegments);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
