/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.Version;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.transform.TransformField;

import java.io.IOException;
import java.util.Objects;

/**
 * A wrapper for grouping transform state and stats when persisting to an index.
 * Not intended to be returned in endpoint responses.
 */
public class TransformStoredDoc implements Writeable, ToXContentObject {

    public static final String NAME = "data_frame_transform_state_and_stats";
    public static final ParseField STATE_FIELD = new ParseField("state");

    private final String id;
    private final TransformState transformState;
    private final TransformIndexerStats transformStats;

    public static final ConstructingObjectParser<TransformStoredDoc, Void> PARSER = new ConstructingObjectParser<>(
            NAME, true,
            a -> new TransformStoredDoc((String) a[0],
                    (TransformState) a[1],
                    (TransformIndexerStats) a[2]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), TransformField.ID);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), TransformState.PARSER::apply, STATE_FIELD);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> TransformIndexerStats.fromXContent(p),
                TransformField.STATS_FIELD);
    }

    public static TransformStoredDoc fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    /**
     * Get the persisted state and stats document name from the Transform Id.
     *
     * @return The id of document the where the transform stats are persisted
     */
    public static String documentId(String transformId) {
        return NAME + "-" + transformId;
    }

    public TransformStoredDoc(String id, TransformState state, TransformIndexerStats stats) {
        this.id = Objects.requireNonNull(id);
        this.transformState = Objects.requireNonNull(state);
        this.transformStats = Objects.requireNonNull(stats);
    }

    public TransformStoredDoc(StreamInput in) throws IOException {
        this.id = in.readString();
        this.transformState = new TransformState(in);
        this.transformStats = new TransformIndexerStats(in);
        if (in.getVersion().before(Version.V_7_4_0)) {
            new TransformCheckpointingInfo(in);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TransformField.ID.getPreferredName(), id);
        builder.field(STATE_FIELD.getPreferredName(), transformState, params);
        builder.field(TransformField.STATS_FIELD.getPreferredName(), transformStats, params);
        builder.field(TransformField.INDEX_DOC_TYPE.getPreferredName(), NAME);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        transformState.writeTo(out);
        transformStats.writeTo(out);
        if (out.getVersion().before(Version.V_7_4_0)) {
            TransformCheckpointingInfo.EMPTY.writeTo(out);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, transformState, transformStats);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        TransformStoredDoc that = (TransformStoredDoc) other;

        return Objects.equals(this.id, that.id)
            && Objects.equals(this.transformState, that.transformState)
            && Objects.equals(this.transformStats, that.transformStats);
    }

    public String getId() {
        return id;
    }

    public TransformIndexerStats getTransformStats() {
        return transformStats;
    }

    public TransformState getTransformState() {
        return transformState;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
