/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.transforms;

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
import org.elasticsearch.xpack.core.dataframe.DataFrameField;

import java.io.IOException;
import java.util.Objects;

/**
 * A wrapper for grouping transform state and stats when persisting to an index.
 * Not intended to be returned in endpoint responses.
 */
public class DataFrameTransformStoredDoc implements Writeable, ToXContentObject {

    public static final String NAME = "data_frame_transform_state_and_stats";
    public static final ParseField STATE_FIELD = new ParseField("state");

    private final String id;
    private final DataFrameTransformState transformState;
    private final DataFrameIndexerTransformStats transformStats;

    public static final ConstructingObjectParser<DataFrameTransformStoredDoc, Void> PARSER = new ConstructingObjectParser<>(
            NAME, true,
            a -> new DataFrameTransformStoredDoc((String) a[0],
                    (DataFrameTransformState) a[1],
                    (DataFrameIndexerTransformStats) a[2]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), DataFrameField.ID);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), DataFrameTransformState.PARSER::apply, STATE_FIELD);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> DataFrameIndexerTransformStats.fromXContent(p),
                DataFrameField.STATS_FIELD);
    }

    public static DataFrameTransformStoredDoc fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    /**
     * Get the persisted state and stats document name from the Data Frame Transform Id.
     *
     * @return The id of document the where the transform stats are persisted
     */
    public static String documentId(String transformId) {
        return NAME + "-" + transformId;
    }

    public DataFrameTransformStoredDoc(String id, DataFrameTransformState state, DataFrameIndexerTransformStats stats) {
        this.id = Objects.requireNonNull(id);
        this.transformState = Objects.requireNonNull(state);
        this.transformStats = Objects.requireNonNull(stats);
    }

    public DataFrameTransformStoredDoc(StreamInput in) throws IOException {
        this.id = in.readString();
        this.transformState = new DataFrameTransformState(in);
        this.transformStats = new DataFrameIndexerTransformStats(in);
        if (in.getVersion().before(Version.V_7_4_0)) {
            new DataFrameTransformCheckpointingInfo(in);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(DataFrameField.ID.getPreferredName(), id);
        builder.field(STATE_FIELD.getPreferredName(), transformState, params);
        builder.field(DataFrameField.STATS_FIELD.getPreferredName(), transformStats, params);
        builder.field(DataFrameField.INDEX_DOC_TYPE.getPreferredName(), NAME);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        transformState.writeTo(out);
        transformStats.writeTo(out);
        if (out.getVersion().before(Version.V_7_4_0)) {
            DataFrameTransformCheckpointingInfo.EMPTY.writeTo(out);
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

        DataFrameTransformStoredDoc that = (DataFrameTransformStoredDoc) other;

        return Objects.equals(this.id, that.id)
            && Objects.equals(this.transformState, that.transformState)
            && Objects.equals(this.transformStats, that.transformStats);
    }

    public String getId() {
        return id;
    }

    public DataFrameIndexerTransformStats getTransformStats() {
        return transformStats;
    }

    public DataFrameTransformState getTransformState() {
        return transformState;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
