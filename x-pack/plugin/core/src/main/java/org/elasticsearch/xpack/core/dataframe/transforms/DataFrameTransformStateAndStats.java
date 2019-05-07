/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.transforms;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.elasticsearch.xpack.core.indexing.IndexerState;

import java.io.IOException;
import java.util.Objects;

public class DataFrameTransformStateAndStats implements Writeable, ToXContentObject {

    private static final String NAME = "data_frame_transform_state_and_stats";
    public static final ParseField STATE_FIELD = new ParseField("state");
    public static final ParseField CHECKPOINTING_INFO_FIELD = new ParseField("checkpointing");

    private final String id;
    private final DataFrameTransformState transformState;
    private final DataFrameIndexerTransformStats transformStats;
    private final DataFrameTransformCheckpointingInfo checkpointingInfo;

    public static final ConstructingObjectParser<DataFrameTransformStateAndStats, Void> PARSER = new ConstructingObjectParser<>(
            NAME, true,
            a -> new DataFrameTransformStateAndStats((String) a[0],
                    (DataFrameTransformState) a[1],
                    (DataFrameIndexerTransformStats) a[2],
                    (DataFrameTransformCheckpointingInfo) a[3]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), DataFrameField.ID);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), DataFrameTransformState.PARSER::apply, STATE_FIELD);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> DataFrameIndexerTransformStats.fromXContent(p),
                DataFrameField.STATS_FIELD);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(),
                                (p, c) -> DataFrameTransformCheckpointingInfo.fromXContent(p), CHECKPOINTING_INFO_FIELD);
    }

    public static DataFrameTransformStateAndStats initialStateAndStats(String id) {
        return initialStateAndStats(id, new DataFrameIndexerTransformStats(id));
    }

    public static DataFrameTransformStateAndStats initialStateAndStats(String id, DataFrameIndexerTransformStats indexerTransformStats) {
        return new DataFrameTransformStateAndStats(id,
            new DataFrameTransformState(DataFrameTransformTaskState.STOPPED, IndexerState.STOPPED, null, 0L, null, null),
            indexerTransformStats,
            DataFrameTransformCheckpointingInfo.EMPTY);
    }

    public DataFrameTransformStateAndStats(String id, DataFrameTransformState state, DataFrameIndexerTransformStats stats,
            DataFrameTransformCheckpointingInfo checkpointingInfo) {
        this.id = Objects.requireNonNull(id);
        this.transformState = Objects.requireNonNull(state);
        this.transformStats = Objects.requireNonNull(stats);
        this.checkpointingInfo = Objects.requireNonNull(checkpointingInfo);
    }

    public DataFrameTransformStateAndStats(StreamInput in) throws IOException {
        this.id = in.readString();
        this.transformState = new DataFrameTransformState(in);
        this.transformStats = new DataFrameIndexerTransformStats(in);
        this.checkpointingInfo = new DataFrameTransformCheckpointingInfo(in);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(DataFrameField.ID.getPreferredName(), id);
        builder.field(STATE_FIELD.getPreferredName(), transformState, params);
        builder.field(DataFrameField.STATS_FIELD.getPreferredName(), transformStats, params);
        builder.field(CHECKPOINTING_INFO_FIELD.getPreferredName(), checkpointingInfo, params);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        transformState.writeTo(out);
        transformStats.writeTo(out);
        checkpointingInfo.writeTo(out);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, transformState, transformStats, checkpointingInfo);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        DataFrameTransformStateAndStats that = (DataFrameTransformStateAndStats) other;

        return Objects.equals(this.id, that.id) && Objects.equals(this.transformState, that.transformState)
                && Objects.equals(this.transformStats, that.transformStats)
                && Objects.equals(this.checkpointingInfo, that.checkpointingInfo);
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

    public DataFrameTransformCheckpointingInfo getCheckpointingInfo() {
        return checkpointingInfo;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
