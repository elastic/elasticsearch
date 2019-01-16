/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.action;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.elasticsearch.xpack.core.dataframe.transform.DataFrameIndexerTransformStats;
import org.elasticsearch.xpack.core.dataframe.transform.DataFrameTransformState;

import java.io.IOException;
import java.util.Objects;

public class DataFrameTransformStateAndStats implements Writeable, ToXContentObject {

    public static final ParseField STATE_FIELD = new ParseField("state");

    private final String id;
    private final DataFrameTransformState transformState;
    private final DataFrameIndexerTransformStats transformStats;

    public static final ConstructingObjectParser<DataFrameTransformStateAndStats, Void> PARSER = new ConstructingObjectParser<>(
            GetDataFrameTransformsAction.NAME,
            a -> new DataFrameTransformStateAndStats((String) a[0], (DataFrameTransformState) a[1], (DataFrameIndexerTransformStats) a[2]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), DataFrameField.ID);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), DataFrameTransformState.PARSER::apply, STATE_FIELD);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> DataFrameIndexerTransformStats.fromXContent(p),
                DataFrameField.STATS_FIELD);
    }

    public DataFrameTransformStateAndStats(String id, DataFrameTransformState state, DataFrameIndexerTransformStats stats) {
        this.id = Objects.requireNonNull(id);
        this.transformState = Objects.requireNonNull(state);
        this.transformStats = Objects.requireNonNull(stats);
    }

    public DataFrameTransformStateAndStats(StreamInput in) throws IOException {
        this.id = in.readString();
        this.transformState = new DataFrameTransformState(in);
        this.transformStats = new DataFrameIndexerTransformStats(in);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(DataFrameField.ID.getPreferredName(), id);
        builder.field(STATE_FIELD.getPreferredName(), transformState);
        builder.field(DataFrameField.STATS_FIELD.getPreferredName(), transformStats);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        transformState.writeTo(out);
        transformStats.writeTo(out);
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

        DataFrameTransformStateAndStats that = (DataFrameTransformStateAndStats) other;

        return Objects.equals(this.id, that.id) && Objects.equals(this.transformState, that.transformState)
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
}
