/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.transforms;

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
 * Checkpoint stats data for 1 checkpoint
 *
 * This is the user-facing side of DataFrameTransformCheckpoint, containing only the stats to be exposed.
 */
public class DataFrameTransformCheckpointStats implements Writeable, ToXContentObject {

    public static DataFrameTransformCheckpointStats EMPTY = new DataFrameTransformCheckpointStats(0L, 0L);

    private final long timestampMillis;
    private final long timeUpperBoundMillis;

    private static final ConstructingObjectParser<DataFrameTransformCheckpointStats, Void> LENIENT_PARSER = new ConstructingObjectParser<>(
            "data_frame_transform_checkpoint_stats", true, args -> {
                long timestamp = args[0] == null ? 0L : (Long) args[0];
                long timeUpperBound = args[1] == null ? 0L : (Long) args[1];

                return new DataFrameTransformCheckpointStats(timestamp, timeUpperBound);
            });

    static {
        LENIENT_PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), DataFrameField.TIMESTAMP_MILLIS);
        LENIENT_PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), DataFrameField.TIME_UPPER_BOUND_MILLIS);
    }

    public DataFrameTransformCheckpointStats(final long timestampMillis, final long timeUpperBoundMillis) {
        this.timestampMillis = timestampMillis;
        this.timeUpperBoundMillis = timeUpperBoundMillis;
    }

    public DataFrameTransformCheckpointStats(StreamInput in) throws IOException {
        this.timestampMillis = in.readLong();
        this.timeUpperBoundMillis = in.readLong();
    }

    public long getTimestampMillis() {
        return timestampMillis;
    }

    public long getTimeUpperBoundMillis() {
        return timeUpperBoundMillis;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.timeField(DataFrameField.TIMESTAMP_MILLIS.getPreferredName(), DataFrameField.TIMESTAMP.getPreferredName(),
                getTimestampMillis());
        if (timeUpperBoundMillis > 0) {
            builder.timeField(DataFrameField.TIME_UPPER_BOUND_MILLIS.getPreferredName(), DataFrameField.TIME_UPPER_BOUND.getPreferredName(),
                    timeUpperBoundMillis);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(timestampMillis);
        out.writeLong(timeUpperBoundMillis);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestampMillis, timeUpperBoundMillis);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        DataFrameTransformCheckpointStats that = (DataFrameTransformCheckpointStats) other;

        return this.timestampMillis == that.timestampMillis &&
                this.timeUpperBoundMillis == that.timeUpperBoundMillis;
    }

    public static DataFrameTransformCheckpointStats fromXContent(XContentParser p) {
        return LENIENT_PARSER.apply(p, null);
    }

}