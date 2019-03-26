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
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;

import java.io.IOException;
import java.util.Objects;

/**
 * Encapsulate stats data from 1 checkpoint
 */
public class SingleCheckpointStats implements Writeable, ToXContentObject {

    public static SingleCheckpointStats EMPTY = new SingleCheckpointStats(0L, 0L);

    private final long timestampMillis;
    private final long timeUpperBoundMillis;

    private static final ConstructingObjectParser<SingleCheckpointStats, Void> LENIENT_PARSER = new ConstructingObjectParser<>(
            "data_frame_transform_checkpoint_stats", true, args -> {
                long timestamp = args[0] == null ? 0L : (Long) args[0];
                long timeUpperBound = args[1] == null ? 0L : (Long) args[1];

                return new SingleCheckpointStats(timestamp, timeUpperBound);
            });

    static {
        LENIENT_PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), DataFrameField.TIMESTAMP_MILLIS);
        LENIENT_PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), DataFrameField.TIME_UPPER_BOUND_MILLIS);
    }

    public SingleCheckpointStats(final long timestampMillis, final long timeUpperBoundMillis) {
        this.timestampMillis = timestampMillis;
        this.timeUpperBoundMillis = timeUpperBoundMillis;
    }

    public SingleCheckpointStats(StreamInput in) throws IOException {
        this.timestampMillis = in.readLong();
        this.timeUpperBoundMillis = in.readLong();
    }

    public long getTimestampMillis() {
        return timestampMillis;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.timeField(DataFrameField.TIMESTAMP_MILLIS.getPreferredName(), DataFrameField.TIMESTAMP.getPreferredName(),
                getTimestampMillis());

        if (timeUpperBoundMillis > 0) {
            builder.timeField(DataFrameField.TIME_UPPER_BOUND_MILLIS.getPreferredName(),
                    DataFrameField.TIME_UPPER_BOUND.getPreferredName(), timeUpperBoundMillis);
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

        SingleCheckpointStats that = (SingleCheckpointStats) other;

        return this.timestampMillis == that.timestampMillis &&
                this.timeUpperBoundMillis == that.timeUpperBoundMillis;
    }

    public static SingleCheckpointStats fromXContent(XContentParser p) {
        return LENIENT_PARSER.apply(p, null);
    }

}