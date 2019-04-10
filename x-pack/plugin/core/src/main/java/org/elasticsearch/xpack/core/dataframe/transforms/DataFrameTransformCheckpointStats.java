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

    public static DataFrameTransformCheckpointStats EMPTY = new DataFrameTransformCheckpointStats(0L, 0L, 0L, 0L);

    static String PERCENT_COMPLETE = "percent_complete";
    private final long timestampMillis;
    private final long timeUpperBoundMillis;
    private final long totalDocs;
    private final long completedDocs;

    private static final ConstructingObjectParser<DataFrameTransformCheckpointStats, Void> LENIENT_PARSER = new ConstructingObjectParser<>(
            "data_frame_transform_checkpoint_stats", true, args -> {
                long timestamp = args[0] == null ? 0L : (Long) args[0];
                long timeUpperBound = args[1] == null ? 0L : (Long) args[1];
                long totalDocs = args[2] == null ? 0L : (Long) args[2];
                long completedDocs = args[3] == null ? 0L : (Long) args[3];

                return new DataFrameTransformCheckpointStats(timestamp, timeUpperBound, totalDocs, completedDocs);
            });

    static {
        LENIENT_PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), DataFrameField.TIMESTAMP_MILLIS);
        LENIENT_PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), DataFrameField.TIME_UPPER_BOUND_MILLIS);
        LENIENT_PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), DataFrameField.TOTAL_DOCS);
        LENIENT_PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), DataFrameField.COMPLETED_DOCS);
    }

    public DataFrameTransformCheckpointStats(final long timestampMillis,
                                             final long timeUpperBoundMillis,
                                             final long totalDocs,
                                             final long completedDocs) {
        this.timestampMillis = timestampMillis;
        this.timeUpperBoundMillis = timeUpperBoundMillis;
        this.totalDocs = totalDocs;
        this.completedDocs = completedDocs;
    }

    public DataFrameTransformCheckpointStats(StreamInput in) throws IOException {
        this.timestampMillis = in.readLong();
        this.timeUpperBoundMillis = in.readLong();
        this.totalDocs = in.readLong();
        this.completedDocs = in.readLong();
    }

    public long getTimestampMillis() {
        return timestampMillis;
    }

    public long getTimeUpperBoundMillis() {
        return timeUpperBoundMillis;
    }

    public long getTotalDocs() {
        return totalDocs;
    }

    public long getCompletedDocs() {
        return completedDocs;
    }

    public double getPercentageCompleted() {
        if (completedDocs >= totalDocs) {
            return 1.0;
        }
        return (double)completedDocs/totalDocs;
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
        builder.field(DataFrameField.TOTAL_DOCS.getPreferredName(), totalDocs);
        builder.field(DataFrameField.COMPLETED_DOCS.getPreferredName(), completedDocs);
        builder.field(PERCENT_COMPLETE, getPercentageCompleted());
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(timestampMillis);
        out.writeLong(timeUpperBoundMillis);
        out.writeLong(totalDocs);
        out.writeLong(completedDocs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestampMillis, timeUpperBoundMillis, completedDocs, totalDocs);
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

        return this.timestampMillis == that.timestampMillis
            && this.timeUpperBoundMillis == that.timeUpperBoundMillis
            && this.totalDocs == that.totalDocs
            && this.completedDocs == that.completedDocs;
    }

    public static DataFrameTransformCheckpointStats fromXContent(XContentParser p) {
        return LENIENT_PARSER.apply(p, null);
    }

}
