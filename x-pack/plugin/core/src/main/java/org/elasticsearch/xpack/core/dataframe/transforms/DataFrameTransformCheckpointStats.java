/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.transforms;

import org.elasticsearch.Version;
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

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Checkpoint stats data for 1 checkpoint
 *
 * This is the user-facing side of DataFrameTransformCheckpoint, containing only the stats to be exposed.
 */
public class DataFrameTransformCheckpointStats implements Writeable, ToXContentObject {

    public static final DataFrameTransformCheckpointStats EMPTY = new DataFrameTransformCheckpointStats(0L, null, null, 0L, 0L);

    private final long checkpoint;
    private final DataFrameIndexerPosition position;
    private final DataFrameTransformProgress checkpointProgress;
    private final long timestampMillis;
    private final long timeUpperBoundMillis;

    static final ConstructingObjectParser<DataFrameTransformCheckpointStats, Void> LENIENT_PARSER = new ConstructingObjectParser<>(
            "data_frame_transform_checkpoint_stats", true, args -> {
        long checkpoint = args[0] == null ? 0L : (Long) args[0];
        DataFrameIndexerPosition position = (DataFrameIndexerPosition) args[1];
        DataFrameTransformProgress checkpointProgress = (DataFrameTransformProgress) args[2];
        long timestamp = args[3] == null ? 0L : (Long) args[3];
        long timeUpperBound = args[4] == null ? 0L : (Long) args[4];

        return new DataFrameTransformCheckpointStats(checkpoint, position, checkpointProgress, timestamp, timeUpperBound);
    });

    static {
        LENIENT_PARSER.declareLong(optionalConstructorArg(), DataFrameField.CHECKPOINT);
        LENIENT_PARSER.declareObject(optionalConstructorArg(), DataFrameIndexerPosition.PARSER, DataFrameField.POSITION);
        LENIENT_PARSER.declareObject(optionalConstructorArg(), DataFrameTransformProgress.PARSER, DataFrameField.CHECKPOINT_PROGRESS);
        LENIENT_PARSER.declareLong(optionalConstructorArg(), DataFrameField.TIMESTAMP_MILLIS);
        LENIENT_PARSER.declareLong(optionalConstructorArg(), DataFrameField.TIME_UPPER_BOUND_MILLIS);
    }

    public DataFrameTransformCheckpointStats(final long checkpoint, final DataFrameIndexerPosition position,
                                             final DataFrameTransformProgress checkpointProgress, final long timestampMillis,
                                             final long timeUpperBoundMillis) {
        this.checkpoint = checkpoint;
        this.position = position;
        this.checkpointProgress = checkpointProgress;
        this.timestampMillis = timestampMillis;
        this.timeUpperBoundMillis = timeUpperBoundMillis;
    }

    public DataFrameTransformCheckpointStats(StreamInput in) throws IOException {
        if (in.getVersion().onOrAfter(Version.V_7_4_0)) {
            this.checkpoint = in.readVLong();
            if (in.readBoolean()) {
                this.position = new DataFrameIndexerPosition(in);
            } else {
                this.position = null;
            }
            if (in.readBoolean()) {
                this.checkpointProgress = new DataFrameTransformProgress(in);
            } else {
                this.checkpointProgress = null;
            }
        } else {
            this.checkpoint = 0;
            this.position = null;
            this.checkpointProgress = null;
        }
        this.timestampMillis = in.readLong();
        this.timeUpperBoundMillis = in.readLong();
    }

    public long getCheckpoint() {
        return checkpoint;
    }

    public DataFrameIndexerPosition getPosition() {
        return position;
    }

    public DataFrameTransformProgress getCheckpointProgress() {
        return checkpointProgress;
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
        builder.field(DataFrameField.CHECKPOINT.getPreferredName(), checkpoint);
        if (position != null) {
            builder.field(DataFrameField.POSITION.getPreferredName(), position);
        }
        if (checkpointProgress != null) {
            builder.field(DataFrameField.CHECKPOINT_PROGRESS.getPreferredName(), checkpointProgress);
        }
        if (timestampMillis > 0) {
            builder.timeField(DataFrameField.TIMESTAMP_MILLIS.getPreferredName(), DataFrameField.TIMESTAMP.getPreferredName(),
                timestampMillis);
        }
        if (timeUpperBoundMillis > 0) {
            builder.timeField(DataFrameField.TIME_UPPER_BOUND_MILLIS.getPreferredName(), DataFrameField.TIME_UPPER_BOUND.getPreferredName(),
                timeUpperBoundMillis);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().onOrAfter(Version.V_7_4_0)) {
            out.writeVLong(checkpoint);
            if (position != null) {
                out.writeBoolean(true);
                position.writeTo(out);
            } else {
                out.writeBoolean(false);
            }
            if (checkpointProgress != null) {
                out.writeBoolean(true);
                checkpointProgress.writeTo(out);
            } else {
                out.writeBoolean(false);
            }
        }
        out.writeLong(timestampMillis);
        out.writeLong(timeUpperBoundMillis);
    }

    @Override
    public int hashCode() {
        return Objects.hash(checkpoint, position, checkpointProgress, timestampMillis, timeUpperBoundMillis);
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

        return this.checkpoint == that.checkpoint
            && Objects.equals(this.position, that.position)
            && Objects.equals(this.checkpointProgress, that.checkpointProgress)
            && this.timestampMillis == that.timestampMillis
            && this.timeUpperBoundMillis == that.timeUpperBoundMillis;
    }

    public static DataFrameTransformCheckpointStats fromXContent(XContentParser p) {
        return LENIENT_PARSER.apply(p, null);
    }
}
