/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.transform.TransformField;

import java.io.IOException;
import java.util.Objects;

/**
 * Checkpoint stats data for 1 checkpoint
 *
 * This is the user-facing side of TransformCheckpoint, containing only the stats to be exposed.
 */
public class TransformCheckpointStats implements Writeable, ToXContentObject {

    public static final TransformCheckpointStats EMPTY = new TransformCheckpointStats(0L, null, null, 0L, 0L);

    private final long checkpoint;
    private final TransformIndexerPosition position;
    private final TransformProgress checkpointProgress;
    private final long timestampMillis;
    private final long timeUpperBoundMillis;

    public TransformCheckpointStats(
        final long checkpoint,
        final TransformIndexerPosition position,
        final TransformProgress checkpointProgress,
        final long timestampMillis,
        final long timeUpperBoundMillis
    ) {
        this.checkpoint = checkpoint;
        this.position = position;
        this.checkpointProgress = checkpointProgress;
        this.timestampMillis = timestampMillis;
        this.timeUpperBoundMillis = timeUpperBoundMillis;
    }

    public TransformCheckpointStats(StreamInput in) throws IOException {
        this.checkpoint = in.readVLong();
        if (in.readBoolean()) {
            this.position = new TransformIndexerPosition(in);
        } else {
            this.position = null;
        }
        if (in.readBoolean()) {
            this.checkpointProgress = new TransformProgress(in);
        } else {
            this.checkpointProgress = null;
        }
        this.timestampMillis = in.readLong();
        this.timeUpperBoundMillis = in.readLong();
    }

    public long getCheckpoint() {
        return checkpoint;
    }

    public TransformIndexerPosition getPosition() {
        return position;
    }

    public TransformProgress getCheckpointProgress() {
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
        builder.field(TransformField.CHECKPOINT.getPreferredName(), checkpoint);
        if (position != null) {
            builder.field(TransformField.POSITION.getPreferredName(), position);
        }
        if (checkpointProgress != null) {
            builder.field(TransformField.CHECKPOINT_PROGRESS.getPreferredName(), checkpointProgress);
        }
        if (timestampMillis > 0) {
            builder.timeField(
                TransformField.TIMESTAMP_MILLIS.getPreferredName(),
                TransformField.TIMESTAMP.getPreferredName(),
                timestampMillis
            );
        }
        if (timeUpperBoundMillis > 0) {
            builder.timeField(
                TransformField.TIME_UPPER_BOUND_MILLIS.getPreferredName(),
                TransformField.TIME_UPPER_BOUND.getPreferredName(),
                timeUpperBoundMillis
            );
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
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

        TransformCheckpointStats that = (TransformCheckpointStats) other;

        return this.checkpoint == that.checkpoint
            && Objects.equals(this.position, that.position)
            && Objects.equals(this.checkpointProgress, that.checkpointProgress)
            && this.timestampMillis == that.timestampMillis
            && this.timeUpperBoundMillis == that.timeUpperBoundMillis;
    }
}
