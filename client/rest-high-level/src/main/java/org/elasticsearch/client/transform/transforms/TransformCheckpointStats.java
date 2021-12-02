/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transform.transforms;

import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class TransformCheckpointStats {

    public static final ParseField CHECKPOINT = new ParseField("checkpoint");
    public static final ParseField POSITION = new ParseField("position");
    public static final ParseField CHECKPOINT_PROGRESS = new ParseField("checkpoint_progress");
    public static final ParseField TIMESTAMP_MILLIS = new ParseField("timestamp_millis");
    public static final ParseField TIME_UPPER_BOUND_MILLIS = new ParseField("time_upper_bound_millis");

    public static final TransformCheckpointStats EMPTY = new TransformCheckpointStats(0L, null, null, 0L, 0L);

    private final long checkpoint;
    private final TransformIndexerPosition position;
    private final TransformProgress checkpointProgress;
    private final long timestampMillis;
    private final long timeUpperBoundMillis;

    public static final ConstructingObjectParser<TransformCheckpointStats, Void> LENIENT_PARSER = new ConstructingObjectParser<>(
        "transform_checkpoint_stats",
        true,
        args -> {
            long checkpoint = args[0] == null ? 0L : (Long) args[0];
            TransformIndexerPosition position = (TransformIndexerPosition) args[1];
            TransformProgress checkpointProgress = (TransformProgress) args[2];
            long timestamp = args[3] == null ? 0L : (Long) args[3];
            long timeUpperBound = args[4] == null ? 0L : (Long) args[4];

            return new TransformCheckpointStats(checkpoint, position, checkpointProgress, timestamp, timeUpperBound);
        }
    );

    static {
        LENIENT_PARSER.declareLong(optionalConstructorArg(), CHECKPOINT);
        LENIENT_PARSER.declareObject(optionalConstructorArg(), TransformIndexerPosition.PARSER, POSITION);
        LENIENT_PARSER.declareObject(optionalConstructorArg(), TransformProgress.PARSER, CHECKPOINT_PROGRESS);
        LENIENT_PARSER.declareLong(optionalConstructorArg(), TIMESTAMP_MILLIS);
        LENIENT_PARSER.declareLong(optionalConstructorArg(), TIME_UPPER_BOUND_MILLIS);
    }

    public static TransformCheckpointStats fromXContent(XContentParser parser) throws IOException {
        return LENIENT_PARSER.parse(parser, null);
    }

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
