/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.time.Instant;
import java.util.Objects;

/**
 * Holds information about checkpointing regarding
 *  - the current checkpoint
 *  - the in progress checkpoint
 *  - the current state of the source
 */
public class TransformCheckpointingInfo implements Writeable, ToXContentObject {

    /**
     * Builder for collecting checkpointing information for the purpose of _stats
     */
    public static class TransformCheckpointingInfoBuilder {
        private TransformIndexerPosition nextCheckpointPosition;
        private TransformProgress nextCheckpointProgress;
        private TransformCheckpoint lastCheckpoint;
        private TransformCheckpoint nextCheckpoint;
        private TransformCheckpoint sourceCheckpoint;
        private Instant changesLastDetectedAt;
        private Instant lastSearchTime;
        private long operationsBehind;

        public TransformCheckpointingInfoBuilder() {}

        public TransformCheckpointingInfo build() {
            if (lastCheckpoint == null) {
                lastCheckpoint = TransformCheckpoint.EMPTY;
            }
            if (nextCheckpoint == null) {
                nextCheckpoint = TransformCheckpoint.EMPTY;
            }
            if (sourceCheckpoint == null) {
                sourceCheckpoint = TransformCheckpoint.EMPTY;
            }

            // checkpointstats requires a non-negative checkpoint number
            long lastCheckpointNumber = lastCheckpoint.getCheckpoint() > 0 ? lastCheckpoint.getCheckpoint() : 0;
            long nextCheckpointNumber = nextCheckpoint.getCheckpoint() > 0 ? nextCheckpoint.getCheckpoint() : 0;

            return new TransformCheckpointingInfo(
                new TransformCheckpointStats(
                    lastCheckpointNumber,
                    null,
                    null,
                    lastCheckpoint.getTimestamp(),
                    lastCheckpoint.getTimeUpperBound()
                ),
                new TransformCheckpointStats(
                    nextCheckpointNumber,
                    nextCheckpointPosition,
                    nextCheckpointProgress,
                    nextCheckpoint.getTimestamp(),
                    nextCheckpoint.getTimeUpperBound()
                ),
                operationsBehind,
                changesLastDetectedAt,
                lastSearchTime
            );
        }

        public TransformCheckpointingInfoBuilder setLastCheckpoint(TransformCheckpoint lastCheckpoint) {
            this.lastCheckpoint = lastCheckpoint;
            return this;
        }

        public TransformCheckpoint getLastCheckpoint() {
            return lastCheckpoint;
        }

        public TransformCheckpointingInfoBuilder setNextCheckpoint(TransformCheckpoint nextCheckpoint) {
            this.nextCheckpoint = nextCheckpoint;
            return this;
        }

        public TransformCheckpoint getNextCheckpoint() {
            return nextCheckpoint;
        }

        public TransformCheckpointingInfoBuilder setSourceCheckpoint(TransformCheckpoint sourceCheckpoint) {
            this.sourceCheckpoint = sourceCheckpoint;
            return this;
        }

        public TransformCheckpoint getSourceCheckpoint() {
            return sourceCheckpoint;
        }

        public TransformCheckpointingInfoBuilder setNextCheckpointProgress(TransformProgress nextCheckpointProgress) {
            this.nextCheckpointProgress = nextCheckpointProgress;
            return this;
        }

        public TransformCheckpointingInfoBuilder setNextCheckpointPosition(TransformIndexerPosition nextCheckpointPosition) {
            this.nextCheckpointPosition = nextCheckpointPosition;
            return this;
        }

        public TransformCheckpointingInfoBuilder setChangesLastDetectedAt(Instant changesLastDetectedAt) {
            this.changesLastDetectedAt = changesLastDetectedAt;
            return this;
        }

        public TransformCheckpointingInfoBuilder setLastSearchTime(Instant lastSearchTime) {
            this.lastSearchTime = lastSearchTime;
            return this;
        }

        public TransformCheckpointingInfoBuilder setOperationsBehind(long operationsBehind) {
            this.operationsBehind = operationsBehind;
            return this;
        }

    }

    public static final TransformCheckpointingInfo EMPTY = new TransformCheckpointingInfo(
        TransformCheckpointStats.EMPTY,
        TransformCheckpointStats.EMPTY,
        0L,
        null,
        null
    );

    public static final String LAST_CHECKPOINT = "last";
    public static final String NEXT_CHECKPOINT = "next";
    public static final String OPERATIONS_BEHIND = "operations_behind";
    public static final String CHANGES_LAST_DETECTED_AT = "changes_last_detected_at";
    public static final String CHANGES_LAST_DETECTED_AT_HUMAN = CHANGES_LAST_DETECTED_AT + "_string";

    public static final String LAST_SEARCH_TIME = "last_search_time";
    public static final String LAST_SEARCH_TIME_HUMAN = LAST_SEARCH_TIME + "_string";

    private final TransformCheckpointStats last;
    private final TransformCheckpointStats next;
    private final long operationsBehind;
    private final Instant changesLastDetectedAt;
    private final Instant lastSearchTime;

    /**
     * Create checkpoint stats object with checkpoint information about the last and next checkpoint as well as the current state
     * of source.
     *
     * @param last stats of the last checkpoint
     * @param next stats of the next checkpoint
     * @param operationsBehind counter of operations the current checkpoint is behind source
     * @param changesLastDetectedAt the last time the source indices changes have been found
     * @param lastSearchTime the last time the source indices were searched
     */
    public TransformCheckpointingInfo(
        TransformCheckpointStats last,
        TransformCheckpointStats next,
        long operationsBehind,
        Instant changesLastDetectedAt,
        Instant lastSearchTime
    ) {
        this.last = Objects.requireNonNull(last);
        this.next = Objects.requireNonNull(next);
        this.operationsBehind = operationsBehind;
        this.changesLastDetectedAt = changesLastDetectedAt;
        this.lastSearchTime = lastSearchTime;
    }

    public TransformCheckpointingInfo(StreamInput in) throws IOException {
        last = new TransformCheckpointStats(in);
        next = new TransformCheckpointStats(in);
        operationsBehind = in.readLong();
        changesLastDetectedAt = in.readOptionalInstant();
        lastSearchTime = in.readOptionalInstant();
    }

    public TransformCheckpointStats getLast() {
        return last;
    }

    public TransformCheckpointStats getNext() {
        return next;
    }

    public long getOperationsBehind() {
        return operationsBehind;
    }

    public Instant getChangesLastDetectedAt() {
        return changesLastDetectedAt;
    }

    public Instant getLastSearchTime() {
        return lastSearchTime;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(LAST_CHECKPOINT, last);
        if (next.getCheckpoint() > 0) {
            builder.field(NEXT_CHECKPOINT, next);
        }
        if (operationsBehind > 0) {
            builder.field(OPERATIONS_BEHIND, operationsBehind);
        }
        if (changesLastDetectedAt != null) {
            builder.timestampFieldsFromUnixEpochMillis(
                CHANGES_LAST_DETECTED_AT,
                CHANGES_LAST_DETECTED_AT_HUMAN,
                changesLastDetectedAt.toEpochMilli()
            );
        }
        if (lastSearchTime != null) {
            builder.timestampFieldsFromUnixEpochMillis(LAST_SEARCH_TIME, LAST_SEARCH_TIME_HUMAN, lastSearchTime.toEpochMilli());
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        last.writeTo(out);
        next.writeTo(out);
        out.writeLong(operationsBehind);
        out.writeOptionalInstant(changesLastDetectedAt);
        out.writeOptionalInstant(lastSearchTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(last, next, operationsBehind, changesLastDetectedAt, lastSearchTime);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        TransformCheckpointingInfo that = (TransformCheckpointingInfo) other;

        return Objects.equals(this.last, that.last)
            && Objects.equals(this.next, that.next)
            && this.operationsBehind == that.operationsBehind
            && Objects.equals(this.changesLastDetectedAt, that.changesLastDetectedAt)
            && Objects.equals(this.lastSearchTime, that.lastSearchTime);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
