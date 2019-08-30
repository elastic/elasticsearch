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
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.common.time.TimeUtils;

import java.io.IOException;
import java.time.Instant;
import java.util.Objects;

/**
 * Holds information about checkpointing regarding
 *  - the current checkpoint
 *  - the in progress checkpoint
 *  - the current state of the source
 */
public class DataFrameTransformCheckpointingInfo implements Writeable, ToXContentObject {

    public static final DataFrameTransformCheckpointingInfo EMPTY = new DataFrameTransformCheckpointingInfo(
        DataFrameTransformCheckpointStats.EMPTY,
        DataFrameTransformCheckpointStats.EMPTY,
        0L,
        null);

    public static final ParseField LAST_CHECKPOINT = new ParseField("last");
    public static final ParseField NEXT_CHECKPOINT = new ParseField("next");
    public static final ParseField OPERATIONS_BEHIND = new ParseField("operations_behind");
    public static final ParseField CHANGES_LAST_DETECTED_AT = new ParseField("changes_last_detected_at");
    private final DataFrameTransformCheckpointStats last;
    private final DataFrameTransformCheckpointStats next;
    private final long operationsBehind;
    private Instant changesLastDetectedAt;

    private static final ConstructingObjectParser<DataFrameTransformCheckpointingInfo, Void> LENIENT_PARSER =
            new ConstructingObjectParser<>(
                "data_frame_transform_checkpointing_info",
                true,
                a -> {
                        long behind = a[2] == null ? 0L : (Long) a[2];
                        Instant changesLastDetectedAt = (Instant)a[3];
                        return new DataFrameTransformCheckpointingInfo(
                            a[0] == null ? DataFrameTransformCheckpointStats.EMPTY : (DataFrameTransformCheckpointStats) a[0],
                            a[1] == null ? DataFrameTransformCheckpointStats.EMPTY : (DataFrameTransformCheckpointStats) a[1],
                            behind,
                            changesLastDetectedAt);
                });

    static {
        LENIENT_PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(),
            DataFrameTransformCheckpointStats.LENIENT_PARSER::apply, LAST_CHECKPOINT);
        LENIENT_PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(),
            DataFrameTransformCheckpointStats.LENIENT_PARSER::apply, NEXT_CHECKPOINT);
        LENIENT_PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), OPERATIONS_BEHIND);
        LENIENT_PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(),
            p -> TimeUtils.parseTimeFieldToInstant(p, CHANGES_LAST_DETECTED_AT.getPreferredName()),
            CHANGES_LAST_DETECTED_AT,
            ObjectParser.ValueType.VALUE);
    }

    /**
     * Create checkpoint stats object with checkpoint information about the last and next checkpoint as well as the current state
     * of source.
     *
     * @param last stats of the last checkpoint
     * @param next stats of the next checkpoint
     * @param operationsBehind counter of operations the current checkpoint is behind source
     * @param changesLastDetectedAt the last time the source indices were checked for changes
     */
    public DataFrameTransformCheckpointingInfo(DataFrameTransformCheckpointStats last,
                                               DataFrameTransformCheckpointStats next,
                                               long operationsBehind,
                                               Instant changesLastDetectedAt) {
        this.last = Objects.requireNonNull(last);
        this.next = Objects.requireNonNull(next);
        this.operationsBehind = operationsBehind;
        this.changesLastDetectedAt = changesLastDetectedAt == null ? null : Instant.ofEpochMilli(changesLastDetectedAt.toEpochMilli());
    }

    public DataFrameTransformCheckpointingInfo(DataFrameTransformCheckpointStats last,
                                               DataFrameTransformCheckpointStats next,
                                               long operationsBehind) {
        this(last, next, operationsBehind, null);
    }

    public DataFrameTransformCheckpointingInfo(StreamInput in) throws IOException {
        last = new DataFrameTransformCheckpointStats(in);
        next = new DataFrameTransformCheckpointStats(in);
        operationsBehind = in.readLong();
        if (in.getVersion().onOrAfter(Version.V_7_4_0)) {
            changesLastDetectedAt = in.readOptionalInstant();
        }
    }

    public DataFrameTransformCheckpointStats getLast() {
        return last;
    }

    public DataFrameTransformCheckpointStats getNext() {
        return next;
    }

    public long getOperationsBehind() {
        return operationsBehind;
    }

    public Instant getChangesLastDetectedAt() {
        return changesLastDetectedAt;
    }

    public DataFrameTransformCheckpointingInfo setChangesLastDetectedAt(Instant changesLastDetectedAt) {
        this.changesLastDetectedAt = Instant.ofEpochMilli(Objects.requireNonNull(changesLastDetectedAt).toEpochMilli());
        return this;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(LAST_CHECKPOINT.getPreferredName(), last);
        if (next.getCheckpoint() > 0) {
            builder.field(NEXT_CHECKPOINT.getPreferredName(), next);
        }
        builder.field(OPERATIONS_BEHIND.getPreferredName(), operationsBehind);
        if (changesLastDetectedAt != null) {
            builder.timeField(CHANGES_LAST_DETECTED_AT.getPreferredName(),
                CHANGES_LAST_DETECTED_AT.getPreferredName() + "_string",
                changesLastDetectedAt.toEpochMilli());
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        last.writeTo(out);
        next.writeTo(out);
        out.writeLong(operationsBehind);
        if (out.getVersion().onOrAfter(Version.V_7_4_0)) {
            out.writeOptionalInstant(changesLastDetectedAt);
        }
    }

    public static DataFrameTransformCheckpointingInfo fromXContent(XContentParser p) {
        return LENIENT_PARSER.apply(p, null);
    }

    @Override
    public int hashCode() {
        return Objects.hash(last, next, operationsBehind, changesLastDetectedAt);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        DataFrameTransformCheckpointingInfo that = (DataFrameTransformCheckpointingInfo) other;

        return Objects.equals(this.last, that.last) &&
            Objects.equals(this.next, that.next) &&
            this.operationsBehind == that.operationsBehind &&
            Objects.equals(this.changesLastDetectedAt, that.changesLastDetectedAt);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
