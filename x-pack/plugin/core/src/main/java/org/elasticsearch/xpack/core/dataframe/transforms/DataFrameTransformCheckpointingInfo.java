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
        false,
        null);

    public static final ParseField LAST_CHECKPOINT = new ParseField("last");
    public static final ParseField NEXT_CHECKPOINT = new ParseField("next");
    public static final ParseField OPERATIONS_BEHIND = new ParseField("operations_behind");
    public static final ParseField FOUND_CHANGES = new ParseField("found_changes");
    public static final ParseField LAST_CHANGE_CHECK = new ParseField("last_change_check");
    private final DataFrameTransformCheckpointStats last;
    private final DataFrameTransformCheckpointStats next;
    private final long operationsBehind;
    private boolean foundChanges = false;
    private Instant lastChangeCheck;

    private static final ConstructingObjectParser<DataFrameTransformCheckpointingInfo, Void> LENIENT_PARSER =
            new ConstructingObjectParser<>(
                "data_frame_transform_checkpointing_info",
                true,
                a -> {
                        long behind = a[2] == null ? 0L : (Long) a[2];
                        boolean foundChanges = a[3] == null ? false : (Boolean) a[3];
                        Instant lastChangeCheck = (Instant)a[4];
                        return new DataFrameTransformCheckpointingInfo(
                            a[0] == null ? DataFrameTransformCheckpointStats.EMPTY : (DataFrameTransformCheckpointStats) a[0],
                            a[1] == null ? DataFrameTransformCheckpointStats.EMPTY : (DataFrameTransformCheckpointStats) a[1],
                            behind,
                            foundChanges,
                            lastChangeCheck);
                });

    static {
        LENIENT_PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(),
            DataFrameTransformCheckpointStats.LENIENT_PARSER::apply, LAST_CHECKPOINT);
        LENIENT_PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(),
            DataFrameTransformCheckpointStats.LENIENT_PARSER::apply, NEXT_CHECKPOINT);
        LENIENT_PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), OPERATIONS_BEHIND);
        LENIENT_PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), FOUND_CHANGES);
        LENIENT_PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(),
            p -> TimeUtils.parseTimeFieldToInstant(p, LAST_CHANGE_CHECK.getPreferredName()),
            LAST_CHANGE_CHECK,
            ObjectParser.ValueType.VALUE);
    }

    /**
     * Create checkpoint stats object with checkpoint information about the last and next checkpoint as well as the current state
     * of source.
     *
     * @param last stats of the last checkpoint
     * @param next stats of the next checkpoint
     * @param operationsBehind counter of operations the current checkpoint is behind source
     * @param foundChanges boolean indicating if changes were found in the last check for changes
     * @param lastChangeCheck the last time the source indices were checked for changes
     */
    public DataFrameTransformCheckpointingInfo(DataFrameTransformCheckpointStats last,
                                               DataFrameTransformCheckpointStats next,
                                               long operationsBehind,
                                               boolean foundChanges,
                                               Instant lastChangeCheck) {
        this.last = Objects.requireNonNull(last);
        this.next = Objects.requireNonNull(next);
        this.operationsBehind = operationsBehind;
        this.foundChanges = foundChanges;
        this.lastChangeCheck = lastChangeCheck == null ? null : Instant.ofEpochMilli(lastChangeCheck.toEpochMilli());
    }

    public DataFrameTransformCheckpointingInfo(DataFrameTransformCheckpointStats last,
                                               DataFrameTransformCheckpointStats next,
                                               long operationsBehind) {
        this(last, next, operationsBehind, false, null);
    }

    public DataFrameTransformCheckpointingInfo(StreamInput in) throws IOException {
        last = new DataFrameTransformCheckpointStats(in);
        next = new DataFrameTransformCheckpointStats(in);
        operationsBehind = in.readLong();
        if (in.getVersion().onOrAfter(Version.V_8_0_0)) {
            foundChanges = in.readBoolean();
            lastChangeCheck = in.readOptionalInstant();
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

    public boolean isFoundChanges() {
        return foundChanges;
    }

    public DataFrameTransformCheckpointingInfo setFoundChanges(boolean foundChanges) {
        this.foundChanges = foundChanges;
        return this;
    }

    public Instant getLastChangeCheck() {
        return lastChangeCheck;
    }

    public DataFrameTransformCheckpointingInfo setLastChangeCheck(Instant lastChangeCheck) {
        this.lastChangeCheck = Instant.ofEpochMilli(Objects.requireNonNull(lastChangeCheck).toEpochMilli());
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
        builder.field(FOUND_CHANGES.getPreferredName(), foundChanges);
        if (lastChangeCheck != null) {
            builder.timeField(LAST_CHANGE_CHECK.getPreferredName(),
                LAST_CHANGE_CHECK.getPreferredName() + "_string",
                lastChangeCheck.toEpochMilli());
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        last.writeTo(out);
        next.writeTo(out);
        out.writeLong(operationsBehind);
        if (out.getVersion().onOrAfter(Version.V_8_0_0)) {
            out.writeBoolean(foundChanges);
            out.writeOptionalInstant(lastChangeCheck);
        }
    }

    public static DataFrameTransformCheckpointingInfo fromXContent(XContentParser p) {
        return LENIENT_PARSER.apply(p, null);
    }

    @Override
    public int hashCode() {
        return Objects.hash(last, next, operationsBehind, lastChangeCheck, foundChanges);
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
            this.foundChanges == that.foundChanges &&
            Objects.equals(this.lastChangeCheck, that.lastChangeCheck);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
