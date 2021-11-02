/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transform.transforms;

import org.elasticsearch.client.common.TimeUtil;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.time.Instant;
import java.util.Objects;

public class TransformCheckpointingInfo {

    public static final ParseField LAST_CHECKPOINT = new ParseField("last", "current");
    public static final ParseField NEXT_CHECKPOINT = new ParseField("next", "in_progress");
    public static final ParseField OPERATIONS_BEHIND = new ParseField("operations_behind");
    public static final ParseField CHANGES_LAST_DETECTED_AT = new ParseField("changes_last_detected_at");
    public static final ParseField LAST_SEARCH_TIME = new ParseField("last_search_time");

    private final TransformCheckpointStats last;
    private final TransformCheckpointStats next;
    private final long operationsBehind;
    private final Instant changesLastDetectedAt;
    private final Instant lastSearchTime;

    private static final ConstructingObjectParser<TransformCheckpointingInfo, Void> LENIENT_PARSER = new ConstructingObjectParser<>(
        "transform_checkpointing_info",
        true,
        a -> {
            long behind = a[2] == null ? 0L : (Long) a[2];
            Instant changesLastDetectedAt = (Instant) a[3];
            Instant lastSearchTime = (Instant) a[4];
            return new TransformCheckpointingInfo(
                a[0] == null ? TransformCheckpointStats.EMPTY : (TransformCheckpointStats) a[0],
                a[1] == null ? TransformCheckpointStats.EMPTY : (TransformCheckpointStats) a[1],
                behind,
                changesLastDetectedAt,
                lastSearchTime
            );
        }
    );

    static {
        LENIENT_PARSER.declareObject(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> TransformCheckpointStats.fromXContent(p),
            LAST_CHECKPOINT
        );
        LENIENT_PARSER.declareObject(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> TransformCheckpointStats.fromXContent(p),
            NEXT_CHECKPOINT
        );
        LENIENT_PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), OPERATIONS_BEHIND);
        LENIENT_PARSER.declareField(
            ConstructingObjectParser.optionalConstructorArg(),
            p -> TimeUtil.parseTimeFieldToInstant(p, CHANGES_LAST_DETECTED_AT.getPreferredName()),
            CHANGES_LAST_DETECTED_AT,
            ObjectParser.ValueType.VALUE
        );
        LENIENT_PARSER.declareField(
            ConstructingObjectParser.optionalConstructorArg(),
            p -> TimeUtil.parseTimeFieldToInstant(p, LAST_SEARCH_TIME.getPreferredName()),
            LAST_SEARCH_TIME,
            ObjectParser.ValueType.VALUE
        );
    }

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

    public TransformCheckpointStats getLast() {
        return last;
    }

    public TransformCheckpointStats getNext() {
        return next;
    }

    public long getOperationsBehind() {
        return operationsBehind;
    }

    @Nullable
    public Instant getChangesLastDetectedAt() {
        return changesLastDetectedAt;
    }

    @Nullable
    public Instant getLastSearchTime() {
        return lastSearchTime;
    }

    public static TransformCheckpointingInfo fromXContent(XContentParser p) {
        return LENIENT_PARSER.apply(p, null);
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

}
