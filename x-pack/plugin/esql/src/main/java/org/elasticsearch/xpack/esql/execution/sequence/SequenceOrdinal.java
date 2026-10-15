/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

/**
 * An event's position within the shared sequence snapshot. The fractional millisecond preserves
 * {@code date_nanos} precision without restricting ordinary dates to the epoch-nanosecond range.
 * {@code shardDoc} must come from the same PIT with a fixed shard set and shard ordering for every stage and page;
 * shard prefiltering must not renumber its high bits. Node-local document coordinates cannot substitute for it.
 * Expanded rows retain their source event's ordinal.
 */
public record SequenceOrdinal(long epochMillis, int nanoOfMilli, long shardDoc) implements Comparable<SequenceOrdinal> {
    public SequenceOrdinal {
        if (nanoOfMilli < 0 || nanoOfMilli >= 1_000_000) {
            throw new IllegalArgumentException("fractional millisecond must be between 0 and 999999");
        }
        if (shardDoc < 0) {
            throw new IllegalArgumentException("PIT shard document ordinal must be non-negative");
        }
    }

    @Override
    public int compareTo(SequenceOrdinal other) {
        int result = Long.compare(epochMillis, other.epochMillis);
        if (result == 0) {
            result = Integer.compare(nanoOfMilli, other.nanoOfMilli);
        }
        return result == 0 ? Long.compare(shardDoc, other.shardDoc) : result;
    }

    /** Tests the inclusive total span without overflowing for dates separated by more than {@code Long.MAX_VALUE} milliseconds. */
    boolean exceedsSpan(SequenceOrdinal start, long maxSpanNanos) {
        long millis = epochMillis - start.epochMillis;
        // Callers only compare ordered timestamps. A negative difference therefore means subtraction overflowed.
        if (millis < 0) {
            return true;
        }
        int nanos = nanoOfMilli - start.nanoOfMilli;
        if (nanos < 0) {
            millis--;
            nanos += 1_000_000;
        }
        long allowedMillis = maxSpanNanos / 1_000_000;
        return millis > allowedMillis || (millis == allowedMillis && nanos > maxSpanNanos % 1_000_000);
    }
}
