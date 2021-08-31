/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.execution.search;

import java.time.Instant;

// wrapper for Unix epoch timestamps with different resolutions.
public abstract class Timestamp {
    static final long MILLIS_PER_SECOND = 1_000L;
    static final long NANOS_PER_MILLI = 1_000_000L;
    static final long NANOS_PER_SECOND = MILLIS_PER_SECOND * NANOS_PER_MILLI;
    private static final long[] MICROS_MULTIPLIER = {0L, 100_000L, 10_000L, 1_000L, 1_00L, 10L};

    private String source;

    abstract Instant timestamp();

    int compareTo(Timestamp other) {
        return timestamp().compareTo(other.timestamp());
    }

    public static Timestamp of(String milliseconds) {
        Timestamp timestamp;
        // ES will provide a <millis>.<micros> with nano-timestamps
        int dotIndex = milliseconds.lastIndexOf('.');
        if (dotIndex > 0) {
            long millis = Long.parseLong(milliseconds.substring(0, dotIndex));
            int digits = milliseconds.length() - dotIndex - 1;
            long micros = (digits >= 6) ? Long.parseLong(milliseconds.substring(dotIndex + 1, dotIndex + 1 + 6)) :
                Long.parseLong(milliseconds.substring(dotIndex + 1)) * MICROS_MULTIPLIER[digits];
            timestamp = new NanosTimestamp(millis, micros);
        } else {
            timestamp = new MillisTimestamp(Long.parseLong(milliseconds));
        }

        timestamp.source = milliseconds;
        return timestamp;
    }

    // time delta in nanos from other to instance
    public long diff(Timestamp other) {
        return diff(other, this);
    }

    // time span in nanos from ts1 to ts2
    static long diff(Timestamp ts1, Timestamp ts2) {
        Instant diff = ts2.timestamp().minusNanos(ts1.timestamp().getNano()).minusSeconds(ts1.timestamp().getEpochSecond());
        return diff.getEpochSecond() * NANOS_PER_SECOND + diff.getNano();
    }

    @Override
    public String toString() {
        return source != null ? source : asString();
    }

    abstract String asString();
}
