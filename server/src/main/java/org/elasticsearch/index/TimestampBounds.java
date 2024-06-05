/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index;

import java.time.Instant;

/**
 * Bounds for the {@code @timestamp} field on this index.
 */
public class TimestampBounds {

    /**
     * @return an updated instance based on current instance with a new end time.
     */
    public static TimestampBounds updateEndTime(TimestampBounds current, Instant newEndTime) {
        long newEndTimeMillis = newEndTime.toEpochMilli();
        if (current.endTime > newEndTimeMillis) {
            throw new IllegalArgumentException(
                "index.time_series.end_time must be larger than current value [" + current.endTime + "] but was [" + newEndTime + "]"
            );
        }
        return new TimestampBounds(current.startTime(), newEndTimeMillis);
    }

    private final long startTime;
    private final long endTime;

    public TimestampBounds(Instant startTime, Instant endTime) {
        this(startTime.toEpochMilli(), endTime.toEpochMilli());
    }

    private TimestampBounds(long startTime, long endTime) {
        this.startTime = startTime;
        this.endTime = endTime;
    }

    /**
     * The first valid {@code @timestamp} for the index.
     */
    public long startTime() {
        return startTime;
    }

    /**
     * The first invalid {@code @timestamp} for the index.
     */
    public long endTime() {
        return endTime;
    }

    @Override
    public String toString() {
        return startTime + "-" + endTime;
    }
}
