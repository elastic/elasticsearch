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
    private final long startTime;
    private final long endTime;

    public TimestampBounds(Instant startTime, Instant endTime) {
        this.startTime = startTime.toEpochMilli();
        this.endTime = endTime.toEpochMilli();
    }

    public TimestampBounds(TimestampBounds previous, Instant newEndTime) {
        long newEndTimeMillis = newEndTime.toEpochMilli();
        if (previous.endTime > newEndTimeMillis) {
            throw new IllegalArgumentException(
                "index.time_series.end_time must be larger than current value [" + previous.endTime + "] but was [" + newEndTime + "]"
            );
        }
        this.startTime = previous.startTime();
        this.endTime = newEndTimeMillis;
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
