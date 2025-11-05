/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index;

import java.time.Instant;

/**
 * Represents the time bounds for the {@code @timestamp} field on an index.
 * Used primarily for time-series indices to enforce temporal boundaries on documents.
 */
public class TimestampBounds {

    /**
     * Creates a new TimestampBounds instance with an updated end time.
     * The new end time must be greater than the current end time.
     *
     * @param current the current TimestampBounds instance
     * @param newEndTime the new end time to set
     * @return a new TimestampBounds instance with the updated end time
     * @throws IllegalArgumentException if the new end time is not greater than the current end time
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * TimestampBounds current = new TimestampBounds(
     *     Instant.parse("2024-01-01T00:00:00Z"),
     *     Instant.parse("2024-02-01T00:00:00Z")
     * );
     * TimestampBounds updated = TimestampBounds.updateEndTime(
     *     current,
     *     Instant.parse("2024-03-01T00:00:00Z")
     * );
     * }</pre>
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

    /**
     * Constructs a TimestampBounds with the specified start and end times.
     *
     * @param startTime the first valid timestamp for the index
     * @param endTime the first invalid timestamp for the index (exclusive upper bound)
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * TimestampBounds bounds = new TimestampBounds(
     *     Instant.parse("2024-01-01T00:00:00Z"),
     *     Instant.parse("2024-02-01T00:00:00Z")
     * );
     * }</pre>
     */
    public TimestampBounds(Instant startTime, Instant endTime) {
        this(startTime.toEpochMilli(), endTime.toEpochMilli());
    }

    private TimestampBounds(long startTime, long endTime) {
        this.startTime = startTime;
        this.endTime = endTime;
    }

    /**
     * Retrieves the first valid {@code @timestamp} for the index in milliseconds since epoch.
     *
     * @return the start time in milliseconds since epoch (inclusive lower bound)
     */
    public long startTime() {
        return startTime;
    }

    /**
     * Retrieves the first invalid {@code @timestamp} for the index in milliseconds since epoch.
     *
     * @return the end time in milliseconds since epoch (exclusive upper bound)
     */
    public long endTime() {
        return endTime;
    }

    @Override
    public String toString() {
        return startTime + "-" + endTime;
    }
}
