/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index;

import org.elasticsearch.common.settings.IndexScopedSettings;

import java.time.Instant;

/**
 * Bounds for the {@code @timestamp} field on this index.
 */
public class TimestampBounds {
    private final long startTime;
    private volatile long endTime;

    TimestampBounds(IndexScopedSettings scopedSettings) {
        startTime = scopedSettings.get(IndexSettings.TIME_SERIES_START_TIME).toEpochMilli();
        endTime = scopedSettings.get(IndexSettings.TIME_SERIES_END_TIME).toEpochMilli();
        scopedSettings.addSettingsUpdateConsumer(IndexSettings.TIME_SERIES_END_TIME, this::updateEndTime);
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

    private void updateEndTime(Instant endTimeInstant) {
        long newEndTime = endTimeInstant.toEpochMilli();
        if (this.endTime > newEndTime) {
            throw new IllegalArgumentException(
                "index.time_series.end_time must be larger than current value [" + this.endTime + "] but was [" + newEndTime + "]"
            );
        }
        this.endTime = newEndTime;
    }

    @Override
    public String toString() {
        return startTime + "-" + endTime;
    }
}
