/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.deployment;

import java.time.Instant;
import java.util.LongSummaryStatistics;

public class ModelStats {

    private final Instant startTime;
    private final LongSummaryStatistics timingStats;
    private final Instant lastUsed;
    private final int pendingCount;
    private final int errorCount;

    ModelStats(Instant startTime, LongSummaryStatistics timingStats, Instant lastUsed, int pendingCount, int errorCount) {
        this.startTime = startTime;
        this.timingStats = timingStats;
        this.lastUsed = lastUsed;
        this.pendingCount = pendingCount;
        this.errorCount = errorCount;
    }

    public Instant getStartTime() {
        return startTime;
    }

    public LongSummaryStatistics getTimingStats() {
        return timingStats;
    }

    public Instant getLastUsed() {
        return lastUsed;
    }

    public int getPendingCount() {
        return pendingCount;
    }

    public int getErrorCount() {
        return errorCount;
    }
}
