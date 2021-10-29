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

    private final LongSummaryStatistics timingStats;
    private final Instant lastUsed;
    private final int queueSize;

    ModelStats(LongSummaryStatistics timingStats, Instant lastUsed, int queueSize) {
        this.timingStats = timingStats;
        this.lastUsed = lastUsed;
        this.queueSize = queueSize;
    }

    public LongSummaryStatistics getTimingStats() {
        return timingStats;
    }

    public Instant getLastUsed() {
        return lastUsed;
    }

    public int getQueueSize() {
        return queueSize;
    }
}
