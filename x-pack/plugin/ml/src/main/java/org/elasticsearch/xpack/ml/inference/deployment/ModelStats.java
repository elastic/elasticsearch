/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.deployment;

import org.elasticsearch.common.unit.ByteSizeValue;

import java.time.Instant;
import java.util.LongSummaryStatistics;

public class ModelStats {

    private final LongSummaryStatistics timingStats;
    private final Instant lastUsed;
    private final ByteSizeValue modelSize;

    ModelStats(LongSummaryStatistics timingStats, Instant lastUsed, Long modelSizeBytes) {
        this.timingStats = timingStats;
        this.lastUsed = lastUsed;
        this.modelSize = modelSizeBytes == null ? null : ByteSizeValue.ofBytes(modelSizeBytes);
    }

    public LongSummaryStatistics getTimingStats() {
        return timingStats;
    }

    public Instant getLastUsed() {
        return lastUsed;
    }

    public ByteSizeValue getModelSize() {
        return modelSize;
    }
}
