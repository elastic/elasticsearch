/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.deployment;

import java.time.Instant;

public record ModelStats(
    Instant startTime,
    long inferenceCount,
    Double averageInferenceTime,
    Double averageInferenceTimeNoCacheHits,
    Instant lastUsed,
    int pendingCount,
    int errorCount,
    long cacheHitCount,
    int rejectedExecutionCount,
    int timeoutCount,
    Integer threadsPerAllocation,
    Integer numberOfAllocations,
    long peakThroughput,
    long throughputLastPeriod,
    Double avgInferenceTimeLastPeriod,
    long cacheHitCountLastPeriod
) {}
