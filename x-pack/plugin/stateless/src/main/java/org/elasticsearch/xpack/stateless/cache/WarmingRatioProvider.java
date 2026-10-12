/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit;

/**
 * SPI for computing the warming ratio for a compound commit.
 * Implementation is to be provided via {@link WarmingRatioProviderFactory}.
 */
public interface WarmingRatioProvider {
    /**
     * Computes the warming ratio for a compound commit, determining what fraction of its data
     * should be pre-warmed into the shared blob cache. Currently, only used on search nodes.
     * A ratio of 0 means no pre-warming; 1 means full pre-warming.
     *
     * @param timestampFieldValueRange the CC's {@code @timestamp} range, or {@code null} if none was recorded
     * @param resolvedCCTimestampMillis representative timestamp for the CC, as resolved by
     *        {@link org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectory#resolveRegionTimestampMillis}
     * @param nowMillis current time in epoch millis
     */
    double getWarmingRatio(
        @Nullable StatelessCompoundCommit.TimestampFieldValueRange timestampFieldValueRange,
        long resolvedCCTimestampMillis,
        long nowMillis
    );
}
