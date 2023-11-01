/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.blobcache;

import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;

public class BlobCacheMetrics {
    private final LongCounter cacheMissCounter;
    private final LongHistogram cacheMissLoadTimes;

    public BlobCacheMetrics(MeterRegistry meterRegistry) {
        this(
            meterRegistry.registerLongCounter(
                "elasticsearch.blob_cache.miss_that_triggered_read",
                "The number of times there was a cache miss that triggered a read from the blob store",
                "count"
            ),
            meterRegistry.registerLongHistogram(
                "elasticsearch.blob_cache.cache_miss_load_times",
                "The timing data for populating entries in the blob store resulting from a cache miss.",
                "count"
            )
        );
    }

    BlobCacheMetrics(LongCounter cacheMissCounter, LongHistogram cacheMissLoadTimes) {
        this.cacheMissCounter = cacheMissCounter;
        this.cacheMissLoadTimes = cacheMissLoadTimes;
    }

    public static BlobCacheMetrics NOOP = new BlobCacheMetrics(TelemetryProvider.NOOP.getMeterRegistry());

    public LongCounter getCacheMissCounter() {
        return cacheMissCounter;
    }

    public LongHistogram getCacheMissLoadTimes() {
        return cacheMissLoadTimes;
    }
}
