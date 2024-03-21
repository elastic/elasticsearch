/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.Map;

class DownsampleMetrics {

    public static final String LATENCY_SHARD = "es.tsdb.downsample.latency.shard.histogram";

    private MeterRegistry meterRegistry = MeterRegistry.NOOP;

    // Singleton.
    public static final DownsampleMetrics INSTANCE = new DownsampleMetrics();

    private DownsampleMetrics() {}

    void setMeterRegistry(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        meterRegistry.registerLongHistogram(LATENCY_SHARD, "Downsampling action latency per shard", "ms");
    }

    enum ShardActionStatus {

        SUCCESS("success"),
        MISSING_DOCS("missing_docs"),
        FAILED("failed");

        public static final String NAME = "status";

        private final String message;

        ShardActionStatus(String message) {
            this.message = message;
        }

        String getMessage() {
            return message;
        }
    }

    void recordLatencyShard(long durationInMilliSeconds, ShardActionStatus status) {
        meterRegistry.getLongHistogram(LATENCY_SHARD).record(durationInMilliSeconds, Map.of(ShardActionStatus.NAME, status.getMessage()));
    }
}
