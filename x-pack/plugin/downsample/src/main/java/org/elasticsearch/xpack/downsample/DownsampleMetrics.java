/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.io.IOException;
import java.util.Map;

public class DownsampleMetrics extends AbstractLifecycleComponent {

    public static final String LATENCY_SHARD = "es.tsdb.downsample.latency.shard.histogram";

    private final MeterRegistry meterRegistry;

    public DownsampleMetrics(MeterRegistry meterRegistry) {
        super();
        this.meterRegistry = meterRegistry;
    }

    @Override
    protected void doStart() {
        meterRegistry.registerLongHistogram(LATENCY_SHARD, "Downsampling action latency per shard", "ms");
    }

    @Override
    protected void doStop() {}

    @Override
    protected void doClose() throws IOException {}

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
