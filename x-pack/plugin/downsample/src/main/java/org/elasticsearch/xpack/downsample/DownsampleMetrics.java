/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.io.IOException;
import java.util.Map;

/**
 * Contains metrics related to downsampling actions.
 * It gets initialized as a component by the {@link Downsample} plugin, can be injected to its actions.
 *
 * In tests, use TestTelemetryPlugin to inject a MeterRegistry for testing purposes
 * and check that metrics get recorded as expected.
 *
 * To add a new metric, you need to:
 *  - Add a constant for its name, following the naming conventions for metrics.
 *  - Register it in method {@link #doStart}.
 *  - Add a function for recording its value.
 *  - If needed, inject {@link DownsampleMetrics} to the action containing the logic
 *    that records the metric value. For reference, see {@link TransportDownsampleIndexerAction}.
 */
public class DownsampleMetrics extends AbstractLifecycleComponent {

    public static final String LATENCY_SHARD = "es.tsdb.downsample.latency.shard.histogram";
    public static final String LATENCY_TOTAL = "es.tsdb.downsample.latency.total.histogram";
    public static final String ACTIONS_SHARD = "es.tsdb.downsample.actions.shard.total";
    public static final String ACTIONS = "es.tsdb.downsample.actions.total";

    private final MeterRegistry meterRegistry;

    public DownsampleMetrics(TelemetryProvider telemetryProvider) {
        this.meterRegistry = telemetryProvider.getMeterRegistry();
    }

    @Override
    protected void doStart() {
        // Register all metrics to track.
        meterRegistry.registerLongHistogram(LATENCY_SHARD, "Downsampling action latency per shard", "ms");
        meterRegistry.registerLongHistogram(LATENCY_TOTAL, "Downsampling latency end-to-end", "ms");
        meterRegistry.registerLongCounter(ACTIONS_SHARD, "Number of shard-level downsampling actions", "count");
        meterRegistry.registerLongCounter(ACTIONS, "Number of downsampling operations", "count");
    }

    @Override
    protected void doStop() {}

    @Override
    protected void doClose() throws IOException {}

    enum ActionStatus {

        SUCCESS("success"),
        MISSING_DOCS("missing_docs"),
        FAILED("failed"),
        INVALID_CONFIGURATION("invalid_configuration");

        static final String NAME = "status";

        private final String message;

        ActionStatus(String message) {
            this.message = message;
        }

        String getMessage() {
            return message;
        }
    }

    void recordShardOperation(long durationInMilliSeconds, ActionStatus status) {
        meterRegistry.getLongHistogram(LATENCY_SHARD).record(durationInMilliSeconds, Map.of(ActionStatus.NAME, status.getMessage()));
        meterRegistry.getLongCounter(ACTIONS_SHARD).incrementBy(1L, Map.of(ActionStatus.NAME, status.getMessage()));
    }

    void recordOperation(long durationInMilliSeconds, ActionStatus status) {
        meterRegistry.getLongHistogram(LATENCY_TOTAL).record(durationInMilliSeconds, Map.of(ActionStatus.NAME, status.getMessage()));
        meterRegistry.getLongCounter(ACTIONS).incrementBy(1L, Map.of(ActionStatus.NAME, status.getMessage()));
    }
}
