/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongHistogram;

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
 */
public class DownsampleMetrics extends AbstractLifecycleComponent {

    public static final String LATENCY_SHARD = "es.tsdb.downsample.latency.shard.histogram";
    public static final String LATENCY_TOTAL = "es.tsdb.downsample.latency.total.histogram";
    public static final String ACTIONS_SHARD = "es.tsdb.downsample.actions.shard.total";
    public static final String ACTIONS_TOTAL = "es.tsdb.downsample.actions.total";

    private final LongHistogram shardLatency;
    private final LongHistogram totalLatency;
    private final LongCounter shardActions;
    private final LongCounter totalActions;

    public DownsampleMetrics(TelemetryProvider telemetryProvider) {
        var meterRegistry = telemetryProvider.getMeterRegistry();
        shardLatency = meterRegistry.registerLongHistogram(LATENCY_SHARD, "Downsampling action latency per shard", "ms");
        totalLatency = meterRegistry.registerLongHistogram(LATENCY_TOTAL, "Downsampling latency end-to-end", "ms");
        shardActions = meterRegistry.registerLongCounter(ACTIONS_SHARD, "Number of shard-level downsampling actions", "count");
        totalActions = meterRegistry.registerLongCounter(ACTIONS_TOTAL, "Number of downsampling operations", "count");
    }

    @Override
    protected void doStart() {}

    @Override
    protected void doStop() {}

    @Override
    protected void doClose() {}

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
        shardLatency.record(durationInMilliSeconds, Map.of(ActionStatus.NAME, status.getMessage()));
        shardActions.incrementBy(1L, Map.of(ActionStatus.NAME, status.getMessage()));
    }

    void recordOperation(long durationInMilliSeconds, ActionStatus status) {
        totalLatency.record(durationInMilliSeconds, Map.of(ActionStatus.NAME, status.getMessage()));
        totalActions.incrementBy(1L, Map.of(ActionStatus.NAME, status.getMessage()));
    }
}
