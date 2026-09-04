/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery.shardinfo;

import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.Map;

/**
 * This collector is collecting runtime metrics in the SearchShardInformationIndexListener for the
 * TransportFetchSearchShardInformationAction
 * It collects the number of successful calls, error and request duration, as this is supposed to be a fast request.
 * This allows to monitor mainly if these requests sent correctly
 * and with the right attributes one can measure if the right shards were queried
 */
public class SearchShardInformationMetricsCollector {

    public static final String REQUESTS_DURATION_HISTOGRAM = "es.search.shards.information.retrieval.requests.duration.histogram";
    public static final String REQUESTS_SUCCESS_TOTAL = "es.search.shards.information.retrieval.requests.success.total";
    public static final String REQUESTS_ERRORS_TOTAL = "es.search.shards.information.retrieval.requests.errors.total";
    public static final String REQUESTS_SHARD_MOVED_TOTAL = "es.search.shards.information.retrieval.requests.shard_moved.total";

    private final LongHistogram histogram;
    private final LongCounter successes;
    private final LongCounter errors;
    private final LongCounter shardMoved;

    public SearchShardInformationMetricsCollector(TelemetryProvider telemetryProvider) {
        final MeterRegistry meterRegistry = telemetryProvider.getMeterRegistry();

        this.histogram = meterRegistry.registerLongHistogram(REQUESTS_DURATION_HISTOGRAM, "duration of shard information calls", "ms");
        this.successes = meterRegistry.registerLongCounter(REQUESTS_SUCCESS_TOTAL, "successful requests", "count");
        this.errors = meterRegistry.registerLongCounter(REQUESTS_ERRORS_TOTAL, "failed requests", "count");
        this.shardMoved = meterRegistry.registerLongCounter(REQUESTS_SHARD_MOVED_TOTAL, "shard moved requests", "count");
    }

    public void recordSuccess(long duration, Map<String, Object> attributes) {
        this.successes.incrementBy(1, attributes);
        this.histogram.record(duration, attributes);
    }

    public void recordError() {
        this.errors.increment();
    }

    public void shardMoved() {
        this.shardMoved.increment();
    }
}
