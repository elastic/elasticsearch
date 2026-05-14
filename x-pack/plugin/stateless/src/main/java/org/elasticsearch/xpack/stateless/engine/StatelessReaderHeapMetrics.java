/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.engine;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;

/**
 * Metrics for the stateless reader-heap budget: bytes reserved, configured limit, and deferred-refresh count.
 */
public record StatelessReaderHeapMetrics(LongCounter refreshDeferredCounter) {

    public static final String RESERVED_BYTES = "es.stateless.reader_heap.reserved.bytes";
    public static final String BUDGET_BYTES = "es.stateless.reader_heap.budget.bytes";
    public static final String REFRESH_DEFERRED_TOTAL = "es.stateless.reader_heap.refresh_deferred.total";

    public static final StatelessReaderHeapMetrics NOOP = new StatelessReaderHeapMetrics(
        MeterRegistry.NOOP.registerLongCounter(REFRESH_DEFERRED_TOTAL + ".noop", "noop", "count")
    );

    public static StatelessReaderHeapMetrics register(MeterRegistry registry, CircuitBreaker breaker) {
        registry.registerLongGauge(
            RESERVED_BYTES,
            "Stateless reader-heap bytes currently reserved",
            "bytes",
            () -> new LongWithAttributes(breaker.getUsed())
        );
        registry.registerLongGauge(
            BUDGET_BYTES,
            "Configured stateless reader-heap limit",
            "bytes",
            () -> new LongWithAttributes(breaker.getLimit())
        );
        LongCounter deferredCounter = registry.registerLongCounter(
            REFRESH_DEFERRED_TOTAL,
            "Refreshes deferred because the reader-heap limit would have been exceeded",
            "count"
        );
        return new StatelessReaderHeapMetrics(deferredCounter);
    }

    public void recordRefreshDeferred() {
        refreshDeferredCounter.increment();
    }
}
