/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.breaker;

import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongUpDownCounter;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.Collection;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * A class collecting circuit breaker metrics (parent, field data, request, in flight requests and custom child circuit
 * breakers).
 *
 * The circuit breaker name is used as an attribute so that we define a single counter metric where the name is mapped to a 'type'
 * attribute. The counter trips for different reasons even if the underlying reason is "too much memory usage". Aggregating them together
 * results in losing the ability to understand where the underlying issue is (too much field data, too many concurrent requests, too large
 * concurrent requests?). As a result we advise in aggregations queries not to "aggregate away" the type attribute so that you treat each
 * circuit breaker as a separate counter.
 *
 * <p>In addition to {@link #ES_BREAKER_TRIP_COUNT_TOTAL}, this class exposes:
 * <ul>
 *     <li>{@link #ES_BREAKER_MEMORY_HELD} - up-down counter (gauge-like) of currently held bytes, broken down by {@code type} and
 *     {@code category}. Incremented on admit, decremented on labeled release. Releases that go through
 *     {@link org.elasticsearch.common.breaker.CircuitBreaker#addWithoutBreaking(long)} (the unlabeled variant) bucket under
 *     {@code category="uncategorized"}, so callers that want per-category attribution should release via
 *     {@link org.elasticsearch.common.breaker.CircuitBreaker#addWithoutBreaking(long, String)}.</li>
 *     <li>{@link #ES_BREAKER_MEMORY_LIMIT} - asynchronous gauge of the configured limit per breaker.</li>
 *     <li>{@link #ES_BREAKER_MEMORY_ESTIMATED} - asynchronous gauge of the current charged bytes per breaker, equivalent to
 *     the {@code estimated_size_in_bytes} field returned by {@code GET /_nodes/stats/breaker}.</li>
 * </ul>
 * The two async gauges are registered via {@link #registerMemoryGauges(Supplier, Supplier)} by the owning breaker service because that is
 * what has access to the breaker map.
 */
public class CircuitBreakerMetrics {
    public static final CircuitBreakerMetrics NOOP = new CircuitBreakerMetrics(TelemetryProvider.NOOP);
    public static final String ES_BREAKER_TRIP_COUNT_TOTAL = "es.breaker.trip.total";
    public static final String ES_BREAKER_MEMORY_HELD = "es.breaker.memory.held.usage";
    public static final String ES_BREAKER_MEMORY_LIMIT = "es.breaker.memory.limit.size";
    public static final String ES_BREAKER_MEMORY_ESTIMATED = "es.breaker.memory.estimated.usage";

    private final MeterRegistry meterRegistry;
    private final LongCounter tripCount;
    private final LongUpDownCounter memoryHeld;

    private CircuitBreakerMetrics(final MeterRegistry meterRegistry, final LongCounter tripCount, final LongUpDownCounter memoryHeld) {
        this.meterRegistry = meterRegistry;
        this.tripCount = tripCount;
        this.memoryHeld = memoryHeld;
    }

    public CircuitBreakerMetrics(final TelemetryProvider telemetryProvider) {
        this(telemetryProvider.getMeterRegistry());
    }

    private CircuitBreakerMetrics(final MeterRegistry meterRegistry) {
        this(
            meterRegistry,
            meterRegistry.registerLongCounter(ES_BREAKER_TRIP_COUNT_TOTAL, "Circuit breaker trip count", "count"),
            meterRegistry.registerLongUpDownCounter(
                ES_BREAKER_MEMORY_HELD,
                "Currently held bytes per circuit breaker type and category (admit/release-balanced)",
                "By"
            )
        );
    }

    /**
     * Backwards-compatible factory for callers that only have a trip {@link LongCounter}
     */
    @Deprecated(forRemoval = true)
    public static CircuitBreakerMetrics fromTripCount(final LongCounter tripCount) {
        return new CircuitBreakerMetrics(MeterRegistry.NOOP, tripCount, LongUpDownCounter.NOOP);
    }

    public LongCounter getTripCount() {
        return tripCount;
    }

    public LongUpDownCounter getMemoryHeld() {
        return memoryHeld;
    }

    public void registerMemoryGauges(
        final Supplier<Collection<LongWithAttributes>> limitSupplier,
        final Supplier<Collection<LongWithAttributes>> estimatedSupplier
    ) {
        meterRegistry.registerLongsGauge(
            ES_BREAKER_MEMORY_LIMIT,
            "Configured memory limit per circuit breaker, in bytes",
            "By",
            limitSupplier
        );
        meterRegistry.registerLongsGauge(
            ES_BREAKER_MEMORY_ESTIMATED,
            "Current estimated charged memory per circuit breaker, in bytes",
            "By",
            estimatedSupplier
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CircuitBreakerMetrics that = (CircuitBreakerMetrics) o;
        return Objects.equals(tripCount, that.tripCount) && Objects.equals(memoryHeld, that.memoryHeld);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tripCount, memoryHeld);
    }

    @Override
    public String toString() {
        return "CircuitBreakerMetrics{tripCount=" + tripCount + ", memoryHeld=" + memoryHeld + '}';
    }

}
