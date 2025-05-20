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

import java.util.Objects;

/**
 * A class collecting trip count metrics for circuit breakers (parent, field data, request, in flight requests and custom child circuit
 * breakers).
 *
 * The circuit breaker name is used as an attribute so that we define a single counter metric where the name is mapped to a 'type'
 * attribute. The counter trips for different reasons even if the underlying reason is "too much memory usage". Aggregating them together
 * results in losing the ability to understand where the underlying issue is (too much field data, too many concurrent requests, too large
 * concurrent requests?). As a result weadvise in aggregations queries not to "aggregate away" the type attribute so that you treat each
 * circuit breaker as a separate counter.
 */
public class CircuitBreakerMetrics {
    public static final CircuitBreakerMetrics NOOP = new CircuitBreakerMetrics(TelemetryProvider.NOOP);
    public static final String ES_BREAKER_TRIP_COUNT_TOTAL = "es.breaker.trip.total";
    private final LongCounter tripCount;

    private CircuitBreakerMetrics(final LongCounter tripCount) {
        this.tripCount = tripCount;
    }

    public CircuitBreakerMetrics(final TelemetryProvider telemetryProvider) {
        this(telemetryProvider.getMeterRegistry().registerLongCounter(ES_BREAKER_TRIP_COUNT_TOTAL, "Circuit breaker trip count", "count"));
    }

    public LongCounter getTripCount() {
        return tripCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CircuitBreakerMetrics that = (CircuitBreakerMetrics) o;
        return Objects.equals(tripCount, that.tripCount);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tripCount);
    }

    @Override
    public String toString() {
        return "CircuitBreakerMetrics{" + ", tripCount=" + tripCount + '}';
    }

}
