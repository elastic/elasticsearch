/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.breaker;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * A class collecting trip counters for circuit breakers (parent, field data, request, in flight requests and custom child circuit breakers).
 *
 * We decided to have the name as part of the (long) counter metric name instead of being an attribute because aggregating distinct circuit
 * breakers trip counter values does not make sense, as for instance, summing field_data.trip_count and in_flight_requests.trip_count.
 * Those counters trip for different reasons even if the underlying reason is "too much memory usage". Aggregating them together results in
 * losing the ability to understand where the underlying issue is (too much field data?, too many concurrent requests, too large concurrent
 * requests?). Aggregating each one of them separately to get, for instance, cluster level or cloud region level statistics is perfectly
 * fine, instead.
 *
 * NOTE: here we have the ability to register custom trip counters too. This ability is something a few plugins take advantage of nowadays.
 * At the time of writing this class it is just "Eql" and "MachineLearning" which track memory used to store "things" that are
 * application/plugin specific such as eql sequence query objects and inference model objects. As a result, we just have a couple of this
 * custom counters. This means we have 6 circuit breaker counter metrics per node (parent, field_data, request, in_flight_requests,
 * eql_sequence and model_inference). We register them a bit differently to keep the ability for plugins to define their own circuit breaker
 * trip counters.
 */
public class CircuitBreakerMetrics {
    public static final String CIRCUIT_BREAKER_PARENT_TRIP_COUNT = "circuit_breaker.parent.trip_count";
    public static final String CIRCUIT_BREAKER_FIELD_DATA_TRIP_COUNT = "circuit_breaker.field_data.trip_count";
    public static final String CIRCUIT_BREAKER_REQUEST_TRIP_COUNT = "circuit_breaker.request.trip_count";
    public static final String CIRCUIT_BREAKER_IN_FLIGHT_REQUESTS_TRIP_COUNT = "circuit_breaker.in_flight_requests.trip_count";

    private static final String CIRCUIT_BREAKER_CUSTOM_TRIP_COUNT_TEMPLATE = "circuit_breaker.%s.trip_count";
    private final MeterRegistry registry;
    private final LongCounter parentTripCount;
    private final LongCounter fielddataTripCount;
    private final LongCounter requestTripCount;
    private final LongCounter inFlightRequestsCount;
    private final Map<String, LongCounter> customTripCounts;

    private CircuitBreakerMetrics(
        final MeterRegistry registry,
        final LongCounter parentTripCount,
        final LongCounter fielddataTripCount,
        final LongCounter requestTripCount,
        final LongCounter inFlightRequestsCount,
        final Map<String, LongCounter> customTripCounts
    ) {
        this.registry = registry;
        this.parentTripCount = parentTripCount;
        this.fielddataTripCount = fielddataTripCount;
        this.requestTripCount = requestTripCount;
        this.inFlightRequestsCount = inFlightRequestsCount;
        this.customTripCounts = customTripCounts;
    }

    public CircuitBreakerMetrics(final TelemetryProvider telemetryProvider, final Map<String, LongCounter> customTripCounters) {
        this(
            telemetryProvider.getMeterRegistry(),
            telemetryProvider.getMeterRegistry()
                .registerLongCounter(CIRCUIT_BREAKER_PARENT_TRIP_COUNT, "Parent circuit breaker trip count", "count"),
            telemetryProvider.getMeterRegistry()
                .registerLongCounter(CIRCUIT_BREAKER_FIELD_DATA_TRIP_COUNT, "Field data circuit breaker trip count", "count"),
            telemetryProvider.getMeterRegistry()
                .registerLongCounter(CIRCUIT_BREAKER_REQUEST_TRIP_COUNT, "Request circuit breaker trip count", "count"),
            telemetryProvider.getMeterRegistry()
                .registerLongCounter(
                    CIRCUIT_BREAKER_IN_FLIGHT_REQUESTS_TRIP_COUNT,
                    "In-flight requests circuit breaker trip count",
                    "count"
                ),
            customTripCounters
        );
    }

    public static CircuitBreakerMetrics NOOP = new CircuitBreakerMetrics(TelemetryProvider.NOOP, Collections.emptyMap());

    public LongCounter getParentTripCount() {
        return parentTripCount;
    }

    public LongCounter getFielddataTripCount() {
        return fielddataTripCount;
    }

    public LongCounter getRequestTripCount() {
        return requestTripCount;
    }

    public LongCounter getInFlightRequestsCount() {
        return inFlightRequestsCount;
    }

    public Map<String, LongCounter> getCustomTripCounts() {
        return customTripCounts;
    }

    public LongCounter getCustomTripCount(final String name, final LongCounter theDefault) {
        return this.customTripCounts.getOrDefault(name, theDefault);
    }

    public LongCounter getCustomTripCount(final String name) {
        return this.customTripCounts.getOrDefault(name, LongCounter.NOOP);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CircuitBreakerMetrics that = (CircuitBreakerMetrics) o;
        return Objects.equals(registry, that.registry)
            && Objects.equals(parentTripCount, that.parentTripCount)
            && Objects.equals(fielddataTripCount, that.fielddataTripCount)
            && Objects.equals(requestTripCount, that.requestTripCount)
            && Objects.equals(inFlightRequestsCount, that.inFlightRequestsCount)
            && Objects.equals(customTripCounts, that.customTripCounts);
    }

    @Override
    public int hashCode() {
        return Objects.hash(registry, parentTripCount, fielddataTripCount, requestTripCount, inFlightRequestsCount, customTripCounts);
    }

    @Override
    public String toString() {
        return "CircuitBreakerMetrics{registry=%s, parentTripCount=%s, fielddataTripCount=%s, requestTripCount=%s, inFlightRequestsCount=%s, customTripCounts=%s}"
            .formatted(registry, parentTripCount, fielddataTripCount, requestTripCount, inFlightRequestsCount, customTripCounts);
    }

    public void addCustomCircuitBreaker(final CircuitBreaker circuitBreaker) {
        if (this.customTripCounts.containsKey(circuitBreaker.getName())) {
            throw new IllegalArgumentException("A circuit circuitBreaker named [" + circuitBreaker.getName() + " already exists");
        }
        final String canonicalName = CIRCUIT_BREAKER_CUSTOM_TRIP_COUNT_TEMPLATE.formatted(circuitBreaker.getName());
        this.customTripCounts.put(
            canonicalName,
            registry.registerLongCounter(canonicalName, "A custom circuit circuitBreaker [" + circuitBreaker.getName() + "]", "count")
        );
    }

}
