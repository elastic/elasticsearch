/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.breaker;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * A class collecting trip counters for circuit breakers (parent, field data, request, in flight requests and custom child circuit
 * breakers).
 *
 * The circuit breaker name is part of the (long) counter metric name instead of being an attribute because aggregating distinct circuit
 * breakers trip counter values does not make sense, as for instance, summing es.breaker.field_data.trip.total and
 * es.breaker.in_flight_requests.trip.total.
 * Those counters trip for different reasons even if the underlying reason is "too much memory usage". Aggregating them together results in
 * losing the ability to understand where the underlying issue is (too much field data, too many concurrent requests, too large concurrent
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
    public static final CircuitBreakerMetrics NOOP = new CircuitBreakerMetrics(TelemetryProvider.NOOP, Collections.emptyMap());
    public static final String ES_BREAKER_PARENT_TRIP_COUNT_TOTAL = "es.breaker.parent.trip.total";
    public static final String ES_BREAKER_FIELD_DATA_TRIP_COUNT_TOTAL = "es.breaker.field_data.trip.total";
    public static final String ES_BREAKER_REQUEST_TRIP_COUNT_TOTAL = "es.breaker.request.trip.total";
    public static final String ES_BREAKER_IN_FLIGHT_REQUESTS_TRIP_COUNT_TOTAL = "es.breaker.in_flight_requests.trip.total";

    private static final String ES_BREAKER_CUSTOM_TRIP_COUNT_TOTAL_TEMPLATE = "es.breaker.%s.trip.total";
    private final MeterRegistry registry;
    private final LongCounter parentTripCountTotal;
    private final LongCounter fielddataTripCountTotal;
    private final LongCounter requestTripCountTotal;
    private final LongCounter inFlightRequestsCountTotal;
    private final Map<String, LongCounter> customTripCountsTotal;

    private CircuitBreakerMetrics(
        final MeterRegistry registry,
        final LongCounter parentTripCountTotal,
        final LongCounter fielddataTripCountTotal,
        final LongCounter requestTripCountTotal,
        final LongCounter inFlightRequestsCountTotal,
        final Map<String, LongCounter> customTripCountsTotal
    ) {
        this.registry = registry;
        this.parentTripCountTotal = parentTripCountTotal;
        this.fielddataTripCountTotal = fielddataTripCountTotal;
        this.requestTripCountTotal = requestTripCountTotal;
        this.inFlightRequestsCountTotal = inFlightRequestsCountTotal;
        this.customTripCountsTotal = customTripCountsTotal;
    }

    public CircuitBreakerMetrics(final TelemetryProvider telemetryProvider, final Map<String, LongCounter> customTripCounters) {
        this(
            telemetryProvider.getMeterRegistry(),
            telemetryProvider.getMeterRegistry()
                .registerLongCounter(ES_BREAKER_PARENT_TRIP_COUNT_TOTAL, "Parent circuit breaker trip count", "count"),
            telemetryProvider.getMeterRegistry()
                .registerLongCounter(ES_BREAKER_FIELD_DATA_TRIP_COUNT_TOTAL, "Field data circuit breaker trip count", "count"),
            telemetryProvider.getMeterRegistry()
                .registerLongCounter(ES_BREAKER_REQUEST_TRIP_COUNT_TOTAL, "Request circuit breaker trip count", "count"),
            telemetryProvider.getMeterRegistry()
                .registerLongCounter(
                    ES_BREAKER_IN_FLIGHT_REQUESTS_TRIP_COUNT_TOTAL,
                    "In-flight requests circuit breaker trip count",
                    "count"
                ),
            customTripCounters
        );
    }

    public LongCounter getParentTripCountTotal() {
        return parentTripCountTotal;
    }

    public LongCounter getFielddataTripCountTotal() {
        return fielddataTripCountTotal;
    }

    public LongCounter getRequestTripCountTotal() {
        return requestTripCountTotal;
    }

    public LongCounter getInFlightRequestsCountTotal() {
        return inFlightRequestsCountTotal;
    }

    public Map<String, LongCounter> getCustomTripCountsTotal() {
        return customTripCountsTotal;
    }

    public LongCounter getCustomTripCount(final String name, final LongCounter theDefault) {
        return this.customTripCountsTotal.getOrDefault(name, theDefault);
    }

    public LongCounter getCustomTripCount(final String name) {
        return this.customTripCountsTotal.getOrDefault(name, LongCounter.NOOP);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CircuitBreakerMetrics that = (CircuitBreakerMetrics) o;
        return Objects.equals(registry, that.registry)
            && Objects.equals(parentTripCountTotal, that.parentTripCountTotal)
            && Objects.equals(fielddataTripCountTotal, that.fielddataTripCountTotal)
            && Objects.equals(requestTripCountTotal, that.requestTripCountTotal)
            && Objects.equals(inFlightRequestsCountTotal, that.inFlightRequestsCountTotal)
            && Objects.equals(customTripCountsTotal, that.customTripCountsTotal);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            registry,
            parentTripCountTotal,
            fielddataTripCountTotal,
            requestTripCountTotal,
            inFlightRequestsCountTotal,
            customTripCountsTotal
        );
    }

    @Override
    public String toString() {
        return "CircuitBreakerMetrics{"
            + "registry="
            + registry
            + ", parentTripCountTotal="
            + parentTripCountTotal
            + ", fielddataTripCountTotal="
            + fielddataTripCountTotal
            + ", requestTripCountTotal="
            + requestTripCountTotal
            + ", inFlightRequestsCountTotal="
            + inFlightRequestsCountTotal
            + ", customTripCountsTotal="
            + customTripCountsTotal
            + '}';
    }

    public void addCustomCircuitBreaker(final CircuitBreaker circuitBreaker) {
        if (this.customTripCountsTotal.containsKey(circuitBreaker.getName())) {
            throw new IllegalArgumentException("A circuit circuitBreaker named [" + circuitBreaker.getName() + " already exists");
        }
        final String canonicalName = Strings.format(ES_BREAKER_CUSTOM_TRIP_COUNT_TOTAL_TEMPLATE, circuitBreaker.getName());
        this.customTripCountsTotal.put(
            canonicalName,
            registry.registerLongCounter(canonicalName, "A custom circuit circuitBreaker [" + circuitBreaker.getName() + "]", "count")
        );
    }

}
