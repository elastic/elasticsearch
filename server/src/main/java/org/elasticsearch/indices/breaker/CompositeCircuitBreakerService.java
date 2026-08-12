/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.breaker;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.telemetry.metric.LongWithAttributes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * A {@link CircuitBreakerService} that delegates to several underlying services and merges their results.
 *
 * <p>This is used to present a unified view of the heap-denominated breakers managed by
 * {@link HierarchyCircuitBreakerService} and the native-memory breakers managed by
 * {@link NativeMemoryCircuitBreakerService}: {@link #getBreaker(String)} and {@link #stats()} check
 * both, while the two services remain structurally independent — native bytes are never seen by the
 * heap parent, and vice versa.
 *
 * <p>Gauge registration for {@code es.breaker.memory.limit.size} and
 * {@code es.breaker.memory.estimated.usage} is performed here, once, with a combined supplier that
 * covers all underlying services. Registering the same gauge name from multiple services on the same
 * {@link CircuitBreakerMetrics} instance would cause a duplicate-registration error in the APM
 * registry.
 */
public class CompositeCircuitBreakerService extends CircuitBreakerService {

    private final List<CircuitBreakerService> services;

    public CompositeCircuitBreakerService(CircuitBreakerMetrics metrics, CircuitBreakerService... services) {
        this.services = List.of(services);
        metrics.registerMemoryGauges(this::collectMemoryLimits, this::collectMemoryEstimates);
    }

    private Collection<LongWithAttributes> collectMemoryLimits() {
        List<LongWithAttributes> out = new ArrayList<>();
        for (CircuitBreakerService service : services) {
            if (service instanceof HierarchyCircuitBreakerService hcbs) {
                out.addAll(hcbs.collectMemoryLimits());
            } else if (service instanceof NativeMemoryCircuitBreakerService nmcbs) {
                out.addAll(nmcbs.collectMemoryLimits());
            }
        }
        return out;
    }

    private Collection<LongWithAttributes> collectMemoryEstimates() {
        List<LongWithAttributes> out = new ArrayList<>();
        for (CircuitBreakerService service : services) {
            if (service instanceof HierarchyCircuitBreakerService hcbs) {
                out.addAll(hcbs.collectMemoryEstimates());
            } else if (service instanceof NativeMemoryCircuitBreakerService nmcbs) {
                out.addAll(nmcbs.collectMemoryEstimates());
            }
        }
        return out;
    }

    @Override
    public CircuitBreaker getBreaker(String name) {
        for (CircuitBreakerService service : services) {
            CircuitBreaker breaker = service.getBreaker(name);
            if (breaker != null) {
                return breaker;
            }
        }
        return null;
    }

    @Override
    public AllCircuitBreakerStats stats() {
        CircuitBreakerStats[] merged = services.stream()
            .flatMap(s -> Arrays.stream(s.stats().getAllStats()))
            .toArray(CircuitBreakerStats[]::new);
        return new AllCircuitBreakerStats(merged);
    }

    @Override
    public CircuitBreakerStats stats(String name) {
        for (CircuitBreakerService service : services) {
            CircuitBreakerStats stats = service.stats(name);
            if (stats != null) {
                return stats;
            }
        }
        return null;
    }

    /** Returns the underlying services, in order, for testing. */
    List<CircuitBreakerService> services() {
        return services;
    }
}
