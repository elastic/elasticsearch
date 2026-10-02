/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.breaker;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.breaker.CircuitBreakerMetrics;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING;
import static org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

public class PreallocatedCircuitBreakerServiceTests extends ESTestCase {
    public void testUseNotPreallocated() {
        HierarchyCircuitBreakerService real = real();
        try (PreallocatedCircuitBreakerService preallocated = preallocateRequest(real, 1024)) {
            CircuitBreaker b = preallocated.getBreaker(CircuitBreaker.REQUEST);
            b.addEstimateBytesAndMaybeBreak(100, "test");
            b.addWithoutBreaking(-100);
        }
        assertThat(real.getBreaker(CircuitBreaker.REQUEST).getUsed(), equalTo(0L));
    }

    public void testUseLessThanPreallocated() {
        HierarchyCircuitBreakerService real = real();
        try (PreallocatedCircuitBreakerService preallocated = preallocateRequest(real, 1024)) {
            CircuitBreaker b = preallocated.getBreaker(CircuitBreaker.REQUEST);
            b.addEstimateBytesAndMaybeBreak(100, "test");
            b.addWithoutBreaking(-100);
        }
        assertThat(real.getBreaker(CircuitBreaker.REQUEST).getUsed(), equalTo(0L));
    }

    public void testCloseIsIdempotent() {
        HierarchyCircuitBreakerService real = real();
        try (PreallocatedCircuitBreakerService preallocated = preallocateRequest(real, 1024)) {
            CircuitBreaker b = preallocated.getBreaker(CircuitBreaker.REQUEST);
            b.addEstimateBytesAndMaybeBreak(100, "test");
            b.addWithoutBreaking(-100);
            preallocated.close();
            assertThat(real.getBreaker(CircuitBreaker.REQUEST).getUsed(), equalTo(0L));
        } // Closes again which should do nothing
        assertThat(real.getBreaker(CircuitBreaker.REQUEST).getUsed(), equalTo(0L));
    }

    public void testUseMoreThanPreallocated() {
        HierarchyCircuitBreakerService real = real();
        try (PreallocatedCircuitBreakerService preallocated = preallocateRequest(real, 1024)) {
            CircuitBreaker b = preallocated.getBreaker(CircuitBreaker.REQUEST);
            b.addEstimateBytesAndMaybeBreak(2048, "test");
            b.addWithoutBreaking(-2048);
        }
        assertThat(real.getBreaker(CircuitBreaker.REQUEST).getUsed(), equalTo(0L));
    }

    public void testPreallocateMoreThanRemains() {
        HierarchyCircuitBreakerService real = real();
        long limit = real.getBreaker(CircuitBreaker.REQUEST).getLimit();
        Exception e = expectThrows(CircuitBreakingException.class, () -> preallocateRequest(real, limit + 1024));
        assertThat(e.getMessage(), startsWith("[request] Data too large, data for [preallocate[test]] would be ["));
    }

    public void testRandom() {
        HierarchyCircuitBreakerService real = real();
        CircuitBreaker realBreaker = real.getBreaker(CircuitBreaker.REQUEST);
        long preallocatedBytes = randomLongBetween(1, (long) (realBreaker.getLimit() * .8));
        try (PreallocatedCircuitBreakerService preallocated = preallocateRequest(real, preallocatedBytes)) {
            CircuitBreaker b = preallocated.getBreaker(CircuitBreaker.REQUEST);
            boolean usedPreallocated = false;
            long current = 0;
            for (int i = 0; i < 10000; i++) {
                if (current >= preallocatedBytes) {
                    usedPreallocated = true;
                }
                if (usedPreallocated) {
                    assertThat(realBreaker.getUsed(), equalTo(current));
                } else {
                    assertThat(realBreaker.getUsed(), equalTo(preallocatedBytes));
                }
                if (current > 0 && randomBoolean()) {
                    long delta = randomLongBetween(-Math.min(current, realBreaker.getLimit() / 100), 0);
                    b.addWithoutBreaking(delta);
                    current += delta;
                    continue;
                }
                long delta = randomLongBetween(0, realBreaker.getLimit() / 100);
                if (randomBoolean()) {
                    b.addWithoutBreaking(delta);
                    current += delta;
                    continue;
                }
                if (current + delta < realBreaker.getLimit()) {
                    b.addEstimateBytesAndMaybeBreak(delta, "test");
                    current += delta;
                    continue;
                }
                Exception e = expectThrows(CircuitBreakingException.class, () -> b.addEstimateBytesAndMaybeBreak(delta, "test"));
                assertThat(e.getMessage(), startsWith("[request] Data too large, data for [test] would be ["));
            }
            b.addWithoutBreaking(-current);
        }
        assertThat(real.getBreaker(CircuitBreaker.REQUEST).getUsed(), equalTo(0L));
    }

    public void testCloseBalancesMemoryHeldUnderPreallocateLabel() {
        final RecordingMeterRegistry meter = new RecordingMeterRegistry();
        final HierarchyCircuitBreakerService real = realWithMeter(meter);

        final long preallocate = 4096L;
        try (
            PreallocatedCircuitBreakerService preallocated = new PreallocatedCircuitBreakerService(
                real,
                CircuitBreaker.REQUEST,
                preallocate,
                "test"
            )
        ) {
            assertThat(preallocated.getBreaker(CircuitBreaker.REQUEST).getName(), equalTo(CircuitBreaker.REQUEST));
        }

        final Map<String, Object> preallocateAttrs = Map.of(
            ChildMemoryCircuitBreaker.BREAKER_METRIC_TYPE_ATTRIBUTE,
            CircuitBreaker.REQUEST,
            ChildMemoryCircuitBreaker.CIRCUIT_BREAKER_CATEGORY_ATTRIBUTE,
            ChildMemoryCircuitBreaker.CATEGORY_PREALLOCATE
        );
        final Map<String, Object> uncategorizedAttrs = Map.of(
            ChildMemoryCircuitBreaker.BREAKER_METRIC_TYPE_ATTRIBUTE,
            CircuitBreaker.REQUEST,
            ChildMemoryCircuitBreaker.CIRCUIT_BREAKER_CATEGORY_ATTRIBUTE,
            ChildMemoryCircuitBreaker.CATEGORY_UNCATEGORIZED
        );

        final Map<Map<String, Object>, Long> heldByAttrs = meter.getRecorder()
            .getMeasurements(InstrumentType.LONG_UP_DOWN_COUNTER, CircuitBreakerMetrics.ES_BREAKER_MEMORY_HELD)
            .stream()
            .collect(Collectors.groupingBy(Measurement::attributes, Collectors.summingLong(Measurement::getLong)));

        assertTrue("expected preallocate admission to be recorded", heldByAttrs.containsKey(preallocateAttrs));
        assertEquals(Long.valueOf(0L), heldByAttrs.get(preallocateAttrs));
        assertNull("preallocate close() must not bucket releases under \"uncategorized\"", heldByAttrs.get(uncategorizedAttrs));
        assertThat(real.getBreaker(CircuitBreaker.REQUEST).getUsed(), equalTo(0L));
    }

    public void testCloseBalancesMemoryHeldUnderPreallocateLabelWhenFullyConsumed() {
        final RecordingMeterRegistry meter = new RecordingMeterRegistry();
        final HierarchyCircuitBreakerService real = realWithMeter(meter);

        final long preallocate = 4096L;
        try (
            PreallocatedCircuitBreakerService preallocated = new PreallocatedCircuitBreakerService(
                real,
                CircuitBreaker.REQUEST,
                preallocate,
                "test"
            )
        ) {
            final CircuitBreaker b = preallocated.getBreaker(CircuitBreaker.REQUEST);
            // Allocate more than was preallocated to drive the breaker into the "used all" state, then release it.
            b.addEstimateBytesAndMaybeBreak(preallocate * 2, "test");
            b.addWithoutBreaking(-(preallocate * 2), "test");
        }

        final Map<String, Object> preallocateAttrs = Map.of(
            ChildMemoryCircuitBreaker.BREAKER_METRIC_TYPE_ATTRIBUTE,
            CircuitBreaker.REQUEST,
            ChildMemoryCircuitBreaker.CIRCUIT_BREAKER_CATEGORY_ATTRIBUTE,
            ChildMemoryCircuitBreaker.CATEGORY_PREALLOCATE
        );

        final Map<Map<String, Object>, Long> heldByAttrs = meter.getRecorder()
            .getMeasurements(InstrumentType.LONG_UP_DOWN_COUNTER, CircuitBreakerMetrics.ES_BREAKER_MEMORY_HELD)
            .stream()
            .collect(Collectors.groupingBy(Measurement::attributes, Collectors.summingLong(Measurement::getLong)));

        assertTrue("expected preallocate admission to be recorded", heldByAttrs.containsKey(preallocateAttrs));
        assertEquals("preallocate label must return to zero even when fully consumed", Long.valueOf(0L), heldByAttrs.get(preallocateAttrs));
        assertThat(real.getBreaker(CircuitBreaker.REQUEST).getUsed(), equalTo(0L));
    }

    private HierarchyCircuitBreakerService real() {
        return realWithMetrics(CircuitBreakerMetrics.NOOP);
    }

    private HierarchyCircuitBreakerService realWithMeter(RecordingMeterRegistry meter) {
        return realWithMetrics(new CircuitBreakerMetrics(new TelemetryProvider.NoopTelemetryProvider() {
            @Override
            public MeterRegistry getMeterRegistry() {
                return meter;
            }
        }));
    }

    private HierarchyCircuitBreakerService realWithMetrics(CircuitBreakerMetrics metrics) {
        return new HierarchyCircuitBreakerService(
            metrics,
            Settings.builder()
                // Pin the limit to something that'll totally fit in the heap we use for the tests
                .put(REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "100mb")
                // Disable the real memory checking because it causes other tests to interfere with this one.
                .put(USE_REAL_MEMORY_USAGE_SETTING.getKey(), false)
                .build(),
            List.of(),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
    }

    private PreallocatedCircuitBreakerService preallocateRequest(CircuitBreakerService real, long bytes) {
        return new PreallocatedCircuitBreakerService(real, CircuitBreaker.REQUEST, bytes, "test");
    }
}
