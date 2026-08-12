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
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class NativeMemoryCircuitBreakerServiceTests extends ESTestCase {

    /**
     * Canonical test: charging the native_memory breaker must never consult the heap parent.
     * The native service has no parent — there is no heap parent to consult.
     */
    public void testNativeMemoryBreakerDoesNotConsultHeapParent() {
        final Settings settings = Settings.builder()
            .put(NativeMemoryCircuitBreakerService.NATIVE_MEMORY_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "100mb")
            .build();
        final NativeMemoryCircuitBreakerService service = new NativeMemoryCircuitBreakerService(
            CircuitBreakerMetrics.NOOP,
            settings,
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        final CircuitBreaker nativeBreaker = service.getBreaker(CircuitBreaker.NATIVE_MEMORY);
        assertNotNull(nativeBreaker);

        // This must not throw [parent] Data too large.
        nativeBreaker.addEstimateBytesAndMaybeBreak(ByteSizeValue.of(10, ByteSizeUnit.MB).getBytes(), "native");
    }

    /**
     * The native_memory breaker must trip on its own limit with TRANSIENT durability.
     */
    public void testNativeMemoryBreakerTripsOnItsOwnLimit() {
        final Settings settings = Settings.builder()
            .put(NativeMemoryCircuitBreakerService.NATIVE_MEMORY_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "10mb")
            .build();
        final NativeMemoryCircuitBreakerService service = new NativeMemoryCircuitBreakerService(
            CircuitBreakerMetrics.NOOP,
            settings,
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        final CircuitBreaker nativeBreaker = service.getBreaker(CircuitBreaker.NATIVE_MEMORY);

        final CircuitBreakingException exception = expectThrows(
            CircuitBreakingException.class,
            () -> nativeBreaker.addEstimateBytesAndMaybeBreak(ByteSizeValue.of(20, ByteSizeUnit.MB).getBytes(), "native-label")
        );

        assertThat(exception.getMessage(), containsString("[native_memory] Data too large"));
        assertThat(exception.getDurability(), equalTo(CircuitBreaker.Durability.TRANSIENT));
        assertEquals(1L, nativeBreaker.getTrippedCount());
    }

    /**
     * The native_memory limit and overhead must be dynamically updatable via ClusterSettings.
     */
    public void testNativeMemoryLimitAndOverheadAreDynamicallyUpdatable() {
        final Settings settings = Settings.builder()
            .put(NativeMemoryCircuitBreakerService.NATIVE_MEMORY_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "10mb")
            .build();
        final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final NativeMemoryCircuitBreakerService service = new NativeMemoryCircuitBreakerService(
            CircuitBreakerMetrics.NOOP,
            settings,
            clusterSettings
        );
        final CircuitBreaker nativeBreaker = service.getBreaker(CircuitBreaker.NATIVE_MEMORY);

        assertEquals(ByteSizeValue.of(10, ByteSizeUnit.MB).getBytes(), nativeBreaker.getLimit());

        clusterSettings.applySettings(
            Settings.builder()
                .put(NativeMemoryCircuitBreakerService.NATIVE_MEMORY_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "50mb")
                .put(NativeMemoryCircuitBreakerService.NATIVE_MEMORY_CIRCUIT_BREAKER_OVERHEAD_SETTING.getKey(), 2.0)
                .build()
        );

        assertEquals(ByteSizeValue.of(50, ByteSizeUnit.MB).getBytes(), nativeBreaker.getLimit());
        assertEquals(2.0, nativeBreaker.getOverhead(), 0.0);
    }

    /**
     * The native_memory breaker must appear in stats() and getBreaker().
     */
    public void testNativeMemoryBreakerAppearsInStats() {
        final NativeMemoryCircuitBreakerService service = new NativeMemoryCircuitBreakerService(
            CircuitBreakerMetrics.NOOP,
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );

        assertNotNull(service.getBreaker(CircuitBreaker.NATIVE_MEMORY));
        assertNotNull(service.stats().getStats(CircuitBreaker.NATIVE_MEMORY));
        assertEquals(CircuitBreaker.NATIVE_MEMORY, service.stats().getStats(CircuitBreaker.NATIVE_MEMORY).getName());
    }

    /**
     * When the native_memory type is set to "noop", the breaker must be a NoopCircuitBreaker
     * and never trip.
     */
    public void testNativeMemoryTypeNoop() {
        final Settings settings = Settings.builder()
            .put(NativeMemoryCircuitBreakerService.NATIVE_MEMORY_CIRCUIT_BREAKER_TYPE_SETTING.getKey(), "noop")
            .build();
        final NativeMemoryCircuitBreakerService service = new NativeMemoryCircuitBreakerService(
            CircuitBreakerMetrics.NOOP,
            settings,
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );

        assertThat(service.getBreaker(CircuitBreaker.NATIVE_MEMORY), instanceOf(NoopCircuitBreaker.class));
        // must not throw regardless of size
        service.getBreaker(CircuitBreaker.NATIVE_MEMORY).addEstimateBytesAndMaybeBreak(Long.MAX_VALUE / 2, "native");
    }

    /**
     * getBreaker for an unknown name returns null; stats(name) for an unknown name returns null.
     */
    public void testUnknownBreakerReturnsNull() {
        final NativeMemoryCircuitBreakerService service = new NativeMemoryCircuitBreakerService(
            CircuitBreakerMetrics.NOOP,
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        assertNull(service.getBreaker(CircuitBreaker.REQUEST));
        assertNull(service.stats(CircuitBreaker.REQUEST));
    }
}
