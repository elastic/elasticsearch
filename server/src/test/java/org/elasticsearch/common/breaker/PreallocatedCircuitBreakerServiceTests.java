/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.breaker;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING;
import static org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

public class PreallocatedCircuitBreakerServiceTests extends ESTestCase {
    public void testUseNotPreallocated() {
        try (HierarchyCircuitBreakerService real = real()) {
            try (PreallocatedCircuitBreakerService preallocated = preallocateRequest(real, 1024)) {
                CircuitBreaker b = preallocated.getBreaker(CircuitBreaker.REQUEST);
                b.addEstimateBytesAndMaybeBreak(100, "test");
                b.addWithoutBreaking(-100);
            }
            assertThat(real.getBreaker(CircuitBreaker.REQUEST).getUsed(), equalTo(0L));
        }
    }

    public void testUseLessThanPreallocated() {
        try (HierarchyCircuitBreakerService real = real()) {
            try (PreallocatedCircuitBreakerService preallocated = preallocateRequest(real, 1024)) {
                CircuitBreaker b = preallocated.getBreaker(CircuitBreaker.REQUEST);
                b.addEstimateBytesAndMaybeBreak(100, "test");
                b.addWithoutBreaking(-100);
            }
            assertThat(real.getBreaker(CircuitBreaker.REQUEST).getUsed(), equalTo(0L));
        }
    }

    public void testCloseIsIdempotent() {
        try (HierarchyCircuitBreakerService real = real()) {
            try (PreallocatedCircuitBreakerService preallocated = preallocateRequest(real, 1024)) {
                CircuitBreaker b = preallocated.getBreaker(CircuitBreaker.REQUEST);
                b.addEstimateBytesAndMaybeBreak(100, "test");
                b.addWithoutBreaking(-100);
                preallocated.close();
                assertThat(real.getBreaker(CircuitBreaker.REQUEST).getUsed(), equalTo(0L));
            } // Closes again which should do nothing
            assertThat(real.getBreaker(CircuitBreaker.REQUEST).getUsed(), equalTo(0L));
        }
    }

    public void testUseMoreThanPreallocated() {
        try (HierarchyCircuitBreakerService real = real()) {
            try (PreallocatedCircuitBreakerService preallocated = preallocateRequest(real, 1024)) {
                CircuitBreaker b = preallocated.getBreaker(CircuitBreaker.REQUEST);
                b.addEstimateBytesAndMaybeBreak(2048, "test");
                b.addWithoutBreaking(-2048);
            }
            assertThat(real.getBreaker(CircuitBreaker.REQUEST).getUsed(), equalTo(0L));
        }
    }

    public void testPreallocateMoreThanRemains() {
        try (HierarchyCircuitBreakerService real = real()) {
            long limit = real.getBreaker(CircuitBreaker.REQUEST).getLimit();
            Exception e = expectThrows(CircuitBreakingException.class, () -> preallocateRequest(real, limit + 1024));
            assertThat(e.getMessage(), startsWith("[request] Data too large, data for [preallocate[test]] would be ["));
        }
    }

    public void testRandom() {
        try (HierarchyCircuitBreakerService real = real()) {
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
    }

    private HierarchyCircuitBreakerService real() {
        return new HierarchyCircuitBreakerService(
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
