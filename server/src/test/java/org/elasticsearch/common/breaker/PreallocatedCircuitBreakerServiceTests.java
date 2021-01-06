/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.breaker;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assume.assumeThat;

public class PreallocatedCircuitBreakerServiceTests extends ESTestCase {
    public void testUseNotPreallocated() {
        try (HierarchyCircuitBreakerService real = real()) {
            try (PreallocatedCircuitBreakerService preallocated = preallocateRequest(real, 1024)) {
                CircuitBreaker b = preallocated.getBreaker(CircuitBreaker.ACCOUNTING);
                b.addEstimateBytesAndMaybeBreak(100, "test");
                b.addWithoutBreaking(-100);
            }
            assertThat(real.getBreaker(CircuitBreaker.REQUEST).getUsed(), equalTo(0L));
            assertThat(real.getBreaker(CircuitBreaker.ACCOUNTING).getUsed(), equalTo(0L));
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
            long limit = ByteSizeValue.ofMb(100).getBytes();
            assumeThat(limit, lessThanOrEqualTo(realBreaker.getLimit()));
            long preallocatedBytes = randomLongBetween(1, limit);
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
                        long delta = randomLongBetween(-Math.min(current, limit / 100), 0);
                        b.addWithoutBreaking(delta);
                        current += delta;
                        continue;
                    }
                    long delta = randomLongBetween(0, limit / 100);
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
            Settings.EMPTY,
            List.of(),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
    }

    private PreallocatedCircuitBreakerService preallocateRequest(CircuitBreakerService real, long bytes) {
        return new PreallocatedCircuitBreakerService(real, CircuitBreaker.REQUEST, bytes, "test");
    }
}
