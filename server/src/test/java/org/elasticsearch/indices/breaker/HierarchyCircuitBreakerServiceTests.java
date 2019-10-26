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

package org.elasticsearch.indices.breaker;


import org.elasticsearch.common.breaker.ChildMemoryCircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class HierarchyCircuitBreakerServiceTests extends ESTestCase {
    public void testThreadedUpdatesToChildBreaker() throws Exception {
        final int NUM_THREADS = scaledRandomIntBetween(3, 15);
        final int BYTES_PER_THREAD = scaledRandomIntBetween(500, 4500);
        final Thread[] threads = new Thread[NUM_THREADS];
        final AtomicBoolean tripped = new AtomicBoolean(false);
        final AtomicReference<Throwable> lastException = new AtomicReference<>(null);

        final AtomicReference<ChildMemoryCircuitBreaker> breakerRef = new AtomicReference<>(null);
        final CircuitBreakerService service = new HierarchyCircuitBreakerService(Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)) {

            @Override
            public CircuitBreaker getBreaker(String name) {
                return breakerRef.get();
            }

            @Override
            public void checkParentLimit(long newBytesReserved, String label) throws CircuitBreakingException {
                // never trip
            }
        };
        final BreakerSettings settings = new BreakerSettings(CircuitBreaker.REQUEST, (BYTES_PER_THREAD * NUM_THREADS) - 1, 1.0);
        final ChildMemoryCircuitBreaker breaker = new ChildMemoryCircuitBreaker(settings, logger,
            (HierarchyCircuitBreakerService)service, CircuitBreaker.REQUEST);
        breakerRef.set(breaker);

        for (int i = 0; i < NUM_THREADS; i++) {
            threads[i] = new Thread(() -> {
                for (int j = 0; j < BYTES_PER_THREAD; j++) {
                    try {
                        breaker.addEstimateBytesAndMaybeBreak(1L, "test");
                    } catch (CircuitBreakingException e) {
                        if (tripped.get()) {
                            assertThat("tripped too many times", true, equalTo(false));
                        } else {
                            assertThat(tripped.compareAndSet(false, true), equalTo(true));
                        }
                    } catch (Exception e) {
                        lastException.set(e);
                    }
                }
            });

            threads[i].start();
        }

        for (Thread t : threads) {
            t.join();
        }

        assertThat("no other exceptions were thrown", lastException.get(), equalTo(null));
        assertThat("breaker was tripped", tripped.get(), equalTo(true));
        assertThat("breaker was tripped at least once", breaker.getTrippedCount(), greaterThanOrEqualTo(1L));
    }

    public void testThreadedUpdatesToChildBreakerWithParentLimit() throws Exception {
        final int NUM_THREADS = scaledRandomIntBetween(3, 15);
        final int BYTES_PER_THREAD = scaledRandomIntBetween(500, 4500);
        final int parentLimit = (BYTES_PER_THREAD * NUM_THREADS) - 2;
        final int childLimit = parentLimit + 10;
        final Thread[] threads = new Thread[NUM_THREADS];
        final AtomicInteger tripped = new AtomicInteger(0);
        final AtomicReference<Throwable> lastException = new AtomicReference<>(null);

        final AtomicInteger parentTripped = new AtomicInteger(0);
        final AtomicReference<ChildMemoryCircuitBreaker> breakerRef = new AtomicReference<>(null);
        final CircuitBreakerService service = new HierarchyCircuitBreakerService(Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)) {

            @Override
            public CircuitBreaker getBreaker(String name) {
                return breakerRef.get();
            }

            @Override
            public void checkParentLimit(long newBytesReserved, String label) throws CircuitBreakingException {
                // Parent will trip right before regular breaker would trip
                long requestBreakerUsed = getBreaker(CircuitBreaker.REQUEST).getUsed();
                if (requestBreakerUsed > parentLimit) {
                    parentTripped.incrementAndGet();
                    logger.info("--> parent tripped");
                    throw new CircuitBreakingException("parent tripped", requestBreakerUsed + newBytesReserved,
                        parentLimit, CircuitBreaker.Durability.PERMANENT);
                }
            }
        };
        final BreakerSettings settings = new BreakerSettings(CircuitBreaker.REQUEST, childLimit, 1.0);
        final ChildMemoryCircuitBreaker breaker = new ChildMemoryCircuitBreaker(settings, logger,
            (HierarchyCircuitBreakerService)service, CircuitBreaker.REQUEST);
        breakerRef.set(breaker);

        for (int i = 0; i < NUM_THREADS; i++) {
            threads[i] = new Thread(() -> {
                for (int j = 0; j < BYTES_PER_THREAD; j++) {
                    try {
                        breaker.addEstimateBytesAndMaybeBreak(1L, "test");
                    } catch (CircuitBreakingException e) {
                        tripped.incrementAndGet();
                    } catch (Exception e) {
                        lastException.set(e);
                    }
                }
            });
        }

        logger.info("--> NUM_THREADS: [{}], BYTES_PER_THREAD: [{}], TOTAL_BYTES: [{}], PARENT_LIMIT: [{}], CHILD_LIMIT: [{}]",
            NUM_THREADS, BYTES_PER_THREAD, (BYTES_PER_THREAD * NUM_THREADS), parentLimit, childLimit);

        logger.info("--> starting threads...");
        for (Thread t : threads) {
            t.start();
        }

        for (Thread t : threads) {
            t.join();
        }

        logger.info("--> child breaker: used: {}, limit: {}", breaker.getUsed(), breaker.getLimit());
        logger.info("--> parent tripped: {}, total trip count: {} (expecting 1-2 for each)", parentTripped.get(), tripped.get());
        assertThat("no other exceptions were thrown", lastException.get(), equalTo(null));
        assertThat("breaker should be reset back to the parent limit after parent breaker trips",
            breaker.getUsed(), greaterThanOrEqualTo((long)parentLimit - NUM_THREADS));
        assertThat("parent breaker was tripped at least once", parentTripped.get(), greaterThanOrEqualTo(1));
        assertThat("total breaker was tripped at least once", tripped.get(), greaterThanOrEqualTo(1));
    }


    /**
     * Test that a breaker correctly redistributes to a different breaker, in
     * this case, the request breaker borrows space from the fielddata breaker
     */
    public void testBorrowingSiblingBreakerMemory() throws Exception {
        Settings clusterSettings = Settings.builder()
            .put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), false)
            .put(HierarchyCircuitBreakerService.TOTAL_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "200mb")
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "150mb")
            .put(HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "150mb")
            .build();
        try (CircuitBreakerService service = new HierarchyCircuitBreakerService(clusterSettings,
            new ClusterSettings(clusterSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS))) {
            CircuitBreaker requestCircuitBreaker = service.getBreaker(CircuitBreaker.REQUEST);
            CircuitBreaker fieldDataCircuitBreaker = service.getBreaker(CircuitBreaker.FIELDDATA);

            assertEquals(new ByteSizeValue(200, ByteSizeUnit.MB).getBytes(),
                service.stats().getStats(CircuitBreaker.PARENT).getLimit());
            assertEquals(new ByteSizeValue(150, ByteSizeUnit.MB).getBytes(), requestCircuitBreaker.getLimit());
            assertEquals(new ByteSizeValue(150, ByteSizeUnit.MB).getBytes(), fieldDataCircuitBreaker.getLimit());

            double fieldDataUsedBytes = fieldDataCircuitBreaker
                .addEstimateBytesAndMaybeBreak(new ByteSizeValue(50, ByteSizeUnit.MB).getBytes(), "should not break");
            assertEquals(new ByteSizeValue(50, ByteSizeUnit.MB).getBytes(), fieldDataUsedBytes, 0.0);
            double requestUsedBytes = requestCircuitBreaker.addEstimateBytesAndMaybeBreak(new ByteSizeValue(50, ByteSizeUnit.MB).getBytes(),
                "should not break");
            assertEquals(new ByteSizeValue(50, ByteSizeUnit.MB).getBytes(), requestUsedBytes, 0.0);
            requestUsedBytes = requestCircuitBreaker.addEstimateBytesAndMaybeBreak(new ByteSizeValue(50, ByteSizeUnit.MB).getBytes(),
                "should not break");
            assertEquals(new ByteSizeValue(100, ByteSizeUnit.MB).getBytes(), requestUsedBytes, 0.0);
            CircuitBreakingException exception = expectThrows(CircuitBreakingException.class, () -> requestCircuitBreaker
                .addEstimateBytesAndMaybeBreak(new ByteSizeValue(50, ByteSizeUnit.MB).getBytes(), "should break"));
            assertThat(exception.getMessage(), containsString("[parent] Data too large, data for [should break] would be"));
            assertThat(exception.getMessage(), containsString("which is larger than the limit of [209715200/200mb]"));
            assertThat(exception.getMessage(),
                containsString("usages [request=157286400/150mb, fielddata=54001664/51.5mb, accounting=0/0b, inflight_requests=0/0b]"));
            assertThat(exception.getDurability(), equalTo(CircuitBreaker.Durability.TRANSIENT));
        }
    }

    public void testParentBreaksOnRealMemoryUsage() throws Exception {
        Settings clusterSettings = Settings.builder()
            .put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), Boolean.TRUE)
            .put(HierarchyCircuitBreakerService.TOTAL_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "200b")
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "350b")
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_OVERHEAD_SETTING.getKey(), 2)
            .build();

        AtomicLong memoryUsage = new AtomicLong();
        final CircuitBreakerService service = new HierarchyCircuitBreakerService(clusterSettings,
            new ClusterSettings(clusterSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)) {
            @Override
            long currentMemoryUsage() {
                return memoryUsage.get();
            }
        };
        final CircuitBreaker requestBreaker = service.getBreaker(CircuitBreaker.REQUEST);

        // anything below 100 bytes should work (overhead) - current memory usage is zero
        requestBreaker.addEstimateBytesAndMaybeBreak(randomLongBetween(0, 99), "request");
        assertEquals(0, requestBreaker.getTrippedCount());
        // assume memory usage has increased to 150 bytes
        memoryUsage.set(150);

        // a reservation that bumps memory usage to less than 200 (150 bytes used  + reservation < 200)
        requestBreaker.addEstimateBytesAndMaybeBreak(randomLongBetween(0, 24), "request");
        assertEquals(0, requestBreaker.getTrippedCount());
        memoryUsage.set(181);

        long reservationInBytes = randomLongBetween(10, 50);
        // anything >= 20 bytes (10 bytes * 2 overhead) reservation breaks the parent but it must be low enough to avoid
        // breaking the child breaker.
        CircuitBreakingException exception = expectThrows(CircuitBreakingException.class, () -> requestBreaker
            .addEstimateBytesAndMaybeBreak(reservationInBytes, "request"));
        // it was the parent that rejected the reservation
        assertThat(exception.getMessage(), containsString("[parent] Data too large, data for [request] would be"));
        assertThat(exception.getMessage(), containsString("which is larger than the limit of [200/200b]"));
        assertThat(exception.getMessage(),
            containsString("real usage: [181/181b], new bytes reserved: [" + (reservationInBytes * 2) +
                "/" + new ByteSizeValue(reservationInBytes * 2) + "]"));
        final long requestCircuitBreakerUsed = (requestBreaker.getUsed() + reservationInBytes) * 2;
        assertThat(exception.getMessage(),
            containsString("usages [request=" + requestCircuitBreakerUsed + "/" + new ByteSizeValue(requestCircuitBreakerUsed) +
                ", fielddata=0/0b, accounting=0/0b, inflight_requests=0/0b]"));
        assertThat(exception.getDurability(), equalTo(CircuitBreaker.Durability.TRANSIENT));
        assertEquals(0, requestBreaker.getTrippedCount());
        assertEquals(1, service.stats().getStats(CircuitBreaker.PARENT).getTrippedCount());

        // lower memory usage again - the same reservation should succeed
        memoryUsage.set(100);
        requestBreaker.addEstimateBytesAndMaybeBreak(reservationInBytes, "request");
        assertEquals(0, requestBreaker.getTrippedCount());
    }

    public void testTrippedCircuitBreakerDurability() {
        Settings clusterSettings = Settings.builder()
            .put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), Boolean.FALSE)
            .put(HierarchyCircuitBreakerService.TOTAL_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "200mb")
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "150mb")
            .put(HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "150mb")
            .build();
        try (CircuitBreakerService service = new HierarchyCircuitBreakerService(clusterSettings,
            new ClusterSettings(clusterSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS))) {
            CircuitBreaker requestCircuitBreaker = service.getBreaker(CircuitBreaker.REQUEST);
            CircuitBreaker fieldDataCircuitBreaker = service.getBreaker(CircuitBreaker.FIELDDATA);

            CircuitBreaker.Durability expectedDurability;
            if (randomBoolean()) {
                fieldDataCircuitBreaker.addEstimateBytesAndMaybeBreak(mb(100), "should not break");
                requestCircuitBreaker.addEstimateBytesAndMaybeBreak(mb(70), "should not break");
                expectedDurability = CircuitBreaker.Durability.PERMANENT;
            } else {
                fieldDataCircuitBreaker.addEstimateBytesAndMaybeBreak(mb(70), "should not break");
                requestCircuitBreaker.addEstimateBytesAndMaybeBreak(mb(120), "should not break");
                expectedDurability = CircuitBreaker.Durability.TRANSIENT;
            }

            CircuitBreakingException exception = expectThrows(CircuitBreakingException.class, () ->
                fieldDataCircuitBreaker.addEstimateBytesAndMaybeBreak(mb(40), "should break"));

            assertThat(exception.getMessage(), containsString("[parent] Data too large, data for [should break] would be"));
            assertThat(exception.getMessage(), containsString("which is larger than the limit of [209715200/200mb]"));
            assertThat("Expected [" + expectedDurability + "] due to [" + exception.getMessage() + "]",
                exception.getDurability(), equalTo(expectedDurability));
        }
    }

    private long mb(long size) {
        return new ByteSizeValue(size, ByteSizeUnit.MB).getBytes();
    }
}
