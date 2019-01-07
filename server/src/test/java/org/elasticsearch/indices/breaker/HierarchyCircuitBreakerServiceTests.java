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
            public void checkParentLimit(String label) throws CircuitBreakingException {
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
            public void checkParentLimit(String label) throws CircuitBreakingException {
                // Parent will trip right before regular breaker would trip
                if (getBreaker(CircuitBreaker.REQUEST).getUsed() > parentLimit) {
                    parentTripped.incrementAndGet();
                    logger.info("--> parent tripped");
                    throw new CircuitBreakingException("parent tripped");
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
                containsString("usages [request=157286400/150mb, fielddata=54001664/51.5mb, in_flight_requests=0/0b, accounting=0/0b]"));
        }
    }
}
