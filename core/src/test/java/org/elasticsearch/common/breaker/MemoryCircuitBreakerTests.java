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

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.indices.breaker.BreakerSettings;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Tests for the Memory Aggregating Circuit Breaker
 */
public class MemoryCircuitBreakerTests extends ESTestCase {

    @Test
    public void testThreadedUpdatesToBreaker() throws Exception {
        final int NUM_THREADS = scaledRandomIntBetween(3, 15);
        final int BYTES_PER_THREAD = scaledRandomIntBetween(500, 4500);
        final Thread[] threads = new Thread[NUM_THREADS];
        final AtomicBoolean tripped = new AtomicBoolean(false);
        final AtomicReference<Throwable> lastException = new AtomicReference<>(null);

        final MemoryCircuitBreaker breaker = new MemoryCircuitBreaker(new ByteSizeValue((BYTES_PER_THREAD * NUM_THREADS) - 1), 1.0, logger);

        for (int i = 0; i < NUM_THREADS; i++) {
            threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    for (int j = 0; j < BYTES_PER_THREAD; j++) {
                        try {
                            breaker.addEstimateBytesAndMaybeBreak(1L, "test");
                        } catch (CircuitBreakingException e) {
                            if (tripped.get()) {
                                assertThat("tripped too many times", true, equalTo(false));
                            } else {
                                assertThat(tripped.compareAndSet(false, true), equalTo(true));
                            }
                        } catch (Throwable e2) {
                            lastException.set(e2);
                        }
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

    @Test
    public void testThreadedUpdatesToChildBreaker() throws Exception {
        final int NUM_THREADS = scaledRandomIntBetween(3, 15);
        final int BYTES_PER_THREAD = scaledRandomIntBetween(500, 4500);
        final Thread[] threads = new Thread[NUM_THREADS];
        final AtomicBoolean tripped = new AtomicBoolean(false);
        final AtomicReference<Throwable> lastException = new AtomicReference<>(null);

        final AtomicReference<ChildMemoryCircuitBreaker> breakerRef = new AtomicReference<>(null);
        final CircuitBreakerService service = new HierarchyCircuitBreakerService(Settings.EMPTY, new NodeSettingsService(Settings.EMPTY)) {

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
            threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    for (int j = 0; j < BYTES_PER_THREAD; j++) {
                        try {
                            breaker.addEstimateBytesAndMaybeBreak(1L, "test");
                        } catch (CircuitBreakingException e) {
                            if (tripped.get()) {
                                assertThat("tripped too many times", true, equalTo(false));
                            } else {
                                assertThat(tripped.compareAndSet(false, true), equalTo(true));
                            }
                        } catch (Throwable e2) {
                            lastException.set(e2);
                        }
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

    @Test
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
        final CircuitBreakerService service = new HierarchyCircuitBreakerService(Settings.EMPTY, new NodeSettingsService(Settings.EMPTY)) {

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
            threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    for (int j = 0; j < BYTES_PER_THREAD; j++) {
                        try {
                            breaker.addEstimateBytesAndMaybeBreak(1L, "test");
                        } catch (CircuitBreakingException e) {
                            tripped.incrementAndGet();
                        } catch (Throwable e2) {
                            lastException.set(e2);
                        }
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

    @Test
    public void testConstantFactor() throws Exception {
        final MemoryCircuitBreaker breaker = new MemoryCircuitBreaker(new ByteSizeValue(15), 1.6, logger);
        String field = "myfield";

        // add only 7 bytes
        breaker.addWithoutBreaking(7);

        try {
            // this won't actually add it because it trips the breaker
            breaker.addEstimateBytesAndMaybeBreak(3, field);
            fail("should never reach this");
        } catch (CircuitBreakingException cbe) {
        }

        // shouldn't throw an exception
        breaker.addEstimateBytesAndMaybeBreak(2, field);

        assertThat(breaker.getUsed(), equalTo(9L));

        // adding 3 more bytes (now at 12)
        breaker.addWithoutBreaking(3);

        try {
            // Adding no bytes still breaks
            breaker.addEstimateBytesAndMaybeBreak(0, field);
            fail("should never reach this");
        } catch (CircuitBreakingException cbe) {
            assertThat("breaker was tripped exactly twice", breaker.getTrippedCount(), equalTo(2L));
            assertThat(cbe.getMessage().contains("field [" + field + "]"), equalTo(true));
        }
    }
}
