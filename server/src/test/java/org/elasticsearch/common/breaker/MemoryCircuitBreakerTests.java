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

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Tests for the Memory Aggregating Circuit Breaker
 */
public class MemoryCircuitBreakerTests extends ESTestCase {
    public void testThreadedUpdatesToBreaker() throws Exception {
        final int NUM_THREADS = scaledRandomIntBetween(3, 15);
        final int BYTES_PER_THREAD = scaledRandomIntBetween(500, 4500);
        final Thread[] threads = new Thread[NUM_THREADS];
        final AtomicBoolean tripped = new AtomicBoolean(false);
        final AtomicReference<Exception> lastException = new AtomicReference<>(null);

        final MemoryCircuitBreaker breaker = new MemoryCircuitBreaker(new ByteSizeValue((BYTES_PER_THREAD * NUM_THREADS) - 1), 1.0, logger);

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

            long newUsed = (long)(breaker.getUsed() * breaker.getOverhead());
            assertThat(cbe.getMessage().contains("would be [" + newUsed + "/"), equalTo(true));
            assertThat(cbe.getMessage().contains("field [" + field + "]"), equalTo(true));
        }
    }
}
