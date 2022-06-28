/*
 * Licensed to Elasticsearch B.V. under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class DeadHostStateTests extends RestClientTestCase {

    private static long[] EXPECTED_TIMEOUTS_SECONDS = new long[] { 60, 84, 120, 169, 240, 339, 480, 678, 960, 1357, 1800 };

    public void testInitialDeadHostStateDefaultTimeSupplier() {
        DeadHostState deadHostState = new DeadHostState(DeadHostState.DEFAULT_TIME_SUPPLIER);
        long currentTime = System.nanoTime();
        assertThat(deadHostState.getDeadUntilNanos(), greaterThanOrEqualTo(currentTime));
        assertThat(deadHostState.getFailedAttempts(), equalTo(1));
    }

    public void testDeadHostStateFromPreviousDefaultTimeSupplier() {
        DeadHostState previous = new DeadHostState(DeadHostState.DEFAULT_TIME_SUPPLIER);
        int iters = randomIntBetween(5, 30);
        for (int i = 0; i < iters; i++) {
            DeadHostState deadHostState = new DeadHostState(previous);
            assertThat(deadHostState.getDeadUntilNanos(), greaterThan(previous.getDeadUntilNanos()));
            assertThat(deadHostState.getFailedAttempts(), equalTo(previous.getFailedAttempts() + 1));
            previous = deadHostState;
        }
    }

    public void testCompareToTimeSupplier() {
        int numObjects = randomIntBetween(EXPECTED_TIMEOUTS_SECONDS.length, 30);
        DeadHostState[] deadHostStates = new DeadHostState[numObjects];
        final AtomicLong time = new AtomicLong(0);
        for (int i = 0; i < numObjects; i++) {
            if (i == 0) {
                // this test requires a strictly increasing timer. This ensures that even if we call this time supplier in a very tight
                // loop we always notice time moving forward. This does not happen for real timer implementations
                // (e.g. on Linux <code>clock_gettime</code> provides microsecond resolution).
                deadHostStates[i] = new DeadHostState(time::incrementAndGet);
            } else {
                deadHostStates[i] = new DeadHostState(deadHostStates[i - 1]);
            }
        }
        for (int k = 1; k < deadHostStates.length; k++) {
            assertThat(deadHostStates[k - 1].getDeadUntilNanos(), lessThan(deadHostStates[k].getDeadUntilNanos()));
            assertThat(deadHostStates[k - 1], lessThan(deadHostStates[k]));
        }
    }

    public void testCompareToDifferingTimeSupplier() {
        try {
            new DeadHostState(DeadHostState.DEFAULT_TIME_SUPPLIER).compareTo(new DeadHostState(() -> 0L));
            fail("expected failure");
        } catch (IllegalArgumentException e) {
            assertEquals(
                "can't compare DeadHostStates holding different time suppliers as they may " + "be based on different clocks",
                e.getMessage()
            );
        }
    }

    public void testShallBeRetried() {
        final AtomicLong time = new AtomicLong(0);
        DeadHostState deadHostState = null;
        for (int i = 0; i < EXPECTED_TIMEOUTS_SECONDS.length; i++) {
            long expectedTimeoutSecond = EXPECTED_TIMEOUTS_SECONDS[i];
            if (i == 0) {
                deadHostState = new DeadHostState(time::get);
            } else {
                deadHostState = new DeadHostState(deadHostState);
            }
            for (int j = 0; j < expectedTimeoutSecond; j++) {
                time.addAndGet(TimeUnit.SECONDS.toNanos(1));
                assertThat(deadHostState.shallBeRetried(), is(false));
            }
            int iters = randomIntBetween(5, 30);
            for (int j = 0; j < iters; j++) {
                time.addAndGet(TimeUnit.SECONDS.toNanos(1));
                assertThat(deadHostState.shallBeRetried(), is(true));
            }
        }
    }

    public void testDeadHostStateTimeouts() {
        DeadHostState previous = new DeadHostState(() -> 0L);
        for (long expectedTimeoutsSecond : EXPECTED_TIMEOUTS_SECONDS) {
            assertThat(TimeUnit.NANOSECONDS.toSeconds(previous.getDeadUntilNanos()), equalTo(expectedTimeoutsSecond));
            previous = new DeadHostState(previous);
        }
        // check that from here on the timeout does not increase
        int iters = randomIntBetween(5, 30);
        for (int i = 0; i < iters; i++) {
            DeadHostState deadHostState = new DeadHostState(previous);
            assertThat(
                TimeUnit.NANOSECONDS.toSeconds(deadHostState.getDeadUntilNanos()),
                equalTo(EXPECTED_TIMEOUTS_SECONDS[EXPECTED_TIMEOUTS_SECONDS.length - 1])
            );
            previous = deadHostState;
        }
    }
}
