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

package org.elasticsearch.client;

import java.util.concurrent.TimeUnit;

import org.elasticsearch.client.DeadHostState.TimeSupplier;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class DeadHostStateTests extends RestClientTestCase {

    private static long[] EXPECTED_TIMEOUTS_SECONDS = new long[]{60, 84, 120, 169, 240, 339, 480, 678, 960, 1357, 1800};

    public void testInitialDeadHostStateDefaultTimeSupplier() {
        assumeFalse("https://github.com/elastic/elasticsearch/issues/33747", System.getProperty("os.name").startsWith("Windows"));
        DeadHostState deadHostState = new DeadHostState(DeadHostState.TimeSupplier.DEFAULT);
        long currentTime = System.nanoTime();
        assertThat(deadHostState.getDeadUntilNanos(), greaterThan(currentTime));
        assertThat(deadHostState.getFailedAttempts(), equalTo(1));
    }

    public void testDeadHostStateFromPreviousDefaultTimeSupplier() {
        DeadHostState previous = new DeadHostState(DeadHostState.TimeSupplier.DEFAULT);
        int iters = randomIntBetween(5, 30);
        for (int i = 0; i < iters; i++) {
            DeadHostState deadHostState = new DeadHostState(previous);
            assertThat(deadHostState.getDeadUntilNanos(), greaterThan(previous.getDeadUntilNanos()));
            assertThat(deadHostState.getFailedAttempts(), equalTo(previous.getFailedAttempts() + 1));
            previous = deadHostState;
        }
    }

    public void testCompareToDefaultTimeSupplier() {
        assumeFalse("https://github.com/elastic/elasticsearch/issues/33747", System.getProperty("os.name").startsWith("Windows"));
        int numObjects = randomIntBetween(EXPECTED_TIMEOUTS_SECONDS.length, 30);
        DeadHostState[] deadHostStates = new DeadHostState[numObjects];
        for (int i = 0; i < numObjects; i++) {
            if (i == 0) {
                deadHostStates[i] = new DeadHostState(DeadHostState.TimeSupplier.DEFAULT);
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
            new DeadHostState(TimeSupplier.DEFAULT).compareTo(
                    new DeadHostState(new ConfigurableTimeSupplier()));
            fail("expected failure");
        } catch (IllegalArgumentException e) {
            assertEquals("can't compare DeadHostStates with different clocks [nanoTime != configured[0]]",
                    e.getMessage());
        }
    }

    public void testShallBeRetried() {
        ConfigurableTimeSupplier timeSupplier = new ConfigurableTimeSupplier();
        DeadHostState deadHostState = null;
        for (int i = 0; i < EXPECTED_TIMEOUTS_SECONDS.length; i++) {
            long expectedTimeoutSecond = EXPECTED_TIMEOUTS_SECONDS[i];
            timeSupplier.nanoTime = 0;
            if (i == 0) {
                deadHostState = new DeadHostState(timeSupplier);
            } else {
                deadHostState = new DeadHostState(deadHostState);
            }
            for (int j = 0; j < expectedTimeoutSecond; j++) {
                timeSupplier.nanoTime += TimeUnit.SECONDS.toNanos(1);
                assertThat(deadHostState.shallBeRetried(), is(false));
            }
            int iters = randomIntBetween(5, 30);
            for (int j = 0; j < iters; j++) {
                timeSupplier.nanoTime += TimeUnit.SECONDS.toNanos(1);
                assertThat(deadHostState.shallBeRetried(), is(true));
            }
        }
    }

    public void testDeadHostStateTimeouts() {
        ConfigurableTimeSupplier zeroTimeSupplier = new ConfigurableTimeSupplier();
        zeroTimeSupplier.nanoTime = 0L;
        DeadHostState previous = new DeadHostState(zeroTimeSupplier);
        for (long expectedTimeoutsSecond : EXPECTED_TIMEOUTS_SECONDS) {
            assertThat(TimeUnit.NANOSECONDS.toSeconds(previous.getDeadUntilNanos()), equalTo(expectedTimeoutsSecond));
            previous = new DeadHostState(previous);
        }
        //check that from here on the timeout does not increase
        int iters = randomIntBetween(5, 30);
        for (int i = 0; i < iters; i++) {
            DeadHostState deadHostState = new DeadHostState(previous);
            assertThat(TimeUnit.NANOSECONDS.toSeconds(deadHostState.getDeadUntilNanos()),
                    equalTo(EXPECTED_TIMEOUTS_SECONDS[EXPECTED_TIMEOUTS_SECONDS.length - 1]));
            previous = deadHostState;
        }
    }

    static class ConfigurableTimeSupplier implements DeadHostState.TimeSupplier {
        long nanoTime;

        @Override
        public long nanoTime() {
            return nanoTime;
        }

        @Override
        public String toString() {
            return "configured[" + nanoTime + "]";
        }
    }
}
