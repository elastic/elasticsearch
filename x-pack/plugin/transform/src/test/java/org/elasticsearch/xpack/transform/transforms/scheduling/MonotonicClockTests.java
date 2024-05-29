/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms.scheduling;

import org.elasticsearch.test.ESTestCase;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;

public class MonotonicClockTests extends ESTestCase {

    public void testMonotonicityWithFakeClock() {
        FakeClock fakeClock = new FakeClock(Instant.now());
        Clock clock = new MonotonicClock(fakeClock);
        long lastTime = clock.millis();
        for (int i = 0; i < 100_000_000; ++i) {
            long currentTime = clock.millis();
            assertThat("At iteration " + i, currentTime, is(greaterThanOrEqualTo(lastTime)));
            lastTime = currentTime;
            // -1 is included in order to simulate that the clock can sometimes go back in time
            fakeClock.advanceTimeBy(Duration.ofMillis(randomLongBetween(-1, 100)));
        }
    }

    public void testMonotonicityWithSystemClock() {
        Clock systemClock = Clock.systemUTC();
        Clock clock = new MonotonicClock(systemClock);
        long lastTime = clock.millis();
        for (int i = 0; i < 100_000_000; ++i) {
            long currentTime = clock.millis();
            assertThat("At iteration " + i, currentTime, is(greaterThanOrEqualTo(lastTime)));
            lastTime = currentTime;
        }
    }
}
