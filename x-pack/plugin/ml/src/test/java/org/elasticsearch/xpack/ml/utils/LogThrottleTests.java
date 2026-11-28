/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.utils;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.equalTo;

public class LogThrottleTests extends ESTestCase {

    private static final TimeValue INTERVAL = TimeValue.timeValueMinutes(1);

    private final AtomicLong clockMillis = new AtomicLong(randomLongBetween(0, 1_000_000));

    private LogThrottle throttle(int maxKeys) {
        return new LogThrottle(clockMillis::get, INTERVAL, maxKeys);
    }

    public void testFirstOccurrenceShouldLogWithoutSuppressedCount() {
        assertThat(throttle(10).recordOccurrence("a"), equalTo(new LogThrottle.Decision(true, 0)));
    }

    public void testRepeatWithinIntervalShouldBeSuppressed() {
        LogThrottle throttle = throttle(10);
        throttle.recordOccurrence("a");
        clockMillis.addAndGet(INTERVAL.millis() - 1);
        assertThat(throttle.recordOccurrence("a"), equalTo(new LogThrottle.Decision(false, 0)));
    }

    public void testOccurrenceAtIntervalBoundaryShouldLogWithSuppressedCount() {
        LogThrottle throttle = throttle(10);
        throttle.recordOccurrence("a");
        throttle.recordOccurrence("a");
        throttle.recordOccurrence("a");
        clockMillis.addAndGet(INTERVAL.millis());
        assertThat(throttle.recordOccurrence("a"), equalTo(new LogThrottle.Decision(true, 2)));
        // the count restarts after being reported
        clockMillis.addAndGet(INTERVAL.millis());
        assertThat(throttle.recordOccurrence("a"), equalTo(new LogThrottle.Decision(true, 0)));
    }

    public void testDistinctKeysShouldBeThrottledIndependently() {
        LogThrottle throttle = throttle(10);
        throttle.recordOccurrence("a");
        assertThat(throttle.recordOccurrence("b"), equalTo(new LogThrottle.Decision(true, 0)));
        assertThat(throttle.recordOccurrence("a"), equalTo(new LogThrottle.Decision(false, 0)));
    }

    public void testKeyBeyondCapacityShouldEvictLeastRecentlyUsedKey() {
        LogThrottle throttle = throttle(2);
        throttle.recordOccurrence("a");
        throttle.recordOccurrence("b");
        // touching "a" makes "b" the least recently used
        assertThat(throttle.recordOccurrence("a").shouldLog(), equalTo(false));
        assertThat(throttle.recordOccurrence("c").shouldLog(), equalTo(true));

        assertThat("a is still tracked", throttle.recordOccurrence("a").shouldLog(), equalTo(false));
        assertThat("b was evicted, so it is treated as new", throttle.recordOccurrence("b").shouldLog(), equalTo(true));
    }

    public void testNonPositiveMaxKeysShouldBeRejected() {
        expectThrows(IllegalArgumentException.class, () -> throttle(0));
    }
}
