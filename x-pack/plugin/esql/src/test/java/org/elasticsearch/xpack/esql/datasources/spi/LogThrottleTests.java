/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.atomic.AtomicLong;

public class LogThrottleTests extends ESTestCase {

    public void testOneLoudLinePerInterval() {
        AtomicLong clock = new AtomicLong(randomLong());
        LogThrottle throttle = new LogThrottle(TimeValue.timeValueMinutes(1), clock::get);
        assertTrue("the first call logs loudly", throttle.tryAcquire());
        assertFalse(throttle.tryAcquire());
        clock.addAndGet(TimeValue.timeValueSeconds(59).nanos());
        assertFalse(throttle.tryAcquire());
        clock.addAndGet(TimeValue.timeValueSeconds(1).nanos());
        assertTrue("a full interval later it logs loudly again", throttle.tryAcquire());
        assertFalse(throttle.tryAcquire());
    }

    public void testResetAllowsTheNextCall() {
        AtomicLong clock = new AtomicLong(randomLong());
        LogThrottle throttle = new LogThrottle(TimeValue.timeValueMinutes(1), clock::get);
        assertTrue(throttle.tryAcquire());
        throttle.reset();
        assertTrue(throttle.tryAcquire());
    }
}
