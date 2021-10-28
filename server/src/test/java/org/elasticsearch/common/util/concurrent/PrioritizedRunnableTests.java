/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.common.Priority;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class PrioritizedRunnableTests extends ESTestCase {

    // test unit conversion with a controlled clock
    public void testGetAgeInMillis() throws Exception {
        AtomicLong time = new AtomicLong();

        PrioritizedRunnable runnable = new PrioritizedRunnable(Priority.NORMAL, time::get) {
            @Override
            public void run() {}
        };
        assertEquals(0, runnable.getAgeInMillis());
        int milliseconds = randomIntBetween(1, 256);
        time.addAndGet(TimeUnit.NANOSECONDS.convert(milliseconds, TimeUnit.MILLISECONDS));
        assertEquals(milliseconds, runnable.getAgeInMillis());
    }

    // test age advances with System#nanoTime
    public void testGetAgeInMillisWithRealClock() throws InterruptedException {
        PrioritizedRunnable runnable = new PrioritizedRunnable(Priority.NORMAL) {
            @Override
            public void run() {}
        };

        long elapsed = spinForAtLeastOneMillisecond();

        // creation happened before start, so age will be at least as
        // large as elapsed
        assertThat(runnable.getAgeInMillis(), greaterThanOrEqualTo(TimeUnit.MILLISECONDS.convert(elapsed, TimeUnit.NANOSECONDS)));
    }

}
