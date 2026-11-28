/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;

public class WindowedRateLimiterTests extends ESTestCase {

    private final int permitsPerWindow = randomIntBetween(1, 10);
    private final long windowNanos = randomLongBetween(1, TimeUnit.HOURS.toNanos(2));

    private WindowedRateLimiter limiterWithClock(AtomicLong clock) {
        return new WindowedRateLimiter(permitsPerWindow, TimeValue.timeValueNanos(windowNanos), clock::get);
    }

    /** Each window starts at its first nanosecond and ends at its last, so the window boundaries are checked exactly. */
    public void testPermitsResetInEachNewWindow() {
        var clock = new AtomicLong();
        var limiter = limiterWithClock(clock);
        long windowStart = randomWindowIndex() * windowNanos;

        int windows = randomIntBetween(2, 5);
        for (int w = 0; w < windows; w++) {
            clock.set(windowStart);
            assertTrue("window " + w, limiter.mayHavePermit());
            for (int i = 0; i < permitsPerWindow; i++) {
                assertTrue("window " + w + " permit " + i, limiter.tryAcquirePermit());
                clock.set(windowStart + randomLongBetween(0, windowNanos - 1));
            }
            assertFalse("window " + w, limiter.tryAcquirePermit());
            clock.set(windowStart + windowNanos - 1);
            assertFalse("window " + w, limiter.mayHavePermit());
            assertFalse("window " + w, limiter.tryAcquirePermit());
            windowStart += randomLongBetween(1, 100) * windowNanos;
        }
    }

    public void testConcurrentCallsGrantExactlyOneWindowOfPermits() {
        var limiter = limiterWithClock(new AtomicLong(randomWindowIndex() * windowNanos));
        var granted = new AtomicInteger();
        int callsPerThread = randomIntBetween(10, 200);

        startInParallel(randomIntBetween(2, 8), thread -> {
            for (int i = 0; i < callsPerThread; i++) {
                if (limiter.tryAcquirePermit()) {
                    granted.incrementAndGet();
                }
            }
        });

        assertThat(granted.get(), equalTo(permitsPerWindow));
    }

    /**
     * The first clock read stalls. While it stalls, other callers use every permit in the next window, and then the read
     * returns the old time. The stale caller must not reset the next window's usage.
     */
    public void testStaleClockReadCannotMoveTheWindowBackwards() {
        long oldWindowTime = randomWindowIndex() * windowNanos;
        long newWindowTime = oldWindowTime + windowNanos;
        var limiterRef = new AtomicReference<WindowedRateLimiter>();
        var stallNextRead = new AtomicBoolean(true);
        var limiter = new WindowedRateLimiter(permitsPerWindow, TimeValue.timeValueNanos(windowNanos), () -> {
            if (stallNextRead.getAndSet(false)) {
                for (int i = 0; i < permitsPerWindow; i++) {
                    assertTrue(limiterRef.get().tryAcquirePermit());
                }
                return oldWindowTime;
            }
            return newWindowTime;
        });
        limiterRef.set(limiter);

        assertFalse(limiter.tryAcquirePermit());
        assertFalse(limiter.mayHavePermit());
        assertFalse(limiter.tryAcquirePermit());
    }

    public void testWindowIndexBeyondIntRangeDoesNotWrap() {
        long smallWindowNanos = TimeUnit.MILLISECONDS.toNanos(1);
        var clock = new AtomicLong(randomLongBetween(0, 1_000_000) * smallWindowNanos);
        var limiter = new WindowedRateLimiter(permitsPerWindow, TimeValue.timeValueNanos(smallWindowNanos), clock::get);
        for (int i = 0; i < permitsPerWindow; i++) {
            assertTrue(limiter.tryAcquirePermit());
        }
        assertFalse(limiter.tryAcquirePermit());

        clock.addAndGet((1L << 32) * smallWindowNanos);

        assertTrue(limiter.mayHavePermit());
        assertTrue(limiter.tryAcquirePermit());
    }

    public void testInvalidParametersAreRejected() {
        expectThrows(
            IllegalArgumentException.class,
            () -> new WindowedRateLimiter(randomIntBetween(-10, 0), TimeValue.timeValueNanos(windowNanos), () -> 0L)
        );
        expectThrows(IllegalArgumentException.class, () -> new WindowedRateLimiter(permitsPerWindow, TimeValue.ZERO, () -> 0L));
    }

    private static long randomWindowIndex() {
        return randomLongBetween(-1000, 1000);
    }
}
