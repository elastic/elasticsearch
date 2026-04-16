/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.atomic.AtomicLong;

public class AdaptiveBackoffTests extends ESTestCase {

    public void testThrottlingIncreasesMultiplier() {
        AtomicLong clock = new AtomicLong(0);
        AdaptiveBackoff backoff = new AdaptiveBackoff(AdaptiveBackoff.MAX_MULTIPLIER, clock::get);

        assertEquals(1, backoff.currentMultiplier());
        backoff.onThrottled();
        assertEquals(2, backoff.currentMultiplier());
        backoff.onThrottled();
        assertEquals(4, backoff.currentMultiplier());
        backoff.onThrottled();
        assertEquals(8, backoff.currentMultiplier());
    }

    public void testSuccessDecreasesMultiplier() {
        AtomicLong clock = new AtomicLong(0);
        AdaptiveBackoff backoff = new AdaptiveBackoff(AdaptiveBackoff.MAX_MULTIPLIER, clock::get);

        backoff.onThrottled();
        backoff.onThrottled();
        backoff.onThrottled();
        assertEquals(8, backoff.currentMultiplier());

        // Advance 1 decay interval → decay halves once: 8 → 4
        clock.addAndGet(AdaptiveBackoff.DECAY_INTERVAL_NANOS);
        backoff.onSuccess();
        assertEquals(4, backoff.currentMultiplier());

        // Advance 1 more interval → decay halves once: 4 → 2
        clock.addAndGet(AdaptiveBackoff.DECAY_INTERVAL_NANOS);
        backoff.onSuccess();
        assertEquals(2, backoff.currentMultiplier());

        // Advance 1 more interval → decay halves once: 2 → 1
        clock.addAndGet(AdaptiveBackoff.DECAY_INTERVAL_NANOS);
        backoff.onSuccess();
        assertEquals(1, backoff.currentMultiplier());
    }

    public void testSuccessWithoutTimeDoesNotDecay() {
        AtomicLong clock = new AtomicLong(0);
        AdaptiveBackoff backoff = new AdaptiveBackoff(AdaptiveBackoff.MAX_MULTIPLIER, clock::get);

        backoff.onThrottled();
        backoff.onThrottled();
        assertEquals(4, backoff.currentMultiplier());

        // No time advance — onSuccess should not decay
        backoff.onSuccess();
        assertEquals(4, backoff.currentMultiplier());
    }

    public void testMultiplierCapped() {
        AtomicLong clock = new AtomicLong(0);
        AdaptiveBackoff backoff = new AdaptiveBackoff(AdaptiveBackoff.MAX_MULTIPLIER, clock::get);

        for (int i = 0; i < 20; i++) {
            backoff.onThrottled();
        }
        assertEquals(AdaptiveBackoff.MAX_MULTIPLIER, backoff.currentMultiplier());
    }

    public void testTimeDecayMultipleIntervals() {
        AtomicLong clock = new AtomicLong(0);
        AdaptiveBackoff backoff = new AdaptiveBackoff(AdaptiveBackoff.MAX_MULTIPLIER, clock::get);

        backoff.onThrottled();
        backoff.onThrottled();
        assertEquals(4, backoff.currentMultiplier());

        // Advance 2 intervals → halves twice: 4 → 2 → 1
        clock.addAndGet(2 * AdaptiveBackoff.DECAY_INTERVAL_NANOS);
        backoff.onSuccess();
        assertEquals(1, backoff.currentMultiplier());
    }

    public void testDisabledIsNoOp() {
        AdaptiveBackoff disabled = AdaptiveBackoff.DISABLED;
        assertFalse(disabled.isEnabled());
        assertEquals(1, disabled.currentMultiplier());
        disabled.onThrottled();
        assertEquals(1, disabled.currentMultiplier());
        disabled.onSuccess();
        assertEquals(1, disabled.currentMultiplier());
    }

    public void testConcurrentAccess() throws Exception {
        AtomicLong clock = new AtomicLong(0);
        AdaptiveBackoff backoff = new AdaptiveBackoff(AdaptiveBackoff.MAX_MULTIPLIER, clock::get);

        Thread[] threads = new Thread[10];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                for (int j = 0; j < 100; j++) {
                    backoff.onThrottled();
                    backoff.currentMultiplier();
                    clock.addAndGet(AdaptiveBackoff.DECAY_INTERVAL_NANOS);
                    backoff.onSuccess();
                }
            });
            threads[i].start();
        }
        for (Thread t : threads) {
            t.join(5000);
        }
        int mult = backoff.currentMultiplier();
        assertTrue("multiplier should be valid: " + mult, mult >= 1 && mult <= AdaptiveBackoff.MAX_MULTIPLIER);
    }
}
