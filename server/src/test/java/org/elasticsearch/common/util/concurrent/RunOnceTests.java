/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class RunOnceTests extends ESTestCase {

    public void testRunOnce() {
        final AtomicInteger counter = new AtomicInteger(0);
        final RunOnce runOnce = new RunOnce(counter::incrementAndGet);
        assertFalse(runOnce.hasRun());

        runOnce.run();
        assertTrue(runOnce.hasRun());
        assertEquals(1, counter.get());

        runOnce.run();
        assertTrue(runOnce.hasRun());
        assertEquals(1, counter.get());
    }

    public void testRunOnceConcurrently() throws InterruptedException {
        final AtomicInteger counter = new AtomicInteger(0);
        final RunOnce runOnce = new RunOnce(counter::incrementAndGet);

        final Thread[] threads = new Thread[between(3, 10)];
        final CountDownLatch latch = new CountDownLatch(1 + threads.length);
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                latch.countDown();
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                runOnce.run();
            });
            threads[i].start();
        }

        latch.countDown();
        for (Thread thread : threads) {
            thread.join();
        }
        assertTrue(runOnce.hasRun());
        assertEquals(1, counter.get());
    }

    public void testRunOnceWithAbstractRunnable() {
        final AtomicInteger onRun = new AtomicInteger(0);
        final AtomicInteger onFailure = new AtomicInteger(0);
        final AtomicInteger onAfter = new AtomicInteger(0);

        final RunOnce runOnce = new RunOnce(new AbstractRunnable() {
            @Override
            protected void doRun() throws Exception {
                onRun.incrementAndGet();
                throw new RuntimeException("failure");
            }

            @Override
            public void onFailure(Exception e) {
                onFailure.incrementAndGet();
            }

            @Override
            public void onAfter() {
                onAfter.incrementAndGet();
            }
        });

        final int iterations = randomIntBetween(1, 10);
        for (int i = 0; i < iterations; i++) {
            runOnce.run();
            assertEquals(1, onRun.get());
            assertEquals(1, onFailure.get());
            assertEquals(1, onAfter.get());
            assertTrue(runOnce.hasRun());
        }
    }
}
