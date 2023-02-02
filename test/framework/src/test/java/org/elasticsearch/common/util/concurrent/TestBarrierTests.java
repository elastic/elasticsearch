/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class TestBarrierTests extends ESTestCase {

    public void testUsage() throws InterruptedException {
        final var barrier = new TestBarrier(2);
        final var completed = new AtomicBoolean();
        final var thread = new Thread(() -> {
            barrier.await();
            assertTrue(completed.compareAndSet(false, true));
        });
        thread.start();
        assertFalse(completed.get());
        assertThat(randomAwait(barrier), Matchers.oneOf(0, 1));
        thread.join();
        assertTrue(completed.get());
    }

    public void testExceptions() throws InterruptedException {
        final var barrier = new TestBarrier(2);
        final var completed = new AtomicBoolean();
        final var thread = new Thread(() -> {
            expectThrows(AssertionError.class, () -> randomAwait(barrier));
            assertTrue(completed.compareAndSet(false, true));
        });
        thread.start();
        do {
            barrier.reset();
            thread.join(10);
        } while (thread.isAlive());
        assertTrue(completed.get());
    }

    @SuppressWarnings("deprecation")
    private int randomAwait(TestBarrier barrier) {
        return switch (between(1, 4)) {
            case 1 -> barrier.await();
            case 2 -> barrier.awaitLong();
            case 3 -> barrier.await(10, TimeUnit.SECONDS);
            case 4 -> barrier.awaitForever();
            default -> throw new AssertionError();
        };
    }

}
