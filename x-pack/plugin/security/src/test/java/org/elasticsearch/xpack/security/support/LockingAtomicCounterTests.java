/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.support;

import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;

public class LockingAtomicCounterTests extends ESTestCase {

    private LockingAtomicCounter lockingAtomicCounter;

    @Before
    public void setup() {
        lockingAtomicCounter = new LockingAtomicCounter();
    }

    public void testRunnableWillRunIfCountMatches() throws Exception {
        final AtomicBoolean done = new AtomicBoolean();
        final long invalidationCount = lockingAtomicCounter.get();
        assertTrue(lockingAtomicCounter.compareAndRun(invalidationCount, () -> done.set(true)));
        assertTrue(done.get());
    }

    public void testIncrementAndRun() {
        final int loop = randomIntBetween(1, 5);
        IntStream.range(0, loop).forEach((ignored) -> {
            try {
                lockingAtomicCounter.increment();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        assertThat((long) loop, equalTo(lockingAtomicCounter.get()));
    }

    public void testRunnableWillNotRunIfCounterHasChanged() throws Exception {
        final AtomicBoolean done = new AtomicBoolean();
        final long invalidationCount = lockingAtomicCounter.get();
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        new Thread(() -> {
            try {
                lockingAtomicCounter.increment();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            countDownLatch.countDown();
        }).start();
        countDownLatch.await();
        assertFalse(lockingAtomicCounter.compareAndRun(invalidationCount, () -> done.set(true)));
        assertFalse(done.get());
    }
}
