/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.support;

import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;

public class CountingRunnerTests extends ESTestCase {

    private CountingRunner countingRunner;

    @Before
    public void setup() {
        countingRunner = new CountingRunner();
    }

    public void testRunnableWillRunIfCountMatches() throws Exception {
        final AtomicBoolean done = new AtomicBoolean();
        final long invalidationCount = countingRunner.getCount();
        assertTrue(countingRunner.runIfCountMatches(() -> done.set(true), invalidationCount));
        assertTrue(done.get());
    }

    public void testIncrementAndRun() {
        final int loop = randomIntBetween(1, 5);
        IntStream.range(0, loop).forEach((ignored) -> {
            try {
                countingRunner.incrementAndRun(() -> {});
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        assertThat(loop, equalTo(countingRunner.getCount()));
    }

    public void testRunnableWillNotRunIfCounterHasChanged() throws Exception {
        final AtomicBoolean done = new AtomicBoolean();
        final long invalidationCount = countingRunner.getCount();
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        new Thread(() -> {
            try {
                countingRunner.incrementAndRun(() -> {});
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            countDownLatch.countDown();
        }).start();
        countDownLatch.await();
        assertFalse(countingRunner.runIfCountMatches(() -> done.set(true), invalidationCount));
        assertFalse(done.get());
    }
}
