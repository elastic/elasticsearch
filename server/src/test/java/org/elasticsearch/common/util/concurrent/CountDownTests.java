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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class CountDownTests extends ESTestCase {
    public void testConcurrent() throws InterruptedException {
        final AtomicInteger count = new AtomicInteger(0);
        final CountDown countDown = new CountDown(scaledRandomIntBetween(10, 1000));
        Thread[] threads = new Thread[between(3, 10)];
        final CountDownLatch latch = new CountDownLatch(1 + threads.length);
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread() {

                @Override
                public void run() {
                    latch.countDown();
                    try {
                        latch.await();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    while (true) {
                        if (frequently()) {
                            if (countDown.isCountedDown()) {
                                break;
                            }
                        }
                        if (countDown.countDown()) {
                            count.incrementAndGet();
                            break;
                        }
                    }
                }
            };
            threads[i].start();
        }
        latch.countDown();
        Thread.yield();
        if (rarely()) {
            if (countDown.fastForward()) {
                count.incrementAndGet();
            }
            assertThat(countDown.isCountedDown(), equalTo(true));
            assertThat(countDown.fastForward(), equalTo(false));

        }

        for (Thread thread : threads) {
            thread.join();
        }
        assertThat(countDown.isCountedDown(), equalTo(true));
        assertThat(count.get(), Matchers.equalTo(1));
    }

    public void testSingleThreaded() {
        int atLeast = scaledRandomIntBetween(10, 1000);
        final CountDown countDown = new CountDown(atLeast);
        while (countDown.isCountedDown() == false) {
            atLeast--;
            if (countDown.countDown()) {
                assertThat(atLeast, equalTo(0));
                assertThat(countDown.isCountedDown(), equalTo(true));
                assertThat(countDown.fastForward(), equalTo(false));
                break;
            }
            if (rarely()) {
                assertThat(countDown.fastForward(), equalTo(true));
                assertThat(countDown.isCountedDown(), equalTo(true));
                assertThat(countDown.fastForward(), equalTo(false));
            }
            assertThat(atLeast, greaterThan(0));
        }
    }
}
