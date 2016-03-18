/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.watch;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

/**
 */
public class WatchLockServiceTests extends ESTestCase {
    public void testLockingNotStarted() {
        WatchLockService lockService = new WatchLockService(new TimeValue(1, TimeUnit.SECONDS));
        try {
            lockService.acquire("_name");
            fail("exception expected");
        } catch (Exception e) {
            assertThat(e.getMessage(), containsString("not running"));
        }
    }

    public void testLocking() {
        WatchLockService lockService = new WatchLockService(new TimeValue(1, TimeUnit.SECONDS));
        lockService.start();
        WatchLockService.Lock lock = lockService.acquire("_name");
        assertThat(lockService.getWatchLocks().hasLockedKeys(), is(true));
        lock.release();
        assertThat(lockService.getWatchLocks().hasLockedKeys(), is(false));
        lockService.stop();
    }

    public void testLockingAlreadyHeld() {
        WatchLockService lockService = new WatchLockService(new TimeValue(1, TimeUnit.SECONDS));
        lockService.start();
        WatchLockService.Lock lock1 = lockService.acquire("_name");
        try {
            lockService.acquire("_name");
            fail("exception expected");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("Lock already acquired"));
        }
        lock1.release();
        lockService.stop();
    }

    public void testLockingStopTimeout(){
        final WatchLockService lockService = new WatchLockService(new TimeValue(1, TimeUnit.SECONDS));
        lockService.start();
        lockService.acquire("_name");
        try {
            lockService.stop();
            fail("Expected ElasticsearchTimeoutException");
        } catch (ElasticsearchTimeoutException e) {
            assertThat(e.getMessage(), startsWith("timed out waiting for watches to complete, after waiting for"));
        }
    }

    public void testLockingFair() throws Exception {
        final WatchLockService lockService = new WatchLockService(new TimeValue(1, TimeUnit.SECONDS));
        lockService.start();
        final AtomicInteger value = new AtomicInteger(0);
        List<Thread> threads = new ArrayList<>();

        class FairRunner implements Runnable {

            final int expectedValue;
            final CountDownLatch startLatch = new CountDownLatch(1);

            FairRunner(int expectedValue) {
                this.expectedValue = expectedValue;
            }

            @Override
            public void run() {
                startLatch.countDown();
                WatchLockService.Lock lock = lockService.acquire("_name");
                try {
                    int actualValue = value.getAndIncrement();
                    assertThat(actualValue, equalTo(expectedValue));
                    Thread.sleep(50);
                } catch(InterruptedException ie) {
                } finally {
                    lock.release();
                }
            }
        }

        List<FairRunner> runners = new ArrayList<>();

        for(int i = 0; i < 50; ++i) {
            FairRunner f = new FairRunner(i);
            runners.add(f);
            threads.add(new Thread(f));
        }

        for(int i = 0; i < threads.size(); ++i) {
            threads.get(i).start();
            runners.get(i).startLatch.await();
            Thread.sleep(25);
        }

        for(Thread t : threads) {
            t.join();
        }
    }

}
