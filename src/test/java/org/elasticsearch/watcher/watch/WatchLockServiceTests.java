/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.watch;

import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.*;

/**
 */
public class WatchLockServiceTests extends ElasticsearchTestCase {

    @Test
    public void testLocking_notStarted() {
        WatchLockService lockService = new WatchLockService(new TimeValue(1, TimeUnit.SECONDS));
        try {
            lockService.acquire("_name");
            fail("exception expected");
        } catch (Exception e) {
            assertThat(e.getMessage(), equalTo("not started"));
        }
    }

    @Test
    public void testLocking() {
        WatchLockService lockService = new WatchLockService(new TimeValue(1, TimeUnit.SECONDS));
        lockService.start();
        WatchLockService.Lock lock = lockService.acquire("_name");
        assertThat(lockService.getWatchLocks().hasLockedKeys(), is(true));
        lock.release();
        assertThat(lockService.getWatchLocks().hasLockedKeys(), is(false));
        lockService.stop();
    }

    @Test
    public void testLocking_alreadyHeld() {
        WatchLockService lockService = new WatchLockService(new TimeValue(1, TimeUnit.SECONDS));
        lockService.start();
        WatchLockService.Lock lock1 = lockService.acquire("_name");
        try {
            lockService.acquire("_name");
            fail("exception expected");
        } catch (ElasticsearchIllegalStateException e) {
            assertThat(e.getMessage(), containsString("Lock already acquired"));
        }
        lock1.release();
        lockService.stop();
    }

    @Test(expected = WatchLockService.TimedOutException.class)
    public void testLocking_stopTimeout(){
        final WatchLockService lockService = new WatchLockService(new TimeValue(1, TimeUnit.SECONDS));
        lockService.start();
        lockService.acquire("_name");
        lockService.stop();
        fail();
    }

    @Test
    public void testLocking_fair() throws Exception {
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
