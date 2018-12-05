/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class AbstractAsyncTaskTests extends ESTestCase {

    private static ThreadPool threadPool;

    @BeforeClass
    public static void setUpThreadPool() {
        threadPool = new TestThreadPool(AbstractAsyncTaskTests.class.getSimpleName());
    }

    @AfterClass
    public static void tearDownThreadPool() {
        terminate(threadPool);
    }

    public void testRepeat() throws InterruptedException {

        final AtomicReference<CountDownLatch> latch1 = new AtomicReference<>(new CountDownLatch(1));
        final AtomicReference<CountDownLatch> latch2 = new AtomicReference<>(new CountDownLatch(1));
        final AtomicInteger count = new AtomicInteger();
        AbstractAsyncTask task = new AbstractAsyncTask(logger, threadPool, TimeValue.timeValueMillis(1)) {

            @Override
            protected boolean mustReschedule() {
                return true;
            }

            @Override
            protected void runInternal() {
                assertTrue("generic threadpool is configured", Thread.currentThread().getName().contains("[generic]"));
                final CountDownLatch l1 = latch1.get();
                final CountDownLatch l2 = latch2.get();
                count.incrementAndGet();
                l1.countDown();
                try {
                    l2.await();
                } catch (InterruptedException e) {
                    fail("interrupted");
                }
                if (randomBoolean()) {
                    throw new RuntimeException("foo");
                }
            }

            @Override
            protected String getThreadPool() {
                return ThreadPool.Names.GENERIC;
            }
        };

        assertFalse(task.isScheduled());
        task.rescheduleIfNecessary();
        assertTrue(task.isScheduled());
        latch1.get().await();
        latch1.set(new CountDownLatch(1));
        assertEquals(1, count.get());
        // here we need to swap first before we let it go otherwise threads might be very fast and run that task twice due to
        // random exception and the schedule interval is 1ms
        latch2.getAndSet(new CountDownLatch(1)).countDown();
        latch1.get().await();
        assertEquals(2, count.get());
        assertTrue(task.isScheduled());
        task.close();
        assertTrue(task.isClosed());
        assertFalse(task.isScheduled());
        latch2.get().countDown();
        assertEquals(2, count.get());
    }

    public void testCloseWithNoRun() {

        AbstractAsyncTask task = new AbstractAsyncTask(logger, threadPool, TimeValue.timeValueMinutes(10)) {

            @Override
            protected boolean mustReschedule() {
                return true;
            }

            @Override
            protected void runInternal() {
            }
        };

        assertFalse(task.isScheduled());
        task.rescheduleIfNecessary();
        assertTrue(task.isScheduled());
        task.close();
        assertTrue(task.isClosed());
        assertFalse(task.isScheduled());
    }

    public void testChangeInterval() throws Exception {

        final CountDownLatch latch = new CountDownLatch(2);

        AbstractAsyncTask task = new AbstractAsyncTask(logger, threadPool, TimeValue.timeValueHours(1)) {

            @Override
            protected boolean mustReschedule() {
                return latch.getCount() > 0;
            }

            @Override
            protected void runInternal() {
                latch.countDown();
            }
        };

        assertFalse(task.isScheduled());
        task.rescheduleIfNecessary();
        assertTrue(task.isScheduled());
        task.setInterval(TimeValue.timeValueMillis(1));
        assertTrue(task.isScheduled());
        // This should only take 2 milliseconds in ideal conditions, but allow 10 seconds in case of VM stalls
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertBusy(() -> assertFalse(task.isScheduled()));
        task.close();
        assertFalse(task.isScheduled());
        assertTrue(task.isClosed());
    }
}
