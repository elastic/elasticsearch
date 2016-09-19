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
package org.elasticsearch.test.disruption;

import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class LongGCDisruptionTest extends ESTestCase {

    static class LockedExecutor {
        ReentrantLock lock = new ReentrantLock();

        public void executeLocked(Runnable r) {
            lock.lock();
            try {
                r.run();
            } finally {
                lock.unlock();
            }
        }
    }

    public void testBlockingTimeout() throws Exception {
        final String nodeName = "test_node";
        LongGCDisruption disruption = new LongGCDisruption(random(), nodeName) {
            @Override
            protected Pattern[] getUnsafeClasses() {
                return new Pattern[]{
                    Pattern.compile(LockedExecutor.class.getSimpleName())
                };
            }

            @Override
            protected long getStoppingTimeoutInMillis() {
                return 100;
            }
        };
        final AtomicBoolean stop = new AtomicBoolean();
        final CountDownLatch underLock = new CountDownLatch(1);
        final CountDownLatch pauseUnderLock = new CountDownLatch(1);
        final LockedExecutor lockedExecutor = new LockedExecutor();
        final AtomicLong ops = new AtomicLong();
        try {
            Thread[] threads = new Thread[10];
            for (int i = 0; i < 10; i++) {
                // at least one locked and one none lock thread
                final boolean lockedExec = (i < 9 && randomBoolean()) || i == 0;
                threads[i] = new Thread(() -> {
                    while (stop.get() == false) {
                        if (lockedExec) {
                            lockedExecutor.executeLocked(() -> {
                                try {
                                    underLock.countDown();
                                    ops.incrementAndGet();
                                    pauseUnderLock.await();
                                } catch (InterruptedException e) {

                                }
                            });
                        } else {
                            ops.incrementAndGet();
                        }
                    }
                });
                threads[i].setName("[" + nodeName + "][" + i + "]");
                threads[i].start();
            }
            // make sure some threads are under lock
            underLock.await();
            RuntimeException e = expectThrows(RuntimeException.class, disruption::startDisrupting);
            assertThat(e.getMessage(), containsString("stopping node threads took too long"));
        } finally {
            stop.set(true);
            pauseUnderLock.countDown();
        }
    }

    /**
     * Checks that a GC disruption never blocks threads while they are doing something "unsafe"
     * but does keep retrying until all threads can be safely paused
     */
    public void testNotBlockingUnsafeStackTraces() throws Exception {
        final String nodeName = "test_node";
        LongGCDisruption disruption = new LongGCDisruption(random(), nodeName) {
            @Override
            protected Pattern[] getUnsafeClasses() {
                return new Pattern[]{
                    Pattern.compile(LockedExecutor.class.getSimpleName())
                };
            }
        };
        final AtomicBoolean stop = new AtomicBoolean();
        final LockedExecutor lockedExecutor = new LockedExecutor();
        final AtomicLong ops = new AtomicLong();
        try {
            Thread[] threads = new Thread[10];
            for (int i = 0; i < 10; i++) {
                threads[i] = new Thread(() -> {
                    for (int iter = 0; stop.get() == false; iter++) {
                        if (iter % 2 == 0) {
                            lockedExecutor.executeLocked(() -> {
                                Thread.yield(); // give some chance to catch this stack trace
                                ops.incrementAndGet();
                            });
                        } else {
                            Thread.yield(); // give some chance to catch this stack trace
                            ops.incrementAndGet();
                        }
                    }
                });
                threads[i].setName("[" + nodeName + "][" + i + "]");
                threads[i].start();
            }
            // make sure some threads are under lock
            disruption.startDisrupting();
            long first = ops.get();
            assertThat(lockedExecutor.lock.isLocked(), equalTo(false)); // no threads should own the lock
            Thread.sleep(100);
            assertThat(ops.get(), equalTo(first));
            disruption.stopDisrupting();
            assertBusy(() -> assertThat(ops.get(), greaterThan(first)));
        } finally {
            stop.set(true);
        }
    }
}
