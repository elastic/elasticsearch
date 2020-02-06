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

import org.elasticsearch.common.Nullable;
import org.elasticsearch.test.ESTestCase;

import java.lang.management.ThreadInfo;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class LongGCDisruptionTests extends ESTestCase {

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
            protected long getSuspendingTimeoutInMillis() {
                return 100;
            }
        };
        final AtomicBoolean stop = new AtomicBoolean();
        final CountDownLatch underLock = new CountDownLatch(1);
        final CountDownLatch pauseUnderLock = new CountDownLatch(1);
        final LockedExecutor lockedExecutor = new LockedExecutor();
        final AtomicLong ops = new AtomicLong();
        final Thread[] threads = new Thread[10];
        try {
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
            assertThat(e.getMessage(), containsString("suspending node threads took too long"));
        } finally {
            stop.set(true);
            pauseUnderLock.countDown();
            for (final Thread thread : threads) {
                thread.join();
            }
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
        final Thread[] threads = new Thread[5];
        final Runnable yieldAndIncrement = () -> {
            Thread.yield(); // give some chance to catch this stack trace
            ops.incrementAndGet();
        };
        try {
            for (int i = 0; i < threads.length; i++) {
                threads[i] = new Thread(() -> {
                    for (int iter = 0; stop.get() == false; iter++) {
                        if (iter % 2 == 0) {
                            lockedExecutor.executeLocked(yieldAndIncrement);
                        } else {
                            yieldAndIncrement.run();
                        }
                    }
                });
                threads[i].setName("[" + nodeName + "][" + i + "]");
                threads[i].start();
            }
            // make sure some threads are under lock
            try {
                disruption.startDisrupting();
            } catch (RuntimeException e) {
                if (e.getMessage().contains("suspending node threads took too long") && disruption.sawSlowSuspendBug()) {
                    return;
                }
                throw new AssertionError(e);
            }
            long first = ops.get();
            assertThat(lockedExecutor.lock.isLocked(), equalTo(false)); // no threads should own the lock
            Thread.sleep(100);
            assertThat(ops.get(), equalTo(first));
            disruption.stopDisrupting();
            assertBusy(() -> assertThat(ops.get(), greaterThan(first)));
        } finally {
            disruption.stopDisrupting();
            stop.set(true);
            for (final Thread thread : threads) {
                thread.join();
            }
        }
    }

    public void testBlockDetection() throws Exception {
        final String disruptedNodeName = "disrupted_node";
        final String blockedNodeName = "blocked_node";
        CountDownLatch waitForBlockDetectionResult = new CountDownLatch(1);
        AtomicReference<ThreadInfo> blockDetectionResult = new AtomicReference<>();
        LongGCDisruption disruption = new LongGCDisruption(random(), disruptedNodeName) {
            @Override
            protected Pattern[] getUnsafeClasses() {
                return new Pattern[0];
            }

            @Override
            protected void onBlockDetected(ThreadInfo blockedThread, @Nullable ThreadInfo blockingThread) {
                blockDetectionResult.set(blockedThread);
                waitForBlockDetectionResult.countDown();
            }

            @Override
            protected long getBlockDetectionIntervalInMillis() {
                return 10L;
            }
        };
        if (disruption.isBlockDetectionSupported() == false) {
            return;
        }
        final AtomicBoolean stop = new AtomicBoolean();
        final CountDownLatch underLock = new CountDownLatch(1);
        final CountDownLatch pauseUnderLock = new CountDownLatch(1);
        final LockedExecutor lockedExecutor = new LockedExecutor();
        final AtomicLong ops = new AtomicLong();
        final List<Thread> threads = new ArrayList<>();
        try {
            for (int i = 0; i < 5; i++) {
                // at least one locked and one none lock thread
                final boolean lockedExec = (i < 4 && randomBoolean()) || i == 0;
                Thread thread = new Thread(() -> {
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

                thread.setName("[" + disruptedNodeName + "][" + i + "]");
                threads.add(thread);
                thread.start();
            }

            for (int i = 0; i < 5; i++) {
                // at least one locked and one none lock thread
                final boolean lockedExec = (i < 4 && randomBoolean()) || i == 0;
                Thread thread = new Thread(() -> {
                    while (stop.get() == false) {
                        if (lockedExec) {
                            lockedExecutor.executeLocked(() -> {
                                ops.incrementAndGet();
                            });
                        } else {
                            ops.incrementAndGet();
                        }
                    }
                });
                thread.setName("[" + blockedNodeName + "][" + i + "]");
                threads.add(thread);
                thread.start();
            }
            // make sure some threads of test_node are under lock
            underLock.await();
            disruption.startDisrupting();
            assertTrue(waitForBlockDetectionResult.await(30, TimeUnit.SECONDS));
            disruption.stopDisrupting();

            ThreadInfo threadInfo = blockDetectionResult.get();
            assertNotNull(threadInfo);
            assertThat(threadInfo.getThreadName(), containsString("[" + blockedNodeName + "]"));
            assertThat(threadInfo.getLockOwnerName(), containsString("[" + disruptedNodeName + "]"));
            assertThat(threadInfo.getLockInfo().getClassName(), containsString(ReentrantLock.class.getName()));
        } finally {
            stop.set(true);
            pauseUnderLock.countDown();
            for (final Thread thread : threads) {
                thread.join();
            }
        }
    }
}
