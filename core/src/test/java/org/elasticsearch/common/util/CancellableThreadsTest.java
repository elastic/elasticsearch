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
package org.elasticsearch.common.util;

import org.elasticsearch.common.util.CancellableThreads.Interruptable;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

public class CancellableThreadsTest extends ElasticsearchTestCase {

    private static class CustomException extends RuntimeException {

        public CustomException(String msg) {
            super(msg);
        }
    }

    private class TestPlan {
        public final int id;
        public final boolean busySpin;
        public final boolean exceptBeforeCancel;
        public final boolean exitBeforeCancel;
        public final boolean exceptAfterCancel;
        public final boolean presetInterrupt;

        private TestPlan(int id) {
            this.id = id;
            this.busySpin = randomBoolean();
            this.exceptBeforeCancel = randomBoolean();
            this.exitBeforeCancel = randomBoolean();
            this.exceptAfterCancel = randomBoolean();
            this.presetInterrupt = randomBoolean();
        }
    }


    @Test
    public void testCancellableThreads() throws InterruptedException {
        Thread[] threads = new Thread[randomIntBetween(3, 10)];
        final TestPlan[] plans = new TestPlan[threads.length];
        final Throwable[] throwables = new Throwable[threads.length];
        final boolean[] interrupted = new boolean[threads.length];
        final CancellableThreads cancellableThreads = new CancellableThreads();
        final CountDownLatch readyForCancel = new CountDownLatch(threads.length);
        for (int i = 0; i < threads.length; i++) {
            final TestPlan plan = new TestPlan(i);
            plans[i] = plan;
            threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        if (plan.presetInterrupt) {
                            Thread.currentThread().interrupt();
                        }
                        cancellableThreads.execute(new Interruptable() {
                            @Override
                            public void run() throws InterruptedException {
                                assertFalse("interrupt thread should have been clear", Thread.currentThread().isInterrupted());
                                if (plan.exceptBeforeCancel) {
                                    throw new CustomException("thread [" + plan.id + "] pre-cancel exception");
                                } else if (plan.exitBeforeCancel) {
                                    return;
                                }
                                readyForCancel.countDown();
                                try {
                                    if (plan.busySpin) {
                                        while (!Thread.currentThread().isInterrupted()) {
                                        }
                                    } else {
                                        Thread.sleep(50000);
                                    }
                                } finally {
                                    if (plan.exceptAfterCancel) {
                                        throw new CustomException("thread [" + plan.id + "] post-cancel exception");
                                    }
                                }
                            }
                        });
                    } catch (Throwable t) {
                        throwables[plan.id] = t;
                    }
                    if (plan.exceptBeforeCancel || plan.exitBeforeCancel) {
                        // we have to mark we're ready now (actually done).
                        readyForCancel.countDown();
                    }
                    interrupted[plan.id] = Thread.currentThread().isInterrupted();

                }
            });
            threads[i].setDaemon(true);
            threads[i].start();
        }

        readyForCancel.await();
        cancellableThreads.cancel("test");
        for (Thread thread : threads) {
            thread.join(20000);
            assertFalse(thread.isAlive());
        }
        for (int i = 0; i < threads.length; i++) {
            TestPlan plan = plans[i];
            if (plan.exceptBeforeCancel) {
                assertThat(throwables[i], Matchers.instanceOf(CustomException.class));
            } else if (plan.exitBeforeCancel) {
                assertNull(throwables[i]);
            } else {
                // in all other cases, we expect a cancellation exception.
                assertThat(throwables[i], Matchers.instanceOf(CancellableThreads.ExecutionCancelledException.class));
                if (plan.exceptAfterCancel) {
                    assertThat(throwables[i].getSuppressed(),
                            Matchers.arrayContaining(
                                    Matchers.instanceOf(CustomException.class)
                            ));
                } else {
                    assertThat(throwables[i].getSuppressed(), Matchers.emptyArray());
                }
            }
            assertThat(interrupted[plan.id], Matchers.equalTo(plan.presetInterrupt));
        }
    }

}
