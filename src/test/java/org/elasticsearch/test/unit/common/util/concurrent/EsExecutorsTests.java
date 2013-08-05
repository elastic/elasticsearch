/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.test.unit.common.util.concurrent;

import com.google.common.base.Predicate;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.ThreadBarrier;
import org.elasticsearch.test.integration.ElasticsearchTestCase;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;

/**
 */
public class EsExecutorsTests extends ElasticsearchTestCase {
    
    private TimeUnit randomTimeUnit() {
        return TimeUnit.values()[between(0, TimeUnit.values().length-1)];
    }

    @Test
    public void testScaleUp() throws Exception {
        final int min = between(1, 3);
        final int max = between(min+1, 6);
        final ThreadBarrier barrier = new ThreadBarrier(max + 1);

        ThreadPoolExecutor pool = EsExecutors.newScalingExecutorService(min, max, between(1, 100), randomTimeUnit(), EsExecutors.daemonThreadFactory("test"));
        assertThat("Min property", pool.getCorePoolSize(), equalTo(min));
        assertThat("Max property", pool.getMaximumPoolSize(), equalTo(max));

        for (int i = 0; i < max; ++i) {
            final CountDownLatch latch = new CountDownLatch(1);
            pool.execute(new Runnable() {
                public void run() {
                    latch.countDown();
                    try {
                        barrier.await();
                        barrier.await();
                    } catch (Throwable e) {
                        barrier.reset(e);
                    }
                }
            });

            //wait until thread executes this task
            //otherwise, a task might be queued
            latch.await();
        }

        barrier.await();
        assertThat("wrong pool size", pool.getPoolSize(), equalTo(max));
        assertThat("wrong active size", pool.getActiveCount(), equalTo(max));
        barrier.await();
        pool.shutdown();
    }

    @Test
    public void testScaleDown() throws Exception {
        final int min = between(1, 3);
        final int max = between(min+1, 6);
        final ThreadBarrier barrier = new ThreadBarrier(max + 1);

        final ThreadPoolExecutor pool = EsExecutors.newScalingExecutorService(min, max, between(1, 100), TimeUnit.MILLISECONDS, EsExecutors.daemonThreadFactory("test"));
        assertThat("Min property", pool.getCorePoolSize(), equalTo(min));
        assertThat("Max property", pool.getMaximumPoolSize(), equalTo(max));

        for (int i = 0; i < max; ++i) {
            final CountDownLatch latch = new CountDownLatch(1);
            pool.execute(new Runnable() {
                public void run() {
                    latch.countDown();
                    try {
                        barrier.await();
                        barrier.await();
                    } catch (Throwable e) {
                        barrier.reset(e);
                    }
                }
            });

            //wait until thread executes this task
            //otherwise, a task might be queued
            latch.await();
        }

        barrier.await();
        assertThat("wrong pool size", pool.getPoolSize(), equalTo(max));
        assertThat("wrong active size", pool.getActiveCount(), equalTo(max));
        barrier.await();
        awaitBusy(new Predicate<Object>() {
            public boolean apply(Object o) {
                return pool.getActiveCount() == 0 && pool.getPoolSize() < max;
            }
        });
        //assertThat("not all tasks completed", pool.getCompletedTaskCount(), equalTo((long) max));
        assertThat("wrong active count", pool.getActiveCount(), equalTo(0));
        //assertThat("wrong pool size. ", min, equalTo(pool.getPoolSize())); //BUG in ThreadPool - Bug ID: 6458662
        //assertThat("idle threads didn't stay above min (" + pool.getPoolSize() + ")", pool.getPoolSize(), greaterThan(0));
        assertThat("idle threads didn't shrink below max. (" + pool.getPoolSize() + ")", pool.getPoolSize(), lessThan(max));
        pool.shutdown();
    }


    @Test
    public void testBlocking() throws Exception {
        final int min = between(1, 3);
        final int max = between(min+1, 6);
        final long waitTime = between(1000, 2000); //1 second
        final ThreadBarrier barrier = new ThreadBarrier(max + 1);

        ThreadPoolExecutor pool = EsExecutors.newBlockingExecutorService(min, max, between(1, 100), randomTimeUnit(), EsExecutors.daemonThreadFactory("test"), 1, waitTime, TimeUnit.MILLISECONDS);
        assertThat("Min property", pool.getCorePoolSize(), equalTo(min));
        assertThat("Max property", pool.getMaximumPoolSize(), equalTo(max));
        for (int i = 0; i < max; ++i) {
            final CountDownLatch latch = new CountDownLatch(1);
            pool.execute(new Runnable() {
                public void run() {
                    latch.countDown();
                    try {
                        barrier.await();
                        barrier.await();
                    } catch (Throwable e) {
                        barrier.reset(e);
                    }
                }
            });
            //wait until thread executes this task
            //otherwise, a task might be queued
            latch.await();
        }

        barrier.await();
        assertThat("wrong pool size", pool.getPoolSize(), equalTo(max));
        assertThat("wrong active size", pool.getActiveCount(), equalTo(max));

        //Queue should be empty, lets occupy it's only free space
        assertThat("queue isn't empty", pool.getQueue().size(), equalTo(0));
        pool.execute(new Runnable() {
            public void run() {
                //dummy task
            }
        });
        assertThat("queue isn't full", pool.getQueue().size(), equalTo(1));

        //request should block since queue is full
        try {
            pool.execute(new Runnable() {
                public void run() {
                    //dummy task
                }
            });
            assertThat("Should have thrown RejectedExecutionException", false, equalTo(true));
        } catch (EsRejectedExecutionException e) {
            //caught expected exception
        }

        barrier.await();
        pool.shutdown();
    }
}
