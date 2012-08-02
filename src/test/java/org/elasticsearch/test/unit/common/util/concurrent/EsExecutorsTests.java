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

import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.ThreadBarrier;
import org.testng.annotations.Test;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;

/**
 */
@Test
public class EsExecutorsTests {

    @Test
    public void testScaleUp() throws Exception {
        final int min = 2;
        final int max = 4;
        final ThreadBarrier barrier = new ThreadBarrier(max + 1);

        ThreadPoolExecutor pool = EsExecutors.newScalingExecutorService(min, max, 100, TimeUnit.DAYS, EsExecutors.daemonThreadFactory("test"));
        assertThat("Min property", pool.getCorePoolSize(), equalTo(min));
        assertThat("Max property", pool.getMaximumPoolSize(), equalTo(max));

        for (int i = 0; i < max; ++i) {
            pool.execute(new Runnable() {
                public void run() {
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
            Thread.sleep(100);
        }

        barrier.await();
        assertThat("wrong pool size", pool.getPoolSize(), equalTo(max));
        assertThat("wrong active size", pool.getActiveCount(), equalTo(max));
        barrier.await();
        pool.shutdown();
    }

    @Test
    public void testScaleDown() throws Exception {
        final int min = 2;
        final int max = 4;
        final ThreadBarrier barrier = new ThreadBarrier(max + 1);

        ThreadPoolExecutor pool = EsExecutors.newScalingExecutorService(min, max, 10, TimeUnit.MILLISECONDS, EsExecutors.daemonThreadFactory("test"));
        assertThat("Min property", pool.getCorePoolSize(), equalTo(min));
        assertThat("Max property", pool.getMaximumPoolSize(), equalTo(max));

        for (int i = 0; i < max; ++i) {
            pool.execute(new Runnable() {
                public void run() {
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
            Thread.sleep(100);
        }

        barrier.await();
        assertThat("wrong pool size", pool.getPoolSize(), equalTo(max));
        assertThat("wrong active size", pool.getActiveCount(), equalTo(max));
        barrier.await();
        Thread.sleep(1000);

//        assertThat("not all tasks completed", pool.getCompletedTaskCount(), equalTo((long) max));
        assertThat("wrong active count", pool.getActiveCount(), equalTo(0));
        //Assert.assertEquals("wrong pool size. ", min, pool.getPoolSize()); //BUG in ThreadPool - Bug ID: 6458662
        //assertThat("idle threads didn't stay above min (" + pool.getPoolSize() + ")", pool.getPoolSize(), greaterThan(0));
        assertThat("idle threads didn't shrink below max. (" + pool.getPoolSize() + ")", pool.getPoolSize(), lessThan(max));
        pool.shutdown();
    }


    @Test
    public void testBlocking() throws Exception {
        final int min = 2;
        final int max = 4;
        final long waitTime = 1000; //1 second
        final ThreadBarrier barrier = new ThreadBarrier(max + 1);

        ThreadPoolExecutor pool = EsExecutors.newBlockingExecutorService(min, max, 60, TimeUnit.SECONDS, EsExecutors.daemonThreadFactory("test"), 1, waitTime, TimeUnit.MILLISECONDS);
        assertThat("Min property", pool.getCorePoolSize(), equalTo(min));
        assertThat("Max property", pool.getMaximumPoolSize(), equalTo(max));

        for (int i = 0; i < max; ++i) {
            pool.execute(new Runnable() {
                public void run() {
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
            Thread.sleep(100);
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
