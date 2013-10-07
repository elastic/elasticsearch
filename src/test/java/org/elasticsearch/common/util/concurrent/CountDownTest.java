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

package org.elasticsearch.common.util.concurrent;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;


public class CountDownTest extends ElasticsearchTestCase {

    @Test @Repeat(iterations = 1000)
    public void testConcurrent() throws InterruptedException {
        final AtomicInteger count = new AtomicInteger(0);
        final CountDown countDown = new CountDown(atLeast(10));
        Thread[] threads = new Thread[atLeast(3)];
        final CountDownLatch latch = new CountDownLatch(1);
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread() {

                public void run() {
                    try {
                        latch.await();
                    } catch (InterruptedException e) {
                        throw new RuntimeException();
                    }
                    while (true) {
                        if(frequently()) {
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
    
    @Test
    public void testSingleThreaded() {
        int atLeast = atLeast(10);
        final CountDown countDown = new CountDown(atLeast);
        while(!countDown.isCountedDown()) {
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
