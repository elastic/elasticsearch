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

package org.elasticsearch.threadpool;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;

import java.util.concurrent.CountDownLatch;

import static org.hamcrest.CoreMatchers.equalTo;

public class FixedThreadPoolTests extends ESThreadPoolTestCase {

    public void testRejectedExecutionCounter() throws InterruptedException {
        final String threadPoolName = randomThreadPool(ThreadPool.ThreadPoolType.FIXED);
        // some of the fixed thread pool are bound by the number of
        // cores so we can not exceed that
        final int size = randomIntBetween(1, EsExecutors.allocatedProcessors(Settings.EMPTY));
        final int queueSize = randomIntBetween(1, 16);
        final long rejections = randomIntBetween(1, 16);

        ThreadPool threadPool = null;
        final Settings nodeSettings =
            Settings.builder()
                .put("node.name", "testRejectedExecutionCounter")
                .put("thread_pool." + threadPoolName + ".size", size)
                .put("thread_pool." + threadPoolName + ".queue_size", queueSize)
                .build();
        try {
            threadPool = new ThreadPool(nodeSettings);

            // these tasks will consume the thread pool causing further
            // submissions to queue
            final CountDownLatch latch = new CountDownLatch(size);
            final CountDownLatch block = new CountDownLatch(1);
            for (int i = 0; i < size; i++) {
                threadPool.executor(threadPoolName).execute(() -> {
                    try {
                        latch.countDown();
                        block.await();
                    } catch (InterruptedException e) {
                        fail(e.toString());
                    }
                });
            }

            // wait for the submitted tasks to be consumed by the thread
            // pool
            latch.await();

            // these tasks will fill the thread pool queue
            for (int i = 0; i < queueSize; i++) {
                threadPool.executor(threadPoolName).execute(() -> {});
            }

            // these tasks will be rejected
            long counter = 0;
            for (int i = 0; i < rejections; i++) {
                try {
                    threadPool.executor(threadPoolName).execute(() -> {});
                } catch (EsRejectedExecutionException e) {
                    counter++;
                }
            }

            block.countDown();

            assertThat(counter, equalTo(rejections));
            assertThat(stats(threadPool, threadPoolName).getRejected(), equalTo(rejections));
        } finally {
            terminateThreadPoolIfNeeded(threadPool);
        }
    }

}
