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
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;
import org.elasticsearch.threadpool.ThreadPool.Names;

import java.lang.reflect.Field;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class UpdateThreadPoolSettingsTests extends ESThreadPoolTestCase {

    public void testCorrectThreadPoolTypePermittedInSettings() throws InterruptedException {
        String threadPoolName = randomThreadPoolName();
        ThreadPool.ThreadPoolType correctThreadPoolType = ThreadPool.THREAD_POOL_TYPES.get(threadPoolName);
        ThreadPool threadPool = null;
        try {
            threadPool = new ThreadPool(Settings.builder()
                .put("node.name", "testCorrectThreadPoolTypePermittedInSettings")
                .put("thread_pool." + threadPoolName + ".type", correctThreadPoolType.getType())
                .build());
            ThreadPool.Info info = info(threadPool, threadPoolName);
            if (ThreadPool.Names.SAME.equals(threadPoolName)) {
                assertNull(info); // we don't report on the "same" thread pool
            } else {
                // otherwise check we have the expected type
                assertEquals(info.getThreadPoolType(), correctThreadPoolType);
            }
        } finally {
            terminateThreadPoolIfNeeded(threadPool);
        }
    }

    public void testIndexingThreadPoolsMaxSize() throws InterruptedException {
        final String name = randomFrom(Names.BULK, Names.INDEX);
        final int maxSize = 1 + EsExecutors.numberOfProcessors(Settings.EMPTY);
        final int tooBig = randomIntBetween(1 + maxSize, Integer.MAX_VALUE);

        // try to create a too big thread pool
        final IllegalArgumentException initial =
            expectThrows(
                IllegalArgumentException.class,
                () -> {
                    ThreadPool tp = null;
                    try {
                        tp = new ThreadPool(Settings.builder()
                            .put("node.name", "testIndexingThreadPoolsMaxSize")
                            .put("thread_pool." + name + ".size", tooBig)
                            .build());
                    } finally {
                        terminateThreadPoolIfNeeded(tp);
                    }
                });

        assertThat(
            initial,
            hasToString(containsString(
                "Failed to parse value [" + tooBig + "] for setting [thread_pool." + name + ".size] must be ")));
    }

    private static int getExpectedThreadPoolSize(Settings settings, String name, int size) {
        if (name.equals(ThreadPool.Names.BULK) || name.equals(ThreadPool.Names.INDEX)) {
            return Math.min(size, EsExecutors.numberOfProcessors(settings));
        } else {
            return size;
        }
    }

    public void testFixedExecutorType() throws InterruptedException {
        String threadPoolName = randomThreadPool(ThreadPool.ThreadPoolType.FIXED);
        ThreadPool threadPool = null;

        try {
            int expectedSize = getExpectedThreadPoolSize(Settings.EMPTY, threadPoolName, 15);
            Settings nodeSettings = Settings.builder()
                .put("node.name", "testFixedExecutorType")
                .put("thread_pool." + threadPoolName + ".size", expectedSize)
                .build();
            threadPool = new ThreadPool(nodeSettings);
            assertThat(threadPool.executor(threadPoolName), instanceOf(EsThreadPoolExecutor.class));

            assertEquals(info(threadPool, threadPoolName).getThreadPoolType(), ThreadPool.ThreadPoolType.FIXED);
            assertThat(threadPool.executor(threadPoolName), instanceOf(EsThreadPoolExecutor.class));
            assertThat(((EsThreadPoolExecutor) threadPool.executor(threadPoolName)).getCorePoolSize(), equalTo(expectedSize));
            assertThat(((EsThreadPoolExecutor) threadPool.executor(threadPoolName)).getMaximumPoolSize(), equalTo(expectedSize));
            assertThat(info(threadPool, threadPoolName).getMin(), equalTo(expectedSize));
            assertThat(info(threadPool, threadPoolName).getMax(), equalTo(expectedSize));
            // keep alive does not apply to fixed thread pools
            assertThat(((EsThreadPoolExecutor) threadPool.executor(threadPoolName)).getKeepAliveTime(TimeUnit.MINUTES), equalTo(0L));
        } finally {
            terminateThreadPoolIfNeeded(threadPool);
        }
    }

    public void testScalingExecutorType() throws InterruptedException {
        String threadPoolName = randomThreadPool(ThreadPool.ThreadPoolType.SCALING);
        ThreadPool threadPool = null;
        try {
            Settings nodeSettings = Settings.builder()
                .put("thread_pool." + threadPoolName + ".max", 10)
                .put("node.name", "testScalingExecutorType")
                .build();
            threadPool = new ThreadPool(nodeSettings);
            final int expectedMinimum = "generic".equals(threadPoolName) ? 4 : 1;
            assertThat(info(threadPool, threadPoolName).getMin(), equalTo(expectedMinimum));
            assertThat(info(threadPool, threadPoolName).getMax(), equalTo(10));
            final long expectedKeepAlive = "generic".equals(threadPoolName) ? 30 : 300;
            assertThat(info(threadPool, threadPoolName).getKeepAlive().seconds(), equalTo(expectedKeepAlive));
            assertEquals(info(threadPool, threadPoolName).getThreadPoolType(), ThreadPool.ThreadPoolType.SCALING);
            assertThat(threadPool.executor(threadPoolName), instanceOf(EsThreadPoolExecutor.class));
        } finally {
            terminateThreadPoolIfNeeded(threadPool);
        }
    }

    public void testShutdownNowInterrupts() throws Exception {
        String threadPoolName = randomThreadPool(ThreadPool.ThreadPoolType.FIXED);
        ThreadPool threadPool = null;
        try {
            Settings nodeSettings = Settings.builder()
                .put("thread_pool." + threadPoolName + ".queue_size", 1000)
                .put("node.name", "testShutdownNowInterrupts")
                .build();
            threadPool = new ThreadPool(nodeSettings);
            assertEquals(info(threadPool, threadPoolName).getQueueSize().getSingles(), 1000L);

            final CountDownLatch shutDownLatch = new CountDownLatch(1);
            final CountDownLatch latch = new CountDownLatch(1);
            ThreadPoolExecutor oldExecutor = (ThreadPoolExecutor) threadPool.executor(threadPoolName);
            threadPool.executor(threadPoolName).execute(() -> {
                        try {
                            shutDownLatch.countDown();
                            new CountDownLatch(1).await();
                        } catch (InterruptedException ex) {
                            latch.countDown();
                            Thread.currentThread().interrupt();
                        }
                    }
            );
            shutDownLatch.await();
            threadPool.shutdownNow();
            latch.await(3, TimeUnit.SECONDS); // if this throws then ThreadPool#shutdownNow did not interrupt
            assertThat(oldExecutor.isShutdown(), equalTo(true));
            assertThat(oldExecutor.isTerminating() || oldExecutor.isTerminated(), equalTo(true));
        } finally {
            terminateThreadPoolIfNeeded(threadPool);
        }
    }

    public void testCustomThreadPool() throws Exception {
        ThreadPool threadPool = null;
        try {


            final ScalingExecutorBuilder scaling =
                new ScalingExecutorBuilder(
                    "my_pool1",
                    1,
                    EsExecutors.numberOfProcessors(Settings.EMPTY),
                    TimeValue.timeValueMinutes(1));

            final FixedExecutorBuilder fixed = new FixedExecutorBuilder(Settings.EMPTY, "my_pool2", 1, 1);

            threadPool = new ThreadPool(Settings.builder().put("node.name", "testCustomThreadPool").build(), scaling, fixed);

            ThreadPoolInfo groups = threadPool.info();
            boolean foundPool1 = false;
            boolean foundPool2 = false;
            outer:
            for (ThreadPool.Info info : groups) {
                if ("my_pool1".equals(info.getName())) {
                    foundPool1 = true;
                    assertEquals(info.getThreadPoolType(), ThreadPool.ThreadPoolType.SCALING);
                } else if ("my_pool2".equals(info.getName())) {
                    foundPool2 = true;
                    assertEquals(info.getThreadPoolType(), ThreadPool.ThreadPoolType.FIXED);
                    assertThat(info.getMin(), equalTo(1));
                    assertThat(info.getMax(), equalTo(1));
                    assertThat(info.getQueueSize().singles(), equalTo(1L));
                } else {
                    for (Field field : Names.class.getFields()) {
                        if (info.getName().equalsIgnoreCase(field.getName())) {
                            // This is ok it is a default thread pool
                            continue outer;
                        }
                    }
                    fail("Unexpected pool name: " + info.getName());
                }
            }
            assertThat(foundPool1, is(true));
            assertThat(foundPool2, is(true));
        } finally {
            terminateThreadPoolIfNeeded(threadPool);
        }
    }

    private String randomThreadPoolName() {
        Set<String> threadPoolNames = ThreadPool.THREAD_POOL_TYPES.keySet();
        return randomFrom(threadPoolNames.toArray(new String[threadPoolNames.size()]));
    }

}
