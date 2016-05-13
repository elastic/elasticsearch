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

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;

public class ScalingThreadPoolTests extends ESThreadPoolTestCase {

    public void testScalingThreadPoolConfiguration() throws InterruptedException {
        final String threadPoolName = randomThreadPool(ThreadPool.ThreadPoolType.SCALING);
        final Settings.Builder builder = Settings.builder();

        final int min;
        if (randomBoolean()) {
            min = randomIntBetween(0, 8);
            builder.put("threadpool." + threadPoolName + ".min", min);
        } else {
            min = "generic".equals(threadPoolName) ? 4 : 1; // the defaults
        }

        final int sizeBasedOnNumberOfProcessors;
        if (randomBoolean()) {
            final int processors = randomIntBetween(1, 64);
            sizeBasedOnNumberOfProcessors = expectedSize(threadPoolName, processors);
            builder.put("processors", processors);
        } else {
            sizeBasedOnNumberOfProcessors = expectedSize(threadPoolName, Math.min(32, Runtime.getRuntime().availableProcessors()));
        }

        final int expectedSize;
        if (sizeBasedOnNumberOfProcessors < min || randomBoolean()) {
            expectedSize = randomIntBetween(min, 16);
            builder.put("threadpool." + threadPoolName + ".size", expectedSize);
        }  else {
            expectedSize = sizeBasedOnNumberOfProcessors;
        }

        final long keepAlive;
        if (randomBoolean()) {
            keepAlive = randomIntBetween(1, 300);
            builder.put("threadpool." + threadPoolName + ".keep_alive", keepAlive + "s");
        } else {
            keepAlive = "generic".equals(threadPoolName) ? 30 : 300; // the defaults
        }

        runScalingThreadPoolTest(builder.build(), (clusterSettings, threadPool) -> {
            final Executor executor = threadPool.executor(threadPoolName);
            assertThat(executor, instanceOf(EsThreadPoolExecutor.class));
            final EsThreadPoolExecutor esThreadPoolExecutor = (EsThreadPoolExecutor)executor;
            final ThreadPool.Info info = info(threadPool, threadPoolName);

            assertThat(info.getName(), equalTo(threadPoolName));
            assertThat(info.getThreadPoolType(), equalTo(ThreadPool.ThreadPoolType.SCALING));

            assertThat(info.getKeepAlive().seconds(), equalTo(keepAlive));
            assertThat(esThreadPoolExecutor.getKeepAliveTime(TimeUnit.SECONDS), equalTo(keepAlive));

            assertNull(info.getQueueSize());
            assertThat(esThreadPoolExecutor.getQueue().remainingCapacity(), equalTo(Integer.MAX_VALUE));

            assertThat(info.getMin(), equalTo(min));
            assertThat(esThreadPoolExecutor.getCorePoolSize(), equalTo(min));
            assertThat(info.getMax(), equalTo(expectedSize));
            assertThat(esThreadPoolExecutor.getMaximumPoolSize(), equalTo(expectedSize));
        });
    }

    @FunctionalInterface
    private interface SizeFunction {
        int size(int numberOfProcessors);
    }

    private int expectedSize(final String threadPoolName, final int numberOfProcessors) {
        final Map<String, SizeFunction> sizes = new HashMap<>();
        sizes.put(ThreadPool.Names.GENERIC, n -> ThreadPool.boundedBy(4 * n, 128, 512));
        sizes.put(ThreadPool.Names.MANAGEMENT, n -> 5);
        sizes.put(ThreadPool.Names.FLUSH, ThreadPool::halfNumberOfProcessorsMaxFive);
        sizes.put(ThreadPool.Names.REFRESH, ThreadPool::halfNumberOfProcessorsMaxTen);
        sizes.put(ThreadPool.Names.WARMER, ThreadPool::halfNumberOfProcessorsMaxFive);
        sizes.put(ThreadPool.Names.SNAPSHOT, ThreadPool::halfNumberOfProcessorsMaxFive);
        sizes.put(ThreadPool.Names.FETCH_SHARD_STARTED, ThreadPool::twiceNumberOfProcessors);
        sizes.put(ThreadPool.Names.FETCH_SHARD_STORE, ThreadPool::twiceNumberOfProcessors);
        return sizes.get(threadPoolName).size(numberOfProcessors);
    }

    public void testValidDynamicKeepAlive() throws InterruptedException {
        final String threadPoolName = randomThreadPool(ThreadPool.ThreadPoolType.SCALING);
        runScalingThreadPoolTest(Settings.EMPTY, (clusterSettings, threadPool) -> {
            final Executor beforeExecutor = threadPool.executor(threadPoolName);
            final long seconds = randomIntBetween(1, 300);
            clusterSettings.applySettings(settings("threadpool." + threadPoolName + ".keep_alive", seconds + "s"));
            final Executor afterExecutor = threadPool.executor(threadPoolName);
            assertSame(beforeExecutor, afterExecutor);
            final ThreadPool.Info info = info(threadPool, threadPoolName);
            assertThat(info.getKeepAlive().seconds(), equalTo(seconds));
        });
    }

    public void testScalingThreadPoolIsBounded() throws InterruptedException {
        final String threadPoolName = randomThreadPool(ThreadPool.ThreadPoolType.SCALING);
        final int size = randomIntBetween(32, 512);
        final Settings settings = Settings.builder().put("threadpool." + threadPoolName + ".size", size).build();
        runScalingThreadPoolTest(settings, (clusterSettings, threadPool) -> {
            final CountDownLatch latch = new CountDownLatch(1);
            final int numberOfTasks = 2 * size;
            final CountDownLatch taskLatch = new CountDownLatch(numberOfTasks);
            for (int i = 0; i < numberOfTasks; i++) {
                threadPool.executor(threadPoolName).execute(() -> {
                    try {
                        latch.await();
                        taskLatch.countDown();
                    } catch (final InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });
            }
            final ThreadPoolStats.Stats stats = stats(threadPool, threadPoolName);
            assertThat(stats.getQueue(), equalTo(numberOfTasks - size));
            assertThat(stats.getLargest(), equalTo(size));
            latch.countDown();
            try {
                taskLatch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public void testScalingThreadPoolThreadsAreTerminatedAfterKeepAlive() throws InterruptedException {
        final String threadPoolName = randomThreadPool(ThreadPool.ThreadPoolType.SCALING);
        final int min = "generic".equals(threadPoolName) ? 4 : 1;
        final Settings settings =
                Settings.builder()
                        .put("threadpool." + threadPoolName + ".size", 128)
                        .put("threadpool." + threadPoolName + ".keep_alive", "1ms")
                        .build();
        runScalingThreadPoolTest(settings, ((clusterSettings, threadPool) -> {
            final CountDownLatch latch = new CountDownLatch(1);
            final CountDownLatch taskLatch = new CountDownLatch(128);
            for (int i = 0; i < 128; i++) {
                threadPool.executor(threadPoolName).execute(() -> {
                    try {
                        latch.await();
                        taskLatch.countDown();
                    } catch (final InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });
            }
            assertThat(stats(threadPool, threadPoolName).getThreads(), equalTo(128));
            latch.countDown();
            // this while loop is the core of this test; if threads
            // are correctly idled down by the pool, the number of
            // threads in the pool will drop to the min for the pool
            // but if threads are not correctly idled down by the pool,
            // this test will just timeout waiting for them to idle
            // down
            do {
                spinForAtLeastOneMillisecond();
            } while (stats(threadPool, threadPoolName).getThreads() > min);
            try {
                taskLatch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }));
    }

    public void testDynamicThreadPoolSize() throws InterruptedException {
        final String threadPoolName = randomThreadPool(ThreadPool.ThreadPoolType.SCALING);
        runScalingThreadPoolTest(Settings.EMPTY, (clusterSettings, threadPool) -> {
            final Executor beforeExecutor = threadPool.executor(threadPoolName);
            int expectedMin = "generic".equals(threadPoolName) ? 4 : 1;
            final int size = randomIntBetween(expectedMin, Integer.MAX_VALUE);
            clusterSettings.applySettings(settings("threadpool." + threadPoolName + ".size", size));
            final Executor afterExecutor = threadPool.executor(threadPoolName);
            assertSame(beforeExecutor, afterExecutor);
            final ThreadPool.Info info = info(threadPool, threadPoolName);
            assertThat(info.getMin(), equalTo(expectedMin));
            assertThat(info.getMax(), equalTo(size));

            assertThat(afterExecutor, instanceOf(EsThreadPoolExecutor.class));
            final EsThreadPoolExecutor executor = (EsThreadPoolExecutor)afterExecutor;
            assertThat(executor.getCorePoolSize(), equalTo(expectedMin));
            assertThat(executor.getMaximumPoolSize(), equalTo(size));
        });
    }

    public void testResizingScalingThreadPoolQueue() throws InterruptedException {
        final String threadPoolName = randomThreadPool(ThreadPool.ThreadPoolType.SCALING);
        runScalingThreadPoolTest(Settings.EMPTY, (clusterSettings, threadPool) -> {
            final int size = randomIntBetween(1, Integer.MAX_VALUE);
            final IllegalArgumentException e = expectThrows(
                    IllegalArgumentException.class,
                    () -> clusterSettings.applySettings(settings("threadpool." + threadPoolName + ".queue_size", size)));
            assertThat(e, hasToString(
                    "java.lang.IllegalArgumentException: thread pool [" + threadPoolName +
                        "] of type scaling can not have its queue re-sized but was [" +
                            size + "]"));
        });
    }

    public void runScalingThreadPoolTest(
            final Settings settings,
            final BiConsumer<ClusterSettings, ThreadPool> consumer) throws InterruptedException {
        ThreadPool threadPool = null;
        try {
            final String test = Thread.currentThread().getStackTrace()[2].getMethodName();
            final Settings nodeSettings = Settings.builder().put(settings).put("node.name", test).build();
            threadPool = new ThreadPool(nodeSettings);
            final ClusterSettings clusterSettings = new ClusterSettings(nodeSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
            threadPool.setClusterSettings(clusterSettings);
            consumer.accept(clusterSettings, threadPool);
        } finally {
            terminateThreadPoolIfNeeded(threadPool);
        }
    }

    private static Settings settings(final String setting, final int value) {
        return settings(setting, Integer.toString(value));
    }

    private static Settings settings(final String setting, final String value) {
        return Settings.builder().put(setting, value).build();
    }

}
