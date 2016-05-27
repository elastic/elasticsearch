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
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;
import org.elasticsearch.threadpool.ThreadPool.Names;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

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
            threadPool.start();
            ThreadPool.Info info = info(threadPool, threadPoolName);
            if (ThreadPool.Names.SAME.equals(threadPoolName)) {
                assertNull(info); // we don't report on the "same" threadpool
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
        ThreadPool threadPool = null;
        try {
            final int maxSize = EsExecutors.boundedNumberOfProcessors(Settings.EMPTY);
            final int tooBig = randomIntBetween(1 + maxSize, Integer.MAX_VALUE);

            // try to create a too-big (maxSize+1) thread pool
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
                            tp.start();
                        } finally {
                            terminateThreadPoolIfNeeded(tp);
                        }
                    });

            assertThat(
                initial,
                hasToString(containsString(
                    "Failed to parse value [" + tooBig + "] for setting [thread_pool." + name + ".size] must be ")));

            threadPool = new ThreadPool(Settings.builder().put("node.name", "testIndexingThreadPoolsMaxSize").build());
            final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
            threadPool.setClusterSettings(clusterSettings);

            // try to update to too-big size:
            final IllegalArgumentException updated =
                expectThrows(
                    IllegalArgumentException.class,
                    () -> clusterSettings.applySettings(
                        Settings.builder()
                            .put("thread_pool." + name + ".size", tooBig)
                            .build()));

            assertThat(
                updated,
                hasToString(containsString(
                    "Failed to parse value [" + tooBig + "] for setting [thread_pool." + name + ".size] must be ")));
        } finally {
            terminateThreadPoolIfNeeded(threadPool);
        }
    }

    private static int getExpectedThreadPoolSize(Settings settings, String name, int size) {
        if (name.equals(ThreadPool.Names.BULK) || name.equals(ThreadPool.Names.INDEX)) {
            return Math.min(size, EsExecutors.boundedNumberOfProcessors(settings));
        } else {
            return size;
        }
    }

    public void testFixedExecutorType() throws InterruptedException {
        String threadPoolName = randomThreadPool(ThreadPool.ThreadPoolType.FIXED);
        ThreadPool threadPool = null;

        try {
            Settings nodeSettings = Settings.builder()
                    .put("node.name", "testFixedExecutorType").build();
            threadPool = new ThreadPool(nodeSettings);
            threadPool.start();
            ClusterSettings clusterSettings = new ClusterSettings(nodeSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
            threadPool.setClusterSettings(clusterSettings);
            assertThat(threadPool.executor(threadPoolName), instanceOf(EsThreadPoolExecutor.class));
            int expectedSize = getExpectedThreadPoolSize(nodeSettings, threadPoolName, 15);
            Settings settings =
                clusterSettings.applySettings(Settings.builder()
                    .put("thread_pool." + threadPoolName + ".size", expectedSize)
                    .build());

            assertEquals(info(threadPool, threadPoolName).getThreadPoolType(), ThreadPool.ThreadPoolType.FIXED);
            assertThat(threadPool.executor(threadPoolName), instanceOf(EsThreadPoolExecutor.class));
            assertThat(((EsThreadPoolExecutor) threadPool.executor(threadPoolName)).getCorePoolSize(), equalTo(expectedSize));
            assertThat(((EsThreadPoolExecutor) threadPool.executor(threadPoolName)).getMaximumPoolSize(), equalTo(expectedSize));
            assertThat(info(threadPool, threadPoolName).getMin(), equalTo(expectedSize));
            assertThat(info(threadPool, threadPoolName).getMax(), equalTo(expectedSize));
            // keep alive does not apply to fixed thread pools
            assertThat(((EsThreadPoolExecutor) threadPool.executor(threadPoolName)).getKeepAliveTime(TimeUnit.MINUTES), equalTo(0L));

            // Change size
            Executor oldExecutor = threadPool.executor(threadPoolName);
            expectedSize = getExpectedThreadPoolSize(nodeSettings, threadPoolName, 10);
            settings = clusterSettings.applySettings(Settings.builder().put(settings).put("thread_pool." + threadPoolName + ".size", expectedSize).build());


            // Make sure size values changed
            assertThat(info(threadPool, threadPoolName).getMax(), equalTo(expectedSize));
            assertThat(info(threadPool, threadPoolName).getMin(), equalTo(expectedSize));
            assertThat(((EsThreadPoolExecutor) threadPool.executor(threadPoolName)).getMaximumPoolSize(), equalTo(expectedSize));
            assertThat(((EsThreadPoolExecutor) threadPool.executor(threadPoolName)).getCorePoolSize(), equalTo(expectedSize));
            // Make sure executor didn't change
            assertEquals(info(threadPool, threadPoolName).getThreadPoolType(), ThreadPool.ThreadPoolType.FIXED);
            assertThat(threadPool.executor(threadPoolName), sameInstance(oldExecutor));

            // Change queue capacity
            clusterSettings.applySettings(Settings.builder().put(settings).put("thread_pool." + threadPoolName + ".queue", "500")
                    .build());
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
                    .put("node.name", "testScalingExecutorType").build();
            threadPool = new ThreadPool(nodeSettings);
            threadPool.start();
            ClusterSettings clusterSettings = new ClusterSettings(nodeSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
            threadPool.setClusterSettings(clusterSettings);
            final int expectedMinimum = "generic".equals(threadPoolName) ? 4 : 1;
            assertThat(info(threadPool, threadPoolName).getMin(), equalTo(expectedMinimum));
            assertThat(info(threadPool, threadPoolName).getMax(), equalTo(10));
            final long expectedKeepAlive = "generic".equals(threadPoolName) ? 30 : 300;
            assertThat(info(threadPool, threadPoolName).getKeepAlive().seconds(), equalTo(expectedKeepAlive));
            assertEquals(info(threadPool, threadPoolName).getThreadPoolType(), ThreadPool.ThreadPoolType.SCALING);
            assertThat(threadPool.executor(threadPoolName), instanceOf(EsThreadPoolExecutor.class));

            // Change settings that doesn't require pool replacement
            Executor oldExecutor = threadPool.executor(threadPoolName);
            clusterSettings.applySettings(Settings.builder()
                    .put("thread_pool." + threadPoolName + ".keep_alive", "10m")
                    .put("thread_pool." + threadPoolName + ".core", "2")
                    .put("thread_pool." + threadPoolName + ".max", "15")
                    .build());
            assertEquals(info(threadPool, threadPoolName).getThreadPoolType(), ThreadPool.ThreadPoolType.SCALING);
            assertThat(threadPool.executor(threadPoolName), instanceOf(EsThreadPoolExecutor.class));
            assertThat(((EsThreadPoolExecutor) threadPool.executor(threadPoolName)).getCorePoolSize(), equalTo(2));
            assertThat(((EsThreadPoolExecutor) threadPool.executor(threadPoolName)).getMaximumPoolSize(), equalTo(15));
            assertThat(info(threadPool, threadPoolName).getMin(), equalTo(2));
            assertThat(info(threadPool, threadPoolName).getMax(), equalTo(15));
            // Make sure keep alive value changed
            assertThat(info(threadPool, threadPoolName).getKeepAlive().minutes(), equalTo(10L));
            assertThat(((EsThreadPoolExecutor) threadPool.executor(threadPoolName)).getKeepAliveTime(TimeUnit.MINUTES), equalTo(10L));
            assertThat(threadPool.executor(threadPoolName), sameInstance(oldExecutor));
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
                    .put("node.name", "testShutdownNowInterrupts").build();
            threadPool = new ThreadPool(nodeSettings);
            threadPool.start();
            ClusterSettings clusterSettings = new ClusterSettings(nodeSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
            threadPool.setClusterSettings(clusterSettings);
            assertEquals(info(threadPool, threadPoolName).getQueueSize().getSingles(), 1000L);

            final CountDownLatch latch = new CountDownLatch(1);
            ThreadPoolExecutor oldExecutor = (ThreadPoolExecutor) threadPool.executor(threadPoolName);
            threadPool.executor(threadPoolName).execute(() -> {
                        try {
                            new CountDownLatch(1).await();
                        } catch (InterruptedException ex) {
                            latch.countDown();
                            Thread.currentThread().interrupt();
                        }
                    }
            );
            clusterSettings.applySettings(Settings.builder().put("thread_pool." + threadPoolName + ".queue_size", 2000).build());
            assertThat(threadPool.executor(threadPoolName), not(sameInstance(oldExecutor)));
            assertThat(oldExecutor.isShutdown(), equalTo(true));
            assertThat(oldExecutor.isTerminating(), equalTo(true));
            assertThat(oldExecutor.isTerminated(), equalTo(false));
            threadPool.shutdownNow(); // should interrupt the thread
            latch.await(3, TimeUnit.SECONDS); // If this throws then ThreadPool#shutdownNow didn't interrupt
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
                    EsExecutors.boundedNumberOfProcessors(Settings.EMPTY),
                    TimeValue.timeValueMinutes(1));

            final FixedExecutorBuilder fixed = new FixedExecutorBuilder(Settings.EMPTY, "my_pool2", 1, 1);

            threadPool = new ThreadPool(Settings.builder().put("node.name", "testCustomThreadPool").build());
            threadPool.add(scaling);
            threadPool.add(fixed);
            threadPool.start();

            ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
            threadPool.setClusterSettings(clusterSettings);
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

            // Updating my_pool2
            Settings settings = Settings.builder()
                    .put("thread_pool.my_pool2.size", "10")
                    .build();
            clusterSettings.applySettings(settings);

            groups = threadPool.info();
            foundPool1 = false;
            foundPool2 = false;
            outer:
            for (ThreadPool.Info info : groups) {
                if ("my_pool1".equals(info.getName())) {
                    foundPool1 = true;
                    assertEquals(info.getThreadPoolType(), ThreadPool.ThreadPoolType.SCALING);
                } else if ("my_pool2".equals(info.getName())) {
                    foundPool2 = true;
                    assertThat(info.getMax(), equalTo(10));
                    assertThat(info.getMin(), equalTo(10));
                    assertThat(info.getQueueSize().singles(), equalTo(1L));
                    assertEquals(info.getThreadPoolType(), ThreadPool.ThreadPoolType.FIXED);
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
