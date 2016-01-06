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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool.Names;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

/**
 */
public class UpdateThreadPoolSettingsTests extends ESTestCase {
    public void testCorrectThreadPoolTypePermittedInSettings() throws InterruptedException {
        String threadPoolName = randomThreadPoolName();
        ThreadPool.ThreadPoolType correctThreadPoolType = ThreadPool.THREAD_POOL_TYPES.get(threadPoolName);
        ThreadPool threadPool = null;
        try {
            threadPool = new ThreadPool(settingsBuilder()
                    .put("name", "testCorrectThreadPoolTypePermittedInSettings")
                    .put("threadpool." + threadPoolName + ".type", correctThreadPoolType.getType())
                    .build());
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

    public void testThreadPoolCanNotOverrideThreadPoolType() throws InterruptedException {
        String threadPoolName = randomThreadPoolName();
        ThreadPool.ThreadPoolType incorrectThreadPoolType = randomIncorrectThreadPoolType(threadPoolName);
        ThreadPool.ThreadPoolType correctThreadPoolType = ThreadPool.THREAD_POOL_TYPES.get(threadPoolName);
        ThreadPool threadPool = null;
        try {
            threadPool = new ThreadPool(
                    settingsBuilder()
                            .put("name", "testThreadPoolCanNotOverrideThreadPoolType")
                            .put("threadpool." + threadPoolName + ".type", incorrectThreadPoolType.getType())
                            .build());
            terminate(threadPool);
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(
                    e.getMessage(),
                    is("setting threadpool." + threadPoolName + ".type to " + incorrectThreadPoolType.getType() + " is not permitted; must be " + correctThreadPoolType.getType()));
        } finally {
            terminateThreadPoolIfNeeded(threadPool);
        }
    }

    public void testUpdateSettingsCanNotChangeThreadPoolType() throws InterruptedException {
        String threadPoolName = randomThreadPoolName();
        ThreadPool.ThreadPoolType invalidThreadPoolType = randomIncorrectThreadPoolType(threadPoolName);
        ThreadPool.ThreadPoolType validThreadPoolType = ThreadPool.THREAD_POOL_TYPES.get(threadPoolName);
        ThreadPool threadPool = null;
        try {
            threadPool = new ThreadPool(settingsBuilder().put("name", "testUpdateSettingsCanNotChangeThreadPoolType").build());
            ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
            threadPool.setClusterSettings(clusterSettings);

            clusterSettings.applySettings(
                    settingsBuilder()
                            .put("threadpool." + threadPoolName + ".type", invalidThreadPoolType.getType())
                            .build()
            );
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals("illegal value can't update [threadpool.] from [{}] to [{" + threadPoolName + ".type=" + invalidThreadPoolType.getType() + "}]", e.getMessage());
            assertThat(
                    e.getCause().getMessage(),
                    is("setting threadpool." + threadPoolName + ".type to " + invalidThreadPoolType.getType() + " is not permitted; must be " + validThreadPoolType.getType()));
        } finally {
            terminateThreadPoolIfNeeded(threadPool);
        }
    }

    public void testCachedExecutorType() throws InterruptedException {
        String threadPoolName = randomThreadPool(ThreadPool.ThreadPoolType.CACHED);
        ThreadPool threadPool = null;
        try {
            Settings nodeSettings = Settings.settingsBuilder()
                    .put("name", "testCachedExecutorType").build();
            threadPool = new ThreadPool(nodeSettings);
            ClusterSettings clusterSettings = new ClusterSettings(nodeSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
            threadPool.setClusterSettings(clusterSettings);

            assertEquals(info(threadPool, threadPoolName).getThreadPoolType(), ThreadPool.ThreadPoolType.CACHED);
            assertThat(threadPool.executor(threadPoolName), instanceOf(EsThreadPoolExecutor.class));

            Settings settings = clusterSettings.applySettings(settingsBuilder()
                    .put("threadpool." + threadPoolName + ".keep_alive", "10m")
                    .build());
            assertEquals(info(threadPool, threadPoolName).getThreadPoolType(), ThreadPool.ThreadPoolType.CACHED);
            assertThat(threadPool.executor(threadPoolName), instanceOf(EsThreadPoolExecutor.class));
            assertThat(((EsThreadPoolExecutor) threadPool.executor(threadPoolName)).getCorePoolSize(), equalTo(0));
            // Make sure keep alive value changed
            assertThat(info(threadPool, threadPoolName).getKeepAlive().minutes(), equalTo(10L));
            assertThat(((EsThreadPoolExecutor) threadPool.executor(threadPoolName)).getKeepAliveTime(TimeUnit.MINUTES), equalTo(10L));

            // Make sure keep alive value reused
            assertThat(info(threadPool, threadPoolName).getKeepAlive().minutes(), equalTo(10L));
            assertThat(threadPool.executor(threadPoolName), instanceOf(EsThreadPoolExecutor.class));

            // Change keep alive
            Executor oldExecutor = threadPool.executor(threadPoolName);
            settings = clusterSettings.applySettings(settingsBuilder().put(settings).put("threadpool." + threadPoolName + ".keep_alive", "1m").build());
            // Make sure keep alive value changed
            assertThat(info(threadPool, threadPoolName).getKeepAlive().minutes(), equalTo(1L));
            assertThat(((EsThreadPoolExecutor) threadPool.executor(threadPoolName)).getKeepAliveTime(TimeUnit.MINUTES), equalTo(1L));
            // Make sure executor didn't change
            assertEquals(info(threadPool, threadPoolName).getThreadPoolType(), ThreadPool.ThreadPoolType.CACHED);
            assertThat(threadPool.executor(threadPoolName), sameInstance(oldExecutor));

            // Set the same keep alive
            settings = clusterSettings.applySettings(settingsBuilder().put(settings).put("threadpool." + threadPoolName + ".keep_alive", "1m").build());
            // Make sure keep alive value didn't change
            assertThat(info(threadPool, threadPoolName).getKeepAlive().minutes(), equalTo(1L));
            assertThat(((EsThreadPoolExecutor) threadPool.executor(threadPoolName)).getKeepAliveTime(TimeUnit.MINUTES), equalTo(1L));
            // Make sure executor didn't change
            assertEquals(info(threadPool, threadPoolName).getThreadPoolType(), ThreadPool.ThreadPoolType.CACHED);
            assertThat(threadPool.executor(threadPoolName), sameInstance(oldExecutor));
        } finally {
            terminateThreadPoolIfNeeded(threadPool);
        }
    }

    public void testFixedExecutorType() throws InterruptedException {
        String threadPoolName = randomThreadPool(ThreadPool.ThreadPoolType.FIXED);
        ThreadPool threadPool = null;

        try {
            Settings nodeSettings = Settings.settingsBuilder()
                    .put("name", "testFixedExecutorType").build();
            threadPool = new ThreadPool(nodeSettings);
            ClusterSettings clusterSettings = new ClusterSettings(nodeSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
            threadPool.setClusterSettings(clusterSettings);
            assertThat(threadPool.executor(threadPoolName), instanceOf(EsThreadPoolExecutor.class));
            Settings settings = clusterSettings.applySettings(settingsBuilder()
                    .put("threadpool." + threadPoolName + ".size", "15")
                    .build());
            assertEquals(info(threadPool, threadPoolName).getThreadPoolType(), ThreadPool.ThreadPoolType.FIXED);
            assertThat(threadPool.executor(threadPoolName), instanceOf(EsThreadPoolExecutor.class));
            assertThat(((EsThreadPoolExecutor) threadPool.executor(threadPoolName)).getCorePoolSize(), equalTo(15));
            assertThat(((EsThreadPoolExecutor) threadPool.executor(threadPoolName)).getMaximumPoolSize(), equalTo(15));
            assertThat(info(threadPool, threadPoolName).getMin(), equalTo(15));
            assertThat(info(threadPool, threadPoolName).getMax(), equalTo(15));
            // keep alive does not apply to fixed thread pools
            assertThat(((EsThreadPoolExecutor) threadPool.executor(threadPoolName)).getKeepAliveTime(TimeUnit.MINUTES), equalTo(0L));

            // Put old type back
            settings = clusterSettings.applySettings(Settings.EMPTY);
            assertEquals(info(threadPool, threadPoolName).getThreadPoolType(), ThreadPool.ThreadPoolType.FIXED);
            // Make sure keep alive value is not used
            assertThat(info(threadPool, threadPoolName).getKeepAlive(), nullValue());
            // Make sure keep pool size value were reused
            assertThat(info(threadPool, threadPoolName).getMin(), equalTo(15));
            assertThat(info(threadPool, threadPoolName).getMax(), equalTo(15));
            assertThat(threadPool.executor(threadPoolName), instanceOf(EsThreadPoolExecutor.class));
            assertThat(((EsThreadPoolExecutor) threadPool.executor(threadPoolName)).getCorePoolSize(), equalTo(15));
            assertThat(((EsThreadPoolExecutor) threadPool.executor(threadPoolName)).getMaximumPoolSize(), equalTo(15));

            // Change size
            Executor oldExecutor = threadPool.executor(threadPoolName);
            settings = clusterSettings.applySettings(settingsBuilder().put(settings).put("threadpool." + threadPoolName + ".size", "10").build());
            // Make sure size values changed
            assertThat(info(threadPool, threadPoolName).getMax(), equalTo(10));
            assertThat(info(threadPool, threadPoolName).getMin(), equalTo(10));
            assertThat(((EsThreadPoolExecutor) threadPool.executor(threadPoolName)).getMaximumPoolSize(), equalTo(10));
            assertThat(((EsThreadPoolExecutor) threadPool.executor(threadPoolName)).getCorePoolSize(), equalTo(10));
            // Make sure executor didn't change
            assertEquals(info(threadPool, threadPoolName).getThreadPoolType(), ThreadPool.ThreadPoolType.FIXED);
            assertThat(threadPool.executor(threadPoolName), sameInstance(oldExecutor));

            // Change queue capacity
            settings = clusterSettings.applySettings(settingsBuilder().put(settings).put("threadpool." + threadPoolName + ".queue", "500")
                    .build());
        } finally {
            terminateThreadPoolIfNeeded(threadPool);
        }
    }

    public void testScalingExecutorType() throws InterruptedException {
        String threadPoolName = randomThreadPool(ThreadPool.ThreadPoolType.SCALING);
        ThreadPool threadPool = null;
        try {
            Settings nodeSettings = settingsBuilder()
                    .put("threadpool." + threadPoolName + ".size", 10)
                    .put("name", "testScalingExecutorType").build();
            threadPool = new ThreadPool(nodeSettings);
            ClusterSettings clusterSettings = new ClusterSettings(nodeSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
            threadPool.setClusterSettings(clusterSettings);
            assertThat(info(threadPool, threadPoolName).getMin(), equalTo(1));
            assertThat(info(threadPool, threadPoolName).getMax(), equalTo(10));
            assertThat(info(threadPool, threadPoolName).getKeepAlive().minutes(), equalTo(5L));
            assertEquals(info(threadPool, threadPoolName).getThreadPoolType(), ThreadPool.ThreadPoolType.SCALING);
            assertThat(threadPool.executor(threadPoolName), instanceOf(EsThreadPoolExecutor.class));

            // Change settings that doesn't require pool replacement
            Executor oldExecutor = threadPool.executor(threadPoolName);
            clusterSettings.applySettings(settingsBuilder()
                    .put("threadpool." + threadPoolName + ".keep_alive", "10m")
                    .put("threadpool." + threadPoolName + ".min", "2")
                    .put("threadpool." + threadPoolName + ".size", "15")
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
            Settings nodeSettings = Settings.settingsBuilder()
                    .put("threadpool." + threadPoolName + ".queue_size", 1000)
                    .put("name", "testCachedExecutorType").build();
            threadPool = new ThreadPool(nodeSettings);
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
            clusterSettings.applySettings(settingsBuilder().put("threadpool." + threadPoolName + ".queue_size", 2000).build());
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
            Settings nodeSettings = Settings.settingsBuilder()
                    .put("threadpool.my_pool1.type", "scaling")
                    .put("threadpool.my_pool2.type", "fixed")
                    .put("threadpool.my_pool2.size", "1")
                    .put("threadpool.my_pool2.queue_size", "1")
                    .put("name", "testCustomThreadPool").build();
            threadPool = new ThreadPool(nodeSettings);
            ClusterSettings clusterSettings = new ClusterSettings(nodeSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
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
                    assertThat(info.getQueueSize().singles(), equalTo(1l));
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
                    .put("threadpool.my_pool2.size", "10")
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
                    assertThat(info.getQueueSize().singles(), equalTo(1l));
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

    private void terminateThreadPoolIfNeeded(ThreadPool threadPool) throws InterruptedException {
        if (threadPool != null) {
            terminate(threadPool);
        }
    }

    private ThreadPool.Info info(ThreadPool threadPool, String name) {
        for (ThreadPool.Info info : threadPool.info()) {
            if (info.getName().equals(name)) {
                return info;
            }
        }
        return null;
    }

    private String randomThreadPoolName() {
        Set<String> threadPoolNames = ThreadPool.THREAD_POOL_TYPES.keySet();
        return randomFrom(threadPoolNames.toArray(new String[threadPoolNames.size()]));
    }

    private ThreadPool.ThreadPoolType randomIncorrectThreadPoolType(String threadPoolName) {
        Set<ThreadPool.ThreadPoolType> set = new HashSet<>();
        set.addAll(Arrays.asList(ThreadPool.ThreadPoolType.values()));
        set.remove(ThreadPool.THREAD_POOL_TYPES.get(threadPoolName));
        ThreadPool.ThreadPoolType invalidThreadPoolType = randomFrom(set.toArray(new ThreadPool.ThreadPoolType[set.size()]));
        return invalidThreadPoolType;
    }

    private String randomThreadPool(ThreadPool.ThreadPoolType type) {
        return randomFrom(ThreadPool.THREAD_POOL_TYPES.entrySet().stream().filter(t -> t.getValue().equals(type)).map(t -> t.getKey()).collect(Collectors.toList()));
    }
}
