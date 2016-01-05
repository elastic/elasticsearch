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
import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.hamcrest.Matchers.*;

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


            threadPool.updateSettings(
                    settingsBuilder()
                            .put("threadpool." + threadPoolName + ".type", invalidThreadPoolType.getType())
                            .build()
            );
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(
                    e.getMessage(),
                    is("setting threadpool." + threadPoolName + ".type to " + invalidThreadPoolType.getType() + " is not permitted; must be " + validThreadPoolType.getType()));
        } finally {
            terminateThreadPoolIfNeeded(threadPool);
        }
    }

    @Test
    public void testCachedExecutorType() throws InterruptedException {
        String threadPoolName = randomThreadPool(ThreadPool.ThreadPoolType.CACHED);
        ThreadPool threadPool = null;
        try {
            threadPool = new ThreadPool(
                    Settings.settingsBuilder()
                            .put("name", "testCachedExecutorType").build());

            assertEquals(info(threadPool, threadPoolName).getThreadPoolType(), ThreadPool.ThreadPoolType.CACHED);
            assertThat(threadPool.executor(threadPoolName), instanceOf(EsThreadPoolExecutor.class));

            threadPool.updateSettings(settingsBuilder()
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
            threadPool.updateSettings(settingsBuilder().put("threadpool." + threadPoolName + ".keep_alive", "1m").build());
            // Make sure keep alive value changed
            assertThat(info(threadPool, threadPoolName).getKeepAlive().minutes(), equalTo(1L));
            assertThat(((EsThreadPoolExecutor) threadPool.executor(threadPoolName)).getKeepAliveTime(TimeUnit.MINUTES), equalTo(1L));
            // Make sure executor didn't change
            assertEquals(info(threadPool, threadPoolName).getThreadPoolType(), ThreadPool.ThreadPoolType.CACHED);
            assertThat(threadPool.executor(threadPoolName), sameInstance(oldExecutor));

            // Set the same keep alive
            threadPool.updateSettings(settingsBuilder().put("threadpool." + threadPoolName + ".keep_alive", "1m").build());
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

    private static int getExpectedThreadPoolSize(Settings settings, String name, int size) {
        if (name.equals(ThreadPool.Names.BULK) || name.equals(ThreadPool.Names.INDEX)) {
            return Math.min(size, EsExecutors.boundedNumberOfProcessors(settings));
        } else {
            return size;
        }
    }

    @Test
    public void testFixedExecutorType() throws InterruptedException {
        String threadPoolName = randomThreadPool(ThreadPool.ThreadPoolType.FIXED);
        ThreadPool threadPool = null;

        try {
            threadPool = new ThreadPool(settingsBuilder()
                    .put("name", "testCachedExecutorType").build());
            assertThat(threadPool.executor(threadPoolName), instanceOf(EsThreadPoolExecutor.class));

            threadPool.updateSettings(settingsBuilder()
                    .put("threadpool." + threadPoolName + ".size", "15")
                    .build());

            int expectedSize = getExpectedThreadPoolSize(settingsBuilder().build(), threadPoolName, 15);
            assertEquals(info(threadPool, threadPoolName).getThreadPoolType(), ThreadPool.ThreadPoolType.FIXED);
            assertThat(threadPool.executor(threadPoolName), instanceOf(EsThreadPoolExecutor.class));
            assertThat(((EsThreadPoolExecutor) threadPool.executor(threadPoolName)).getCorePoolSize(), equalTo(expectedSize));
            assertThat(((EsThreadPoolExecutor) threadPool.executor(threadPoolName)).getMaximumPoolSize(), equalTo(expectedSize));
            assertThat(info(threadPool, threadPoolName).getMin(), equalTo(expectedSize));
            assertThat(info(threadPool, threadPoolName).getMax(), equalTo(expectedSize));
            // keep alive does not apply to fixed thread pools
            assertThat(((EsThreadPoolExecutor) threadPool.executor(threadPoolName)).getKeepAliveTime(TimeUnit.MINUTES), equalTo(0L));

            // Put old type back
            threadPool.updateSettings(Settings.EMPTY);
            assertEquals(info(threadPool, threadPoolName).getThreadPoolType(), ThreadPool.ThreadPoolType.FIXED);
            // Make sure keep alive value is not used
            assertThat(info(threadPool, threadPoolName).getKeepAlive(), nullValue());
            // Make sure keep pool size value were reused
            assertThat(info(threadPool, threadPoolName).getMin(), equalTo(expectedSize));
            assertThat(info(threadPool, threadPoolName).getMax(), equalTo(expectedSize));
            assertThat(threadPool.executor(threadPoolName), instanceOf(EsThreadPoolExecutor.class));
            assertThat(((EsThreadPoolExecutor) threadPool.executor(threadPoolName)).getCorePoolSize(), equalTo(expectedSize));
            assertThat(((EsThreadPoolExecutor) threadPool.executor(threadPoolName)).getMaximumPoolSize(), equalTo(expectedSize));

            // Change size
            Executor oldExecutor = threadPool.executor(threadPoolName);
            threadPool.updateSettings(settingsBuilder().put("threadpool." + threadPoolName + ".size", "10").build());

            expectedSize = getExpectedThreadPoolSize(settingsBuilder().build(), threadPoolName, 10);

            // Make sure size values changed
            assertThat(info(threadPool, threadPoolName).getMax(), equalTo(expectedSize));
            assertThat(info(threadPool, threadPoolName).getMin(), equalTo(expectedSize));
            assertThat(((EsThreadPoolExecutor) threadPool.executor(threadPoolName)).getMaximumPoolSize(), equalTo(expectedSize));
            assertThat(((EsThreadPoolExecutor) threadPool.executor(threadPoolName)).getCorePoolSize(), equalTo(expectedSize));
            // Make sure executor didn't change
            assertEquals(info(threadPool, threadPoolName).getThreadPoolType(), ThreadPool.ThreadPoolType.FIXED);
            assertThat(threadPool.executor(threadPoolName), sameInstance(oldExecutor));

            // Change queue capacity
            threadPool.updateSettings(settingsBuilder()
                    .put("threadpool." + threadPoolName + ".queue", "500")
                    .build());
        } finally {
            terminateThreadPoolIfNeeded(threadPool);
        }
    }


    @Test
    public void testScalingExecutorType() throws InterruptedException {
        String threadPoolName = randomThreadPool(ThreadPool.ThreadPoolType.SCALING);
        ThreadPool threadPool = null;
        try {
            threadPool = new ThreadPool(settingsBuilder()
                    .put("threadpool." + threadPoolName + ".size", 10)
                    .put("name", "testCachedExecutorType").build());
            assertThat(info(threadPool, threadPoolName).getMin(), equalTo(1));
            assertThat(info(threadPool, threadPoolName).getMax(), equalTo(10));
            assertThat(info(threadPool, threadPoolName).getKeepAlive().minutes(), equalTo(5L));
            assertEquals(info(threadPool, threadPoolName).getThreadPoolType(), ThreadPool.ThreadPoolType.SCALING);
            assertThat(threadPool.executor(threadPoolName), instanceOf(EsThreadPoolExecutor.class));

            // Change settings that doesn't require pool replacement
            Executor oldExecutor = threadPool.executor(threadPoolName);
            threadPool.updateSettings(settingsBuilder()
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
            threadPool = new ThreadPool(Settings.settingsBuilder()
                    .put("threadpool." + threadPoolName + ".queue_size", 1000)
                    .put("name", "testCachedExecutorType").build());
            assertEquals(info(threadPool, threadPoolName).getQueueSize().getSingles(), 1000L);

            final CountDownLatch latch = new CountDownLatch(1);
            ThreadPoolExecutor oldExecutor = (ThreadPoolExecutor) threadPool.executor(threadPoolName);
            threadPool.executor(threadPoolName).execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        new CountDownLatch(1).await();
                    } catch (InterruptedException ex) {
                        latch.countDown();
                        Thread.currentThread().interrupt();
                    }
                }
            });
            threadPool.updateSettings(settingsBuilder().put("threadpool." + threadPoolName + ".queue_size", 2000).build());
            assertThat(threadPool.executor(threadPoolName), not(sameInstance((Executor)oldExecutor)));
            assertThat(oldExecutor.isShutdown(), equalTo(true));
            assertThat(oldExecutor.isTerminating(), equalTo(true));
            assertThat(oldExecutor.isTerminated(), equalTo(false));
            threadPool.shutdownNow(); // should interrupt the thread
            latch.await(3, TimeUnit.SECONDS); // If this throws then ThreadPool#shutdownNow didn't interrupt
        } finally {
            terminateThreadPoolIfNeeded(threadPool);
        }
    }

    @Test
    public void testCustomThreadPool() throws Exception {
        ThreadPool threadPool = null;
        try {
            threadPool = new ThreadPool(Settings.settingsBuilder()
                    .put("threadpool.my_pool1.type", "scaling")
                    .put("threadpool.my_pool2.type", "fixed")
                    .put("threadpool.my_pool2.size", "1")
                    .put("threadpool.my_pool2.queue_size", "1")
                    .put("name", "testCustomThreadPool").build());
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
            threadPool.updateSettings(settings);

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
        List<String> threadPoolTypes = new ArrayList<>();
        for (Map.Entry<String, ThreadPool.ThreadPoolType> entry : ThreadPool.THREAD_POOL_TYPES.entrySet()) {
            if (entry.getValue() == type) {
                threadPoolTypes.add(entry.getKey());
            }
        }
        return randomFrom(threadPoolTypes);
    }
}
