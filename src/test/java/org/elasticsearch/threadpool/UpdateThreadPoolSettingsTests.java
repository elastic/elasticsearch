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

import com.google.common.util.concurrent.MoreExecutors;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.Matchers.*;

/**
 */
public class UpdateThreadPoolSettingsTests extends ElasticsearchTestCase {

    private ThreadPool.Info info(ThreadPool threadPool, String name) {
        for (ThreadPool.Info info : threadPool.info()) {
            if (info.getName().equals(name)) {
                return info;
            }
        }
        return null;
    }

    @Test
    public void testCachedExecutorType() throws InterruptedException {
        ThreadPool threadPool = new ThreadPool(
                ImmutableSettings.settingsBuilder()
                        .put("threadpool.search.type", "cached")
                        .put("name","testCachedExecutorType").build(), null);

        assertThat(info(threadPool, Names.SEARCH).getType(), equalTo("cached"));
        assertThat(info(threadPool, Names.SEARCH).getKeepAlive().minutes(), equalTo(5L));
        assertThat(threadPool.executor(Names.SEARCH), instanceOf(EsThreadPoolExecutor.class));

        // Replace with different type
        threadPool.updateSettings(settingsBuilder().put("threadpool.search.type", "same").build());
        assertThat(info(threadPool, Names.SEARCH).getType(), equalTo("same"));
        assertThat(threadPool.executor(Names.SEARCH), instanceOf(MoreExecutors.directExecutor().getClass()));

        // Replace with different type again
        threadPool.updateSettings(settingsBuilder()
                .put("threadpool.search.type", "scaling")
                .put("threadpool.search.keep_alive", "10m")
                .build());
        assertThat(info(threadPool, Names.SEARCH).getType(), equalTo("scaling"));
        assertThat(threadPool.executor(Names.SEARCH), instanceOf(EsThreadPoolExecutor.class));
        assertThat(((EsThreadPoolExecutor) threadPool.executor(Names.SEARCH)).getCorePoolSize(), equalTo(1));
        // Make sure keep alive value changed
        assertThat(info(threadPool, Names.SEARCH).getKeepAlive().minutes(), equalTo(10L));
        assertThat(((EsThreadPoolExecutor) threadPool.executor(Names.SEARCH)).getKeepAliveTime(TimeUnit.MINUTES), equalTo(10L));

        // Put old type back
        threadPool.updateSettings(settingsBuilder().put("threadpool.search.type", "cached").build());
        assertThat(info(threadPool, Names.SEARCH).getType(), equalTo("cached"));
        // Make sure keep alive value reused
        assertThat(info(threadPool, Names.SEARCH).getKeepAlive().minutes(), equalTo(10L));
        assertThat(threadPool.executor(Names.SEARCH), instanceOf(EsThreadPoolExecutor.class));

        // Change keep alive
        Executor oldExecutor = threadPool.executor(Names.SEARCH);
        threadPool.updateSettings(settingsBuilder().put("threadpool.search.keep_alive", "1m").build());
        // Make sure keep alive value changed
        assertThat(info(threadPool, Names.SEARCH).getKeepAlive().minutes(), equalTo(1L));
        assertThat(((EsThreadPoolExecutor) threadPool.executor(Names.SEARCH)).getKeepAliveTime(TimeUnit.MINUTES), equalTo(1L));
        // Make sure executor didn't change
        assertThat(info(threadPool, Names.SEARCH).getType(), equalTo("cached"));
        assertThat(threadPool.executor(Names.SEARCH), sameInstance(oldExecutor));

        // Set the same keep alive
        threadPool.updateSettings(settingsBuilder().put("threadpool.search.keep_alive", "1m").build());
        // Make sure keep alive value didn't change
        assertThat(info(threadPool, Names.SEARCH).getKeepAlive().minutes(), equalTo(1L));
        assertThat(((EsThreadPoolExecutor) threadPool.executor(Names.SEARCH)).getKeepAliveTime(TimeUnit.MINUTES), equalTo(1L));
        // Make sure executor didn't change
        assertThat(info(threadPool, Names.SEARCH).getType(), equalTo("cached"));
        assertThat(threadPool.executor(Names.SEARCH), sameInstance(oldExecutor));
        terminate(threadPool);
    }

    @Test
    public void testFixedExecutorType() throws InterruptedException {
        ThreadPool threadPool = new ThreadPool(settingsBuilder()
                .put("threadpool.search.type", "fixed")
                .put("name","testCachedExecutorType").build(), null);

        assertThat(threadPool.executor(Names.SEARCH), instanceOf(EsThreadPoolExecutor.class));

        // Replace with different type
        threadPool.updateSettings(settingsBuilder()
                .put("threadpool.search.type", "scaling")
                .put("threadpool.search.keep_alive", "10m")
                .put("threadpool.search.min", "2")
                .put("threadpool.search.size", "15")
                .build());
        assertThat(info(threadPool, Names.SEARCH).getType(), equalTo("scaling"));
        assertThat(threadPool.executor(Names.SEARCH), instanceOf(EsThreadPoolExecutor.class));
        assertThat(((EsThreadPoolExecutor) threadPool.executor(Names.SEARCH)).getCorePoolSize(), equalTo(2));
        assertThat(((EsThreadPoolExecutor) threadPool.executor(Names.SEARCH)).getMaximumPoolSize(), equalTo(15));
        assertThat(info(threadPool, Names.SEARCH).getMin(), equalTo(2));
        assertThat(info(threadPool, Names.SEARCH).getMax(), equalTo(15));
        // Make sure keep alive value changed
        assertThat(info(threadPool, Names.SEARCH).getKeepAlive().minutes(), equalTo(10L));
        assertThat(((EsThreadPoolExecutor) threadPool.executor(Names.SEARCH)).getKeepAliveTime(TimeUnit.MINUTES), equalTo(10L));

        // Put old type back
        threadPool.updateSettings(settingsBuilder()
                .put("threadpool.search.type", "fixed")
                .build());
        assertThat(info(threadPool, Names.SEARCH).getType(), equalTo("fixed"));
        // Make sure keep alive value is not used
        assertThat(info(threadPool, Names.SEARCH).getKeepAlive(), nullValue());
        // Make sure keep pool size value were reused
        assertThat(info(threadPool, Names.SEARCH).getMin(), equalTo(15));
        assertThat(info(threadPool, Names.SEARCH).getMax(), equalTo(15));
        assertThat(threadPool.executor(Names.SEARCH), instanceOf(EsThreadPoolExecutor.class));
        assertThat(((EsThreadPoolExecutor) threadPool.executor(Names.SEARCH)).getCorePoolSize(), equalTo(15));
        assertThat(((EsThreadPoolExecutor) threadPool.executor(Names.SEARCH)).getMaximumPoolSize(), equalTo(15));

        // Change size
        Executor oldExecutor = threadPool.executor(Names.SEARCH);
        threadPool.updateSettings(settingsBuilder().put("threadpool.search.size", "10").build());
        // Make sure size values changed
        assertThat(info(threadPool, Names.SEARCH).getMax(), equalTo(10));
        assertThat(info(threadPool, Names.SEARCH).getMin(), equalTo(10));
        assertThat(((EsThreadPoolExecutor) threadPool.executor(Names.SEARCH)).getMaximumPoolSize(), equalTo(10));
        assertThat(((EsThreadPoolExecutor) threadPool.executor(Names.SEARCH)).getCorePoolSize(), equalTo(10));
        // Make sure executor didn't change
        assertThat(info(threadPool, Names.SEARCH).getType(), equalTo("fixed"));
        assertThat(threadPool.executor(Names.SEARCH), sameInstance(oldExecutor));

        // Change queue capacity
        threadPool.updateSettings(settingsBuilder()
                .put("threadpool.search.queue", "500")
                .build());

        terminate(threadPool);
    }


    @Test
    public void testScalingExecutorType() throws InterruptedException {
        ThreadPool threadPool = new ThreadPool(settingsBuilder()
                .put("threadpool.search.type", "scaling")
                .put("threadpool.search.size", 10)
                .put("name","testCachedExecutorType").build(), null);

        assertThat(info(threadPool, Names.SEARCH).getMin(), equalTo(1));
        assertThat(info(threadPool, Names.SEARCH).getMax(), equalTo(10));
        assertThat(info(threadPool, Names.SEARCH).getKeepAlive().minutes(), equalTo(5L));
        assertThat(info(threadPool, Names.SEARCH).getType(), equalTo("scaling"));
        assertThat(threadPool.executor(Names.SEARCH), instanceOf(EsThreadPoolExecutor.class));

        // Change settings that doesn't require pool replacement
        Executor oldExecutor = threadPool.executor(Names.SEARCH);
        threadPool.updateSettings(settingsBuilder()
                .put("threadpool.search.type", "scaling")
                .put("threadpool.search.keep_alive", "10m")
                .put("threadpool.search.min", "2")
                .put("threadpool.search.size", "15")
                .build());
        assertThat(info(threadPool, Names.SEARCH).getType(), equalTo("scaling"));
        assertThat(threadPool.executor(Names.SEARCH), instanceOf(EsThreadPoolExecutor.class));
        assertThat(((EsThreadPoolExecutor) threadPool.executor(Names.SEARCH)).getCorePoolSize(), equalTo(2));
        assertThat(((EsThreadPoolExecutor) threadPool.executor(Names.SEARCH)).getMaximumPoolSize(), equalTo(15));
        assertThat(info(threadPool, Names.SEARCH).getMin(), equalTo(2));
        assertThat(info(threadPool, Names.SEARCH).getMax(), equalTo(15));
        // Make sure keep alive value changed
        assertThat(info(threadPool, Names.SEARCH).getKeepAlive().minutes(), equalTo(10L));
        assertThat(((EsThreadPoolExecutor) threadPool.executor(Names.SEARCH)).getKeepAliveTime(TimeUnit.MINUTES), equalTo(10L));
        assertThat(threadPool.executor(Names.SEARCH), sameInstance(oldExecutor));

        terminate(threadPool);
    }

    @Test(timeout = 10000)
    public void testShutdownDownNowDoesntBlock() throws Exception {
        ThreadPool threadPool = new ThreadPool(ImmutableSettings.settingsBuilder()
                .put("threadpool.search.type", "cached")
                .put("name","testCachedExecutorType").build(), null);

        final CountDownLatch latch = new CountDownLatch(1);
        Executor oldExecutor = threadPool.executor(Names.SEARCH);
        threadPool.executor(Names.SEARCH).execute(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(20000);
                } catch (InterruptedException ex) {
                    latch.countDown();
                    Thread.currentThread().interrupt();
                }
            }
        });
        threadPool.updateSettings(settingsBuilder().put("threadpool.search.type", "fixed").build());
        assertThat(threadPool.executor(Names.SEARCH), not(sameInstance(oldExecutor)));
        assertThat(((ThreadPoolExecutor) oldExecutor).isShutdown(), equalTo(true));
        assertThat(((ThreadPoolExecutor) oldExecutor).isTerminating(), equalTo(true));
        assertThat(((ThreadPoolExecutor) oldExecutor).isTerminated(), equalTo(false));
        threadPool.shutdownNow(); // interrupt the thread
        latch.await();
        terminate(threadPool);
    }

    @Test
    public void testCustomThreadPool() throws Exception {
        ThreadPool threadPool = new ThreadPool(ImmutableSettings.settingsBuilder()
                .put("threadpool.my_pool1.type", "cached")
                .put("threadpool.my_pool2.type", "fixed")
                .put("threadpool.my_pool2.size", "1")
                .put("threadpool.my_pool2.queue_size", "1")
                .put("name", "testCustomThreadPool").build(), null);

        ThreadPoolInfo groups = threadPool.info();
        boolean foundPool1 = false;
        boolean foundPool2 = false;
        outer: for (ThreadPool.Info info : groups) {
            if ("my_pool1".equals(info.getName())) {
                foundPool1 = true;
                assertThat(info.getType(), equalTo("cached"));
            } else if ("my_pool2".equals(info.getName())) {
                foundPool2 = true;
                assertThat(info.getType(), equalTo("fixed"));
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
        Settings settings = ImmutableSettings.builder()
                .put("threadpool.my_pool2.size", "10")
                .build();
        threadPool.updateSettings(settings);

        groups = threadPool.info();
        foundPool1 = false;
        foundPool2 = false;
        outer: for (ThreadPool.Info info : groups) {
            if ("my_pool1".equals(info.getName())) {
                foundPool1 = true;
                assertThat(info.getType(), equalTo("cached"));
            } else if ("my_pool2".equals(info.getName())) {
                foundPool2 = true;
                assertThat(info.getMax(), equalTo(10));
                assertThat(info.getMin(), equalTo(10));
                assertThat(info.getQueueSize().singles(), equalTo(1l));
                assertThat(info.getType(), equalTo("fixed"));
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
        terminate(threadPool);
    }

}
