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

package org.elasticsearch.action.support;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ThreadedActionListenerTests extends ESTestCase {

    private ThreadPool threadPool;

    @Before
    public void createThreadPool() {
        threadPool = new TestThreadPool(ThreadedActionListenerTests.class.getName());
    }

    @After
    public void terminateThreadPool() throws InterruptedException {
        terminate(threadPool);
    }

    public void testThreadedActionListenerWithContextPreservingListener() throws InterruptedException {
        final ThreadedActionListener<Void> listener;
        final ThreadContext context = threadPool.getThreadContext();
        final CountDownLatch responseLatch = new CountDownLatch(1);
        final CountDownLatch exceptionLatch = new CountDownLatch(1);
        try (ThreadContext.StoredContext ignore = context.newStoredContext(false)) {
            context.putHeader("foo", "bar");
            context.putTransient("bar", "baz");
            listener = new ThreadedActionListener<>(logger, threadPool, Names.GENERIC,
                ContextPreservingActionListener.wrap(ActionListener.wrap(r -> {
                    assertEquals("bar", context.getHeader("foo"));
                    assertEquals("baz", context.getTransient("bar"));
                    responseLatch.countDown();
                },
                e -> {
                    assertEquals("bar", context.getHeader("foo"));
                    assertEquals("baz", context.getTransient("bar"));
                    exceptionLatch.countDown();
                }), threadPool.getThreadContext(), false), randomBoolean());
        }

        assertNull(context.getHeader("foo"));
        assertNull(context.getTransient("bar"));
        assertEquals(1, responseLatch.getCount());

        listener.onResponse(null);
        responseLatch.await(1L, TimeUnit.HOURS);
        assertEquals(0, responseLatch.getCount());

        assertNull(context.getHeader("foo"));
        assertNull(context.getTransient("bar"));
        assertEquals(1, exceptionLatch.getCount());

        listener.onFailure(null);
        exceptionLatch.await(1L, TimeUnit.HOURS);
        assertEquals(0, exceptionLatch.getCount());
        assertNull(context.getHeader("foo"));
        assertNull(context.getTransient("bar"));
    }
}
