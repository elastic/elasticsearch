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
package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.EvilThreadPoolTests;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;

public class BaseFutureTests extends ESTestCase {

    private ThreadPool threadPool;

    @Before
    public void setUpThreadPool() {
        threadPool = new TestThreadPool(EvilThreadPoolTests.class.getName());
    }

    @After
    public void tearDownThreadPool() {
        terminate(threadPool);
    }

    public void testErrorCaught() throws InterruptedException {
        Consumer<Throwable> expected = t -> {
            assertThat(t, instanceOf(Error.class));
            assertThat(t, hasToString(containsString("future error")));
        };

        runExecutionTest(
            () -> new BaseFuture<>().completeExceptionally(new Error("future error")),
            expected);

        runExecutionTest(
            () -> {
                BaseFuture<Object> fut = new BaseFuture<>();
                fut.thenRun(() -> {
                    throw new Error("future error");
                });
                fut.complete(new Object());
            },
            expected);

        runExecutionTest(
            () -> {
                BaseFuture<Object> fut = new BaseFuture<>();
                fut.thenRunAsync(() -> {
                    throw new Error("future error");
                }, threadPool.generic());
                fut.complete(new Object());
            },
            expected);
    }

    private void runExecutionTest(final Runnable runnable, final Consumer<Throwable> consumer) throws InterruptedException {
        final AtomicReference<Throwable> throwableReference = new AtomicReference<>();
        final Thread.UncaughtExceptionHandler uncaughtExceptionHandler = Thread.getDefaultUncaughtExceptionHandler();
        final CountDownLatch uncaughtExceptionHandlerLatch = new CountDownLatch(1);

        try {
            Thread.setDefaultUncaughtExceptionHandler((t, e) -> {
                throwableReference.set(e);
                uncaughtExceptionHandlerLatch.countDown();
            });

            runnable.run();

            uncaughtExceptionHandlerLatch.await();
            consumer.accept(throwableReference.get());
        } finally {
            Thread.setDefaultUncaughtExceptionHandler(uncaughtExceptionHandler);
        }
    }

}
