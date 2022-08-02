/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.security.AccessControlException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class EsThreadPoolExecutorTests extends ESSingleNodeTestCase {

    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
            .put("node.name", "es-thread-pool-executor-tests")
            .put("thread_pool.write.size", 1)
            .put("thread_pool.write.queue_size", 0)
            .put("thread_pool.search.size", 1)
            .put("thread_pool.search.queue_size", 1)
            .build();
    }

    public void testRejectedExecutionExceptionContainsNodeName() {
        // we test a fixed and an auto-queue executor but not scaling since it does not reject
        runThreadPoolExecutorTest(1, ThreadPool.Names.WRITE);
        runThreadPoolExecutorTest(2, ThreadPool.Names.SEARCH);

    }

    private void runThreadPoolExecutorTest(final int fill, final String executor) {
        final CountDownLatch latch = new CountDownLatch(1);
        for (int i = 0; i < fill; i++) {
            node().injector().getInstance(ThreadPool.class).executor(executor).execute(() -> {
                try {
                    latch.await();
                } catch (final InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        final AtomicBoolean rejected = new AtomicBoolean();
        node().injector().getInstance(ThreadPool.class).executor(executor).execute(new AbstractRunnable() {
            @Override
            public void onFailure(final Exception e) {

            }

            @Override
            public void onRejection(final Exception e) {
                rejected.set(true);
                assertThat(e, hasToString(containsString("name = es-thread-pool-executor-tests/" + executor + ", ")));
            }

            @Override
            protected void doRun() throws Exception {

            }
        });

        latch.countDown();
        assertTrue(rejected.get());
    }

    public void testExecuteThrowsException() {
        final RuntimeException exception = randomFrom(
            new RuntimeException("unexpected"),
            new AccessControlException("unexpected"),
            new EsRejectedExecutionException("unexpected")
        );

        final ThrowingEsThreadPoolExecutor executor = new ThrowingEsThreadPoolExecutor(getTestName(), 0, 1, exception);
        try {
            final AtomicBoolean doRun = new AtomicBoolean();
            final AtomicBoolean onAfter = new AtomicBoolean();
            final AtomicReference<Exception> onFailure = new AtomicReference<>();
            final AtomicReference<Exception> onRejection = new AtomicReference<>();

            executor.execute(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    onFailure.set(e);
                }

                @Override
                public void onRejection(Exception e) {
                    onRejection.set(e);
                }

                @Override
                protected void doRun() {
                    doRun.set(true);
                }

                @Override
                public void onAfter() {
                    onAfter.set(true);
                }
            });

            assertThat(doRun.get(), equalTo(false));
            assertThat(onAfter.get(), equalTo(true));
            assertThat(onFailure.get(), nullValue());
            assertThat(onRejection.get(), sameInstance(exception));
            assertThat(
                executor.lastLoggedException.get(),
                exception instanceof EsRejectedExecutionException ? nullValue() : sameInstance(exception)
            );
        } finally {
            terminate(executor);
        }
    }

    /**
     * EsThreadPoolExecutor that throws a given exception, preventing {@link Runnable} to be added to the thread pool work queue.
     */
    private class ThrowingEsThreadPoolExecutor extends EsThreadPoolExecutor {

        final AtomicReference<Exception> lastLoggedException = new AtomicReference<>();

        ThrowingEsThreadPoolExecutor(String name, int corePoolSize, int maximumPoolSize, RuntimeException exception) {
            super(name, corePoolSize, maximumPoolSize, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>() {
                @Override
                public boolean offer(Runnable r) {
                    throw exception;
                }
            }, EsExecutors.daemonThreadFactory("test"), new ThreadContext(Settings.EMPTY));
        }

        @Override
        void logException(AbstractRunnable task, Exception e) {
            lastLoggedException.set(e);
        }
    }
}
