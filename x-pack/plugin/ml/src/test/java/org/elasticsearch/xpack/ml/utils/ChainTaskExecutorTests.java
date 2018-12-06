/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.utils;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

public class ChainTaskExecutorTests extends ESTestCase {

    private final ThreadPool threadPool = new TestThreadPool(getClass().getName());
    private final CountDownLatch latch = new CountDownLatch(1);

    @Override
    @After
    public void tearDown() throws Exception {
        try {
            terminate(threadPool);
        } finally {
            super.tearDown();
        }
    }

    public void testExecute() throws InterruptedException {
        final List<String> strings = new ArrayList<>();
        ActionListener<Void> finalListener = createBlockingListener(() -> strings.add("last"), e -> fail());
        ChainTaskExecutor chainTaskExecutor = new ChainTaskExecutor(threadPool.generic(), false);
        chainTaskExecutor.add(listener -> {
            strings.add("first");
            listener.onResponse(null);
        });
        chainTaskExecutor.add(listener -> {
            strings.add("second");
            listener.onResponse(null);
        });

        chainTaskExecutor.execute(finalListener);

        latch.await();

        assertThat(strings, contains("first", "second", "last"));
    }

    public void testExecute_GivenSingleFailureAndShortCircuit() throws InterruptedException {
        final List<String> strings = new ArrayList<>();
        ActionListener<Void> finalListener = createBlockingListener(() -> fail(),
                e -> assertThat(e.getMessage(), equalTo("some error")));
        ChainTaskExecutor chainTaskExecutor = new ChainTaskExecutor(threadPool.generic(), true);
        chainTaskExecutor.add(listener -> {
            strings.add("before");
            listener.onResponse(null);
        });
        chainTaskExecutor.add(listener -> {
            throw new RuntimeException("some error");
        });
        chainTaskExecutor.add(listener -> {
            strings.add("after");
            listener.onResponse(null);
        });

        chainTaskExecutor.execute(finalListener);

        latch.await();

        assertThat(strings, contains("before"));
    }

    public void testExecute_GivenMultipleFailuresAndShortCircuit() throws InterruptedException {
        final List<String> strings = new ArrayList<>();
        ActionListener<Void> finalListener = createBlockingListener(() -> fail(),
                e -> assertThat(e.getMessage(), equalTo("some error 1")));
        ChainTaskExecutor chainTaskExecutor = new ChainTaskExecutor(threadPool.generic(), true);
        chainTaskExecutor.add(listener -> {
            strings.add("before");
            listener.onResponse(null);
        });
        chainTaskExecutor.add(listener -> {
            throw new RuntimeException("some error 1");
        });
        chainTaskExecutor.add(listener -> {
            throw new RuntimeException("some error 2");
        });

        chainTaskExecutor.execute(finalListener);

        latch.await();

        assertThat(strings, contains("before"));
    }

    public void testExecute_GivenFailureAndNoShortCircuit() throws InterruptedException {
        final List<String> strings = new ArrayList<>();
        ActionListener<Void> finalListener = createBlockingListener(() -> strings.add("last"), e -> fail());
        ChainTaskExecutor chainTaskExecutor = new ChainTaskExecutor(threadPool.generic(), false);
        chainTaskExecutor.add(listener -> {
            strings.add("before");
            listener.onResponse(null);
        });
        chainTaskExecutor.add(listener -> {
            throw new RuntimeException("some error");
        });
        chainTaskExecutor.add(listener -> {
            strings.add("after");
            listener.onResponse(null);
        });

        chainTaskExecutor.execute(finalListener);

        latch.await();

        assertThat(strings, contains("before", "after", "last"));
    }

    public void testExecute_GivenNoTasksAdded() throws InterruptedException {
        final List<String> strings = new ArrayList<>();
        ActionListener<Void> finalListener = createBlockingListener(() -> strings.add("last"), e -> fail());
        ChainTaskExecutor chainTaskExecutor = new ChainTaskExecutor(threadPool.generic(), false);

        chainTaskExecutor.execute(finalListener);

        latch.await();

        assertThat(strings, contains("last"));
    }

    private ActionListener<Void> createBlockingListener(Runnable runnable, Consumer<Exception> errorHandler) {
        return ActionListener.wrap(nullValue -> {
            runnable.run();
            latch.countDown();
        }, e -> {
            errorHandler.accept(e);
            latch.countDown();
        });
    }
}