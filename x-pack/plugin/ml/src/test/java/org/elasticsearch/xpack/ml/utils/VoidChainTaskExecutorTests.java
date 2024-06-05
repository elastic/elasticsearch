/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
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

public class VoidChainTaskExecutorTests extends ESTestCase {

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
        ActionListener<List<Void>> finalListener = createBlockingListener(() -> strings.add("last"), e -> fail());
        VoidChainTaskExecutor voidChainTaskExecutor = new VoidChainTaskExecutor(threadPool.generic(), false);
        voidChainTaskExecutor.add(listener -> {
            strings.add("first");
            listener.onResponse(null);
        });
        voidChainTaskExecutor.add(listener -> {
            strings.add("second");
            listener.onResponse(null);
        });

        voidChainTaskExecutor.execute(finalListener);

        latch.await();

        assertThat(strings, contains("first", "second", "last"));
    }

    public void testExecute_GivenSingleFailureAndShortCircuit() throws InterruptedException {
        final List<String> strings = new ArrayList<>();
        ActionListener<List<Void>> finalListener = createBlockingListener(
            () -> fail(),
            e -> assertThat(e.getMessage(), equalTo("some error"))
        );
        VoidChainTaskExecutor voidChainTaskExecutor = new VoidChainTaskExecutor(threadPool.generic(), true);
        voidChainTaskExecutor.add(listener -> {
            strings.add("before");
            listener.onResponse(null);
        });
        voidChainTaskExecutor.add(listener -> { throw new RuntimeException("some error"); });
        voidChainTaskExecutor.add(listener -> {
            strings.add("after");
            listener.onResponse(null);
        });

        voidChainTaskExecutor.execute(finalListener);

        latch.await();

        assertThat(strings, contains("before"));
    }

    public void testExecute_GivenMultipleFailuresAndShortCircuit() throws InterruptedException {
        final List<String> strings = new ArrayList<>();
        ActionListener<List<Void>> finalListener = createBlockingListener(
            () -> fail(),
            e -> assertThat(e.getMessage(), equalTo("some error 1"))
        );
        VoidChainTaskExecutor voidChainTaskExecutor = new VoidChainTaskExecutor(threadPool.generic(), true);
        voidChainTaskExecutor.add(listener -> {
            strings.add("before");
            listener.onResponse(null);
        });
        voidChainTaskExecutor.add(listener -> { throw new RuntimeException("some error 1"); });
        voidChainTaskExecutor.add(listener -> { throw new RuntimeException("some error 2"); });

        voidChainTaskExecutor.execute(finalListener);

        latch.await();

        assertThat(strings, contains("before"));
    }

    public void testExecute_GivenFailureAndNoShortCircuit() throws InterruptedException {
        final List<String> strings = new ArrayList<>();
        ActionListener<List<Void>> finalListener = createBlockingListener(() -> strings.add("last"), e -> fail());
        VoidChainTaskExecutor voidChainTaskExecutor = new VoidChainTaskExecutor(threadPool.generic(), false);
        voidChainTaskExecutor.add(listener -> {
            strings.add("before");
            listener.onResponse(null);
        });
        voidChainTaskExecutor.add(listener -> { throw new RuntimeException("some error"); });
        voidChainTaskExecutor.add(listener -> {
            strings.add("after");
            listener.onResponse(null);
        });

        voidChainTaskExecutor.execute(finalListener);

        latch.await();

        assertThat(strings, contains("before", "after", "last"));
    }

    public void testExecute_GivenNoTasksAdded() throws InterruptedException {
        final List<String> strings = new ArrayList<>();
        ActionListener<List<Void>> finalListener = createBlockingListener(() -> strings.add("last"), e -> fail());
        VoidChainTaskExecutor voidChainTaskExecutor = new VoidChainTaskExecutor(threadPool.generic(), false);

        voidChainTaskExecutor.execute(finalListener);

        latch.await();

        assertThat(strings, contains("last"));
    }

    private ActionListener<List<Void>> createBlockingListener(Runnable runnable, Consumer<Exception> errorHandler) {
        return ActionListener.wrap(nullValue -> {
            runnable.run();
            latch.countDown();
        }, e -> {
            errorHandler.accept(e);
            latch.countDown();
        });
    }
}
