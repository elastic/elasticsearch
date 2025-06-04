/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exception.ElasticsearchTimeoutException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;

import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class ListenerTimeoutsTests extends ESTestCase {

    private final TimeValue timeout = TimeValue.timeValueMillis(10);
    private DeterministicTaskQueue taskQueue;
    private ThreadPool threadPool;
    private Executor timeoutExecutor;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        taskQueue = new DeterministicTaskQueue();
        threadPool = taskQueue.getThreadPool();
        timeoutExecutor = threadPool.generic();
    }

    public void testListenerTimeout() {
        AtomicBoolean success = new AtomicBoolean(false);
        AtomicReference<Exception> exception = new AtomicReference<>();
        ActionListener<Void> listener = wrap(success, exception);

        ActionListener<Void> wrapped = ListenerTimeouts.wrapWithTimeout(threadPool, listener, timeout, timeoutExecutor, "test");
        assertTrue(taskQueue.hasDeferredTasks());
        taskQueue.advanceTime();
        taskQueue.runAllRunnableTasks();

        wrapped.onResponse(null);
        wrapped.onFailure(new IOException("incorrect exception"));

        assertFalse(success.get());
        assertThat(exception.get(), instanceOf(ElasticsearchTimeoutException.class));
    }

    public void testFinishNormallyBeforeTimeout() {
        AtomicBoolean success = new AtomicBoolean(false);
        AtomicReference<Exception> exception = new AtomicReference<>();
        ActionListener<Void> listener = wrap(success, exception);

        ActionListener<Void> wrapped = ListenerTimeouts.wrapWithTimeout(threadPool, listener, timeout, timeoutExecutor, "test");
        wrapped.onResponse(null);
        wrapped.onFailure(new IOException("boom"));
        wrapped.onResponse(null);

        assertTrue(taskQueue.hasDeferredTasks());
        taskQueue.advanceTime();
        taskQueue.runAllRunnableTasks();

        assertTrue(success.get());
        assertNull(exception.get());
    }

    public void testFinishExceptionallyBeforeTimeout() {
        AtomicBoolean success = new AtomicBoolean(false);
        AtomicReference<Exception> exception = new AtomicReference<>();
        ActionListener<Void> listener = wrap(success, exception);

        ActionListener<Void> wrapped = ListenerTimeouts.wrapWithTimeout(threadPool, listener, timeout, timeoutExecutor, "test");
        wrapped.onFailure(new IOException("boom"));

        assertTrue(taskQueue.hasDeferredTasks());
        taskQueue.advanceTime();
        taskQueue.runAllRunnableTasks();

        assertFalse(success.get());
        assertThat(exception.get(), instanceOf(IOException.class));
    }

    private ActionListener<Void> wrap(AtomicBoolean success, AtomicReference<Exception> exception) {
        return new ActionListener<Void>() {

            private final AtomicBoolean completed = new AtomicBoolean();

            @Override
            public void onResponse(Void aVoid) {
                assertTrue(completed.compareAndSet(false, true));
                assertTrue(success.compareAndSet(false, true));
            }

            @Override
            public void onFailure(Exception e) {
                assertTrue(completed.compareAndSet(false, true));
                assertTrue(exception.compareAndSet(null, e));
            }
        };
    }
}
