/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.service;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateAckListener;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Releasable;

import java.util.Collection;
import java.util.function.Consumer;

import static org.elasticsearch.test.ESTestCase.fail;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

/**
 * Utilities for running a {@link ClusterStateTaskExecutor} in tests.
 */
public class ClusterStateTaskExecutorUtils {

    private ClusterStateTaskExecutorUtils() {
        throw new UnsupportedOperationException("utils class, do not instantiate");
    }

    public static <T extends ClusterStateTaskListener> ClusterState executeAndAssertSuccessful(
        ClusterState originalState,
        ClusterStateTaskExecutor<T> executor,
        Collection<T> tasks
    ) throws Exception {
        return executeHandlingResults(originalState, executor, tasks, task -> {}, (task, e) -> fail(e));
    }

    public static <T extends ClusterStateTaskListener> ClusterState executeAndThrowFirstFailure(
        ClusterState originalState,
        ClusterStateTaskExecutor<T> executor,
        Collection<T> tasks
    ) throws Exception {
        return executeHandlingResults(originalState, executor, tasks, task -> {}, (task, e) -> { throw e; });
    }

    public static <T extends ClusterStateTaskListener> ClusterState executeIgnoringFailures(
        ClusterState originalState,
        ClusterStateTaskExecutor<T> executor,
        Collection<T> tasks
    ) throws Exception {
        return executeHandlingResults(originalState, executor, tasks, task -> {}, (task, e) -> {});
    }

    public static <T extends ClusterStateTaskListener> ClusterState executeHandlingResults(
        ClusterState originalState,
        ClusterStateTaskExecutor<T> executor,
        Collection<T> tasks,
        CheckedConsumer<T, Exception> onTaskSuccess,
        CheckedBiConsumer<T, Exception, Exception> onTaskFailure
    ) throws Exception {
        final var taskContexts = tasks.stream().map(TestTaskContext::new).toList();
        ClusterState resultingState = executor.execute(
            new ClusterStateTaskExecutor.BatchExecutionContext<>(originalState, taskContexts, () -> null)
        );
        assertNotNull(resultingState);
        boolean allSuccess = true;
        for (final var testTaskContext : taskContexts) {
            assertFalse(testTaskContext + " should have completed", testTaskContext.incomplete());
            if (testTaskContext.succeeded()) {
                onTaskSuccess.accept(testTaskContext.getTask());
            } else {
                onTaskFailure.accept(testTaskContext.getTask(), testTaskContext.getFailure());
                allSuccess = false;
            }
        }

        if (allSuccess) {
            taskContexts.forEach(TestTaskContext::onPublishSuccess);
        }

        return resultingState;
    }

    private static class TestTaskContext<T extends ClusterStateTaskListener> implements ClusterStateTaskExecutor.TaskContext<T> {
        private final T task;
        private Exception failure;
        private boolean succeeded;
        private Runnable onPublishSuccess;

        TestTaskContext(T task) {
            this.task = task;
        }

        @Override
        public T getTask() {
            return task;
        }

        boolean incomplete() {
            return succeeded == false && failure == null;
        }

        boolean succeeded() {
            return succeeded;
        }

        Exception getFailure() {
            assert failure != null;
            return failure;
        }

        void onPublishSuccess() {
            assert onPublishSuccess != null;
            onPublishSuccess.run();
        }

        @Override
        public void onFailure(Exception failure) {
            assert incomplete();
            assert failure != null;
            this.failure = failure;
        }

        @Override
        public void success(Runnable onPublishSuccess, ClusterStateAckListener clusterStateAckListener) {
            assert incomplete();
            assert onPublishSuccess != null;
            assert clusterStateAckListener != null;
            assert task == clusterStateAckListener || (task instanceof ClusterStateAckListener == false);
            this.succeeded = true;
            this.onPublishSuccess = onPublishSuccess;
        }

        @Override
        public void success(Runnable onPublishSuccess) {
            assert incomplete();
            assert onPublishSuccess != null;
            assert task instanceof ClusterStateAckListener == false;
            this.succeeded = true;
            this.onPublishSuccess = onPublishSuccess;
        }

        @Override
        public void success(Consumer<ClusterState> publishedStateListener, ClusterStateAckListener clusterStateAckListener) {
            assert incomplete();
            assert publishedStateListener != null;
            assert clusterStateAckListener != null;
            assert task == clusterStateAckListener || (task instanceof ClusterStateAckListener == false);
            this.succeeded = true;
        }

        @Override
        public void success(Consumer<ClusterState> publishedStateListener) {
            assert incomplete();
            assert publishedStateListener != null;
            assert task instanceof ClusterStateAckListener == false;
            this.succeeded = true;
        }

        @Override
        public Releasable captureResponseHeaders() {
            return () -> {};
        }

        @Override
        public String toString() {
            return "TestTaskContext[" + task + "]";
        }
    }
}
