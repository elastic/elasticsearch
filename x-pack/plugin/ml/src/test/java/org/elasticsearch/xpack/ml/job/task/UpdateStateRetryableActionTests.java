/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.job.task;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.config.JobTaskState;
import org.junit.After;
import org.junit.Before;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class UpdateStateRetryableActionTests extends ESTestCase {

    private ThreadPool threadPool;
    private JobTask jobTask;
    private JobTaskState jobTaskState;

    @Before
    public void setup() {
        threadPool = new TestThreadPool("test");
        jobTask = mock(JobTask.class);
        jobTaskState = new JobTaskState(JobState.FAILED, 1L, "test reason", Instant.now());
    }

    @After
    public void teardown() {
        threadPool.shutdownNow();
    }

    public void testShouldRetry_returnsTrue_forGenericException() {
        var action = new UpdateStateRetryableAction(logger, threadPool, jobTask, jobTaskState, ActionListener.noop());
        assertTrue(action.shouldRetry(new RuntimeException("transient")));
        assertTrue(action.shouldRetry(new IllegalStateException("state error")));
        assertTrue(
            action.shouldRetry(
                new org.elasticsearch.ElasticsearchStatusException(
                    "service unavailable",
                    org.elasticsearch.rest.RestStatus.SERVICE_UNAVAILABLE
                )
            )
        );
    }

    public void testShouldRetry_returnsFalse_forResourceNotFoundException() {
        var action = new UpdateStateRetryableAction(logger, threadPool, jobTask, jobTaskState, ActionListener.noop());
        assertFalse(action.shouldRetry(new ResourceNotFoundException("task not found")));
    }

    public void testShouldRetry_returnsFalse_forWrappedResourceNotFoundException() {
        var action = new UpdateStateRetryableAction(logger, threadPool, jobTask, jobTaskState, ActionListener.noop());
        var wrapped = new org.elasticsearch.transport.RemoteTransportException("remote", new ResourceNotFoundException("task not found"));
        assertFalse(action.shouldRetry(wrapped));
    }

    public void testTryAction_callsUpdatePersistentTaskState() {
        var listenerCalled = new AtomicBoolean(false);
        @SuppressWarnings("unchecked")
        ActionListener<PersistentTasksCustomMetadata.PersistentTask<?>> listener = ActionListener.wrap(
            r -> listenerCalled.set(true),
            e -> fail("unexpected failure: " + e)
        );

        doAnswer(invocation -> {
            ActionListener<PersistentTasksCustomMetadata.PersistentTask<?>> l = invocation.getArgument(1);
            l.onResponse(null);
            return null;
        }).when(jobTask).updatePersistentTaskState(any(), any());

        var action = new UpdateStateRetryableAction(logger, threadPool, jobTask, jobTaskState, listener);
        action.tryAction(listener);

        verify(jobTask).updatePersistentTaskState(any(), any());
        assertTrue(listenerCalled.get());
    }

    public void testCustomTimeout_constructor_usesProvidedTimeout() {
        var customTimeout = TimeValue.timeValueMinutes(60);
        // Just verify the action can be constructed with a custom timeout (no exception thrown)
        var action = new UpdateStateRetryableAction(logger, threadPool, jobTask, jobTaskState, customTimeout, ActionListener.noop());
        assertNotNull(action);
    }

    public void testDefaultTimeout_usesDefaultLongTimeout() {
        // Default constructor uses PERSISTENT_TASK_MASTER_NODE_TIMEOUT (365 days)
        // Verify it can be constructed without specifying a timeout
        var action = new UpdateStateRetryableAction(logger, threadPool, jobTask, jobTaskState, ActionListener.noop());
        assertNotNull(action);
    }
}
