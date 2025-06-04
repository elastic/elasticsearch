/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.inference.ChunkInferenceInput;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.InputTypeTests;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityPool;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class RequestTaskTests extends ESTestCase {

    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);
    private ThreadPool threadPool;

    @Before
    public void init() throws Exception {
        threadPool = createThreadPool(inferenceUtilityPool());
    }

    @After
    public void shutdown() {
        terminate(threadPool);
    }

    public void testExecuting_DoesNotCallOnFailureForTimeout_AfterIllegalArgumentException() {
        AtomicReference<Runnable> onTimeout = new AtomicReference<>();
        var mockThreadPool = mockThreadPoolForTimeout(onTimeout);

        @SuppressWarnings("unchecked")
        ActionListener<InferenceServiceResults> listener = mock(ActionListener.class);

        var requestTask = new RequestTask(
            OpenAiEmbeddingsRequestManagerTests.makeCreator("url", null, "key", "model", null, "id", threadPool),
            new EmbeddingsInput(List.of(new ChunkInferenceInput("abc")), InputTypeTests.randomWithNull()),
            TimeValue.timeValueMillis(1),
            mockThreadPool,
            listener
        );

        requestTask.getListener().onFailure(new IllegalArgumentException("failed"));
        verify(listener, times(1)).onFailure(any());
        assertTrue(requestTask.hasCompleted());
        assertTrue(requestTask.getRequestCompletedFunction().get());

        onTimeout.get().run();
        verifyNoMoreInteractions(listener);
    }

    public void testRequest_ReturnsTimeoutException() {

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        var requestTask = new RequestTask(
            OpenAiEmbeddingsRequestManagerTests.makeCreator("url", null, "key", "model", null, "id", threadPool),
            new EmbeddingsInput(List.of(new ChunkInferenceInput("abc")), InputTypeTests.randomWithNull()),
            TimeValue.timeValueMillis(1),
            threadPool,
            listener
        );

        var thrownException = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TIMEOUT));
        assertThat(thrownException.getMessage(), is(format("Request timed out after [%s]", TimeValue.timeValueMillis(1))));
        assertTrue(requestTask.hasCompleted());
        assertTrue(requestTask.getRequestCompletedFunction().get());
        assertThat(thrownException.status().getStatus(), is(408));
    }

    public void testRequest_DoesNotCallOnFailureTwiceWhenTimingOut() throws Exception {
        @SuppressWarnings("unchecked")
        ActionListener<InferenceServiceResults> listener = mock(ActionListener.class);
        var calledOnFailureLatch = new CountDownLatch(1);
        doAnswer(invocation -> {
            calledOnFailureLatch.countDown();
            return Void.TYPE;
        }).when(listener).onFailure(any());

        var requestTask = new RequestTask(
            OpenAiEmbeddingsRequestManagerTests.makeCreator("url", null, "key", "model", null, "id", threadPool),
            new EmbeddingsInput(List.of(new ChunkInferenceInput("abc")), InputTypeTests.randomWithNull()),
            TimeValue.timeValueMillis(1),
            threadPool,
            listener
        );

        calledOnFailureLatch.await(TIMEOUT.millis(), TimeUnit.MILLISECONDS);

        ArgumentCaptor<Exception> argument = ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(argument.capture());
        assertThat(argument.getValue().getMessage(), is(format("Request timed out after [%s]", TimeValue.timeValueMillis(1))));
        assertTrue(requestTask.hasCompleted());
        assertTrue(requestTask.getRequestCompletedFunction().get());

        requestTask.getListener().onFailure(new IllegalArgumentException("failed"));
        verifyNoMoreInteractions(listener);
    }

    public void testRequest_DoesNotCallOnResponseAfterTimingOut() throws Exception {
        @SuppressWarnings("unchecked")
        ActionListener<InferenceServiceResults> listener = mock(ActionListener.class);
        var calledOnFailureLatch = new CountDownLatch(1);
        doAnswer(invocation -> {
            calledOnFailureLatch.countDown();
            return Void.TYPE;
        }).when(listener).onFailure(any());

        var requestTask = new RequestTask(
            OpenAiEmbeddingsRequestManagerTests.makeCreator("url", null, "key", "model", null, "id", threadPool),
            new EmbeddingsInput(List.of(new ChunkInferenceInput("abc")), InputTypeTests.randomWithNull()),
            TimeValue.timeValueMillis(1),
            threadPool,
            listener
        );

        calledOnFailureLatch.await(TIMEOUT.millis(), TimeUnit.MILLISECONDS);

        ArgumentCaptor<Exception> argument = ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(argument.capture());
        assertThat(argument.getValue().getMessage(), is(format("Request timed out after [%s]", TimeValue.timeValueMillis(1))));
        assertTrue(requestTask.hasCompleted());
        assertTrue(requestTask.getRequestCompletedFunction().get());

        requestTask.getListener().onResponse(mock(InferenceServiceResults.class));
        verifyNoMoreInteractions(listener);
    }

    public void testRequest_DoesNotCallOnFailureForTimeout_AfterAlreadyCallingOnResponse() throws Exception {
        AtomicReference<Runnable> onTimeout = new AtomicReference<>();
        var mockThreadPool = mockThreadPoolForTimeout(onTimeout);

        @SuppressWarnings("unchecked")
        ActionListener<InferenceServiceResults> listener = mock(ActionListener.class);

        var requestTask = new RequestTask(
            OpenAiEmbeddingsRequestManagerTests.makeCreator("url", null, "key", "model", null, "id", threadPool),
            new EmbeddingsInput(List.of(new ChunkInferenceInput("abc")), InputTypeTests.randomWithNull()),
            TimeValue.timeValueMillis(1),
            mockThreadPool,
            listener
        );

        requestTask.getListener().onResponse(mock(InferenceServiceResults.class));
        verify(listener, times(1)).onResponse(any());
        assertTrue(requestTask.hasCompleted());
        assertTrue(requestTask.getRequestCompletedFunction().get());

        onTimeout.get().run();
        verifyNoMoreInteractions(listener);
    }

    private ThreadPool mockThreadPoolForTimeout(AtomicReference<Runnable> onTimeoutRunnable) {
        var mockThreadPool = mock(ThreadPool.class);
        when(mockThreadPool.executor(any())).thenReturn(mock(ExecutorService.class));
        when(mockThreadPool.getThreadContext()).thenReturn(threadPool.getThreadContext());

        doAnswer(invocation -> {
            Runnable runnable = (Runnable) invocation.getArguments()[0];
            onTimeoutRunnable.set(runnable);
            return mock(Scheduler.ScheduledCancellable.class);
        }).when(mockThreadPool).schedule(any(Runnable.class), any(), any());

        return mockThreadPool;
    }
}
