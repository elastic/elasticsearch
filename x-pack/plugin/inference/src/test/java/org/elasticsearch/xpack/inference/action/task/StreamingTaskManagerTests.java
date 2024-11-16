/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action.task;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskAwareRequest;
import org.elasticsearch.tasks.TaskCancelHelper;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.util.Map;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StreamingTaskManagerTests extends ESTestCase {
    private static final String taskType = "taskType";
    private static final String taskAction = "taskAction";

    private TaskManager taskManager;
    private StreamingTaskManager streamingTaskManager;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        taskManager = mock();
        ThreadPool threadPool = mock();
        streamingTaskManager = new StreamingTaskManager(taskManager, threadPool);

        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        doAnswer(ans -> {
            TaskAwareRequest taskAwareRequest = ans.getArgument(2);
            return taskAwareRequest.createTask(1L, taskType, taskAction, TaskId.EMPTY_TASK_ID, Map.of());
        }).when(taskManager).register(any(), any(), any());
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    public void testSubscribeRegistersTask() {
        var processor = streamingTaskManager.create(taskType, taskAction);

        processor.subscribe(mock());

        verify(taskManager, only()).register(eq(taskType), eq(taskAction), any());
    }

    public void testCancelPropagatesUpstreamAndDownstream() {
        var task = new AtomicReference<CancellableTask>();
        doAnswer(ans -> {
            TaskAwareRequest taskAwareRequest = ans.getArgument(2);
            var registeredTask = (CancellableTask) taskAwareRequest.createTask(1L, taskType, taskAction, TaskId.EMPTY_TASK_ID, Map.of());
            task.set(registeredTask);
            return registeredTask;
        }).when(taskManager).register(any(), any(), any());

        Flow.Subscriber<Object> downstream = mock();
        Flow.Subscription upstream = mock();

        var processor = streamingTaskManager.create(taskType, taskAction);
        processor.subscribe(downstream);
        processor.onSubscribe(upstream);

        TaskCancelHelper.cancel(task.get(), "for testing");

        verify(downstream, times(1)).onComplete();
        verify(upstream, times(1)).cancel();
    }

    public void testRequestBeforeOnSubscribe() {
        var processor = streamingTaskManager.create(taskType, taskAction);
        var expectedRequestCount = randomLongBetween(2, 100);

        Flow.Subscriber<Object> downstream = mock();
        processor.subscribe(downstream);

        var subscription = ArgumentCaptor.forClass(Flow.Subscription.class);
        verify(downstream, times(1)).onSubscribe(subscription.capture());
        subscription.getValue().request(expectedRequestCount);

        Flow.Subscription upstream = mock();
        processor.onSubscribe(upstream);
        verify(upstream, times(1)).request(eq(expectedRequestCount));
    }

    public void testRequestAfterOnSubscribe() {
        var processor = streamingTaskManager.create(taskType, taskAction);
        var expectedRequestCount = randomLongBetween(2, 100);

        Flow.Subscription upstream = mock();
        processor.onSubscribe(upstream);
        verify(upstream, never()).request(anyInt());

        Flow.Subscriber<Object> downstream = mock();
        processor.subscribe(downstream);

        var subscription = ArgumentCaptor.forClass(Flow.Subscription.class);
        verify(downstream, times(1)).onSubscribe(subscription.capture());

        subscription.getValue().request(expectedRequestCount);
        verify(upstream, times(1)).request(eq(expectedRequestCount));
    }

    public void testOnErrorUnregistersTask() {
        var expectedError = new IllegalStateException("blah");
        var processor = streamingTaskManager.create(taskType, taskAction);
        var downstream = establishFlow(processor);

        processor.onError(expectedError);

        verify(downstream, times(1)).onError(expectedError);
        verify(taskManager, times(1)).unregister(any());
    }

    private Flow.Subscriber<Object> establishFlow(Flow.Processor<Object, Object> processor) {
        Flow.Subscription upstream = mock();
        processor.onSubscribe(upstream);
        Flow.Subscriber<Object> downstream = mock();
        processor.subscribe(downstream);
        return downstream;
    }

    public void testOnCompleteUnregistersTask() {
        var processor = streamingTaskManager.create(taskType, taskAction);
        var downstream = establishFlow(processor);

        processor.onComplete();

        verify(downstream, times(1)).onComplete();
        verify(taskManager, times(1)).unregister(any());
    }

    public void testOnNextForwardsItem() {
        var expectedItem = new Object();
        var processor = streamingTaskManager.create(taskType, taskAction);
        var downstream = establishFlow(processor);

        processor.onNext(expectedItem);

        verify(downstream, times(1)).onNext(same(expectedItem));
    }

    public void testOnNextAfterCancelDoesNotForwardItem() {
        var expectedItem = new Object();
        var task = new AtomicReference<CancellableTask>();
        doAnswer(ans -> {
            TaskAwareRequest taskAwareRequest = ans.getArgument(2);
            var registeredTask = (CancellableTask) taskAwareRequest.createTask(1L, taskType, taskAction, TaskId.EMPTY_TASK_ID, Map.of());
            task.set(registeredTask);
            return registeredTask;
        }).when(taskManager).register(any(), any(), any());

        var processor = streamingTaskManager.create(taskType, taskAction);
        var downstream = establishFlow(processor);

        TaskCancelHelper.cancel(task.get(), "test");
        processor.onNext(expectedItem);

        verify(downstream, never()).onNext(any());
    }

    public void testCancelBeforeSubscriptionThrowsException() {
        var processor = streamingTaskManager.create(taskType, taskAction);

        assertThrows(IllegalStateException.class, () -> processor.onError(new NullPointerException()));
    }
}
