/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.deployment;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.ml.inference.pytorch.process.PyTorchProcess;
import org.elasticsearch.xpack.ml.inference.pytorch.process.PyTorchResultProcessor;
import org.elasticsearch.xpack.ml.inference.pytorch.results.ThreadSettings;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ThreadSettingsControlMessagePytorchActionTests extends ESTestCase {

    public void testBuildControlMessage() throws IOException {
        DeploymentManager.ProcessContext processContext = mock(DeploymentManager.ProcessContext.class);
        ThreadPool tp = mock(ThreadPool.class);
        @SuppressWarnings("unchecked")
        ThreadSettingsControlMessagePytorchAction action = new ThreadSettingsControlMessagePytorchAction(
            "model_id",
            1,
            4,
            TimeValue.MINUS_ONE,
            processContext,
            tp,
            ActionListener.NOOP
        );
        var message = action.buildControlMessage("foo");
        assertEquals("{\"request_id\":\"foo\",\"control\":0,\"num_allocations\":4}", message.utf8ToString());
    }

    @SuppressWarnings("unchecked")
    public void testRunNotCalledAfterNotified() {
        DeploymentManager.ProcessContext processContext = mock(DeploymentManager.ProcessContext.class);
        PyTorchResultProcessor resultProcessor = mock(PyTorchResultProcessor.class);
        when(processContext.getResultProcessor()).thenReturn(resultProcessor);
        AtomicInteger timeoutCount = new AtomicInteger();
        when(processContext.getTimeoutCount()).thenReturn(timeoutCount);

        Scheduler.ScheduledCancellable cancellable = mock(Scheduler.ScheduledCancellable.class);
        ThreadPool tp = mock(ThreadPool.class);
        when(tp.schedule(any(), any(), any())).thenReturn(cancellable);

        {
            ActionListener<ThreadSettings> listener = mock(ActionListener.class);
            ThreadSettingsControlMessagePytorchAction action = new ThreadSettingsControlMessagePytorchAction(
                "test-model",
                1,
                1,
                TimeValue.MAX_VALUE,
                processContext,
                tp,
                listener
            );
            action.init();
            action.onTimeout();
            action.run();
            verify(resultProcessor, times(1)).ignoreResponseWithoutNotifying("1");
            verify(resultProcessor, never()).registerRequest(anyString(), any());
            verify(listener, times(1)).onFailure(any());
            verify(listener, never()).onResponse(any());
        }
        {
            ActionListener<ThreadSettings> listener = mock(ActionListener.class);
            ThreadSettingsControlMessagePytorchAction action = new ThreadSettingsControlMessagePytorchAction(
                "test-model",
                1,
                1,
                TimeValue.MAX_VALUE,
                processContext,
                tp,
                listener
            );
            action.init();

            action.onFailure(new IllegalStateException());
            action.run();
            verify(resultProcessor, never()).registerRequest(anyString(), any());
            verify(listener, times(1)).onFailure(any());
            verify(listener, never()).onResponse(any());
        }
    }

    @SuppressWarnings("unchecked")
    public void testDoRun() throws IOException {
        DeploymentManager.ProcessContext processContext = mock(DeploymentManager.ProcessContext.class);
        PyTorchResultProcessor resultProcessor = mock(PyTorchResultProcessor.class);
        when(processContext.getResultProcessor()).thenReturn(resultProcessor);
        AtomicInteger timeoutCount = new AtomicInteger();
        when(processContext.getTimeoutCount()).thenReturn(timeoutCount);
        SetOnce<PyTorchProcess> process = new SetOnce<>();
        PyTorchProcess pp = mock(PyTorchProcess.class);
        process.set(pp);
        when(processContext.getProcess()).thenReturn(process);

        Scheduler.ScheduledCancellable cancellable = mock(Scheduler.ScheduledCancellable.class);
        ThreadPool tp = mock(ThreadPool.class);
        when(tp.schedule(any(), any(), any())).thenReturn(cancellable);

        ActionListener<ThreadSettings> listener = mock(ActionListener.class);
        ArgumentCaptor<BytesReference> messageCapture = ArgumentCaptor.forClass(BytesReference.class);
        doNothing().when(pp).writeInferenceRequest(messageCapture.capture());

        ThreadSettingsControlMessagePytorchAction action = new ThreadSettingsControlMessagePytorchAction(
            "test-model",
            1,
            1,
            TimeValue.MAX_VALUE,
            processContext,
            tp,
            listener
        );
        action.init();

        action.run();

        verify(resultProcessor).registerRequest(eq("1"), any());
        verify(listener, never()).onFailure(any());
        assertEquals("{\"request_id\":\"1\",\"control\":0,\"num_allocations\":1}", messageCapture.getValue().utf8ToString());
    }
}
