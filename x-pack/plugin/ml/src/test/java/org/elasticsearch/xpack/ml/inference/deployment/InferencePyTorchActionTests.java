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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskAwareRequest;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelInput;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelPrefixStrings;
import org.elasticsearch.xpack.core.ml.inference.results.WarningInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.PassThroughConfig;
import org.elasticsearch.xpack.ml.inference.nlp.NlpTask;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.TokenizationResult;
import org.elasticsearch.xpack.ml.inference.pytorch.process.PyTorchProcess;
import org.elasticsearch.xpack.ml.inference.pytorch.process.PyTorchResultProcessor;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.xpack.ml.MachineLearning.UTILITY_THREAD_POOL_NAME;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class InferencePyTorchActionTests extends ESTestCase {

    private ThreadPool tp;

    @Before
    public void managerSetup() {
        tp = new TestThreadPool(
            "DeploymentManagerTests",
            new ScalingExecutorBuilder(
                UTILITY_THREAD_POOL_NAME,
                1,
                4,
                TimeValue.timeValueMinutes(10),
                false,
                "xpack.ml.utility_thread_pool"
            )
        );
    }

    @After
    public void shutdownThreadpool() {
        tp.shutdown();
    }

    public void testInferListenerOnlyCalledOnce() {
        DeploymentManager.ProcessContext processContext = mock(DeploymentManager.ProcessContext.class);
        PyTorchResultProcessor resultProcessor = new PyTorchResultProcessor("1", threadSettings -> {});
        when(processContext.getResultProcessor()).thenReturn(resultProcessor);
        AtomicInteger timeoutCount = new AtomicInteger();
        when(processContext.getTimeoutCount()).thenReturn(timeoutCount);

        TestListenerCounter listener = new TestListenerCounter();
        InferencePyTorchAction action = new InferencePyTorchAction(
            "test-model",
            1,
            TimeValue.MAX_VALUE,
            processContext,
            new PassThroughConfig(null, null, null),
            NlpInferenceInput.fromText("foo"),
            TrainedModelPrefixStrings.PrefixType.NONE,
            tp,
            null,
            listener
        );
        action.init();
        action.onSuccess(new WarningInferenceResults("foo"));
        for (int i = 0; i < 10; i++) {
            action.onSuccess(new WarningInferenceResults("foo"));
            action.onFailure(new Exception("foo"));
            action.onTimeout();
        }
        assertThat(listener.failureCounts, equalTo(0));
        assertThat(listener.responseCounts, equalTo(1));

        action = new InferencePyTorchAction(
            "test-model",
            1,
            TimeValue.MAX_VALUE,
            processContext,
            new PassThroughConfig(null, null, null),
            NlpInferenceInput.fromText("foo"),
            TrainedModelPrefixStrings.PrefixType.NONE,
            tp,
            null,
            listener
        );
        action.init();
        action.onTimeout();
        for (int i = 0; i < 10; i++) {
            action.onSuccess(new WarningInferenceResults("foo"));
            action.onFailure(new Exception("foo"));
            action.onTimeout();
        }
        assertThat(listener.failureCounts, equalTo(1));
        assertThat(listener.responseCounts, equalTo(1));
        assertThat(timeoutCount.intValue(), equalTo(1));

        action = new InferencePyTorchAction(
            "test-model",
            1,
            TimeValue.MAX_VALUE,
            processContext,
            new PassThroughConfig(null, null, null),
            NlpInferenceInput.fromText("foo"),
            TrainedModelPrefixStrings.PrefixType.NONE,
            tp,
            null,
            listener
        );
        action.init();
        action.onFailure(new Exception("bar"));
        for (int i = 0; i < 10; i++) {
            action.onSuccess(new WarningInferenceResults("foo"));
            action.onFailure(new Exception("foo"));
            action.onTimeout();
        }
        assertThat(listener.failureCounts, equalTo(2));
        assertThat(listener.responseCounts, equalTo(1));
    }

    public void testRunNotCalledAfterNotified() {
        DeploymentManager.ProcessContext processContext = mock(DeploymentManager.ProcessContext.class);
        PyTorchResultProcessor resultProcessor = mock(PyTorchResultProcessor.class);
        when(processContext.getResultProcessor()).thenReturn(resultProcessor);
        AtomicInteger timeoutCount = new AtomicInteger();
        when(processContext.getTimeoutCount()).thenReturn(timeoutCount);

        TestListenerCounter listener = new TestListenerCounter();
        {
            InferencePyTorchAction action = new InferencePyTorchAction(
                "test-model",
                1,
                TimeValue.MAX_VALUE,
                processContext,
                new PassThroughConfig(null, null, null),
                NlpInferenceInput.fromText("foo"),
                TrainedModelPrefixStrings.PrefixType.NONE,
                tp,
                null,
                listener
            );
            action.init();
            action.onTimeout();
            action.run();
            verify(resultProcessor, times(1)).ignoreResponseWithoutNotifying("1");
            verify(resultProcessor, never()).registerRequest(anyString(), any());
        }
        {
            InferencePyTorchAction action = new InferencePyTorchAction(
                "test-model",
                1,
                TimeValue.MAX_VALUE,
                processContext,
                new PassThroughConfig(null, null, null),
                NlpInferenceInput.fromText("foo"),
                TrainedModelPrefixStrings.PrefixType.NONE,
                tp,
                null,
                listener
            );
            action.init();
            action.onFailure(new IllegalStateException());
            action.run();
            verify(resultProcessor, never()).registerRequest(anyString(), any());
        }
    }

    public void testCallingRunAfterParentTaskCancellation() throws Exception {
        DeploymentManager.ProcessContext processContext = mock(DeploymentManager.ProcessContext.class);
        PyTorchResultProcessor resultProcessor = mock(PyTorchResultProcessor.class);
        when(processContext.getResultProcessor()).thenReturn(resultProcessor);
        AtomicInteger timeoutCount = new AtomicInteger();
        when(processContext.getTimeoutCount()).thenReturn(timeoutCount);
        TaskManager taskManager = new TaskManager(Settings.EMPTY, tp, Set.of());
        TestListenerCounter listener = new TestListenerCounter();
        CancellableTask cancellableTask = (CancellableTask) taskManager.register("test_task", "testAction", new TaskAwareRequest() {
            @Override
            public void setParentTask(TaskId taskId) {}

            @Override
            public void setRequestId(long requestId) {}

            @Override
            public TaskId getParentTask() {
                return TaskId.EMPTY_TASK_ID;
            }

            @Override
            public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
                return new CancellableTask(id, type, action, getDescription(), parentTaskId, headers);
            }
        });
        InferencePyTorchAction action = new InferencePyTorchAction(
            "test-model",
            1,
            TimeValue.MAX_VALUE,
            processContext,
            new PassThroughConfig(null, null, null),
            NlpInferenceInput.fromText("foo"),
            TrainedModelPrefixStrings.PrefixType.NONE,
            tp,
            cancellableTask,
            listener
        );
        action.init();
        taskManager.cancel(cancellableTask, "test", () -> {});

        action.doRun();
        assertThat(listener.failureCounts, equalTo(1));
        assertThat(listener.responseCounts, equalTo(0));
        verify(resultProcessor, never()).registerRequest(anyString(), any());
    }

    @SuppressWarnings("unchecked")
    public void testPrefixStrings() throws Exception {
        DeploymentManager.ProcessContext processContext = mock(DeploymentManager.ProcessContext.class);

        TrainedModelPrefixStrings prefixStrings = new TrainedModelPrefixStrings("ingest_prefix: ", "search_prefix: ");
        when(processContext.getPrefixStrings()).thenReturn(new SetOnce<>(prefixStrings));

        TrainedModelInput modelInput = new TrainedModelInput(List.of("text_field"));
        when(processContext.getModelInput()).thenReturn(new SetOnce<>(modelInput));

        NlpTask.Processor nlpProcessor = mock(NlpTask.Processor.class);
        NlpTask.RequestBuilder requestBuilder = mock(NlpTask.RequestBuilder.class);
        when(nlpProcessor.getRequestBuilder(any())).thenReturn(requestBuilder);

        NlpTask.Request builtRequest = new NlpTask.Request(mock(TokenizationResult.class), mock(BytesReference.class));
        when(requestBuilder.buildRequest(anyList(), anyString(), any(), anyInt())).thenReturn(builtRequest);

        when(processContext.getNlpTaskProcessor()).thenReturn(new SetOnce<>(nlpProcessor));
        PyTorchResultProcessor resultProcessor = new PyTorchResultProcessor("1", threadSettings -> {});

        PyTorchProcess pyTorchProcess = mock(PyTorchProcess.class);
        when(processContext.getProcess()).thenReturn(new SetOnce<>(pyTorchProcess));

        when(processContext.getResultProcessor()).thenReturn(resultProcessor);
        AtomicInteger timeoutCount = new AtomicInteger();
        when(processContext.getTimeoutCount()).thenReturn(timeoutCount);

        TestListenerCounter listener = new TestListenerCounter();
        {
            // test for search prefix
            InferencePyTorchAction action = new InferencePyTorchAction(
                "test-model",
                1,
                TimeValue.MAX_VALUE,
                processContext,
                new PassThroughConfig(null, null, null),
                NlpInferenceInput.fromText("foo"),
                TrainedModelPrefixStrings.PrefixType.SEARCH,
                tp,
                null,
                listener
            );
            action.init();
            action.doRun();

            ArgumentCaptor<List<String>> inputsCapture = ArgumentCaptor.forClass(List.class);
            verify(nlpProcessor).validateInputs(inputsCapture.capture());

            assertThat(inputsCapture.getValue(), contains("search_prefix: foo"));
        }
        {
            // Clear the previously verified invocations on this mock.
            // Using this function is slightly controversal as it is
            // not recommended by Mockito however, it does save a lot
            // of code rebuilding the mocks for each test.
            Mockito.clearInvocations(nlpProcessor);
            // test for ingest prefix
            InferencePyTorchAction action = new InferencePyTorchAction(
                "test-model",
                1,
                TimeValue.MAX_VALUE,
                processContext,
                new PassThroughConfig(null, null, null),
                NlpInferenceInput.fromText("foo"),
                TrainedModelPrefixStrings.PrefixType.INGEST,
                tp,
                null,
                listener
            );
            action.init();
            action.doRun();

            ArgumentCaptor<List<String>> inputsCapture = ArgumentCaptor.forClass(List.class);
            verify(nlpProcessor).validateInputs(inputsCapture.capture());

            assertThat(inputsCapture.getValue(), contains("ingest_prefix: foo"));
        }
        {
            Mockito.clearInvocations(nlpProcessor);
            // test no prefix
            InferencePyTorchAction action = new InferencePyTorchAction(
                "test-model",
                1,
                TimeValue.MAX_VALUE,
                processContext,
                new PassThroughConfig(null, null, null),
                NlpInferenceInput.fromText("foo"),
                TrainedModelPrefixStrings.PrefixType.NONE,
                tp,
                null,
                listener
            );
            action.init();
            action.doRun();

            ArgumentCaptor<List<String>> inputsCapture = ArgumentCaptor.forClass(List.class);
            verify(nlpProcessor).validateInputs(inputsCapture.capture());

            assertThat(inputsCapture.getValue(), contains("foo"));
        }
        {
            // test search only prefix
            TrainedModelPrefixStrings searchOnlyPrefix = new TrainedModelPrefixStrings(null, "search_prefix: ");
            when(processContext.getPrefixStrings()).thenReturn(new SetOnce<>(searchOnlyPrefix));
            boolean isForSearch = randomBoolean();

            Mockito.clearInvocations(nlpProcessor);
            InferencePyTorchAction action = new InferencePyTorchAction(
                "test-model",
                1,
                TimeValue.MAX_VALUE,
                processContext,
                new PassThroughConfig(null, null, null),
                NlpInferenceInput.fromText("foo"),
                isForSearch ? TrainedModelPrefixStrings.PrefixType.SEARCH : TrainedModelPrefixStrings.PrefixType.INGEST,
                tp,
                null,
                listener
            );
            action.init();
            action.doRun();

            ArgumentCaptor<List<String>> inputsCapture = ArgumentCaptor.forClass(List.class);
            verify(nlpProcessor).validateInputs(inputsCapture.capture());

            if (isForSearch) {
                assertThat(inputsCapture.getValue(), contains("search_prefix: foo"));
            } else {
                assertThat(inputsCapture.getValue(), contains("foo"));
            }
        }
        {
            // test ingest only prefix
            TrainedModelPrefixStrings ingestOnlyPrefix = new TrainedModelPrefixStrings("ingest_prefix: ", null);
            when(processContext.getPrefixStrings()).thenReturn(new SetOnce<>(ingestOnlyPrefix));
            boolean isForSearch = randomBoolean();

            Mockito.clearInvocations(nlpProcessor);
            InferencePyTorchAction action = new InferencePyTorchAction(
                "test-model",
                1,
                TimeValue.MAX_VALUE,
                processContext,
                new PassThroughConfig(null, null, null),
                NlpInferenceInput.fromText("foo"),
                isForSearch ? TrainedModelPrefixStrings.PrefixType.SEARCH : TrainedModelPrefixStrings.PrefixType.INGEST,
                tp,
                null,
                listener
            );
            action.init();
            action.doRun();

            ArgumentCaptor<List<String>> inputsCapture = ArgumentCaptor.forClass(List.class);
            verify(nlpProcessor).validateInputs(inputsCapture.capture());

            if (isForSearch) {
                assertThat(inputsCapture.getValue(), contains("foo"));
            } else {
                assertThat(inputsCapture.getValue(), contains("ingest_prefix: foo"));
            }
        }
    }

    static class TestListenerCounter implements ActionListener<InferenceResults> {
        private int responseCounts;
        private int failureCounts;

        @Override
        public void onResponse(InferenceResults inferenceResults) {
            responseCounts++;
        }

        @Override
        public void onFailure(Exception e) {
            failureCounts++;
        }
    }
}
