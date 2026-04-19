/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnparsedModel;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.InferenceContext;
import org.elasticsearch.xpack.core.inference.action.EmbeddingAction;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.action.InferenceActionProxy;
import org.elasticsearch.xpack.core.inference.action.UnifiedCompletionAction;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.util.Collections;

import static org.elasticsearch.xpack.core.inference.action.BaseInferenceActionRequest.TIMEOUT_NOT_DETERMINED;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportInferenceActionProxyTests extends ESTestCase {
    private Client client;
    private ThreadPool threadPool;
    private TransportInferenceActionProxy action;
    private ModelRegistry modelRegistry;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        client = mock(Client.class);
        threadPool = new TestThreadPool("test");
        when(client.threadPool()).thenReturn(threadPool);
        modelRegistry = mock(ModelRegistry.class);

        action = new TransportInferenceActionProxy(mock(TransportService.class), mock(ActionFilters.class), modelRegistry, client);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        terminate(threadPool);
    }

    public void testExecutesAUnifiedCompletionRequest_WhenTaskTypeIsChatCompletion_InRequest_TimeoutSpecified() {
        testExecutesAUnifiedCompletionRequest_WhenTaskTypeIsChatCompletion_InRequest(TimeValue.ONE_MINUTE, TimeValue.ONE_MINUTE);
    }

    public void testExecutesAUnifiedCompletionRequest_WhenTaskTypeIsChatCompletion_InRequest_NullTimeout() {
        testExecutesAUnifiedCompletionRequest_WhenTaskTypeIsChatCompletion_InRequest(null, TimeValue.timeValueSeconds(120));
    }

    public void testExecutesAUnifiedCompletionRequest_WhenTaskTypeIsChatCompletion_InRequest_TimeoutNotDetermined() {
        testExecutesAUnifiedCompletionRequest_WhenTaskTypeIsChatCompletion_InRequest(
            TIMEOUT_NOT_DETERMINED,
            TimeValue.timeValueSeconds(120)
        );
    }

    private void testExecutesAUnifiedCompletionRequest_WhenTaskTypeIsChatCompletion_InRequest(
        TimeValue timeout,
        TimeValue expectedTimeout
    ) {
        String requestJson = """
            {
                "model": "gpt-4o",
                "messages": [
                    {
                       "role": "user",
                       "content": [
                            {
                                "text": "some text",
                                "type": "text"
                            }
                        ]
                    }
                ]
            }
            """;

        @SuppressWarnings("unchecked")
        ActionListener<InferenceAction.Response> listener = (ActionListener<InferenceAction.Response>) mock(ActionListener.class);
        var request = new InferenceActionProxy.Request(
            TaskType.CHAT_COMPLETION,
            "id",
            new BytesArray(requestJson),
            XContentType.JSON,
            timeout,
            true,
            InferenceContext.EMPTY_INSTANCE
        );

        action.doExecute(mock(Task.class), request, listener);

        var captor = ArgumentCaptor.forClass(UnifiedCompletionAction.Request.class);
        verify(client, times(1)).execute(eq(UnifiedCompletionAction.INSTANCE), captor.capture(), any());
        assertThat(captor.getValue().getTimeout(), is(expectedTimeout));
    }

    public void testExecutesAUnifiedCompletionRequest_WhenTaskTypeIsChatCompletion_FromStorage_TimeoutSpecified() {
        testExecutesAUnifiedCompletionRequest_WhenTaskTypeIsChatCompletion_FromStorage(TimeValue.ONE_MINUTE, TimeValue.ONE_MINUTE);
    }

    public void testExecutesAUnifiedCompletionRequest_WhenTaskTypeIsChatCompletion_FromStorage_NullTimeout() {
        testExecutesAUnifiedCompletionRequest_WhenTaskTypeIsChatCompletion_FromStorage(null, TimeValue.timeValueSeconds(120));
    }

    public void testExecutesAUnifiedCompletionRequest_WhenTaskTypeIsChatCompletion_FromStorage_TimeoutNotDetermined() {
        testExecutesAUnifiedCompletionRequest_WhenTaskTypeIsChatCompletion_FromStorage(
            TIMEOUT_NOT_DETERMINED,
            TimeValue.timeValueSeconds(120)
        );
    }

    private void testExecutesAUnifiedCompletionRequest_WhenTaskTypeIsChatCompletion_FromStorage(
        TimeValue timeout,
        TimeValue expectedTimeout
    ) {
        String requestJson = """
            {
                "model": "gpt-4o",
                "messages": [
                    {
                       "role": "user",
                       "content": [
                            {
                                "text": "some text",
                                "type": "text"
                            }
                        ]
                    }
                ]
            }
            """;

        doAnswer(invocation -> {
            ActionListener<UnparsedModel> listener = invocation.getArgument(1);
            listener.onResponse(
                new UnparsedModel("id", TaskType.CHAT_COMPLETION, "service", Collections.emptyMap(), Collections.emptyMap())
            );

            return Void.TYPE;
        }).when(modelRegistry).getModelWithSecrets(any(), any());

        var listener = new PlainActionFuture<InferenceAction.Response>();
        var request = new InferenceActionProxy.Request(
            TaskType.ANY,
            "id",
            new BytesArray(requestJson),
            XContentType.JSON,
            timeout,
            true,
            InferenceContext.EMPTY_INSTANCE
        );

        action.doExecute(mock(Task.class), request, listener);

        var captor = ArgumentCaptor.forClass(UnifiedCompletionAction.Request.class);
        verify(client, times(1)).execute(eq(UnifiedCompletionAction.INSTANCE), captor.capture(), any());
        assertThat(captor.getValue().getTimeout(), is(expectedTimeout));
    }

    public void testExecutesAnInferenceAction_WhenTaskTypeIsCompletion_InRequest_TimeoutSpecified() {
        testExecutesAnInferenceAction_WhenTaskTypeIsCompletion_InRequest(TimeValue.ONE_MINUTE, TimeValue.ONE_MINUTE);
    }

    public void testExecutesAnInferenceAction_WhenTaskTypeIsCompletion_InRequest_NullTimeout() {
        testExecutesAnInferenceAction_WhenTaskTypeIsCompletion_InRequest(null, TimeValue.timeValueSeconds(120));
    }

    public void testExecutesAnInferenceAction_WhenTaskTypeIsCompletion_InRequest_TimeoutNotDetermined() {
        testExecutesAnInferenceAction_WhenTaskTypeIsCompletion_InRequest(TIMEOUT_NOT_DETERMINED, TimeValue.timeValueSeconds(120));
    }

    private void testExecutesAnInferenceAction_WhenTaskTypeIsCompletion_InRequest(TimeValue timeout, TimeValue expectedTimeout) {
        String requestJson = """
            {
                "input": ["some text"]
            }
            """;

        @SuppressWarnings("unchecked")
        ActionListener<InferenceAction.Response> listener = (ActionListener<InferenceAction.Response>) mock(ActionListener.class);
        var request = new InferenceActionProxy.Request(
            TaskType.COMPLETION,
            "id",
            new BytesArray(requestJson),
            XContentType.JSON,
            timeout,
            true,
            InferenceContext.EMPTY_INSTANCE
        );

        action.doExecute(mock(Task.class), request, listener);

        var captor = ArgumentCaptor.forClass(InferenceAction.Request.class);
        verify(client, times(1)).execute(eq(InferenceAction.INSTANCE), captor.capture(), any());
        assertThat(captor.getValue().getInferenceTimeout(), is(expectedTimeout));
    }

    public void testExecutesAnInferenceAction_WhenTaskTypeIsCompletion_FromStorage_TimeoutSpecified() {
        testExecutesAnInferenceAction_WhenTaskTypeIsCompletion_FromStorage(TimeValue.ONE_MINUTE, TimeValue.ONE_MINUTE);
    }

    public void testExecutesAnInferenceAction_WhenTaskTypeIsCompletion_FromStorage_NullTimeout() {
        testExecutesAnInferenceAction_WhenTaskTypeIsCompletion_FromStorage(null, TimeValue.timeValueSeconds(120));
    }

    public void testExecutesAnInferenceAction_WhenTaskTypeIsCompletion_FromStorage_TimeoutNotDetermined() {
        testExecutesAnInferenceAction_WhenTaskTypeIsCompletion_FromStorage(TIMEOUT_NOT_DETERMINED, TimeValue.timeValueSeconds(120));
    }

    private void testExecutesAnInferenceAction_WhenTaskTypeIsCompletion_FromStorage(TimeValue timeout, TimeValue expectedTimeout) {
        String requestJson = """
            {
                "input": ["some text"]
            }
            """;

        doAnswer(invocation -> {
            ActionListener<UnparsedModel> listener = invocation.getArgument(1);
            listener.onResponse(new UnparsedModel("id", TaskType.COMPLETION, "service", Collections.emptyMap(), Collections.emptyMap()));

            return Void.TYPE;
        }).when(modelRegistry).getModelWithSecrets(any(), any());

        var listener = new PlainActionFuture<InferenceAction.Response>();
        var request = new InferenceActionProxy.Request(
            TaskType.ANY,
            "id",
            new BytesArray(requestJson),
            XContentType.JSON,
            timeout,
            true,
            InferenceContext.EMPTY_INSTANCE
        );

        action.doExecute(mock(Task.class), request, listener);

        var captor = ArgumentCaptor.forClass(InferenceAction.Request.class);
        verify(client, times(1)).execute(eq(InferenceAction.INSTANCE), captor.capture(), any());
        assertThat(captor.getValue().getInferenceTimeout(), is(expectedTimeout));
    }

    public void testExecutesAnEmbeddingAction_WhenTaskTypeIsEmbedding_InRequest_TimeoutSpecified() {
        testExecutesAnEmbeddingAction_WhenTaskTypeIsEmbedding_InRequest(TimeValue.ONE_MINUTE, TimeValue.ONE_MINUTE);
    }

    public void testExecutesAnEmbeddingAction_WhenTaskTypeIsEmbedding_InRequest_NullTimeout() {
        testExecutesAnEmbeddingAction_WhenTaskTypeIsEmbedding_InRequest(null, TimeValue.timeValueSeconds(30));
    }

    public void testExecutesAnEmbeddingAction_WhenTaskTypeIsEmbedding_InRequest_TimeoutNotDetermined() {
        testExecutesAnEmbeddingAction_WhenTaskTypeIsEmbedding_InRequest(TIMEOUT_NOT_DETERMINED, TimeValue.timeValueSeconds(30));
    }

    private void testExecutesAnEmbeddingAction_WhenTaskTypeIsEmbedding_InRequest(TimeValue timeout, TimeValue expectedTimeout) {
        String requestJson = """
            {
                "input": [
                    {
                       "content": {
                            "value": "some text",
                            "type": "text"
                        }
                    }
                ]
            }
            """;

        @SuppressWarnings("unchecked")
        ActionListener<InferenceAction.Response> listener = (ActionListener<InferenceAction.Response>) mock(ActionListener.class);
        var request = new InferenceActionProxy.Request(
            TaskType.EMBEDDING,
            "id",
            new BytesArray(requestJson),
            XContentType.JSON,
            timeout,
            false,
            InferenceContext.EMPTY_INSTANCE
        );

        action.doExecute(mock(Task.class), request, listener);

        var captor = ArgumentCaptor.forClass(EmbeddingAction.Request.class);
        verify(client, times(1)).execute(eq(EmbeddingAction.INSTANCE), captor.capture(), any());
        assertThat(captor.getValue().getTimeout(), is(expectedTimeout));
    }

    public void testExecutesAnEmbeddingRequest_WhenTaskTypeIsEmbedding_FromStorage_TimeoutSpecified() {
        testExecutesAnEmbeddingRequest_WhenTaskTypeIsEmbedding_FromStorage(TimeValue.ONE_MINUTE, TimeValue.ONE_MINUTE);
    }

    public void testExecutesAnEmbeddingRequest_WhenTaskTypeIsEmbedding_FromStorage_NullTimeout() {
        testExecutesAnEmbeddingRequest_WhenTaskTypeIsEmbedding_FromStorage(null, TimeValue.timeValueSeconds(30));
    }

    public void testExecutesAnEmbeddingRequest_WhenTaskTypeIsEmbedding_FromStorage_TimeoutNotDetermined() {
        testExecutesAnEmbeddingRequest_WhenTaskTypeIsEmbedding_FromStorage(TIMEOUT_NOT_DETERMINED, TimeValue.timeValueSeconds(30));
    }

    private void testExecutesAnEmbeddingRequest_WhenTaskTypeIsEmbedding_FromStorage(TimeValue timeout, TimeValue expectedTimeout) {
        String requestJson = """
            {
                "input": [
                    {
                       "content": {
                            "value": "some text",
                            "type": "text"
                        }
                    }
                ]
            }
            """;

        doAnswer(invocation -> {
            ActionListener<UnparsedModel> listener = invocation.getArgument(1);
            listener.onResponse(new UnparsedModel("id", TaskType.EMBEDDING, "service", Collections.emptyMap(), Collections.emptyMap()));

            return Void.TYPE;
        }).when(modelRegistry).getModelWithSecrets(any(), any());

        var listener = new PlainActionFuture<InferenceAction.Response>();
        var request = new InferenceActionProxy.Request(
            TaskType.ANY,
            "id",
            new BytesArray(requestJson),
            XContentType.JSON,
            timeout,
            false,
            InferenceContext.EMPTY_INSTANCE
        );

        action.doExecute(mock(Task.class), request, listener);

        var captor = ArgumentCaptor.forClass(EmbeddingAction.Request.class);
        verify(client, times(1)).execute(eq(EmbeddingAction.INSTANCE), captor.capture(), any());
        assertThat(captor.getValue().getTimeout(), is(expectedTimeout));
    }
}
