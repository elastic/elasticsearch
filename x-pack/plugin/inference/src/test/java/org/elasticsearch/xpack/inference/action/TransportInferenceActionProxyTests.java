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
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.action.InferenceActionProxy;
import org.elasticsearch.xpack.core.inference.action.UnifiedCompletionAction;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;

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

    public void testExecutesAUnifiedCompletionRequest_WhenTaskTypeIsChatCompletion_InRequest() {
        String requestJson = """
            {
                "model": "gpt-4o",
                "messages": [
                    {
                       "role": "user",
                       "content": [
                            {
                                "text": "some text",
                                "type": "string"
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
            TimeValue.ONE_MINUTE,
            true,
            InferenceContext.EMPTY_INSTANCE
        );

        action.doExecute(mock(Task.class), request, listener);

        verify(client, times(1)).execute(eq(UnifiedCompletionAction.INSTANCE), any(), any());
    }

    public void testExecutesAUnifiedCompletionRequest_WhenTaskTypeIsChatCompletion_FromStorage() {
        String requestJson = """
            {
                "model": "gpt-4o",
                "messages": [
                    {
                       "role": "user",
                       "content": [
                            {
                                "text": "some text",
                                "type": "string"
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
            TimeValue.ONE_MINUTE,
            true,
            InferenceContext.EMPTY_INSTANCE
        );

        action.doExecute(mock(Task.class), request, listener);

        verify(client, times(1)).execute(eq(UnifiedCompletionAction.INSTANCE), any(), any());
    }

    public void testExecutesAnInferenceAction_WhenTaskTypeIsCompletion_InRequest() {
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
            TimeValue.ONE_MINUTE,
            true,
            InferenceContext.EMPTY_INSTANCE
        );

        action.doExecute(mock(Task.class), request, listener);

        verify(client, times(1)).execute(eq(InferenceAction.INSTANCE), any(), any());
    }

    public void testExecutesAnInferenceAction_WhenTaskTypeIsCompletion_FromStorage() {
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
            TimeValue.ONE_MINUTE,
            true,
            InferenceContext.EMPTY_INSTANCE
        );

        action.doExecute(mock(Task.class), request, listener);

        verify(client, times(1)).execute(eq(InferenceAction.INSTANCE), any(), any());
    }
}
