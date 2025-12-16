/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.alibabacloudsearch;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.retry.RequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.ChatCompletionInput;
import org.elasticsearch.xpack.inference.external.http.sender.EmbeddingsInput;
import org.elasticsearch.xpack.inference.external.http.sender.ExecutableInferenceRequest;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.completion.AlibabaCloudSearchCompletionModelTests;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.completion.AlibabaCloudSearchCompletionServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.completion.AlibabaCloudSearchCompletionTaskSettingsTests;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.request.completion.AlibabaCloudSearchCompletionRequest;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.rest.RestStatus.BAD_REQUEST;
import static org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettingsTests.getSecretSettingsMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AlibabaCloudSearchCompletionRequestManagerTests extends ESTestCase {
    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);

    public void testExecute_executesRequest() {
        var inputs = new ChatCompletionInput(List.of("input1", "input2", "input3"));
        RequestSender mockSender = mock(RequestSender.class);
        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();

        ExecutorService mockExecutorService = mock(ExecutorService.class);
        var requestManager = createRequestManagerWithMockExecutor(mockExecutorService);
        requestManager.execute(inputs, mockSender, () -> false, listener);

        ArgumentCaptor<ExecutableInferenceRequest> captor = ArgumentCaptor.forClass(ExecutableInferenceRequest.class);
        verify(mockExecutorService).execute(captor.capture());

        ExecutableInferenceRequest executableRequest = captor.getValue();
        assertThat(executableRequest.request(), is(instanceOf(AlibabaCloudSearchCompletionRequest.class)));
        assertThat(executableRequest.responseHandler().getRequestType(), is("alibaba cloud search completion"));
    }

    public void testExecute_throwsElasticsearchStatusException_whenNumberOfInputsIsEven() {
        var inputs = new ChatCompletionInput(List.of("input1", "input2"));
        RequestSender mockSender = mock(RequestSender.class);
        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();

        var requestManager = createRequestManagerWithMockExecutor(mock(ExecutorService.class));
        requestManager.execute(inputs, mockSender, () -> false, listener);

        var thrownException = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TIMEOUT));

        assertThat(thrownException.status(), is(BAD_REQUEST));
        assertThat(thrownException.getMessage(), containsString("Alibaba Completion's inputs must be an odd number"));
    }

    public void testExecute_throwsIllegalArgumentException_whenInputIsNotChatCompletion() {
        var inputs = new EmbeddingsInput(List.of("input1"), InputType.SEARCH);
        RequestSender mockSender = mock(RequestSender.class);
        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();

        var requestManager = createRequestManagerWithMockExecutor(mock(ExecutorService.class));
        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> requestManager.execute(inputs, mockSender, () -> false, listener)
        );

        assertThat(thrownException.getMessage(), containsString("Unable to convert inference inputs type"));
    }

    private AlibabaCloudSearchCompletionRequestManager createRequestManagerWithMockExecutor(ExecutorService mockExecutorService) {
        ThreadPool mockThreadPool = mock(ThreadPool.class);
        when(mockThreadPool.executor(anyString())).thenReturn(mockExecutorService);

        var model = AlibabaCloudSearchCompletionModelTests.createModel(
            "completion_test",
            TaskType.COMPLETION,
            AlibabaCloudSearchCompletionServiceSettingsTests.getServiceSettingsMap("completion_test", "host", "default"),
            AlibabaCloudSearchCompletionTaskSettingsTests.getTaskSettingsMap(null),
            getSecretSettingsMap("secret")
        );
        var account = new AlibabaCloudSearchAccount(model.getSecretSettings().apiKey());
        return AlibabaCloudSearchCompletionRequestManager.of(account, model, mockThreadPool);
    }
}
