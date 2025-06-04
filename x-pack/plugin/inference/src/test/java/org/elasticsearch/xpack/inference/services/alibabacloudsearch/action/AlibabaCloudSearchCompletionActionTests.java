/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.alibabacloudsearch.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.ChatCompletionResults;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.sender.ChatCompletionInput;
import org.elasticsearch.xpack.inference.external.http.sender.EmbeddingsInput;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.services.ServiceComponentsTests;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.completion.AlibabaCloudSearchCompletionModelTests;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.completion.AlibabaCloudSearchCompletionServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.completion.AlibabaCloudSearchCompletionTaskSettingsTests;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.lucene.tests.util.LuceneTestCase.expectThrows;
import static org.elasticsearch.xpack.core.inference.results.ChatCompletionResultsTests.buildExpectationCompletion;
import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityPool;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;
import static org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettingsTests.getSecretSettingsMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

public class AlibabaCloudSearchCompletionActionTests extends ESTestCase {

    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);
    private final MockWebServer webServer = new MockWebServer();
    private ThreadPool threadPool;
    private HttpClientManager clientManager;

    @Before
    public void init() throws IOException {
        webServer.start();
        threadPool = createThreadPool(inferenceUtilityPool());
        clientManager = HttpClientManager.create(Settings.EMPTY, threadPool, mockClusterServiceEmpty(), mock(ThrottlerManager.class));
    }

    @After
    public void shutdown() throws IOException {
        clientManager.close();
        terminate(threadPool);
        webServer.close();
    }

    public void testExecute_Success() {
        var sender = mock(Sender.class);

        var resultString = randomAlphaOfLength(100);
        doAnswer(invocation -> {
            ActionListener<InferenceServiceResults> listener = invocation.getArgument(3);
            listener.onResponse(new ChatCompletionResults(List.of(new ChatCompletionResults.Result(resultString))));

            return Void.TYPE;
        }).when(sender).send(any(), any(), any(), any());
        var action = createAction(threadPool, sender);

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        action.execute(new ChatCompletionInput(List.of(randomAlphaOfLength(10))), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

        var result = listener.actionGet(TIMEOUT);
        assertThat(result.asMap(), is(buildExpectationCompletion(List.of(resultString))));
    }

    public void testExecute_ListenerThrowsElasticsearchException_WhenSenderThrowsElasticsearchException() {
        var sender = mock(Sender.class);
        doThrow(new ElasticsearchException("error")).when(sender).send(any(), any(), any(), any());
        var action = createAction(threadPool, sender);

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        action.execute(new ChatCompletionInput(List.of(randomAlphaOfLength(10))), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

        var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));
        assertThat(thrownException.getMessage(), is("error"));
    }

    public void testExecute_ListenerThrowsInternalServerError_WhenSenderThrowsException() {
        var sender = mock(Sender.class);
        doThrow(new RuntimeException("error")).when(sender).send(any(), any(), any(), any());
        var action = createAction(threadPool, sender);

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        action.execute(new ChatCompletionInput(List.of(randomAlphaOfLength(10))), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

        var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));
        assertThat(thrownException.getMessage(), is(constructFailedToSendRequestMessage("AlibabaCloud Search completion")));
    }

    public void testExecute_ThrowsIllegalArgumentException_WhenInputIsNotChatCompletionInput() {
        var action = createAction(threadPool, mock(Sender.class));

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        assertThrows(IllegalArgumentException.class, () -> {
            action.execute(
                new EmbeddingsInput(List.of(randomAlphaOfLength(10)), null, InputType.INGEST),
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );
        });
    }

    public void testExecute_ListenerThrowsElasticsearchStatusException_WhenInputSizeIsEven() {
        var action = createAction(threadPool, mock(Sender.class));

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        action.execute(
            new ChatCompletionInput(List.of(randomAlphaOfLength(10), randomAlphaOfLength(10))),
            InferenceAction.Request.DEFAULT_TIMEOUT,
            listener
        );

        var thrownException = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TIMEOUT));
        assertThat(
            thrownException.getMessage(),
            is(
                "Alibaba Completion's inputs must be an odd number. The last input is the current query, "
                    + "all preceding inputs are the completion history as pairs of user input and the assistant's response."
            )
        );
        assertThat(thrownException.status(), is(RestStatus.BAD_REQUEST));
    }

    private ExecutableAction createAction(ThreadPool threadPool, Sender sender) {
        var model = AlibabaCloudSearchCompletionModelTests.createModel(
            "completion_test",
            TaskType.COMPLETION,
            AlibabaCloudSearchCompletionServiceSettingsTests.getServiceSettingsMap("completion_test", "host", "default"),
            AlibabaCloudSearchCompletionTaskSettingsTests.getTaskSettingsMap(null),
            getSecretSettingsMap("secret")
        );

        var serviceComponents = ServiceComponentsTests.createWithEmptySettings(threadPool);
        return new AlibabaCloudSearchCompletionAction(sender, model, serviceComponents);
    }
}
