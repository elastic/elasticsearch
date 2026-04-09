/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.SenderExecutableAction;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.sender.ChatCompletionInput;
import org.elasticsearch.xpack.inference.external.http.sender.GenericRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.services.googlevertexai.GoogleModelGardenProvider;
import org.elasticsearch.xpack.inference.services.googlevertexai.completion.GoogleVertexAiChatCompletionModelTests;
import org.elasticsearch.xpack.inference.services.googlevertexai.completion.ThinkingConfig;
import org.elasticsearch.xpack.inference.services.googlevertexai.request.completion.GoogleVertexAiUnifiedChatCompletionRequest;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityExecutors;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;
import static org.elasticsearch.xpack.inference.services.googlevertexai.action.GoogleVertexAiActionCreator.USER_ROLE;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

public class GoogleVertexAiUnifiedChatCompletionActionTests extends ESTestCase {

    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);
    private final MockWebServer webServer = new MockWebServer();
    private ThreadPool threadPool;
    private HttpClientManager clientManager;

    @Before
    public void init() throws Exception {
        webServer.start();
        threadPool = createThreadPool(inferenceUtilityExecutors());
        clientManager = HttpClientManager.create(Settings.EMPTY, threadPool, mockClusterServiceEmpty(), mock(ThrottlerManager.class));
    }

    @After
    public void shutdown() throws IOException {
        clientManager.close();
        terminate(threadPool);
        webServer.close();
    }

    private static UnifiedChatInput createUnifiedChatInput(List<String> messages) {
        boolean stream = true;
        return new UnifiedChatInput(messages, "user", stream);
    }

    // Successful case would typically be tested via end-to-end notebook tests in AppEx repo

    public void testExecute_ThrowsElasticsearchExceptionGoogleVertexAi() {
        testExecute_ThrowsElasticsearchException(GoogleModelGardenProvider.GOOGLE);
    }

    public void testExecute_ThrowsElasticsearchException_WhenSenderOnFailureIsCalledGoogleVertexAi() {
        testExecute_ThrowsElasticsearchException_WhenSenderOnFailureIsCalled(GoogleModelGardenProvider.GOOGLE);
    }

    public void testExecute_ThrowsExceptionGoogleVertexAi() {
        testExecute_ThrowsIllegalArgumentException(GoogleModelGardenProvider.GOOGLE);
    }

    public void testExecute_ThrowsElasticsearchExceptionAnthropic() {
        testExecute_ThrowsElasticsearchException(GoogleModelGardenProvider.ANTHROPIC);
    }

    public void testExecute_ThrowsElasticsearchException_WhenSenderOnFailureIsCalledAnthropic() {
        testExecute_ThrowsElasticsearchException_WhenSenderOnFailureIsCalled(GoogleModelGardenProvider.ANTHROPIC);
    }

    public void testExecute_ThrowsExceptionAnthropic() {
        testExecute_ThrowsIllegalArgumentException(GoogleModelGardenProvider.ANTHROPIC);
    }

    public void testExecute_ThrowsElasticsearchExceptionMeta() {
        testExecute_ThrowsElasticsearchException(GoogleModelGardenProvider.META);
    }

    public void testExecute_ThrowsElasticsearchException_WhenSenderOnFailureIsCalledMeta() {
        testExecute_ThrowsElasticsearchException_WhenSenderOnFailureIsCalled(GoogleModelGardenProvider.META);
    }

    public void testExecute_ThrowsExceptionMeta() {
        testExecute_ThrowsIllegalArgumentException(GoogleModelGardenProvider.META);
    }

    public void testExecute_ThrowsElasticsearchExceptionMistral() {
        testExecute_ThrowsElasticsearchException(GoogleModelGardenProvider.MISTRAL);
    }

    public void testExecute_ThrowsElasticsearchException_WhenSenderOnFailureIsCalledMistral() {
        testExecute_ThrowsElasticsearchException_WhenSenderOnFailureIsCalled(GoogleModelGardenProvider.MISTRAL);
    }

    public void testExecute_ThrowsExceptionMistral() {
        testExecute_ThrowsIllegalArgumentException(GoogleModelGardenProvider.MISTRAL);
    }

    public void testExecute_ThrowsElasticsearchExceptionHuggingFace() {
        testExecute_ThrowsElasticsearchException(GoogleModelGardenProvider.HUGGING_FACE);
    }

    public void testExecute_ThrowsElasticsearchException_WhenSenderOnFailureIsCalledHuggingFace() {
        testExecute_ThrowsElasticsearchException_WhenSenderOnFailureIsCalled(GoogleModelGardenProvider.HUGGING_FACE);
    }

    public void testExecute_ThrowsExceptionHuggingFace() {
        testExecute_ThrowsIllegalArgumentException(GoogleModelGardenProvider.HUGGING_FACE);
    }

    public void testExecute_ThrowsElasticsearchExceptionAi21() {
        testExecute_ThrowsElasticsearchException(GoogleModelGardenProvider.AI21);
    }

    public void testExecute_ThrowsElasticsearchException_WhenSenderOnFailureIsCalledAi21() {
        testExecute_ThrowsElasticsearchException_WhenSenderOnFailureIsCalled(GoogleModelGardenProvider.AI21);
    }

    public void testExecute_ThrowsExceptionAi21() {
        testExecute_ThrowsIllegalArgumentException(GoogleModelGardenProvider.AI21);
    }

    private void testExecute_ThrowsIllegalArgumentException(GoogleModelGardenProvider provider) {
        testExecute_ThrowsException(
            provider,
            new IllegalArgumentException("failed"),
            "Failed to send Google Vertex AI chat completion request. Cause: failed"
        );
    }

    private void testExecute_ThrowsElasticsearchException(GoogleModelGardenProvider provider) {
        testExecute_ThrowsException(provider, new ElasticsearchException("failed"), "failed");
    }

    private void testExecute_ThrowsException(GoogleModelGardenProvider provider, Exception exception, String expectedExceptionMessage) {
        var sender = mock(Sender.class);
        doThrow(exception).when(sender).send(any(), any(), any(), any());

        var action = createAction(sender, provider, provider.getChatCompletionResponseHandler());

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        action.execute(createUnifiedChatInput(List.of("test query")), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

        var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));
        assertThat(thrownException.getMessage(), is(expectedExceptionMessage));
    }

    private void testExecute_ThrowsElasticsearchException_WhenSenderOnFailureIsCalled(GoogleModelGardenProvider provider) {
        var sender = mock(Sender.class);

        doAnswer(invocation -> {
            ActionListener<InferenceServiceResults> listenerArg = invocation.getArgument(3);
            listenerArg.onFailure(new IllegalStateException("failed"));
            return Void.TYPE;
        }).when(sender).send(any(), any(), any(), any());

        var action = createAction(sender, provider, provider.getChatCompletionResponseHandler());

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        action.execute(createUnifiedChatInput(List.of("test query")), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

        var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));
        assertThat(thrownException.getMessage(), is("Failed to send Google Vertex AI chat completion request. Cause: failed"));
    }

    private ExecutableAction createAction(Sender sender, GoogleModelGardenProvider provider, ResponseHandler handler) {
        var model = GoogleVertexAiChatCompletionModelTests.createCompletionModel(
            null,
            null,
            null,
            "api-key",
            new RateLimitSettings(100),
            new ThinkingConfig(256),
            provider,
            null,
            123
        );

        var manager = new GenericRequestManager<>(
            threadPool,
            model,
            handler,
            inputs -> new GoogleVertexAiUnifiedChatCompletionRequest(new UnifiedChatInput(inputs, USER_ROLE), model),
            ChatCompletionInput.class
        );
        var failedToSendRequestErrorMessage = constructFailedToSendRequestMessage("Google Vertex AI chat completion");
        return new SenderExecutableAction(sender, manager, failedToSendRequestErrorMessage);
    }

}
