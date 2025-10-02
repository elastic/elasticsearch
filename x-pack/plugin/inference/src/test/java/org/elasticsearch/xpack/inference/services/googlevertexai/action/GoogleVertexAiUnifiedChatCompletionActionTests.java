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
import org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiService;
import org.elasticsearch.xpack.inference.services.googlevertexai.completion.GoogleVertexAiChatCompletionModelTests;
import org.elasticsearch.xpack.inference.services.googlevertexai.completion.ThinkingConfig;
import org.elasticsearch.xpack.inference.services.googlevertexai.request.completion.GoogleVertexAiUnifiedChatCompletionRequest;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityExecutors;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;
import static org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiService.GOOGLE_VERTEX_AI_CHAT_COMPLETION_HANDLER;
import static org.elasticsearch.xpack.inference.services.googlevertexai.action.GoogleVertexAiActionCreator.GOOGLE_MODEL_GARDEN_ANTHROPIC_COMPLETION_HANDLER;
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
        testExecute_ThrowsElasticsearchException(
            "us-central1",
            "test-project-id",
            "chat-bison",
            null,
            null,
            GoogleVertexAiService.GOOGLE_VERTEX_AI_CHAT_COMPLETION_HANDLER
        );
    }

    public void testExecute_ThrowsElasticsearchException_WhenSenderOnFailureIsCalledGoogleVertexAi() {
        testExecute_ThrowsElasticsearchException_WhenSenderOnFailureIsCalled(
            "us-central1",
            "test-project-id",
            "chat-bison",
            null,
            null,
            GoogleVertexAiService.GOOGLE_VERTEX_AI_CHAT_COMPLETION_HANDLER
        );
    }

    public void testExecute_ThrowsExceptionGoogleVertexAi() {
        testExecute_ThrowsException("us-central1", "test-project-id", "chat-bison", null, null, GOOGLE_VERTEX_AI_CHAT_COMPLETION_HANDLER);
    }

    public void testExecute_ThrowsElasticsearchExceptionAnthropic() throws URISyntaxException {
        testExecute_ThrowsElasticsearchException(
            null,
            null,
            null,
            GoogleModelGardenProvider.ANTHROPIC,
            new URI("http://localhost:9200"),
            GOOGLE_MODEL_GARDEN_ANTHROPIC_COMPLETION_HANDLER
        );
    }

    public void testExecute_ThrowsElasticsearchException_WhenSenderOnFailureIsCalledAnthropic() throws URISyntaxException {
        testExecute_ThrowsElasticsearchException_WhenSenderOnFailureIsCalled(
            null,
            null,
            null,
            GoogleModelGardenProvider.ANTHROPIC,
            new URI("http://localhost:9200"),
            GOOGLE_MODEL_GARDEN_ANTHROPIC_COMPLETION_HANDLER
        );
    }

    public void testExecute_ThrowsExceptionAnthropic() throws URISyntaxException {
        testExecute_ThrowsException(
            null,
            null,
            null,
            GoogleModelGardenProvider.ANTHROPIC,
            new URI("http://localhost:9200"),
            GoogleVertexAiActionCreator.GOOGLE_MODEL_GARDEN_ANTHROPIC_COMPLETION_HANDLER
        );
    }

    private void testExecute_ThrowsException(
        String location,
        String projectId,
        String actualModelId,
        GoogleModelGardenProvider provider,
        URI uri,
        ResponseHandler handler
    ) {
        var sender = mock(Sender.class);
        doThrow(new IllegalArgumentException("failed")).when(sender).send(any(), any(), any(), any());

        var action = createAction(location, projectId, actualModelId, sender, provider, uri, handler);

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        action.execute(createUnifiedChatInput(List.of("test query")), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

        var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));
        assertThat(thrownException.getMessage(), is("Failed to send Google Vertex AI chat completion request. Cause: failed"));
    }

    private void testExecute_ThrowsElasticsearchException(
        String location,
        String projectId,
        String actualModelId,
        GoogleModelGardenProvider googleModelGardenProvider,
        URI uri,
        ResponseHandler googleModelGardenAnthropicCompletionHandler
    ) {
        var sender = mock(Sender.class);
        doThrow(new ElasticsearchException("failed")).when(sender).send(any(), any(), any(), any());

        var action = createAction(
            location,
            projectId,
            actualModelId,
            sender,
            googleModelGardenProvider,
            uri,
            googleModelGardenAnthropicCompletionHandler
        );

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        action.execute(createUnifiedChatInput(List.of("test query")), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

        var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));
        assertThat(thrownException.getMessage(), is("failed"));
    }

    private void testExecute_ThrowsElasticsearchException_WhenSenderOnFailureIsCalled(
        String location,
        String projectId,
        String actualModelId,
        GoogleModelGardenProvider googleModelGardenProvider,
        URI uri,
        ResponseHandler googleModelGardenAnthropicCompletionHandler
    ) {
        var sender = mock(Sender.class);

        doAnswer(invocation -> {
            ActionListener<InferenceServiceResults> listenerArg = invocation.getArgument(3);
            listenerArg.onFailure(new IllegalStateException("failed"));
            return Void.TYPE;
        }).when(sender).send(any(), any(), any(), any());

        var action = createAction(
            location,
            projectId,
            actualModelId,
            sender,
            googleModelGardenProvider,
            uri,
            googleModelGardenAnthropicCompletionHandler
        );

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        action.execute(createUnifiedChatInput(List.of("test query")), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

        var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));
        assertThat(thrownException.getMessage(), is("Failed to send Google Vertex AI chat completion request. Cause: failed"));
    }

    private ExecutableAction createAction(
        String location,
        String projectId,
        String actualModelId,
        Sender sender,
        GoogleModelGardenProvider provider,
        URI uri,
        ResponseHandler handler
    ) {
        var model = GoogleVertexAiChatCompletionModelTests.createCompletionModel(
            projectId,
            location,
            actualModelId,
            "api-key",
            new RateLimitSettings(100),
            new ThinkingConfig(256),
            provider,
            uri,
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
