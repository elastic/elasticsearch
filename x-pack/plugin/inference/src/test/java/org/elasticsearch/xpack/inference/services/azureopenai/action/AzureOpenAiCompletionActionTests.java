/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.action;

import org.apache.http.HttpHeaders;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.ChatCompletionResults;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.SingleInputSenderExecutableAction;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.sender.ChatCompletionInput;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiCompletionRequestManager;
import org.elasticsearch.xpack.inference.services.azureopenai.request.AzureOpenAiUtils;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityPool;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;
import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests.createSender;
import static org.elasticsearch.xpack.inference.services.azureopenai.action.AzureOpenAiActionCreatorTests.getContentOfMessageInRequestMap;
import static org.elasticsearch.xpack.inference.services.azureopenai.completion.AzureOpenAiCompletionModelTests.createCompletionModel;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

public class AzureOpenAiCompletionActionTests extends ESTestCase {

    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);
    private final MockWebServer webServer = new MockWebServer();
    private ThreadPool threadPool;
    private HttpClientManager clientManager;

    @Before
    public void init() throws Exception {
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

    public void testExecute_ReturnsSuccessfulResponse() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var sender = createSender(senderFactory)) {
            sender.start();

            String responseJson = """
                {
                    "choices": [
                                {
                                    "finish_reason": "stop",
                                    "index": 0,
                                    "logprobs": null,
                                    "message": {
                                        "content": "response",
                                        "role": "assistant"
                                        }
                                    }
                                ],
                                "model": "gpt-4",
                                "object": "chat.completion"
                                ]
                }""";

            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var user = "user";
            var apiKey = "api_key";
            var completionInput = "some input";

            var action = createAction("resource", "deployment", "apiversion", user, apiKey, sender, "id");

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(new ChatCompletionInput(List.of(completionInput)), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

            var result = listener.actionGet(TIMEOUT);

            assertThat(webServer.requests(), hasSize(1));

            var request = webServer.requests().get(0);
            assertNull(request.getUri().getQuery());
            assertThat(request.getHeader(HttpHeaders.CONTENT_TYPE), is(XContentType.JSON.mediaType()));
            assertThat(request.getHeader(AzureOpenAiUtils.API_KEY_HEADER), is(apiKey));

            assertThat(
                result.asMap(),
                is(Map.of(ChatCompletionResults.COMPLETION, List.of(Map.of(ChatCompletionResults.Result.RESULT, "response"))))
            );

            var requestMap = entityAsMap(request.getBody());
            assertThat(requestMap.size(), is(3));
            assertThat(getContentOfMessageInRequestMap(requestMap), is(completionInput));
            assertThat(requestMap.get("user"), is(user));
            assertThat(requestMap.get("n"), is(1));
        }
    }

    public void testExecute_ThrowsElasticsearchException() {
        var sender = mock(Sender.class);
        doThrow(new ElasticsearchException("failed")).when(sender).send(any(), any(), any(), any());

        var action = createAction("resource", "deployment", "apiVersion", "user", "apikey", sender, "id");

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        action.execute(new ChatCompletionInput(List.of("abc")), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

        var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));

        assertThat(thrownException.getMessage(), is("failed"));
    }

    public void testExecute_ThrowsElasticsearchException_WhenSenderOnFailureIsCalled() {
        var sender = mock(Sender.class);

        doAnswer(invocation -> {
            ActionListener<InferenceServiceResults> listener = invocation.getArgument(3);
            listener.onFailure(new IllegalStateException("failed"));

            return Void.TYPE;
        }).when(sender).send(any(), any(), any(), any());

        var action = createAction("resource", "deployment", "apiVersion", "user", "apikey", sender, "id");

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        action.execute(new ChatCompletionInput(List.of("abc")), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

        var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));

        assertThat(thrownException.getMessage(), is("Failed to send Azure OpenAI completion request. Cause: failed"));
    }

    public void testExecute_ThrowsException() {
        var sender = mock(Sender.class);
        doThrow(new IllegalArgumentException("failed")).when(sender).send(any(), any(), any(), any());

        var action = createAction("resource", "deployment", "apiVersion", "user", "apikey", sender, "id");

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        action.execute(new ChatCompletionInput(List.of("abc")), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

        var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));

        assertThat(thrownException.getMessage(), is("Failed to send Azure OpenAI completion request. Cause: failed"));
    }

    private ExecutableAction createAction(
        String resourceName,
        String deploymentId,
        String apiVersion,
        @Nullable String user,
        String apiKey,
        Sender sender,
        String inferenceEntityId
    ) {
        try {
            var model = createCompletionModel(resourceName, deploymentId, apiVersion, user, apiKey, null, inferenceEntityId);
            model.setUri(new URI(getUrl(webServer)));
            var requestCreator = new AzureOpenAiCompletionRequestManager(model, threadPool);
            var errorMessage = constructFailedToSendRequestMessage("Azure OpenAI completion");
            return new SingleInputSenderExecutableAction(sender, requestCreator, errorMessage, "Azure OpenAI completion");
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
