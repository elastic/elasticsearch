/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.action.openai;

import org.apache.http.HttpHeaders;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockRequest;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.ChatCompletionResults;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.external.request.openai.OpenAiUtils.ORGANIZATION_HEADER;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityPool;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.elasticsearch.xpack.inference.services.openai.completion.OpenAiChatCompletionModelTests.createChatCompletionModel;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

public class OpenAiChatCompletionActionTests extends ESTestCase {

    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);
    private final MockWebServer webServer = new MockWebServer();
    private ThreadPool threadPool;
    private HttpClientManager clientManager;

    @Before
    public void init() throws Exception{
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
        var senderFactory = new HttpRequestSender.Factory(
            createWithEmptySettings(threadPool),
            clientManager,
            mockClusterServiceEmpty()
        );

        try(var sender = senderFactory.createSender("test_service")){
            sender.start();

            String responseJson = """
                {
                  "id": "chatcmpl-123",
                  "object": "chat.completion",
                  "created": 1677652288,
                  "model": "gpt-3.5-turbo-0125",
                  "system_fingerprint": "fp_44709d6fcb",
                  "choices": [
                      {
                        "index": 0,
                        "message": {
                          "role": "assistant",
                          "content": "result content"
                        },
                        "logprobs": null,
                        "finish_reason": "stop"
                      }
                  ],
                  "usage": {
                    "prompt_tokens": 9,
                    "completion_tokens": 12,
                    "total_tokens": 21
                  }
                }
                """;

            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var action = createAction(getUrl(webServer), "org", "secret", "model", "user", sender);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(List.of("abc"), listener);

            var result = listener.actionGet(TIMEOUT);

            assertThat(result.asMap(), is(buildExpectedChatCompletionResultMap(List.of("result content"))));
            assertThat(webServer.requests(), hasSize(1));

            MockRequest request = webServer.requests().get(0);

            assertNull(request.getUri().getQuery());
            assertThat(request.getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));
            assertThat(request.getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer secret"));
            assertThat(request.getHeader(ORGANIZATION_HEADER), equalTo("org"));

            var requestMap = entityAsMap(request.getBody());
            assertThat(requestMap.size(), is(3));
            assertThat(requestMap.get("messages"), is(List.of(Map.of("role", "user", "content", "abc"))));
            assertThat(requestMap.get("model"), is("model"));
            assertThat(requestMap.get("user"), is("user"));
        }
    }

    public void testExecute_ThrowsURISyntaxException_ForInvalidUrl() throws IOException {
        try (var sender = mock(Sender.class)) {
            var thrownException = expectThrows(
                IllegalArgumentException.class,
                () -> createAction("^^", "org", "secret", "model", "user", sender)
            );
            assertThat(thrownException.getMessage(), is("unable to parse url [^^]"));
        }
    }

    public void testExecute_ThrowsElasticsearchException() {
        var sender = mock(Sender.class);
        doThrow(new ElasticsearchException("failed")).when(sender).send(any(), any(), any());

        var action = createAction(getUrl(webServer), "org", "secret", "model", "user", sender);

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        action.execute(List.of("abc"), listener);

        var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));

        assertThat(thrownException.getMessage(), is("failed"));
    }

    public void testExecute_ThrowsElasticsearchException_WhenSenderOnFailureIsCalled() {
        var sender = mock(Sender.class);

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<InferenceServiceResults> listener = (ActionListener<InferenceServiceResults>) invocation.getArguments()[1];
            listener.onFailure(new IllegalStateException("failed"));

            return Void.TYPE;
        }).when(sender).send(any(), any(), any());

        var action = createAction(getUrl(webServer), "org", "secret", "model", "user", sender);

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        action.execute(List.of("abc"), listener);

        var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));

        assertThat(thrownException.getMessage(), is(format("Failed to send OpenAI chat completions request to [%s]", getUrl(webServer))));
    }

    public void testExecute_ThrowsElasticsearchException_WhenSenderOnFailureIsCalled_WhenUrlIsNull() {
        var sender = mock(Sender.class);

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<InferenceServiceResults> listener = (ActionListener<InferenceServiceResults>) invocation.getArguments()[1];
            listener.onFailure(new IllegalStateException("failed"));

            return Void.TYPE;
        }).when(sender).send(any(), any(), any());

        var action = createAction(null, "org", "secret", "model", "user", sender);

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        action.execute(List.of("abc"), listener);

        var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));

        assertThat(thrownException.getMessage(), is("Failed to send OpenAI chat completions request"));
    }

    public void testExecute_ThrowsException() {
        var sender = mock(Sender.class);
        doThrow(new IllegalArgumentException("failed")).when(sender).send(any(), any(), any());

        var action = createAction(getUrl(webServer), "org", "secret", "model", "user", sender);

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        action.execute(List.of("abc"), listener);

        var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));

        assertThat(thrownException.getMessage(), is(format("Failed to send OpenAI chat completions request to [%s]", getUrl(webServer))));
    }

    public void testExecute_ThrowsExceptionWithNullUrl() {
        var sender = mock(Sender.class);
        doThrow(new IllegalArgumentException("failed")).when(sender).send(any(), any(), any());

        var action = createAction(null, "org", "secret", "model", "user", sender);

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        action.execute(List.of("abc"), listener);

        var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));

        assertThat(thrownException.getMessage(), is("Failed to send OpenAI chat completions request"));
    }

    public static Map<String, Object> buildExpectedChatCompletionResultMap(List<String> results){
        return Map.of(
            ChatCompletionResults.COMPLETION,
            results.stream().map(result -> Map.of(ChatCompletionResults.Result.RESULT, result)).toList()
        );
    }

    private OpenAiChatCompletionAction createAction(
        String url,
        String org,
        String apiKey,
        String modelName,
        @Nullable String user,
        Sender sender
    ){
        var model = createChatCompletionModel(url, org, apiKey, modelName, user);

        return new OpenAiChatCompletionAction(sender, model, createWithEmptySettings(threadPool));
    }
}
