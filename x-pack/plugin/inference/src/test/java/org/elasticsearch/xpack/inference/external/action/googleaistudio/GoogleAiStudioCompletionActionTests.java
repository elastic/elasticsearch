/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.action.googleaistudio;

import org.apache.http.HttpHeaders;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.SingleInputSenderExecutableAction;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.sender.DocumentsOnlyInput;
import org.elasticsearch.xpack.inference.external.http.sender.GoogleAiStudioCompletionRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.services.googleaistudio.completion.GoogleAiStudioCompletionModelTests;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityPool;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;
import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.services.googleaistudio.GoogleAiStudioServiceTests.buildExpectationCompletions;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

public class GoogleAiStudioCompletionActionTests extends ESTestCase {

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

        try (var sender = HttpRequestSenderTests.createSender(senderFactory)) {
            sender.start();

            String responseJson = """
                {
                    "candidates": [
                        {
                            "content": {
                                "parts": [
                                    {
                                        "text": "result"
                                    }
                                ],
                                "role": "model"
                            },
                            "finishReason": "STOP",
                            "index": 0,
                            "safetyRatings": [
                                {
                                    "category": "HARM_CATEGORY_SEXUALLY_EXPLICIT",
                                    "probability": "NEGLIGIBLE"
                                },
                                {
                                    "category": "HARM_CATEGORY_HATE_SPEECH",
                                    "probability": "NEGLIGIBLE"
                                },
                                {
                                    "category": "HARM_CATEGORY_HARASSMENT",
                                    "probability": "NEGLIGIBLE"
                                },
                                {
                                    "category": "HARM_CATEGORY_DANGEROUS_CONTENT",
                                    "probability": "NEGLIGIBLE"
                                }
                            ]
                        }
                    ],
                    "usageMetadata": {
                        "promptTokenCount": 4,
                        "candidatesTokenCount": 566,
                        "totalTokenCount": 570
                    }
                }
                """;

            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var action = createAction(getUrl(webServer), "secret", "model", sender);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(new DocumentsOnlyInput(List.of("input")), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

            var result = listener.actionGet(TIMEOUT);

            assertThat(result.asMap(), is(buildExpectationCompletions(List.of("result"))));
            assertThat(webServer.requests(), hasSize(1));
            assertThat(webServer.requests().get(0).getUri().getQuery(), is("key=secret"));
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            assertThat(
                requestMap,
                is(
                    Map.of(
                        "contents",
                        List.of(Map.of("role", "user", "parts", List.of(Map.of("text", "input")))),
                        "generationConfig",
                        Map.of("candidateCount", 1)
                    )
                )
            );
        }
    }

    public void testExecute_ThrowsElasticsearchException() {
        var sender = mock(Sender.class);
        doThrow(new ElasticsearchException("failed")).when(sender).send(any(), any(), any(), any());

        var action = createAction(getUrl(webServer), "secret", "model", sender);

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        action.execute(new DocumentsOnlyInput(List.of("abc")), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

        var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));

        assertThat(thrownException.getMessage(), is("failed"));
    }

    public void testExecute_ThrowsElasticsearchException_WhenSenderOnFailureIsCalled() {
        var sender = mock(Sender.class);

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<InferenceServiceResults> listener = (ActionListener<InferenceServiceResults>) invocation.getArguments()[2];
            listener.onFailure(new IllegalStateException("failed"));

            return Void.TYPE;
        }).when(sender).send(any(), any(), any(), any());

        var action = createAction(getUrl(webServer), "secret", "model", sender);

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        action.execute(new DocumentsOnlyInput(List.of("abc")), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

        var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));

        assertThat(
            thrownException.getMessage(),
            is(format("Failed to send Google AI Studio completion request to [%s]", getUrl(webServer)))
        );
    }

    public void testExecute_ThrowsException() {
        var sender = mock(Sender.class);
        doThrow(new IllegalArgumentException("failed")).when(sender).send(any(), any(), any(), any());

        var action = createAction(getUrl(webServer), "secret", "model", sender);

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        action.execute(new DocumentsOnlyInput(List.of("abc")), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

        var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));

        assertThat(
            thrownException.getMessage(),
            is(format("Failed to send Google AI Studio completion request to [%s]", getUrl(webServer)))
        );
    }

    public void testExecute_ThrowsException_WhenInputIsGreaterThanOne() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var sender = HttpRequestSenderTests.createSender(senderFactory)) {
            sender.start();

            String responseJson = """
                {
                    "candidates": [
                        {
                            "content": {
                                "parts": [
                                    {
                                        "text": "result"
                                    }
                                ],
                                "role": "model"
                            },
                            "finishReason": "STOP",
                            "index": 0,
                            "safetyRatings": [
                                {
                                    "category": "HARM_CATEGORY_SEXUALLY_EXPLICIT",
                                    "probability": "NEGLIGIBLE"
                                },
                                {
                                    "category": "HARM_CATEGORY_HATE_SPEECH",
                                    "probability": "NEGLIGIBLE"
                                },
                                {
                                    "category": "HARM_CATEGORY_HARASSMENT",
                                    "probability": "NEGLIGIBLE"
                                },
                                {
                                    "category": "HARM_CATEGORY_DANGEROUS_CONTENT",
                                    "probability": "NEGLIGIBLE"
                                }
                            ]
                        }
                    ],
                    "usageMetadata": {
                        "promptTokenCount": 4,
                        "candidatesTokenCount": 566,
                        "totalTokenCount": 570
                    }
                }
                """;

            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var action = createAction(getUrl(webServer), "secret", "model", sender);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(new DocumentsOnlyInput(List.of("abc", "def")), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

            var thrownException = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TIMEOUT));

            assertThat(thrownException.getMessage(), is("Google AI Studio completion only accepts 1 input"));
            assertThat(thrownException.status(), is(RestStatus.BAD_REQUEST));
        }
    }

    private ExecutableAction createAction(String url, String apiKey, String modelName, Sender sender) {
        var model = GoogleAiStudioCompletionModelTests.createModel(modelName, url, apiKey);
        var requestManager = new GoogleAiStudioCompletionRequestManager(model, threadPool);
        var failedToSendRequestErrorMessage = constructFailedToSendRequestMessage(model.uri(), "Google AI Studio completion");
        return new SingleInputSenderExecutableAction(
            sender,
            requestManager,
            failedToSendRequestErrorMessage,
            "Google AI Studio completion"
        );
    }

}
