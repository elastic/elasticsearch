/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.action;

import org.apache.http.HttpHeaders;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.inference.ChunkInferenceInput;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.inference.InputTypeTests;
import org.elasticsearch.xpack.inference.common.TruncatorTests;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.SenderExecutableAction;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.sender.EmbeddingsInput;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.external.http.sender.TruncatingRequestManager;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.services.ServiceComponentsTests;
import org.elasticsearch.xpack.inference.services.openai.request.OpenAiEmbeddingsRequest;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.core.inference.results.TextEmbeddingFloatResultsTests.buildExpectationFloat;
import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityPool;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;
import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.services.openai.OpenAiUtils.ORGANIZATION_HEADER;
import static org.elasticsearch.xpack.inference.services.openai.action.OpenAiActionCreator.EMBEDDINGS_HANDLER;
import static org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsModelTests.createModel;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

public class OpenAiEmbeddingsActionTests extends ESTestCase {
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
        var senderFactory = new HttpRequestSender.Factory(
            ServiceComponentsTests.createWithEmptySettings(threadPool),
            clientManager,
            mockClusterServiceEmpty()
        );

        try (var sender = senderFactory.createSender()) {
            sender.start();

            String responseJson = """
                {
                  "object": "list",
                  "data": [
                      {
                          "object": "embedding",
                          "index": 0,
                          "embedding": [
                              0.0123,
                              -0.0123
                          ]
                      }
                  ],
                  "model": "text-embedding-ada-002-v2",
                  "usage": {
                      "prompt_tokens": 8,
                      "total_tokens": 8
                  }
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var action = createAction(getUrl(webServer), "org", "secret", "model", "user", sender);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(
                new EmbeddingsInput(List.of("abc"), null, InputTypeTests.randomWithNull()),
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var result = listener.actionGet(TIMEOUT);

            assertThat(result.asMap(), is(buildExpectationFloat(List.of(new float[] { 0.0123F, -0.0123F }))));
            assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().get(0).getUri().getQuery());
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer secret"));
            assertThat(webServer.requests().get(0).getHeader(ORGANIZATION_HEADER), equalTo("org"));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            assertThat(requestMap.size(), is(3));
            assertThat(requestMap.get("input"), is(List.of("abc")));
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
            assertThat(thrownException.getMessage(), containsString("unable to parse url [^^]"));
        }
    }

    public void testExecute_ThrowsElasticsearchException() {
        var sender = mock(Sender.class);
        doThrow(new ElasticsearchException("failed")).when(sender).send(any(), any(), any(), any());

        var action = createAction(getUrl(webServer), "org", "secret", "model", "user", sender);

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        action.execute(
            new EmbeddingsInput(List.of("abc"), null, InputTypeTests.randomWithNull()),
            InferenceAction.Request.DEFAULT_TIMEOUT,
            listener
        );

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

        var action = createAction(getUrl(webServer), "org", "secret", "model", "user", sender);

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        action.execute(
            new EmbeddingsInput(List.of(new ChunkInferenceInput("abc")), InputTypeTests.randomWithNull()),
            InferenceAction.Request.DEFAULT_TIMEOUT,
            listener
        );

        var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));

        assertThat(thrownException.getMessage(), is("Failed to send OpenAI embeddings request. Cause: failed"));
    }

    public void testExecute_ThrowsElasticsearchException_WhenSenderOnFailureIsCalled_WhenUrlIsNull() {
        var sender = mock(Sender.class);

        doAnswer(invocation -> {
            ActionListener<InferenceServiceResults> listener = invocation.getArgument(3);
            listener.onFailure(new IllegalStateException("failed"));

            return Void.TYPE;
        }).when(sender).send(any(), any(), any(), any());

        var action = createAction(null, "org", "secret", "model", "user", sender);

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        action.execute(
            new EmbeddingsInput(List.of(new ChunkInferenceInput("abc")), InputTypeTests.randomWithNull()),
            InferenceAction.Request.DEFAULT_TIMEOUT,
            listener
        );

        var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));

        assertThat(thrownException.getMessage(), is("Failed to send OpenAI embeddings request. Cause: failed"));
    }

    public void testExecute_ThrowsException() {
        var sender = mock(Sender.class);
        doThrow(new IllegalArgumentException("failed")).when(sender).send(any(), any(), any(), any());

        var action = createAction(getUrl(webServer), "org", "secret", "model", "user", sender);

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        action.execute(
            new EmbeddingsInput(List.of(new ChunkInferenceInput("abc")), InputTypeTests.randomWithNull()),
            InferenceAction.Request.DEFAULT_TIMEOUT,
            listener
        );

        var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));

        assertThat(thrownException.getMessage(), is("Failed to send OpenAI embeddings request. Cause: failed"));
    }

    public void testExecute_ThrowsExceptionWithNullUrl() {
        var sender = mock(Sender.class);
        doThrow(new IllegalArgumentException("failed")).when(sender).send(any(), any(), any(), any());

        var action = createAction(null, "org", "secret", "model", "user", sender);

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        action.execute(
            new EmbeddingsInput(List.of(new ChunkInferenceInput("abc")), InputTypeTests.randomWithNull()),
            InferenceAction.Request.DEFAULT_TIMEOUT,
            listener
        );

        var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));

        assertThat(thrownException.getMessage(), is("Failed to send OpenAI embeddings request. Cause: failed"));
    }

    private ExecutableAction createAction(String url, String org, String apiKey, String modelName, @Nullable String user, Sender sender) {
        var model = createModel(url, org, apiKey, modelName, user);
        var manager = new TruncatingRequestManager(
            threadPool,
            model,
            EMBEDDINGS_HANDLER,
            (truncationResult) -> new OpenAiEmbeddingsRequest(TruncatorTests.createTruncator(), truncationResult, model),
            null
        );

        var errorMessage = constructFailedToSendRequestMessage("OpenAI embeddings");
        return new SenderExecutableAction(sender, manager, errorMessage);
    }

}
