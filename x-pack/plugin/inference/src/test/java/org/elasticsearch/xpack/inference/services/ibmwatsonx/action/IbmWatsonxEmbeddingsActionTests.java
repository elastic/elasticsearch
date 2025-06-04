/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ibmwatsonx.action;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.inference.InputTypeTests;
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.common.TruncatorTests;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.SenderExecutableAction;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.sender.EmbeddingsInput;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.IbmWatsonxEmbeddingsRequestManager;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.embeddings.IbmWatsonxEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.request.IbmWatsonxEmbeddingsRequest;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.core.inference.results.TextEmbeddingFloatResultsTests.buildExpectationFloat;
import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityPool;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;
import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.elasticsearch.xpack.inference.services.ibmwatsonx.embeddings.IbmWatsonxEmbeddingsModelTests.createModel;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

public class IbmWatsonxEmbeddingsActionTests extends ESTestCase {
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
        var apiKey = "apiKey";
        var model = "model";
        var input = "input";
        var projectId = "projectId";
        URI uri = URI.create("https://abc.com");
        var apiVersion = "apiVersion";

        var senderFactory = new HttpRequestSender.Factory(createWithEmptySettings(threadPool), clientManager, mockClusterServiceEmpty());

        try (var sender = senderFactory.createSender()) {
            sender.start();

            String responseJson = """
                    {
                        "results": [
                           {
                               "embedding": [
                                  0.0123,
                                  -0.0123
                               ],
                              "input": "abc"
                           }
                        ]
                   }
                """;

            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var action = createAction(getUrl(webServer), apiKey, model, projectId, uri, apiVersion, sender);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(
                new EmbeddingsInput(List.of(input), null, InputTypeTests.randomWithNull()),
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var result = listener.actionGet(TIMEOUT);

            assertThat(result.asMap(), is(buildExpectationFloat(List.of(new float[] { 0.0123F, -0.0123F }))));
            assertThat(webServer.requests(), hasSize(1));
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            assertThat(requestMap, aMapWithSize(3));
            assertThat(requestMap, is(Map.of("project_id", "projectId", "inputs", List.of(input), "model_id", "model")));
        }
    }

    public void testExecute_ThrowsElasticsearchException() {
        var sender = mock(Sender.class);
        var apiKey = "apiKey";
        var model = "model";
        var projectId = "projectId";
        URI uri = URI.create("https://abc.com");
        var apiVersion = "apiVersion";
        doThrow(new ElasticsearchException("failed")).when(sender).send(any(), any(), any(), any());

        var action = createAction(getUrl(webServer), apiKey, model, projectId, uri, apiVersion, sender);

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
        var apiKey = "apiKey";
        var model = "model";
        var projectId = "projectId";
        URI uri = URI.create("https://abc.com");
        var apiVersion = "apiVersion";

        doAnswer(invocation -> {
            ActionListener<InferenceServiceResults> listener = invocation.getArgument(3);
            listener.onFailure(new IllegalStateException("failed"));

            return Void.TYPE;
        }).when(sender).send(any(), any(), any(), any());

        var action = createAction(getUrl(webServer), apiKey, model, projectId, uri, apiVersion, sender);

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        action.execute(
            new EmbeddingsInput(List.of("abc"), null, InputTypeTests.randomWithNull()),
            InferenceAction.Request.DEFAULT_TIMEOUT,
            listener
        );

        var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));

        assertThat(thrownException.getMessage(), is("Failed to send IBM Watsonx embeddings request. Cause: failed"));
    }

    public void testExecute_ThrowsException() {
        var sender = mock(Sender.class);
        var apiKey = "apiKey";
        var model = "model";
        var projectId = "projectId";
        URI uri = URI.create("https://abc.com");
        var apiVersion = "apiVersion";

        doThrow(new IllegalArgumentException("failed")).when(sender).send(any(), any(), any(), any());

        var action = createAction(getUrl(webServer), apiKey, model, projectId, uri, apiVersion, sender);

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        action.execute(
            new EmbeddingsInput(List.of("abc"), null, InputTypeTests.randomWithNull()),
            InferenceAction.Request.DEFAULT_TIMEOUT,
            listener
        );

        var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));

        assertThat(thrownException.getMessage(), is("Failed to send IBM Watsonx embeddings request. Cause: failed"));
    }

    private ExecutableAction createAction(
        String url,
        String apiKey,
        String modelName,
        String projectId,
        URI uri,
        String apiVersion,
        Sender sender
    ) {
        var model = createModel(modelName, projectId, uri, apiVersion, apiKey, url);
        var requestManager = new IbmWatsonxEmbeddingsRequestManagerWithoutAuth(model, TruncatorTests.createTruncator(), threadPool);
        var failedToSendRequestErrorMessage = constructFailedToSendRequestMessage("IBM Watsonx embeddings");
        return new SenderExecutableAction(sender, requestManager, failedToSendRequestErrorMessage);
    }

    private static class IbmWatsonxEmbeddingsRequestManagerWithoutAuth extends IbmWatsonxEmbeddingsRequestManager {
        IbmWatsonxEmbeddingsRequestManagerWithoutAuth(IbmWatsonxEmbeddingsModel model, Truncator truncator, ThreadPool threadPool) {
            super(model, truncator, threadPool);
        }

        @Override
        protected IbmWatsonxEmbeddingsRequest getEmbeddingRequest(
            Truncator truncator,
            Truncator.TruncationResult truncatedInput,
            IbmWatsonxEmbeddingsModel model
        ) {
            return new IbmWatsonxEmbeddingsWithoutAuthRequest(truncator, truncatedInput, model);
        }

    }

    private static class IbmWatsonxEmbeddingsWithoutAuthRequest extends IbmWatsonxEmbeddingsRequest {
        private static final String AUTH_HEADER_VALUE = "foo";

        IbmWatsonxEmbeddingsWithoutAuthRequest(Truncator truncator, Truncator.TruncationResult input, IbmWatsonxEmbeddingsModel model) {
            super(truncator, input, model);
        }

        @Override
        public void decorateWithAuth(HttpPost httpPost) {
            httpPost.setHeader(HttpHeaders.AUTHORIZATION, AUTH_HEADER_VALUE);
        }

        @Override
        public Request truncate() {
            IbmWatsonxEmbeddingsRequest embeddingsRequest = (IbmWatsonxEmbeddingsRequest) super.truncate();
            return new IbmWatsonxEmbeddingsWithoutAuthRequest(
                embeddingsRequest.truncator(),
                embeddingsRequest.truncationResult(),
                embeddingsRequest.model()
            );
        }
    }
}
