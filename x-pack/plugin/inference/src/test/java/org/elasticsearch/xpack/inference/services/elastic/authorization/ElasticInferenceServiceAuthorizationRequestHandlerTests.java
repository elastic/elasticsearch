/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.authorization;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.results.ChatCompletionResults;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityPool;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.external.http.retry.RetryingHttpSender.MAX_RETIES;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class ElasticInferenceServiceAuthorizationRequestHandlerTests extends ESTestCase {
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

    public void testDoesNotAttempt_ToRetrieveAuthorization_IfBaseUrlIsNull() throws Exception {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        var logger = mock(Logger.class);
        var authHandler = new ElasticInferenceServiceAuthorizationRequestHandler(null, threadPool, logger);

        try (var sender = senderFactory.createSender()) {
            PlainActionFuture<ElasticInferenceServiceAuthorizationModel> listener = new PlainActionFuture<>();
            authHandler.getAuthorization(listener, sender);

            var authResponse = listener.actionGet(TIMEOUT);
            assertTrue(authResponse.getAuthorizedTaskTypes().isEmpty());
            assertTrue(authResponse.getAuthorizedModelIds().isEmpty());
            assertFalse(authResponse.isAuthorized());

            var loggerArgsCaptor = ArgumentCaptor.forClass(String.class);
            verify(logger, times(2)).debug(loggerArgsCaptor.capture());
            var messages = loggerArgsCaptor.getAllValues();
            assertThat(messages.getFirst(), is("Retrieving authorization information from the Elastic Inference Service."));
            assertThat(messages.get(1), is("The base URL for the authorization service is not valid, rejecting authorization."));
        }
    }

    public void testDoesNotAttempt_ToRetrieveAuthorization_IfBaseUrlIsEmpty() throws Exception {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        var logger = mock(Logger.class);
        var authHandler = new ElasticInferenceServiceAuthorizationRequestHandler("", threadPool, logger);

        try (var sender = senderFactory.createSender()) {
            PlainActionFuture<ElasticInferenceServiceAuthorizationModel> listener = new PlainActionFuture<>();
            authHandler.getAuthorization(listener, sender);

            var authResponse = listener.actionGet(TIMEOUT);
            assertTrue(authResponse.getAuthorizedTaskTypes().isEmpty());
            assertTrue(authResponse.getAuthorizedModelIds().isEmpty());
            assertFalse(authResponse.isAuthorized());

            var loggerArgsCaptor = ArgumentCaptor.forClass(String.class);
            verify(logger, times(2)).debug(loggerArgsCaptor.capture());
            var messages = loggerArgsCaptor.getAllValues();
            assertThat(messages.getFirst(), is("Retrieving authorization information from the Elastic Inference Service."));
            assertThat(messages.get(1), is("The base URL for the authorization service is not valid, rejecting authorization."));
        }
    }

    public void testGetAuthorization_FailsWhenAnInvalidFieldIsFound() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        var eisGatewayUrl = getUrl(webServer);
        var logger = mock(Logger.class);
        var authHandler = new ElasticInferenceServiceAuthorizationRequestHandler(eisGatewayUrl, threadPool, logger);

        try (var sender = senderFactory.createSender()) {
            String responseJson = """
                {
                    "models": [
                        {
                          "invalid-field": "model-a",
                          "task-types": ["embed/text/sparse", "chat"]
                        }
                    ]
                }
                """;

            queueWebServerResponsesForRetries(responseJson);

            PlainActionFuture<ElasticInferenceServiceAuthorizationModel> listener = new PlainActionFuture<>();
            authHandler.getAuthorization(listener, sender);

            var authResponse = listener.actionGet(TIMEOUT);
            assertTrue(authResponse.getAuthorizedTaskTypes().isEmpty());
            assertTrue(authResponse.getAuthorizedModelIds().isEmpty());
            assertFalse(authResponse.isAuthorized());

            var loggerArgsCaptor = ArgumentCaptor.forClass(String.class);
            verify(logger).warn(loggerArgsCaptor.capture());
            var message = loggerArgsCaptor.getValue();
            assertThat(
                message,
                is(
                    "Failed to retrieve the authorization information from the Elastic Inference Service."
                        + " Encountered an exception: org.elasticsearch.xcontent.XContentParseException: [4:28] "
                        + "[ElasticInferenceServiceAuthorizationResponseEntity] failed to parse field [models]"
                )
            );
        }
    }

    /**
     * Queues the required number of responses to handle the retries of the internal sender.
     */
    private void queueWebServerResponsesForRetries(String responseJson) {
        for (int i = 0; i < MAX_RETIES; i++) {
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));
        }
    }

    public void testGetAuthorization_ReturnsAValidResponse() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        var eisGatewayUrl = getUrl(webServer);
        var logger = mock(Logger.class);
        var authHandler = new ElasticInferenceServiceAuthorizationRequestHandler(eisGatewayUrl, threadPool, logger);

        try (var sender = senderFactory.createSender()) {
            String responseJson = """
                {
                    "models": [
                        {
                          "model_name": "model-a",
                          "task_types": ["embed/text/sparse", "chat"]
                        }
                    ]
                }
                """;

            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            PlainActionFuture<ElasticInferenceServiceAuthorizationModel> listener = new PlainActionFuture<>();
            authHandler.getAuthorization(listener, sender);

            var authResponse = listener.actionGet(TIMEOUT);
            assertThat(authResponse.getAuthorizedTaskTypes(), is(EnumSet.of(TaskType.SPARSE_EMBEDDING, TaskType.CHAT_COMPLETION)));
            assertThat(authResponse.getAuthorizedModelIds(), is(Set.of("model-a")));
            assertTrue(authResponse.isAuthorized());

            var loggerArgsCaptor = ArgumentCaptor.forClass(String.class);
            verify(logger, times(1)).debug(loggerArgsCaptor.capture());

            var message = loggerArgsCaptor.getValue();
            assertThat(message, is("Retrieving authorization information from the Elastic Inference Service."));
            verifyNoMoreInteractions(logger);
        }
    }

    @SuppressWarnings("unchecked")
    public void testGetAuthorization_OnResponseCalledOnce() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        var eisGatewayUrl = getUrl(webServer);
        var logger = mock(Logger.class);
        var authHandler = new ElasticInferenceServiceAuthorizationRequestHandler(eisGatewayUrl, threadPool, logger);

        ActionListener<ElasticInferenceServiceAuthorizationModel> listener = mock(ActionListener.class);
        String responseJson = """
                {
                    "models": [
                        {
                          "model_name": "model-a",
                          "task_types": ["embed/text/sparse", "chat"]
                        }
                    ]
                }
            """;
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

        try (var sender = senderFactory.createSender()) {
            authHandler.getAuthorization(listener, sender);
            authHandler.waitForAuthRequestCompletion(TIMEOUT);

            verify(listener, times(1)).onResponse(any());
            var loggerArgsCaptor = ArgumentCaptor.forClass(String.class);
            verify(logger, times(1)).debug(loggerArgsCaptor.capture());

            var message = loggerArgsCaptor.getValue();
            assertThat(message, is("Retrieving authorization information from the Elastic Inference Service."));
            verifyNoMoreInteractions(logger);
        }
    }

    public void testGetAuthorization_InvalidResponse() throws IOException {
        var senderMock = mock(Sender.class);
        var senderFactory = mock(HttpRequestSender.Factory.class);
        when(senderFactory.createSender()).thenReturn(senderMock);

        doAnswer(invocationOnMock -> {
            ActionListener<InferenceServiceResults> listener = invocationOnMock.getArgument(4);
            listener.onResponse(new ChatCompletionResults(List.of(new ChatCompletionResults.Result("awesome"))));
            return Void.TYPE;
        }).when(senderMock).sendWithoutQueuing(any(), any(), any(), any(), any());

        var logger = mock(Logger.class);
        var authHandler = new ElasticInferenceServiceAuthorizationRequestHandler("abc", threadPool, logger);

        try (var sender = senderFactory.createSender()) {
            PlainActionFuture<ElasticInferenceServiceAuthorizationModel> listener = new PlainActionFuture<>();

            authHandler.getAuthorization(listener, sender);
            var result = listener.actionGet(TIMEOUT);

            assertThat(result, is(ElasticInferenceServiceAuthorizationModel.newDisabledService()));

            var loggerArgsCaptor = ArgumentCaptor.forClass(String.class);
            verify(logger).warn(loggerArgsCaptor.capture());
            var message = loggerArgsCaptor.getValue();
            assertThat(
                message,
                is(
                    "Failed to retrieve the authorization information from the Elastic Inference Service."
                        + " Received an invalid response type: ChatCompletionResults"
                )
            );
        }

    }
}
