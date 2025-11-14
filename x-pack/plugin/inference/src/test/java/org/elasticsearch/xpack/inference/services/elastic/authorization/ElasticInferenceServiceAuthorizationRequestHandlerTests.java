/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.authorization;

import org.apache.hc.core5.http.HttpHeaders;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockRequest;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xpack.core.inference.results.ChatCompletionResults;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityExecutors;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.external.http.retry.RetryingHttpSender.MAX_RETIES;
import static org.elasticsearch.xpack.inference.external.request.RequestUtils.bearerToken;
import static org.elasticsearch.xpack.inference.services.SenderServiceTests.createMockSender;
import static org.elasticsearch.xpack.inference.services.elastic.ccm.CCMAuthenticationApplierFactoryTests.createApplierFactory;
import static org.elasticsearch.xpack.inference.services.elastic.ccm.CCMAuthenticationApplierFactoryTests.createNoopApplierFactory;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ElasticInferenceServiceAuthorizationRequestHandlerTests extends ESTestCase {
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

    public void testDoesNotAttempt_ToRetrieveAuthorization_IfBaseUrlIsNull() throws Exception {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        var logger = mock(Logger.class);
        var authHandler = new ElasticInferenceServiceAuthorizationRequestHandler(null, threadPool, logger, createNoopApplierFactory());

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
        var authHandler = new ElasticInferenceServiceAuthorizationRequestHandler("", threadPool, logger, createNoopApplierFactory());

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
        var authHandler = new ElasticInferenceServiceAuthorizationRequestHandler(
            eisGatewayUrl,
            threadPool,
            logger,
            createNoopApplierFactory()
        );

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

            var exception = expectThrows(XContentParseException.class, () -> listener.actionGet(TIMEOUT));
            assertThat(exception.getMessage(), containsString("failed to parse field [models]"));

            var stringCaptor = ArgumentCaptor.forClass(String.class);
            var exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
            verify(logger).warn(stringCaptor.capture(), exceptionCaptor.capture());
            var message = stringCaptor.getValue();
            assertThat(message, containsString("failed to parse field [models]"));

            var capturedException = exceptionCaptor.getValue();
            assertThat(capturedException, instanceOf(XContentParseException.class));
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
        var authHandler = new ElasticInferenceServiceAuthorizationRequestHandler(
            eisGatewayUrl,
            threadPool,
            logger,
            createNoopApplierFactory()
        );

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

            assertNoAuthHeader(webServer.requests());
        }
    }

    private static void assertNoAuthHeader(List<MockRequest> requests) {
        assertThat(requests.size(), is(1));
        assertNull(requests.get(0).getHeader(HttpHeaders.AUTHORIZATION));
    }

    public void testGetAuthorization_ReturnsAValidResponse_WithAuthHeader() throws IOException {
        var secret = "secret-token";
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        var eisGatewayUrl = getUrl(webServer);
        var logger = mock(Logger.class);

        var authHandler = new ElasticInferenceServiceAuthorizationRequestHandler(
            eisGatewayUrl,
            threadPool,
            logger,
            createApplierFactory(secret)
        );

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

            assertAuthHeader(webServer.requests(), secret);
        }
    }

    private static void assertAuthHeader(List<MockRequest> requests, String secret) {
        assertThat(requests.size(), is(1));
        assertThat(requests.get(0).getHeader(HttpHeaders.AUTHORIZATION), is(bearerToken(secret)));
    }

    public void testGetAuthorization_OnResponseCalledOnce() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        var eisGatewayUrl = getUrl(webServer);
        var logger = mock(Logger.class);
        var authHandler = new ElasticInferenceServiceAuthorizationRequestHandler(
            eisGatewayUrl,
            threadPool,
            logger,
            createNoopApplierFactory()
        );

        PlainActionFuture<ElasticInferenceServiceAuthorizationModel> listener = new PlainActionFuture<>();
        ActionListener<ElasticInferenceServiceAuthorizationModel> onlyOnceListener = ActionListener.assertOnce(listener);
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
            authHandler.getAuthorization(onlyOnceListener, sender);
            authHandler.waitForAuthRequestCompletion(TIMEOUT);

            var authResponse = listener.actionGet(TIMEOUT);
            assertThat(authResponse.getAuthorizedTaskTypes(), is(EnumSet.of(TaskType.SPARSE_EMBEDDING, TaskType.CHAT_COMPLETION)));
            assertThat(authResponse.getAuthorizedModelIds(), is(Set.of("model-a")));
            assertTrue(authResponse.isAuthorized());

            var loggerArgsCaptor = ArgumentCaptor.forClass(String.class);
            verify(logger, times(1)).debug(loggerArgsCaptor.capture());

            var message = loggerArgsCaptor.getValue();
            assertThat(message, is("Retrieving authorization information from the Elastic Inference Service."));
        }
    }

    public void testGetAuthorization_InvalidResponse() throws IOException {
        var senderMock = createMockSender();
        var senderFactory = mock(HttpRequestSender.Factory.class);
        when(senderFactory.createSender()).thenReturn(senderMock);

        doAnswer(invocationOnMock -> {
            ActionListener<InferenceServiceResults> listener = invocationOnMock.getArgument(4);
            listener.onResponse(new ChatCompletionResults(List.of(new ChatCompletionResults.Result("awesome"))));
            return Void.TYPE;
        }).when(senderMock).sendWithoutQueuing(any(), any(), any(), any(), any());

        var logger = mock(Logger.class);
        var authHandler = new ElasticInferenceServiceAuthorizationRequestHandler("abc", threadPool, logger, createNoopApplierFactory());

        try (var sender = senderFactory.createSender()) {
            PlainActionFuture<ElasticInferenceServiceAuthorizationModel> listener = new PlainActionFuture<>();

            authHandler.getAuthorization(listener, sender);
            var exception = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));

            assertThat(exception.getMessage(), containsString("Received an invalid response type from the Elastic Inference Service"));

            var loggerArgsCaptor = ArgumentCaptor.forClass(String.class);
            verify(logger).warn(loggerArgsCaptor.capture());
            var message = loggerArgsCaptor.getValue();
            assertThat(message, containsString("Failed to retrieve the authorization information from the Elastic Inference Service."));
        }
    }
}
