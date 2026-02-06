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
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceModel;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMFeature;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMService;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityExecutors;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.external.http.retry.RetryingHttpSender.MAX_RETRIES;
import static org.elasticsearch.xpack.inference.external.request.RequestUtils.apiKey;
import static org.elasticsearch.xpack.inference.services.SenderServiceTests.createMockSender;
import static org.elasticsearch.xpack.inference.services.elastic.ccm.CCMAuthenticationApplierFactoryTests.createApplierFactory;
import static org.elasticsearch.xpack.inference.services.elastic.ccm.CCMAuthenticationApplierFactoryTests.createNoopApplierFactory;
import static org.elasticsearch.xpack.inference.services.elastic.response.ElasticInferenceServiceAuthorizationResponseEntityTests.ELSER_V2_ENDPOINT_ID;
import static org.elasticsearch.xpack.inference.services.elastic.response.ElasticInferenceServiceAuthorizationResponseEntityTests.getEisAuthorizationResponseWithMultipleEndpoints;
import static org.elasticsearch.xpack.inference.services.elastic.response.ElasticInferenceServiceAuthorizationResponseEntityTests.getEisElserAuthorizationResponse;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ElasticInferenceServiceAuthorizationRequestHandlerTests extends ESTestCase {
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
        var authHandler = new ElasticInferenceServiceAuthorizationRequestHandler(
            null,
            threadPool,
            logger,
            createNoopApplierFactory(),
            createMockCcmFeature(false),
            createMockCcmService(false)
        );

        try (var sender = senderFactory.createSender()) {
            PlainActionFuture<ElasticInferenceServiceAuthorizationModel> listener = new PlainActionFuture<>();
            authHandler.getAuthorization(listener, sender);

            var authResponse = listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT);
            assertTrue(authResponse.getTaskTypes().isEmpty());
            assertTrue(authResponse.getEndpointIds().isEmpty());
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
        var authHandler = new ElasticInferenceServiceAuthorizationRequestHandler(
            "",
            threadPool,
            logger,
            createNoopApplierFactory(),
            createMockCcmFeature(false),
            createMockCcmService(false)
        );

        try (var sender = senderFactory.createSender()) {
            PlainActionFuture<ElasticInferenceServiceAuthorizationModel> listener = new PlainActionFuture<>();
            authHandler.getAuthorization(listener, sender);

            var authResponse = listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT);
            assertTrue(authResponse.getTaskTypes().isEmpty());
            assertTrue(authResponse.getEndpointIds().isEmpty());
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
            createNoopApplierFactory(),
            createMockCcmFeature(false),
            createMockCcmService(false)
        );

        try (var sender = senderFactory.createSender()) {
            String responseWithInvalidIdField = """
                {
                  "inference_endpoints": [
                    {
                      "id": 123,
                      "model_name": "jina-reranker-v2",
                      "task_type": "rerank",
                      "status": "preview",
                      "properties": [],
                      "release_date": "2024-05-01"
                    }
                  ]
                }
                """;

            queueWebServerResponsesForRetries(responseWithInvalidIdField);

            PlainActionFuture<ElasticInferenceServiceAuthorizationModel> listener = new PlainActionFuture<>();
            authHandler.getAuthorization(listener, sender);

            var exception = expectThrows(XContentParseException.class, () -> listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT));
            assertThat(exception.getMessage(), containsString("failed to parse field [inference_endpoints]"));

            var stringCaptor = ArgumentCaptor.forClass(String.class);
            var exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
            verify(logger).warn(stringCaptor.capture(), exceptionCaptor.capture());
            var message = stringCaptor.getValue();
            assertThat(message, containsString("failed to parse field [inference_endpoints]"));

            var capturedException = exceptionCaptor.getValue();
            assertThat(capturedException, instanceOf(XContentParseException.class));
        }
    }

    /**
     * Queues the required number of responses to handle the retries of the internal sender.
     */
    private void queueWebServerResponsesForRetries(String responseJson) {
        for (int i = 0; i < MAX_RETRIES; i++) {
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));
        }
    }

    public void testGetAuthorization_ReturnsAValidResponse() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        var eisGatewayUrl = getUrl(webServer);
        var logger = mock(Logger.class);
        var mockCcmFeature = createMockCcmFeature(false);
        var mockCcmService = createMockCcmService(false);

        var authHandler = new ElasticInferenceServiceAuthorizationRequestHandler(
            eisGatewayUrl,
            threadPool,
            logger,
            createNoopApplierFactory(),
            mockCcmFeature,
            mockCcmService
        );

        try (var sender = senderFactory.createSender()) {
            var responseData = getEisAuthorizationResponseWithMultipleEndpoints(eisGatewayUrl);

            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseData.responseJson()));

            PlainActionFuture<ElasticInferenceServiceAuthorizationModel> listener = new PlainActionFuture<>();
            authHandler.getAuthorization(listener, sender);

            var authResponse = listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT);
            assertThat(
                authResponse.getTaskTypes(),
                is(
                    EnumSet.of(
                        TaskType.CHAT_COMPLETION,
                        TaskType.SPARSE_EMBEDDING,
                        TaskType.TEXT_EMBEDDING,
                        TaskType.EMBEDDING,
                        TaskType.RERANK,
                        TaskType.COMPLETION
                    )
                )
            );
            assertThat(authResponse.getEndpointIds(), containsInAnyOrder(responseData.inferenceIds().toArray(String[]::new)));
            assertTrue(authResponse.isAuthorized());
            assertThat(
                authResponse.getEndpoints(responseData.inferenceIds()),
                containsInAnyOrder(responseData.expectedEndpoints().toArray(ElasticInferenceServiceModel[]::new))
            );

            var loggerArgsCaptor = ArgumentCaptor.forClass(String.class);
            verify(logger, times(1)).debug(loggerArgsCaptor.capture());

            var message = loggerArgsCaptor.getValue();
            assertThat(message, is("Retrieving authorization information from the Elastic Inference Service."));

            assertNoAuthHeader(webServer.requests());

            authHandler.waitForAuthRequestCompletion(ESTestCase.TEST_REQUEST_TIMEOUT);

            // It should never check if the CCM environment is supported since getAuthorization does not attempt to skip the authorization
            // check
            verify(mockCcmFeature, never()).isCcmSupportedEnvironment();
            verify(mockCcmService, never()).isEnabled(any());
        }
    }

    private static void assertNoAuthHeader(List<MockRequest> requests) {
        assertThat(requests.size(), is(1));
        assertNull(requests.get(0).getHeader(HttpHeaders.AUTHORIZATION));
    }

    public void testGetAuthorizationIfPermittedEnvironment_ReturnsFailure_WhenExceptionThrown() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        var eisGatewayUrl = getUrl(webServer);

        var exceptionToThrow = new IllegalStateException("exception");
        var mockCcmFeature = mock(CCMFeature.class);
        when(mockCcmFeature.isCcmSupportedEnvironment()).thenThrow(exceptionToThrow);
        var mockCcmService = createMockCcmService(false);

        var authHandler = new ElasticInferenceServiceAuthorizationRequestHandler(
            eisGatewayUrl,
            threadPool,
            createNoopApplierFactory(),
            mockCcmFeature,
            mockCcmService
        );

        try (var sender = senderFactory.createSender()) {
            PlainActionFuture<ElasticInferenceServiceAuthorizationModel> listener = new PlainActionFuture<>();
            authHandler.getAuthorizationIfPermittedEnvironment(listener, sender);

            var exception = expectThrows(IllegalStateException.class, () -> listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT));
            assertThat(exception, is(exceptionToThrow));

            // There should be no requests made to EIS because an exception should be thrown before the request is made
            assertThat(webServer.requests(), empty());

            authHandler.waitForAuthRequestCompletion(TimeValue.THIRTY_SECONDS);

            verify(mockCcmService, never()).isEnabled(any());
        }
    }

    public void testGetAuthorizationIfPermittedEnvironment_ReturnsAValidResponse_WhenNotCcmSupportedEnvironment() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        var eisGatewayUrl = getUrl(webServer);

        var mockCcmFeature = createMockCcmFeature(false);
        var mockCcmService = createMockCcmService(randomBoolean());

        assertReturnsValidResponse(eisGatewayUrl, mockCcmFeature, mockCcmService, senderFactory, 0);
    }

    private void assertReturnsValidResponse(
        String eisGatewayUrl,
        CCMFeature mockCcmFeature,
        CCMService mockCcmService,
        HttpRequestSender.Factory senderFactory,
        int numIsEnabledCalls
    ) throws IOException {
        var authHandler = new ElasticInferenceServiceAuthorizationRequestHandler(
            eisGatewayUrl,
            threadPool,
            logger,
            createNoopApplierFactory(),
            mockCcmFeature,
            mockCcmService
        );

        try (var sender = senderFactory.createSender()) {
            var responseData = getEisAuthorizationResponseWithMultipleEndpoints(eisGatewayUrl);

            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseData.responseJson()));

            PlainActionFuture<ElasticInferenceServiceAuthorizationModel> listener = new PlainActionFuture<>();
            authHandler.getAuthorizationIfPermittedEnvironment(listener, sender);

            var authResponse = listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT);
            assertThat(
                authResponse.getTaskTypes(),
                is(
                    EnumSet.of(
                        TaskType.CHAT_COMPLETION,
                        TaskType.SPARSE_EMBEDDING,
                        TaskType.TEXT_EMBEDDING,
                        TaskType.EMBEDDING,
                        TaskType.RERANK,
                        TaskType.COMPLETION
                    )
                )
            );
            assertThat(authResponse.getEndpointIds(), containsInAnyOrder(responseData.inferenceIds().toArray(String[]::new)));
            assertTrue(authResponse.isAuthorized());
            assertThat(
                authResponse.getEndpoints(responseData.inferenceIds()),
                containsInAnyOrder(responseData.expectedEndpoints().toArray(ElasticInferenceServiceModel[]::new))
            );

            assertNoAuthHeader(webServer.requests());

            authHandler.waitForAuthRequestCompletion(TimeValue.THIRTY_SECONDS);

            verify(mockCcmFeature, times(1)).isCcmSupportedEnvironment();
            verify(mockCcmService, times(numIsEnabledCalls)).isEnabled(any());
        }
    }

    public void testGetAuthorizationIfPermittedEnvironment_ReturnsAValidResponse_WhenCcmSupportedEnvironmentAndEnabled()
        throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        var eisGatewayUrl = getUrl(webServer);

        var mockCcmFeature = createMockCcmFeature(true);
        var mockCcmService = createMockCcmService(true);

        assertReturnsValidResponse(eisGatewayUrl, mockCcmFeature, mockCcmService, senderFactory, 1);
    }

    public void testGetAuthorizationIfPermittedEnvironment_ReturnsFailure_ForCcmSupportedEnvironment_WhenEnabledThrows()
        throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        var eisGatewayUrl = getUrl(webServer);

        var mockCcmFeature = createMockCcmFeature(true);

        var exceptionToThrow = new IllegalStateException("exception");
        var mockCcmService = createMockCcmServiceWithOnFailureCall(exceptionToThrow);

        var authHandler = new ElasticInferenceServiceAuthorizationRequestHandler(
            eisGatewayUrl,
            threadPool,
            createNoopApplierFactory(),
            mockCcmFeature,
            mockCcmService
        );

        try (var sender = senderFactory.createSender()) {
            var responseData = getEisAuthorizationResponseWithMultipleEndpoints(eisGatewayUrl);

            PlainActionFuture<ElasticInferenceServiceAuthorizationModel> listener = new PlainActionFuture<>();
            authHandler.getAuthorizationIfPermittedEnvironment(listener, sender);

            var authResponse = listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT);
            assertThat(authResponse.getTaskTypes(), is(EnumSet.noneOf(TaskType.class)));
            assertThat(authResponse.getEndpointIds(), empty());
            assertFalse(authResponse.isAuthorized());
            assertThat(authResponse.getEndpoints(responseData.inferenceIds()), empty());

            // There should be no requests made to EIS because it is not configured
            assertThat(webServer.requests(), empty());

            authHandler.waitForAuthRequestCompletion(TimeValue.THIRTY_SECONDS);

            verify(mockCcmFeature, times(1)).isCcmSupportedEnvironment();
        }
    }

    public void testGetAuthorizationIfPermittedEnvironment_ReturnsUnauthorized_WhenCcmSupportedEnvironmentAndDisabled() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        var eisGatewayUrl = getUrl(webServer);
        var logger = mock(Logger.class);

        var mockCcmFeature = createMockCcmFeature(true);
        var mockCcmService = createMockCcmService(false);

        var authHandler = new ElasticInferenceServiceAuthorizationRequestHandler(
            eisGatewayUrl,
            threadPool,
            logger,
            createNoopApplierFactory(),
            mockCcmFeature,
            mockCcmService
        );

        try (var sender = senderFactory.createSender()) {
            var responseData = getEisAuthorizationResponseWithMultipleEndpoints(eisGatewayUrl);

            PlainActionFuture<ElasticInferenceServiceAuthorizationModel> listener = new PlainActionFuture<>();
            authHandler.getAuthorizationIfPermittedEnvironment(listener, sender);

            var authResponse = listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT);
            assertThat(authResponse.getTaskTypes(), is(EnumSet.noneOf(TaskType.class)));
            assertThat(authResponse.getEndpointIds(), empty());
            assertFalse(authResponse.isAuthorized());
            assertThat(authResponse.getEndpoints(responseData.inferenceIds()), empty());

            // There should be no requests made to EIS because it is not configured
            assertThat(webServer.requests(), empty());

            authHandler.waitForAuthRequestCompletion(TimeValue.THIRTY_SECONDS);

            verify(mockCcmFeature, times(1)).isCcmSupportedEnvironment();
            verify(mockCcmService, times(1)).isEnabled(any());
        }
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
            createApplierFactory(secret),
            createMockCcmFeature(false),
            createMockCcmService(false)
        );

        var elserResponseBody = getEisElserAuthorizationResponse(eisGatewayUrl).responseJson();

        try (var sender = senderFactory.createSender()) {
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(elserResponseBody));

            PlainActionFuture<ElasticInferenceServiceAuthorizationModel> listener = new PlainActionFuture<>();
            authHandler.getAuthorization(listener, sender);

            var authResponse = listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT);
            assertThat(authResponse.getTaskTypes(), is(EnumSet.of(TaskType.SPARSE_EMBEDDING)));

            assertThat(authResponse.getEndpointIds(), is(Set.of(ELSER_V2_ENDPOINT_ID)));
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
        assertThat(requests.get(0).getHeader(HttpHeaders.AUTHORIZATION), is(apiKey(secret)));
    }

    public void testGetAuthorization_OnResponseCalledOnce() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        var eisGatewayUrl = getUrl(webServer);
        var logger = mock(Logger.class);
        var authHandler = new ElasticInferenceServiceAuthorizationRequestHandler(
            eisGatewayUrl,
            threadPool,
            logger,
            createNoopApplierFactory(),
            createMockCcmFeature(false),
            createMockCcmService(false)
        );

        PlainActionFuture<ElasticInferenceServiceAuthorizationModel> listener = new PlainActionFuture<>();
        ActionListener<ElasticInferenceServiceAuthorizationModel> onlyOnceListener = ActionListener.assertOnce(listener);

        var elserResponse = getEisElserAuthorizationResponse(eisGatewayUrl);

        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(elserResponse.responseJson()));

        try (var sender = senderFactory.createSender()) {
            authHandler.getAuthorization(onlyOnceListener, sender);
            authHandler.waitForAuthRequestCompletion(ESTestCase.TEST_REQUEST_TIMEOUT);

            var authResponse = listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT);
            assertThat(authResponse.getTaskTypes(), is(EnumSet.of(TaskType.SPARSE_EMBEDDING)));
            assertThat(authResponse.getEndpointIds(), is(Set.of(ELSER_V2_ENDPOINT_ID)));
            assertTrue(authResponse.isAuthorized());
            assertThat(authResponse.getEndpoints(Set.of(ELSER_V2_ENDPOINT_ID)), is(elserResponse.expectedEndpoints()));

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
        var authHandler = new ElasticInferenceServiceAuthorizationRequestHandler(
            "abc",
            threadPool,
            logger,
            createNoopApplierFactory(),
            createMockCcmFeature(false),
            createMockCcmService(false)
        );

        try (var sender = senderFactory.createSender()) {
            PlainActionFuture<ElasticInferenceServiceAuthorizationModel> listener = new PlainActionFuture<>();

            authHandler.getAuthorization(listener, sender);
            var exception = expectThrows(ElasticsearchException.class, () -> listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT));

            assertThat(exception.getMessage(), containsString("Received an invalid response type from the Elastic Inference Service"));

            var loggerArgsCaptor = ArgumentCaptor.forClass(String.class);
            verify(logger).warn(loggerArgsCaptor.capture());
            var message = loggerArgsCaptor.getValue();
            assertThat(message, containsString("Failed to retrieve the authorization information from the Elastic Inference Service."));
        }
    }

    private static CCMFeature createMockCcmFeature(boolean isCcmSupportedEnvironment) {
        var ccmFeature = mock(CCMFeature.class);
        when(ccmFeature.isCcmSupportedEnvironment()).thenReturn(isCcmSupportedEnvironment);
        return ccmFeature;
    }

    private static CCMService createMockCcmService(boolean isEnabled) {
        var ccmService = mock(CCMService.class);
        doAnswer(invocation -> {
            ActionListener<Boolean> listener = invocation.getArgument(0);
            listener.onResponse(isEnabled);
            return Void.TYPE;
        }).when(ccmService).isEnabled(any());
        return ccmService;
    }

    private static CCMService createMockCcmServiceWithOnFailureCall(Exception exception) {
        var ccmService = mock(CCMService.class);
        doAnswer(invocation -> {
            ActionListener<Boolean> listener = invocation.getArgument(0);
            listener.onFailure(exception);
            return Void.TYPE;
        }).when(ccmService).isEnabled(any());
        return ccmService;
    }
}
