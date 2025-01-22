/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.authorization;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.EnumSet;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityPool;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.external.http.retry.RetryingHttpSender.MAX_RETIES;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

public class ElasticInferenceServiceAuthorizationHandlerTests extends ESTestCase {
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

    public void testGetAuthorization_FailsWhenAnInvalidFieldIsFound() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        var eisGatewayUrl = getUrl(webServer);
        var logger = mock(Logger.class);
        var authHandler = new ElasticInferenceServiceAuthorizationHandler(eisGatewayUrl, threadPool, logger);

        try (var sender = senderFactory.createSender()) {
            String responseJson = """
                {
                    "models": [
                        {
                          "invalid-field": "model-a",
                          "task-types": ["embedding/text/sparse", "chat/completion"]
                        }
                    ]
                }
                """;

            queueWebServerResponsesForRetries(responseJson);

            PlainActionFuture<ElasticInferenceServiceAuthorization> listener = new PlainActionFuture<>();
            authHandler.getAuth(listener, sender);

            var authResponse = listener.actionGet(TIMEOUT);
            assertTrue(authResponse.enabledTaskTypes().isEmpty());
            assertFalse(authResponse.isEnabled());

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
        var authHandler = new ElasticInferenceServiceAuthorizationHandler(eisGatewayUrl, threadPool, logger);

        try (var sender = senderFactory.createSender()) {
            String responseJson = """
                {
                    "models": [
                        {
                          "model_name": "model-a",
                          "task_types": ["embedding/text/sparse", "chat/completion"]
                        }
                    ]
                }
                """;

            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            PlainActionFuture<ElasticInferenceServiceAuthorization> listener = new PlainActionFuture<>();
            authHandler.getAuth(listener, sender);

            var authResponse = listener.actionGet(TIMEOUT);
            assertThat(authResponse.enabledTaskTypes(), is(EnumSet.of(TaskType.SPARSE_EMBEDDING, TaskType.CHAT_COMPLETION)));
            assertTrue(authResponse.isEnabled());

            verifyNoInteractions(logger);
        }
    }
}
