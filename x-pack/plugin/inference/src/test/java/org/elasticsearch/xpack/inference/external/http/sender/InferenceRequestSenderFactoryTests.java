/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.apache.http.HttpHeaders;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.http.HttpClient;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.batching.HuggingFaceInferenceRequestCreatorTests;
import org.elasticsearch.xpack.inference.external.http.batching.HuggingFaceRequestBatcherFactory;
import org.elasticsearch.xpack.inference.external.http.batching.RequestCreator;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.ServiceComponentsTests;
import org.elasticsearch.xpack.inference.services.huggingface.elser.HuggingFaceElserModelTests;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityPool;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class InferenceRequestSenderFactoryTests extends ESTestCase {
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
    public void shutdown() throws IOException, InterruptedException {
        clientManager.close();
        terminate(threadPool);
        webServer.close();
    }

    public void testCreateSender_SendsRequestAndReceivesResponse() throws Exception {
        var senderFactory = createFactoryWithEmptySettings(threadPool, clientManager);

        try (var sender = senderFactory.createSender("test_service", HuggingFaceRequestBatcherFactory::new)) {
            sender.start();

            String responseJson = """
                [
                    {
                        ".": 0.133155956864357
                    }
                ]
                """;

            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));
            var model = HuggingFaceElserModelTests.createModel(getUrl(webServer), "secret");
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            sender.send(HuggingFaceInferenceRequestCreatorTests.create(model), List.of("abc"), listener);

            listener.actionGet(TIMEOUT);
            assertThat(webServer.requests(), hasSize(1));
            assertThat(
                webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE),
                equalTo(XContentType.JSON.mediaTypeWithoutParameters())
            );

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            assertThat(requestMap.size(), is(1));
            assertThat(requestMap.get("inputs"), is(List.of("abc")));
        }
    }

    @SuppressWarnings("unchecked")
    public void testHttpRequestSender_Throws_WhenCallingSendBeforeStart() throws Exception {
        var senderFactory = createFactoryWithEmptySettings(threadPool, clientManager);

        try (var sender = senderFactory.createSender("test_service", HuggingFaceRequestBatcherFactory::new)) {
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            var thrownException = expectThrows(
                AssertionError.class,
                () -> sender.send(mock(RequestCreator.class), List.of("abc"), listener)
            );
            assertThat(thrownException.getMessage(), is("call start() before sending a request"));
        }
    }

    public void testHttpRequestSender_Throws_WhenATimeoutOccurs() throws Exception {
        var mockManager = mock(HttpClientManager.class);
        when(mockManager.getHttpClient()).thenReturn(mock(HttpClient.class));

        var senderFactory = new InferenceRequestSenderFactory(
            ServiceComponentsTests.createWithEmptySettings(threadPool),
            mockManager,
            mockClusterServiceEmpty()
        );

        try (var sender = senderFactory.createSender("test_service", HuggingFaceRequestBatcherFactory::new)) {
            sender.start();

            var model = HuggingFaceElserModelTests.createModel(getUrl(webServer), "secret");

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            sender.send(HuggingFaceInferenceRequestCreatorTests.create(model), List.of("abc"), TimeValue.timeValueNanos(1), listener);

            var thrownException = expectThrows(ElasticsearchTimeoutException.class, () -> listener.actionGet(TIMEOUT));

            assertThat(
                thrownException.getMessage(),
                is(format("Request timed out waiting to be sent after [%s]", TimeValue.timeValueNanos(1)))
            );
        }
    }

    public void testHttpRequestSenderWithTimeout_Throws_WhenATimeoutOccurs() throws Exception {
        var mockManager = mock(HttpClientManager.class);
        when(mockManager.getHttpClient()).thenReturn(mock(HttpClient.class));

        var senderFactory = new InferenceRequestSenderFactory(
            ServiceComponentsTests.createWithEmptySettings(threadPool),
            mockManager,
            mockClusterServiceEmpty()
        );

        try (var sender = senderFactory.createSender("test_service", HuggingFaceRequestBatcherFactory::new)) {
            sender.start();

            var model = HuggingFaceElserModelTests.createModel(getUrl(webServer), "secret");

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            sender.send(HuggingFaceInferenceRequestCreatorTests.create(model), List.of("abc"), TimeValue.timeValueNanos(1), listener);

            var thrownException = expectThrows(ElasticsearchTimeoutException.class, () -> listener.actionGet(TIMEOUT));

            assertThat(
                thrownException.getMessage(),
                is(format("Request timed out waiting to be sent after [%s]", TimeValue.timeValueNanos(1)))
            );
        }
    }

    public static InferenceRequestSenderFactory createFactoryWithEmptySettings(ThreadPool threadPool, HttpClientManager clientManager) {
        return new InferenceRequestSenderFactory(
            ServiceComponentsTests.createWithEmptySettings(threadPool),
            clientManager,
            mockClusterServiceEmpty()
        );
    }

    /**
     * This initializes the factory with a {@link ServiceComponents} that contains the specified settings. This does not affect
     * the initialized {@link org.elasticsearch.cluster.service.ClusterService}.
     */
    public static InferenceRequestSenderFactory createFactoryWithSettings(
        ThreadPool threadPool,
        HttpClientManager clientManager,
        Settings settings
    ) {
        return new InferenceRequestSenderFactory(
            ServiceComponentsTests.createWithSettings(threadPool, settings),
            clientManager,
            mockClusterServiceEmpty()
        );
    }
}
