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
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.common.TruncatorTests;
import org.elasticsearch.xpack.inference.external.http.HttpClient;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.batching.HuggingFaceInferenceRequestCreator;
import org.elasticsearch.xpack.inference.external.http.batching.HuggingFaceRequestBatcherFactory;
import org.elasticsearch.xpack.inference.external.http.batching.RequestCreator;
import org.elasticsearch.xpack.inference.external.http.retry.AlwaysRetryingResponseHandler;
import org.elasticsearch.xpack.inference.external.huggingface.HuggingFaceAccount;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.ServiceComponentsTests;
import org.elasticsearch.xpack.inference.services.huggingface.elser.HuggingFaceElserModelTests;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityPool;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class InferenceRequestSenderFactoryTests extends ESTestCase {
    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);
    private final MockWebServer webServer = new MockWebServer();
    private ThreadPool threadPool;
    private HttpClientManager clientManager;
    private final AtomicReference<Thread> threadRef = new AtomicReference<>();

    @Before
    public void init() throws Exception {
        webServer.start();
        threadPool = createThreadPool(inferenceUtilityPool());
        clientManager = HttpClientManager.create(Settings.EMPTY, threadPool, mockClusterServiceEmpty(), mock(ThrottlerManager.class));
        threadRef.set(null);
    }

    @After
    public void shutdown() throws IOException, InterruptedException {
        if (threadRef.get() != null) {
            threadRef.get().join(TIMEOUT.millis());
        }

        clientManager.close();
        terminate(threadPool);
        webServer.close();
    }

    public void testCreateSender_SendsRequestAndReceivesResponse() throws Exception {
        // var senderFactory = createSenderFactory(clientManager, threadRef);
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
            sender.send(
                new HuggingFaceInferenceRequestCreator(
                    new HuggingFaceAccount(model.getUri(), model.getApiKey()),
                    new AlwaysRetryingResponseHandler("test", (result) -> null),
                    TruncatorTests.createTruncator(),
                    100
                ),
                List.of("abc"),
                listener
            );

            listener.actionGet(TIMEOUT);
            assertThat(webServer.requests(), hasSize(1));
            assertThat(webServer.requests().get(0).getUri().getPath(), equalTo(getUrl(webServer)));
            assertThat(
                webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE),
                equalTo(XContentType.JSON.mediaTypeWithoutParameters())
            );
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

    @SuppressWarnings("unchecked")
    public void testHttpRequestSender_Throws_WhenATimeoutOccurs() throws Exception {
        var mockManager = mock(HttpClientManager.class);
        when(mockManager.getHttpClient()).thenReturn(mock(HttpClient.class));

        var senderFactory = new InferenceRequestSenderFactory(
            ServiceComponentsTests.createWithEmptySettings(threadPool),
            mockManager,
            mockClusterServiceEmpty()
        );

        try (var sender = senderFactory.createSender("test_service", HuggingFaceRequestBatcherFactory::new)) {
            assertThat(sender, instanceOf(InferenceRequestSenderFactory.InferenceRequestSender.class));
            // hack to get around the sender interface so we can set the timeout directly
            var httpSender = (InferenceRequestSenderFactory.InferenceRequestSender<HuggingFaceAccount>) sender;
            httpSender.setMaxRequestTimeout(TimeValue.timeValueNanos(1));
            sender.start();

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            sender.send(mock(RequestCreator.class), List.of("abc"), TimeValue.timeValueNanos(1), listener);

            var thrownException = expectThrows(ElasticsearchTimeoutException.class, () -> listener.actionGet(TIMEOUT));

            assertThat(
                thrownException.getMessage(),
                is(format("Request timed out waiting to be executed after [%s]", TimeValue.timeValueNanos(1)))
            );
        }
    }

    @SuppressWarnings("unchecked")
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

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            sender.send(mock(RequestCreator.class), List.of("abc"), TimeValue.timeValueNanos(1), listener);

            var thrownException = expectThrows(ElasticsearchTimeoutException.class, () -> listener.actionGet(TIMEOUT));

            assertThat(
                thrownException.getMessage(),
                is(format("Request timed out waiting to be executed after [%s]", TimeValue.timeValueNanos(1)))
            );
        }
    }

    // TODO remove
    private static InferenceRequestSenderFactory createSenderFactory(HttpClientManager clientManager, AtomicReference<Thread> threadRef) {
        var mockExecutorService = mock(ExecutorService.class);
        doAnswer(invocation -> {
            Runnable runnable = (Runnable) invocation.getArguments()[0];
            threadRef.set(new Thread(runnable));
            threadRef.get().start();

            return Void.TYPE;
        }).when(mockExecutorService).execute(any(Runnable.class));

        var mockThreadPool = mock(ThreadPool.class);
        when(mockThreadPool.executor(anyString())).thenReturn(mockExecutorService);
        when(mockThreadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(mockThreadPool.schedule(any(Runnable.class), any(), any())).thenReturn(mock(Scheduler.ScheduledCancellable.class));

        return new InferenceRequestSenderFactory(
            new ServiceComponents(mockThreadPool, mock(ThrottlerManager.class), Settings.EMPTY, TruncatorTests.createTruncator()),
            clientManager,
            mockClusterServiceEmpty()
        );
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
