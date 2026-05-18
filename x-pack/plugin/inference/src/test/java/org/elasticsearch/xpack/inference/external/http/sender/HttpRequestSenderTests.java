/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.apache.http.HttpHeaders;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.TestPlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.http.HttpClient;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.RequestExecutor;
import org.elasticsearch.xpack.inference.external.http.retry.RequestSender;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.request.OutboundRequest;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.services.ServiceComponentsTests;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceResponseHandler;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMAuthenticationApplierFactory;
import org.elasticsearch.xpack.inference.services.elastic.request.ElasticInferenceServiceAuthorizationRequest;
import org.elasticsearch.xpack.inference.services.elastic.response.ElasticInferenceServiceAuthorizationResponseEntity;
import org.elasticsearch.xpack.inference.telemetry.TraceContext;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.inference.results.DenseEmbeddingFloatResultsTests.buildExpectationFloat;
import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityExecutors;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceService.ELASTIC_INFERENCE_SERVICE_IDENTIFIER;
import static org.elasticsearch.xpack.inference.services.elastic.request.ElasticInferenceServiceRequestTests.randomElasticInferenceServiceRequestMetadata;
import static org.elasticsearch.xpack.inference.services.elastic.response.ElasticInferenceServiceAuthorizationResponseEntityTests.getEisElserAuthorizationResponse;
import static org.elasticsearch.xpack.inference.services.openai.OpenAiUtils.ORGANIZATION_HEADER;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class HttpRequestSenderTests extends ESTestCase {
    private static final String INFERENCE_ID = "id";
    private static final TimeValue ONE_NANOSECOND = TimeValue.timeValueNanos(1);

    private final MockWebServer webServer = new MockWebServer();
    private ThreadPool threadPool;
    private HttpClientManager clientManager;
    private final AtomicReference<Thread> threadRef = new AtomicReference<>();
    private CountDownLatch blockStartLatch;

    @Before
    public void init() throws Exception {
        webServer.start();
        threadPool = createThreadPool(inferenceUtilityExecutors());
        clientManager = HttpClientManager.create(Settings.EMPTY, threadPool, mockClusterServiceEmpty(), mock(ThrottlerManager.class));
        threadRef.set(null);
        blockStartLatch = new CountDownLatch(1);
    }

    @After
    public void shutdown() throws IOException, InterruptedException {
        // Unblock any service.start() calls blocked on blockStartLatch before terminating the pool
        blockStartLatch.countDown();

        if (threadRef.get() != null) {
            threadRef.get().join(TEST_REQUEST_TIMEOUT.millis());
        }

        clientManager.close();
        terminate(threadPool);
        webServer.close();
    }

    public void testCreateSender_ReturnsSameRequestExecutorInstance() {
        var senderFactory = new HttpRequestSender.Factory(createWithEmptySettings(threadPool), clientManager, mockClusterServiceEmpty());

        var sender1 = createSender(senderFactory);
        var sender2 = createSender(senderFactory);

        assertThat(sender1, instanceOf(HttpRequestSender.class));
        assertThat(sender2, instanceOf(HttpRequestSender.class));
        assertThat(sender1, sameInstance(sender2));
    }

    public void testCreateSender_CanCallStartMultipleTimes() throws Exception {
        var mockManager = createMockHttpClientManager();

        var senderFactory = new HttpRequestSender.Factory(createWithEmptySettings(threadPool), mockManager, mockClusterServiceEmpty());

        try (var sender = createSender(senderFactory)) {
            startSynchronously(sender);
            startSynchronously(sender);
            startSynchronously(sender);
        }

        verify(mockManager, times(1)).start();
    }

    private HttpClientManager createMockHttpClientManager() {
        var mockManager = mock(HttpClientManager.class);
        when(mockManager.getHttpClient()).thenReturn(mock(HttpClient.class));

        return mockManager;
    }

    public void testStart_ThrowsExceptionWaitingForStartToComplete_WhenAnErrorOccurs() throws IOException {
        var mockManager = createMockHttpClientManager();
        doThrow(new ElasticsearchException("failed")).when(mockManager).start();

        var senderFactory = new HttpRequestSender.Factory(
            ServiceComponentsTests.createWithEmptySettings(threadPool),
            mockManager,
            mockClusterServiceEmpty()
        );

        try (var sender = (HttpRequestSender) senderFactory.createSender()) {
            var exception = expectThrows(ElasticsearchException.class, () -> startSynchronously(sender));

            assertThat(exception.getMessage(), is("Failed to initialize inference components"));
            assertThat(exception.getCause().getMessage(), is("failed"));
        }
    }

    private RequestExecutor createMockRequestExecutorThatBlocksOnStart() {
        var mockService = mock(RequestExecutor.class);
        doAnswer(invocation -> {
            // Block service.start() to simulate a hung startup; blockStartLatch is counted down in @After
            blockStartLatch.await();
            return null;
        }).when(mockService).start();
        return mockService;
    }

    public void testStartAsync_ThrowsServiceUnavailableWhenStartupTimesOut() throws Exception {
        var mockManager = createMockHttpClientManager();

        var mockService = createMockRequestExecutorThatBlocksOnStart();
        var sender = new HttpRequestSender(threadPool, mockManager, mock(RequestSender.class), mockService);

        var listener = new PlainActionFuture<Void>();
        // Pass a very short timeout so the test doesn't hang
        sender.startAsynchronously(listener, TimeValue.timeValueMillis(1));

        var exception = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TEST_REQUEST_TIMEOUT));
        assertThat(exception.getMessage(), is("Http sender startup did not complete in time"));
        assertThat(exception.status(), is(RestStatus.SERVICE_UNAVAILABLE));
    }

    public void testStartAsync_PerCallerTimeoutDoesNotPoisonSharedState() {
        var mockManager = createMockHttpClientManager();

        var mockService = createMockRequestExecutorThatBlocksOnStart();
        var sender = new HttpRequestSender(threadPool, mockManager, mock(RequestSender.class), mockService);

        // First caller times out — per-caller ListenerTimeouts fires, not the shared startupNotifier
        var timedListener = new TestPlainActionFuture<Void>();
        sender.startAsynchronously(timedListener, TimeValue.timeValueMillis(1));
        var ex = expectThrows(ElasticsearchStatusException.class, () -> timedListener.actionGet(TEST_REQUEST_TIMEOUT));
        assertThat(ex.getMessage(), is("Http sender startup did not complete in time"));
        assertThat(ex.status(), is(RestStatus.SERVICE_UNAVAILABLE));

        // Second caller with no timeout should get notified when startup completes, not get a timeout from the first caller's timeout
        var noTimeoutListener = new TestPlainActionFuture<Void>();
        sender.startAsynchronously(noTimeoutListener, null);

        // Unblock startup so startupNotifier resolves successfully
        blockStartLatch.countDown();

        // The second caller should succeed, indicating that the first caller's timeout did not poison the shared startup state
        assertNull(noTimeoutListener.actionGet(TEST_REQUEST_TIMEOUT));

        // Third caller with no timeout should succeed after the shared startup state is successfully resolved
        var successListener = new TestPlainActionFuture<Void>();
        sender.startAsynchronously(successListener, null);
        assertNull(successListener.actionGet(TEST_REQUEST_TIMEOUT));
        verify(mockManager, times(1)).start();
    }

    public void testStartAsync_LateListenerGetsImmediateNotification() {
        var mockManager = createMockHttpClientManager();
        var sender = new HttpRequestSender(threadPool, mockManager, mock(RequestSender.class), mock(RequestExecutor.class));

        startSynchronously(sender);  // startup completes, startupNotifier already resolved
        verify(mockManager, times(1)).start();

        // Listener registered after startup completes should be notified immediately
        var listener = new TestPlainActionFuture<Void>();
        sender.startAsynchronously(listener, null);
        assertNull(listener.actionGet(TEST_REQUEST_TIMEOUT));
        verifyNoMoreInteractions(mockManager);
    }

    public void testConcurrentStartAsync_AllListenersReceiveFailure() throws InterruptedException {
        var mockManager = createMockHttpClientManager();
        doAnswer(invocation -> {
            blockStartLatch.await();
            throw new ElasticsearchException("failed");
        }).when(mockManager).start();
        var sender = new HttpRequestSender(threadPool, mockManager, mock(RequestSender.class), mock(RequestExecutor.class));

        final var threadCount = between(4, 8);
        final var barrier = new CyclicBarrier(threadCount);
        final var threads = new ArrayList<Thread>();
        final var futures = new ArrayList<TestPlainActionFuture<Void>>();

        for (int i = 0; i < threadCount; i++) {
            final var future = new TestPlainActionFuture<Void>();
            futures.add(future);
            threads.add(new Thread(() -> {
                safeAwait(barrier);
                sender.startAsynchronously(future, null);
            }));
        }

        for (var thread : threads) {
            thread.start();
        }
        blockStartLatch.countDown();  // unblock → manager.start() throws → startupNotifier.onFailure
        for (var thread : threads) {
            thread.join(TEST_REQUEST_TIMEOUT.millis());
        }

        for (var future : futures) {
            var exception = expectThrows(ElasticsearchException.class, () -> future.actionGet(TEST_REQUEST_TIMEOUT));
            assertThat(exception.getMessage(), is("Failed to initialize inference components"));
            assertThat(exception.getCause().getMessage(), is("failed"));
        }
        verify(mockManager, times(1)).start();
    }

    public void testCreateSender_CanCallStartAsyncMultipleTimes() throws Exception {
        var mockManager = createMockHttpClientManager();
        var asyncCalls = 3;
        var senderFactory = new HttpRequestSender.Factory(createWithEmptySettings(threadPool), mockManager, mockClusterServiceEmpty());

        try (var sender = createSender(senderFactory)) {
            var listenerList = new ArrayList<PlainActionFuture<Void>>();

            for (int i = 0; i < asyncCalls; i++) {
                PlainActionFuture<Void> listener = new PlainActionFuture<>();
                listenerList.add(listener);
                sender.startAsynchronously(listener, null);
            }

            for (int i = 0; i < asyncCalls; i++) {
                PlainActionFuture<Void> listener = listenerList.get(i);
                assertNull(listener.actionGet(TEST_REQUEST_TIMEOUT));
            }
        }

        verify(mockManager, times(1)).start();
    }

    public void testCreateSender_CanCallStartAsyncAndSyncMultipleTimes() throws Exception {
        var mockManager = createMockHttpClientManager();
        var asyncCalls = 3;
        var senderFactory = new HttpRequestSender.Factory(createWithEmptySettings(threadPool), mockManager, mockClusterServiceEmpty());

        try (var sender = createSender(senderFactory)) {
            var listenerList = new ArrayList<PlainActionFuture<Void>>();

            for (int i = 0; i < asyncCalls; i++) {
                PlainActionFuture<Void> listener = new PlainActionFuture<>();
                listenerList.add(listener);
                sender.startAsynchronously(listener, null);
                startSynchronously(sender);
            }

            for (int i = 0; i < asyncCalls; i++) {
                PlainActionFuture<Void> listener = listenerList.get(i);
                assertNull(listener.actionGet(TEST_REQUEST_TIMEOUT));
            }
        }

        verify(mockManager, times(1)).start();
    }

    public void testCreateSender_SendsRequestAndReceivesResponse() throws Exception {
        var senderFactory = new HttpRequestSender.Factory(createWithEmptySettings(threadPool), clientManager, mockClusterServiceEmpty());

        try (var sender = createSender(senderFactory)) {
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

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            sender.send(
                OpenAiEmbeddingsRequestManagerTests.makeCreator(getUrl(webServer), null, "key", "model", null, threadPool),
                new EmbeddingsInput(List.of("abc"), null),
                null,
                listener
            );

            var result = listener.actionGet(TEST_REQUEST_TIMEOUT);
            assertThat(result.asMap(), is(buildExpectationFloat(List.of(new float[] { 0.0123F, -0.0123F }))));

            assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().get(0).getUri().getQuery());
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer key"));
            assertNull(webServer.requests().get(0).getHeader(ORGANIZATION_HEADER));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            assertThat(requestMap.size(), is(2));
            assertThat(requestMap.get("input"), is(List.of("abc")));
            assertThat(requestMap.get("model"), is("model"));
        }
    }

    public void testSendWithoutQueuing_SendsRequestAndReceivesResponse() throws Exception {
        var senderFactory = createSenderFactory(clientManager, threadRef);

        try (var sender = createSender(senderFactory)) {
            var url = getUrl(webServer);
            var elserResponse = getEisElserAuthorizationResponse(url);
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(elserResponse.responseJson()));

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            var request = new ElasticInferenceServiceAuthorizationRequest(
                getUrl(webServer),
                new TraceContext("", ""),
                randomElasticInferenceServiceRequestMetadata(),
                CCMAuthenticationApplierFactory.NOOP_APPLIER
            );
            var responseHandler = new ElasticInferenceServiceResponseHandler(
                String.format(Locale.ROOT, "%s sparse embeddings", ELASTIC_INFERENCE_SERVICE_IDENTIFIER),
                ElasticInferenceServiceAuthorizationResponseEntity::fromResponse
            );

            sender.sendWithoutQueuing(mock(Logger.class), request, responseHandler, null, listener);

            var result = listener.actionGet(TEST_REQUEST_TIMEOUT);
            assertThat(result, instanceOf(ElasticInferenceServiceAuthorizationResponseEntity.class));
            var authResponse = (ElasticInferenceServiceAuthorizationResponseEntity) result;
            assertThat(authResponse.authorizedEndpoints(), is(elserResponse.responseEntity().authorizedEndpoints()));
        }
    }

    public void testHttpRequestSender_Throws_WhenATimeoutOccurs() throws Exception {
        var mockManager = createMockHttpClientManager();

        var senderFactory = new HttpRequestSender.Factory(
            ServiceComponentsTests.createWithEmptySettings(threadPool),
            mockManager,
            mockClusterServiceEmpty()
        );

        try (var sender = senderFactory.createSender()) {
            assertThat(sender, instanceOf(HttpRequestSender.class));

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            sender.send(
                RequestManagerTests.createMockWithRateLimitingEnabled(INFERENCE_ID),
                new EmbeddingsInput(List.of(), null),
                TimeValue.timeValueNanos(1),
                listener
            );

            var thrownException = expectThrows(ElasticsearchTimeoutException.class, () -> listener.actionGet(TEST_REQUEST_TIMEOUT));

            assertThat(
                thrownException.getMessage(),
                is(format("Request timed out after [%s] for inference id [%s]", ONE_NANOSECOND, INFERENCE_ID))
            );
            assertThat(thrownException.status(), is(RestStatus.TOO_MANY_REQUESTS));
        }
    }

    public void testSendWithoutQueuingWithTimeout_Throws_WhenATimeoutOccurs() throws Exception {
        var mockManager = createMockHttpClientManager();

        var senderFactory = new HttpRequestSender.Factory(
            ServiceComponentsTests.createWithEmptySettings(threadPool),
            mockManager,
            mockClusterServiceEmpty()
        );

        try (var sender = senderFactory.createSender()) {
            var request = mock(OutboundRequest.class);
            when(request.getInferenceEntityId()).thenReturn(INFERENCE_ID);
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            sender.sendWithoutQueuing(mock(Logger.class), request, mock(ResponseHandler.class), TimeValue.timeValueNanos(1), listener);

            var thrownException = expectThrows(ElasticsearchTimeoutException.class, () -> listener.actionGet(TEST_REQUEST_TIMEOUT));

            assertThat(
                thrownException.getMessage(),
                is(format("Request timed out after [%s] for inference id [%s]", ONE_NANOSECOND, INFERENCE_ID))
            );
            assertThat(thrownException.status(), is(RestStatus.TOO_MANY_REQUESTS));
        }
    }

    public void testConcurrentSend_OnlyStartsOnce() throws Exception {
        // At a minimum we need 1 for the startAsynchronously to kick off the init tasks, and allowing 1 more to allow for a race to occur
        final var maxUtilityThreads = 2;
        final var maxResponseThreads = 2;
        try (var smallThreadPool = createThreadPool(inferenceUtilityExecutors(maxUtilityThreads, maxResponseThreads))) {
            var mockManager = createMockHttpClientManager();
            var mockService = mock(RequestExecutor.class);
            doAnswer(invocation -> {
                ActionListener<InferenceServiceResults> listener = invocation.getArgument(3);
                listener.onFailure(new ElasticsearchStatusException("test", RestStatus.SERVICE_UNAVAILABLE));
                return null;
            }).when(mockService).execute(any(), any(), any(), any());
            var sender = new HttpRequestSender(smallThreadPool, mockManager, mock(RequestSender.class), mockService);

            runConcurrentSendTest(mockManager, maxUtilityThreads, (barrier, future) -> new Thread(() -> {
                safeAwait(barrier);
                sender.send(
                    RequestManagerTests.createMockWithRateLimitingEnabled(),
                    new EmbeddingsInput(List.of(), null),
                    TimeValue.timeValueNanos(1),
                    future
                );
            }), ElasticsearchStatusException.class);
        }
    }

    public void testConcurrentSendWithoutQueuing_OnlyStartsOnce() throws Exception {
        // At a minimum we need 1 for the startAsynchronously to kick off the init tasks, and allowing 1 more to allow for a race to occur
        final var maxUtilityThreads = 2;
        final var maxResponseThreads = 2;
        try (var smallThreadPool = createThreadPool(inferenceUtilityExecutors(maxUtilityThreads, maxResponseThreads))) {
            var mockManager = createMockHttpClientManager();
            var sender = new HttpRequestSender(smallThreadPool, mockManager, mock(RequestSender.class), mock(RequestExecutor.class));

            runConcurrentSendTest(mockManager, maxUtilityThreads, (barrier, future) -> {
                var request = mock(OutboundRequest.class);
                when(request.getInferenceEntityId()).thenReturn(INFERENCE_ID);
                return new Thread(() -> {
                    safeAwait(barrier);
                    sender.sendWithoutQueuing(mock(Logger.class), request, mock(ResponseHandler.class), ONE_NANOSECOND, future);
                });
            }, ElasticsearchTimeoutException.class);
        }
    }

    private <E extends Exception> void runConcurrentSendTest(
        HttpClientManager mockManager,
        int maxUtilityThreads,
        BiFunction<CyclicBarrier, PlainActionFuture<InferenceServiceResults>, Thread> threadFactory,
        Class<E> expectedExceptionClass
    ) throws InterruptedException {
        // Use more threads than the utility thread pool size
        final var threadCountMinimum = maxUtilityThreads + 2;
        final var threadCount = between(threadCountMinimum, threadCountMinimum + 5);
        final var barrier = new CyclicBarrier(threadCount);
        final var threads = new ArrayList<Thread>();
        final var futures = new ArrayList<PlainActionFuture<InferenceServiceResults>>();

        for (int i = 0; i < threadCount; i++) {
            final var future = new PlainActionFuture<InferenceServiceResults>();
            futures.add(future);
            threads.add(threadFactory.apply(barrier, future));
        }

        for (var thread : threads) {
            thread.start();
        }
        for (var thread : threads) {
            thread.join(TEST_REQUEST_TIMEOUT.millis());
        }

        for (var future : futures) {
            expectThrows(expectedExceptionClass, () -> future.actionGet(TEST_REQUEST_TIMEOUT));
        }

        verify(mockManager, times(1)).start();
    }

    private static HttpRequestSender.Factory createSenderFactory(HttpClientManager clientManager, AtomicReference<Thread> threadRef) {
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
        when(mockThreadPool.scheduleWithFixedDelay(any(Runnable.class), any(), any())).thenReturn(mock(Scheduler.Cancellable.class));

        return new HttpRequestSender.Factory(
            ServiceComponentsTests.createWithEmptySettings(mockThreadPool),
            clientManager,
            mockClusterServiceEmpty()
        );
    }

    public static HttpRequestSender.Factory createSenderFactory(ThreadPool threadPool, HttpClientManager httpClientManager) {
        return new HttpRequestSender.Factory(
            ServiceComponentsTests.createWithEmptySettings(threadPool),
            httpClientManager,
            mockClusterServiceEmpty()
        );
    }

    public static HttpRequestSender.Factory createSenderFactory(
        ThreadPool threadPool,
        HttpClientManager httpClientManager,
        Settings settings
    ) {
        return new HttpRequestSender.Factory(
            ServiceComponentsTests.createWithSettings(threadPool, settings),
            httpClientManager,
            mockClusterServiceEmpty()
        );
    }

    public static HttpRequestSender createSender(HttpRequestSender.Factory factory) {
        return (HttpRequestSender) factory.createSender();
    }

    private static void startSynchronously(HttpRequestSender sender) {
        startSynchronously(sender, TEST_REQUEST_TIMEOUT);
    }

    private static void startSynchronously(HttpRequestSender sender, TimeValue timeout) {
        var listener = new TestPlainActionFuture<Void>();
        sender.startAsynchronously(listener, timeout);
        // Intentionally using a test timeout. That way a short timeout can be passed to startAsynchronously
        // to test a valid timeout scenario without the actionGet() accidentally completing first.
        listener.actionGet(TEST_REQUEST_TIMEOUT);
    }
}
