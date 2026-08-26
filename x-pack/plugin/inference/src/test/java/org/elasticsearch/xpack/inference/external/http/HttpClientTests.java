/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http;

import org.apache.hc.client5.http.async.methods.SimpleRequestBuilder;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManager;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.client5.http.protocol.HttpClientContext;
import org.apache.hc.core5.concurrent.FutureCallback;
import org.apache.hc.core5.function.Supplier;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.nio.AsyncPushConsumer;
import org.apache.hc.core5.http.nio.AsyncRequestProducer;
import org.apache.hc.core5.http.nio.AsyncResponseConsumer;
import org.apache.hc.core5.http.nio.HandlerFactory;
import org.apache.hc.core5.http.protocol.HttpContext;
import org.apache.hc.core5.io.CloseMode;
import org.apache.hc.core5.net.URIBuilder;
import org.apache.hc.core5.reactor.IOReactorStatus;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.TestPlainActionFuture;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.TestCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.UncategorizedExecutionException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityExecutors;
import static org.elasticsearch.xpack.inference.Utils.mockClusterService;
import static org.elasticsearch.xpack.inference.logging.ThrottlerManagerTests.mockThrottlerManager;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class HttpClientTests extends ESTestCase {
    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);
    private final MockWebServer webServer = new MockWebServer();
    private ThreadPool threadPool;

    @Before
    public void init() throws Exception {
        webServer.start();
        threadPool = createThreadPool(inferenceUtilityExecutors());
    }

    @After
    public void shutdown() {
        terminate(threadPool);
        webServer.close();
    }

    public void testSend_MockServerReceivesRequest() throws Exception {
        int responseCode = randomIntBetween(200, 203);
        String body = randomAlphaOfLengthBetween(2, 8096);
        webServer.enqueue(new MockResponse().setResponseCode(responseCode).setBody(body));

        String paramKey = randomAlphaOfLength(3);
        String paramValue = randomAlphaOfLength(3);
        var httpPost = createHttpPost(webServer.getPort(), paramKey, paramValue);

        try (
            var httpClient = HttpClient.create(
                emptyHttpSettings(),
                threadPool,
                createConnectionManager(),
                mockThrottlerManager(),
                new TestCircuitBreaker()
            )
        ) {
            httpClient.start();

            PlainActionFuture<HttpResult> listener = new PlainActionFuture<>();
            httpClient.send(httpPost, HttpClientContext.create(), listener);

            var result = listener.actionGet(TIMEOUT);

            assertThat(result.response().getCode(), equalTo(responseCode));
            assertThat(new String(result.body(), StandardCharsets.UTF_8), is(body));
            assertThat(webServer.requests(), hasSize(1));
            assertThat(webServer.requests().get(0).getUri().getPath(), equalTo(httpPost.httpRequest().getUri().getPath()));
            assertThat(webServer.requests().get(0).getUri().getQuery(), equalTo(paramKey + "=" + paramValue));
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));
        }
    }

    public void testSend_ThrowsErrorIfCalledBeforeStart() throws Exception {
        try (
            var httpClient = HttpClient.create(
                emptyHttpSettings(),
                threadPool,
                createConnectionManager(),
                mockThrottlerManager(),
                new TestCircuitBreaker()
            )
        ) {
            var listener = new TestPlainActionFuture<HttpResult>();
            var httpPost = createHttpPost(webServer.getPort(), "key", "value");
            httpClient.send(httpPost, HttpClientContext.create(), listener);
            var thrownException = expectThrows(IllegalStateException.class, () -> listener.actionGet(TimeValue.THIRTY_SECONDS));

            assertThat(thrownException.getMessage(), containsString("Http client is not running, please retry the request"));
        }
    }

    public void testSend_FailedCallsOnFailure() throws Exception {
        var asyncClient = new CallbackInvokingHttpAsyncClient(callback -> callback.failed(new ElasticsearchException("failure")));

        var httpPost = createHttpPost(webServer.getPort(), "a", "b");

        try (var client = new HttpClient(emptyHttpSettings(), asyncClient, threadPool, mockThrottlerManager(), new TestCircuitBreaker())) {
            client.start();

            PlainActionFuture<HttpResult> listener = new PlainActionFuture<>();
            client.send(httpPost, HttpClientContext.create(), listener);

            var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));
            assertThat(thrownException.getMessage(), is("failure"));
        }
    }

    public void testSend_CancelledCallsOnFailure() throws Exception {
        var asyncClient = new CallbackInvokingHttpAsyncClient(FutureCallback::cancelled);

        var httpPost = createHttpPost(webServer.getPort(), "a", "b");

        try (var client = new HttpClient(emptyHttpSettings(), asyncClient, threadPool, mockThrottlerManager(), new TestCircuitBreaker())) {
            client.start();

            PlainActionFuture<HttpResult> listener = new PlainActionFuture<>();
            client.send(httpPost, HttpClientContext.create(), listener);

            var thrownException = expectThrows(CancellationException.class, () -> listener.actionGet(TIMEOUT));
            assertThat(
                thrownException.getMessage(),
                is(Strings.format("Request from inference entity id [%s] was cancelled", httpPost.inferenceEntityId()))
            );
        }
    }

    public void testStream_FailedCallsOnFailure() throws Exception {
        var asyncClient = new CallbackInvokingHttpAsyncClient(callback -> callback.failed(new ElasticsearchException("failure")));

        var httpPost = createHttpPost(webServer.getPort(), "a", "b");

        try (var client = new HttpClient(emptyHttpSettings(), asyncClient, threadPool, mockThrottlerManager(), new TestCircuitBreaker())) {
            client.start();

            PlainActionFuture<StreamingHttpResult> listener = new PlainActionFuture<>();
            client.stream(httpPost, HttpClientContext.create(), listener);

            var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));
            assertThat(thrownException.getMessage(), is("failure"));
        }
    }

    public void testStream_CancelledCallsOnFailure() throws Exception {
        var asyncClient = new CallbackInvokingHttpAsyncClient(FutureCallback::cancelled);

        var httpPost = createHttpPost(webServer.getPort(), "a", "b");

        try (var client = new HttpClient(emptyHttpSettings(), asyncClient, threadPool, mockThrottlerManager(), new TestCircuitBreaker())) {
            client.start();

            PlainActionFuture<StreamingHttpResult> listener = new PlainActionFuture<>();
            client.stream(httpPost, HttpClientContext.create(), listener);

            var thrownException = expectThrows(CancellationException.class, () -> listener.actionGet(TIMEOUT));
            assertThat(
                thrownException.getMessage(),
                is(Strings.format("Request from inference entity id [%s] was cancelled", httpPost.inferenceEntityId()))
            );
        }
    }

    public void testStart_MultipleCallsOnlyStartTheClientOnce() throws Exception {
        var asyncClient = new CallbackInvokingHttpAsyncClient(callback -> {});

        var httpPost = createHttpPost(webServer.getPort(), "a", "b");

        try (var client = new HttpClient(emptyHttpSettings(), asyncClient, threadPool, mockThrottlerManager(), new TestCircuitBreaker())) {
            client.start();

            PlainActionFuture<HttpResult> listener = new PlainActionFuture<>();
            client.send(httpPost, HttpClientContext.create(), listener);
            client.send(httpPost, HttpClientContext.create(), listener);

            assertThat(asyncClient.startCalls(), equalTo(1));
        }
    }

    /**
     * Given a streaming response where the server holds the connection open after sending an initial chunk
     * When the subscriber cancels the subscription without ever calling request()
     * Then the connection lease must be released back to the pool.
     *
     * The downstream cancel() must propagate through {@link ByteArrayFlowPublisher} to the reactive response
     * consumer, which cancels the exchange at the channel level and releases the lease. Without that propagation
     * the lease stays held until TCP keepalive (~hours) or until the server side closes the socket. The standard
     * MockWebServer closes immediately after each response, which would mask the bug, so this test uses a raw
     * ServerSocket that keeps the socket open until the test signals completion.
     */
    public void testStream_CancelAfterPauseReleasesConnection() throws Exception {
        var serverDone = new CountDownLatch(1);
        var chunkSent = new CountDownLatch(1);
        long serverThreadJoinTimeoutMillis = TimeUnit.SECONDS.toMillis(5);
        var serverSocket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
        var serverThread = new Thread(() -> {
            try (Socket socket = serverSocket.accept()) {
                drainHttpRequestHeaders(socket.getInputStream());

                OutputStream out = socket.getOutputStream();
                out.write("""
                    HTTP/1.1 200 OK\r
                    Content-Type: application/octet-stream\r
                    Transfer-Encoding: chunked\r
                    \r
                    """.getBytes(StandardCharsets.US_ASCII));
                byte[] chunk = randomAlphaOfLength(8192).getBytes(StandardCharsets.UTF_8);
                out.write((Integer.toHexString(chunk.length) + "\r\n").getBytes(StandardCharsets.US_ASCII));
                out.write(chunk);
                out.write("\r\n".getBytes(StandardCharsets.US_ASCII));
                out.flush();
                chunkSent.countDown();

                serverDone.await(TEST_REQUEST_TIMEOUT.seconds(), TimeUnit.SECONDS);
            } catch (IOException | InterruptedException e) {
                // Expected when the test closes the server socket or the client tears down the connection.
            }
        }, "test-stream-server");
        serverThread.setDaemon(true);
        serverThread.start();

        try {
            var connectionManager = createConnectionManager();
            try (
                var httpClient = HttpClient.create(
                    emptyHttpSettings(),
                    threadPool,
                    connectionManager,
                    mockThrottlerManager(),
                    new TestCircuitBreaker()
                )
            ) {
                httpClient.start();

                URI uri = new URIBuilder().setScheme("http")
                    .setHost("localhost")
                    .setPort(serverSocket.getLocalPort())
                    .setPath("/" + randomAlphaOfLength(5))
                    .build();
                var httpPost = SimpleRequestBuilder.post(uri)
                    .setBody(randomAlphaOfLength(5).getBytes(StandardCharsets.UTF_8), ContentType.APPLICATION_JSON)
                    .setHeader(HttpHeaders.CONTENT_TYPE, XContentType.JSON.mediaType())
                    .build();
                var request = new HttpRequest(httpPost, "inferenceEntityId");

                var listener = new TestPlainActionFuture<StreamingHttpResult>();
                httpClient.stream(request, HttpClientContext.create(), listener);

                var streamingResult = listener.actionGet(TEST_REQUEST_TIMEOUT);

                var subscriptionRef = new AtomicReference<Flow.Subscription>();
                var subscribed = new CountDownLatch(1);
                streamingResult.body().subscribe(new Flow.Subscriber<>() {
                    @Override
                    public void onSubscribe(Flow.Subscription subscription) {
                        subscriptionRef.set(subscription);
                        subscribed.countDown();
                        // Intentionally do NOT call subscription.request — without downstream demand the exchange
                        // never progresses, which is the scenario where cancel() must still release the lease.
                    }

                    @Override
                    public void onNext(byte[] item) {}

                    @Override
                    public void onError(Throwable throwable) {}

                    @Override
                    public void onComplete() {}
                });
                assertTrue("subscriber must be onSubscribe'd", subscribed.await(TEST_REQUEST_TIMEOUT.seconds(), TimeUnit.SECONDS));

                assertBusy(
                    () -> assertThat(connectionManager.getTotalStats().getLeased(), equalTo(1)),
                    TEST_REQUEST_TIMEOUT.seconds(),
                    TimeUnit.SECONDS
                );

                assertTrue("server must send the body chunk", chunkSent.await(TEST_REQUEST_TIMEOUT.seconds(), TimeUnit.SECONDS));

                subscriptionRef.get().cancel();

                // With the cancel propagated: the reactive consumer cancels the exchange, the channel is torn down,
                // and the lease is released. Without it: the connection stays leased indefinitely (the server never
                // closes), and this assertBusy times out.
                assertBusy(
                    () -> assertThat(connectionManager.getTotalStats().getLeased(), equalTo(0)),
                    TEST_REQUEST_TIMEOUT.seconds(),
                    TimeUnit.SECONDS
                );
            }
        } finally {
            serverDone.countDown();
            serverSocket.close();
            serverThread.join(serverThreadJoinTimeoutMillis);
        }
    }

    private static void drainHttpRequestHeaders(InputStream in) throws IOException {
        // Read through the end of the headers (\r\n\r\n) so the server does not need to parse the request.
        byte[] terminator = { '\r', '\n', '\r', '\n' };
        int matched = 0;
        int b;
        while ((b = in.read()) != -1) {
            if (b == terminator[matched]) {
                matched++;
                if (matched == terminator.length) {
                    return;
                }
            } else {
                matched = (b == terminator[0]) ? 1 : 0;
            }
        }
    }

    public void testSend_FailsWhenMaxBytesReadIsExceeded() throws Exception {
        int responseCode = randomIntBetween(200, 203);
        String body = randomAlphaOfLengthBetween(10, 8096);
        webServer.enqueue(new MockResponse().setResponseCode(responseCode).setBody(body));

        String paramKey = randomAlphaOfLength(3);
        String paramValue = randomAlphaOfLength(3);
        var httpPost = createHttpPost(webServer.getPort(), paramKey, paramValue);

        Settings settings = Settings.builder().put(HttpSettings.MAX_HTTP_RESPONSE_SIZE.getKey(), ByteSizeValue.ONE).build();
        var httpSettings = createHttpSettings(settings);

        try (
            var httpClient = HttpClient.create(
                httpSettings,
                threadPool,
                createConnectionManager(),
                mockThrottlerManager(),
                new TestCircuitBreaker()
            )
        ) {
            httpClient.start();

            PlainActionFuture<HttpResult> listener = new PlainActionFuture<>();
            httpClient.send(httpPost, HttpClientContext.create(), listener);

            var throwException = expectThrows(UncategorizedExecutionException.class, () -> listener.actionGet(TIMEOUT));
            assertThat(throwException.getCause().getCause().getMessage(), is("Maximum limit of [1] bytes reached"));
        }
    }

    public static HttpRequest createHttpPost(int port, String paramKey, String paramValue) throws URISyntaxException {
        URI uri = new URIBuilder().setScheme("http")
            .setHost("localhost")
            .setPort(port)
            .setPath("/" + randomAlphaOfLength(5))
            .setParameter(paramKey, paramValue)
            .build();

        var httpPost = SimpleRequestBuilder.post(uri)
            .setBody(randomAlphaOfLength(5).getBytes(StandardCharsets.UTF_8), ContentType.APPLICATION_JSON)
            .setHeader(HttpHeaders.CONTENT_TYPE, XContentType.JSON.mediaType())
            .build();

        return new HttpRequest(httpPost, "inferenceEntityId");
    }

    public static PoolingAsyncClientConnectionManager createConnectionManager() {
        return PoolingAsyncClientConnectionManagerBuilder.create().build();
    }

    public static HttpSettings emptyHttpSettings() {
        return createHttpSettings(Settings.EMPTY);
    }

    private static HttpSettings createHttpSettings(Settings settings) {
        return new HttpSettings(settings, mockClusterService(settings));
    }

    /**
     * A minimal {@link CloseableHttpAsyncClient} that immediately hands the {@link FutureCallback} of every execution to the
     * given consumer. A hand-rolled subclass is used instead of a Mockito mock because all of the client's {@code execute}
     * methods are final and funnel into the protected {@code doExecute}, which a mock cannot stub.
     */
    private static class CallbackInvokingHttpAsyncClient extends CloseableHttpAsyncClient {
        private final Consumer<FutureCallback<?>> callbackConsumer;
        private final AtomicInteger startCalls = new AtomicInteger(0);

        CallbackInvokingHttpAsyncClient(Consumer<FutureCallback<?>> callbackConsumer) {
            this.callbackConsumer = callbackConsumer;
        }

        int startCalls() {
            return startCalls.get();
        }

        @Override
        public void start() {
            startCalls.incrementAndGet();
        }

        @Override
        public IOReactorStatus getStatus() {
            return IOReactorStatus.ACTIVE;
        }

        @Override
        public void awaitShutdown(org.apache.hc.core5.util.TimeValue waitTime) {}

        @Override
        public void initiateShutdown() {}

        @Override
        protected <T> Future<T> doExecute(
            HttpHost target,
            AsyncRequestProducer requestProducer,
            AsyncResponseConsumer<T> responseConsumer,
            HandlerFactory<AsyncPushConsumer> pushHandlerFactory,
            HttpContext context,
            FutureCallback<T> callback
        ) {
            callbackConsumer.accept(callback);
            return new CompletableFuture<>();
        }

        @Override
        public void register(String hostname, String uriPattern, Supplier<AsyncPushConsumer> supplier) {}

        @Override
        public void close(CloseMode closeMode) {}

        @Override
        public void close() {}
    }
}
