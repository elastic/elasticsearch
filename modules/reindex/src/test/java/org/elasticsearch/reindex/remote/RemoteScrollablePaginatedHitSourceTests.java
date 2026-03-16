/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex.remote;

import org.apache.http.ContentTooLongException;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.ProtocolVersion;
import org.apache.http.StatusLine;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.message.BasicStatusLine;
import org.apache.http.nio.protocol.HttpAsyncRequestProducer;
import org.apache.http.nio.protocol.HttpAsyncResponseConsumer;
import org.elasticsearch.Version;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.HeapBufferedAsyncResponseConsumer;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.BackoffPolicy;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.reindex.PaginatedHitSource;
import org.elasticsearch.index.reindex.PaginatedHitSource.Response;
import org.elasticsearch.index.reindex.RejectAwareActionListener;
import org.elasticsearch.index.reindex.RemoteInfo;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.elasticsearch.core.TimeValue.timeValueMillis;
import static org.elasticsearch.core.TimeValue.timeValueMinutes;
import static org.elasticsearch.reindex.remote.RemoteReindexingUtils.execute;
import static org.elasticsearch.reindex.remote.RemoteResponseParsers.RESPONSE_PARSER;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RemoteScrollablePaginatedHitSourceTests extends ESTestCase {
    private static final String FAKE_SCROLL_ID = "DnF1ZXJ5VGhlbkZldGNoBQAAAfakescroll";
    private int retries;
    private ThreadPool threadPool;
    private SearchRequest searchRequest;
    private int retriesAllowed;

    private final Queue<PaginatedHitSource.AsyncResponse> responseQueue = new LinkedBlockingQueue<>();

    private final Queue<Throwable> failureQueue = new LinkedBlockingQueue<>();

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName()) {
            @Override
            public ExecutorService executor(String name) {
                return EsExecutors.DIRECT_EXECUTOR_SERVICE;
            }

            @Override
            public ScheduledCancellable schedule(Runnable command, TimeValue delay, Executor name) {
                command.run();
                return null;
            }
        };
        retries = 0;
        searchRequest = new SearchRequest();
        searchRequest.scroll(timeValueMinutes(5));
        searchRequest.source(new SearchSourceBuilder().size(10).version(true).sort("_doc").size(123));
        retriesAllowed = 0;
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        terminate(threadPool);
    }

    @After
    public void validateAllConsumed() {
        assertThat(failureQueue, empty());
        assertThat(responseQueue, empty());
    }

    public void testParseStartOk() throws Exception {
        AtomicBoolean called = new AtomicBoolean();
        sourceWithMockedRemoteCall("start_ok.json").doStart(wrapAsListener(r -> {
            assertFalse(r.isTimedOut());
            assertEquals(FAKE_SCROLL_ID, r.getScrollId());
            assertEquals(4, r.getTotalHits());
            assertThat(r.getFailures(), empty());
            assertThat(r.getHits(), hasSize(1));
            assertEquals("test", r.getHits().get(0).getIndex());
            assertEquals("AVToMiC250DjIiBO3yJ_", r.getHits().get(0).getId());
            assertEquals("{\"test\":\"test2\"}", r.getHits().get(0).getSource().utf8ToString());
            assertNull(r.getHits().get(0).getRouting());
            called.set(true);
        }));
        assertTrue(called.get());
    }

    public void testParseScrollOk() throws Exception {
        AtomicBoolean called = new AtomicBoolean();
        sourceWithMockedRemoteCall("scroll_ok.json").doStartNextScroll("", timeValueMillis(0), wrapAsListener(r -> {
            assertFalse(r.isTimedOut());
            assertEquals(FAKE_SCROLL_ID, r.getScrollId());
            assertEquals(4, r.getTotalHits());
            assertThat(r.getFailures(), empty());
            assertThat(r.getHits(), hasSize(1));
            assertEquals("test", r.getHits().get(0).getIndex());
            assertEquals("AVToMiDL50DjIiBO3yKA", r.getHits().get(0).getId());
            assertEquals("{\"test\":\"test3\"}", r.getHits().get(0).getSource().utf8ToString());
            assertNull(r.getHits().get(0).getRouting());
            called.set(true);
        }));
        assertTrue(called.get());
    }

    /**
     * Test for parsing _ttl, _timestamp, _routing, and _parent.
     */
    public void testParseScrollFullyLoaded() throws Exception {
        AtomicBoolean called = new AtomicBoolean();
        sourceWithMockedRemoteCall("scroll_fully_loaded.json").doStartNextScroll("", timeValueMillis(0), wrapAsListener(r -> {
            assertEquals("AVToMiDL50DjIiBO3yKA", r.getHits().get(0).getId());
            assertEquals("{\"test\":\"test3\"}", r.getHits().get(0).getSource().utf8ToString());
            assertEquals("testrouting", r.getHits().get(0).getRouting());
            called.set(true);
        }));
        assertTrue(called.get());
    }

    /**
     * Test for parsing _ttl, _routing, and _parent. _timestamp isn't available.
     */
    public void testParseScrollFullyLoadedFrom1_7() throws Exception {
        AtomicBoolean called = new AtomicBoolean();
        sourceWithMockedRemoteCall("scroll_fully_loaded_1_7.json").doStartNextScroll("", timeValueMillis(0), wrapAsListener(r -> {
            assertEquals("AVToMiDL50DjIiBO3yKA", r.getHits().get(0).getId());
            assertEquals("{\"test\":\"test3\"}", r.getHits().get(0).getSource().utf8ToString());
            assertEquals("testrouting", r.getHits().get(0).getRouting());
            called.set(true);
        }));
        assertTrue(called.get());
    }

    /**
     * Versions of Elasticsearch before 2.1.0 don't support sort:_doc and instead need to use search_type=scan. Scan doesn't return
     * documents the first iteration but reindex doesn't like that. So we jump start strait to the next iteration.
     */
    public void testScanJumpStart() throws Exception {
        AtomicBoolean called = new AtomicBoolean();
        sourceWithMockedRemoteCall("start_scan.json", "scroll_ok.json").doStart(wrapAsListener(r -> {
            assertFalse(r.isTimedOut());
            assertEquals(FAKE_SCROLL_ID, r.getScrollId());
            assertEquals(4, r.getTotalHits());
            assertThat(r.getFailures(), empty());
            assertThat(r.getHits(), hasSize(1));
            assertEquals("test", r.getHits().get(0).getIndex());
            assertEquals("AVToMiDL50DjIiBO3yKA", r.getHits().get(0).getId());
            assertEquals("{\"test\":\"test3\"}", r.getHits().get(0).getSource().utf8ToString());
            assertNull(r.getHits().get(0).getRouting());
            called.set(true);
        }));
        assertTrue(called.get());
    }

    public void testParseRejection() throws Exception {
        // The rejection comes through in the handler because the mocked http response isn't marked as an error
        AtomicBoolean called = new AtomicBoolean();
        // Handling a scroll rejection is the same as handling a search rejection so we reuse the verification code
        Consumer<Response> checkResponse = r -> {
            assertFalse(r.isTimedOut());
            assertEquals(FAKE_SCROLL_ID, r.getScrollId());
            assertEquals(4, r.getTotalHits());
            assertThat(r.getFailures(), hasSize(1));
            assertEquals("test", r.getFailures().get(0).getIndex());
            assertEquals((Integer) 0, r.getFailures().get(0).getShardId());
            assertEquals("87A7NvevQxSrEwMbtRCecg", r.getFailures().get(0).getNodeId());
            assertThat(r.getFailures().get(0).getReason(), instanceOf(EsRejectedExecutionException.class));
            assertEquals(
                "rejected execution of org.elasticsearch.transport.TransportService$5@52d06af2 on "
                    + "EsThreadPoolExecutor[search, queue capacity = 1000, org.elasticsearch.common.util.concurrent."
                    + "EsThreadPoolExecutor@778ea553[Running, pool size = 7, active threads = 7, queued tasks = 1000, "
                    + "completed tasks = 4182]]",
                r.getFailures().get(0).getReason().getMessage()
            );
            assertThat(r.getHits(), hasSize(1));
            assertEquals("test", r.getHits().get(0).getIndex());
            assertEquals("AVToMiC250DjIiBO3yJ_", r.getHits().get(0).getId());
            assertEquals("{\"test\":\"test1\"}", r.getHits().get(0).getSource().utf8ToString());
            called.set(true);
        };
        sourceWithMockedRemoteCall("rejection.json").doStart(wrapAsListener(checkResponse));
        assertTrue(called.get());
        called.set(false);
        sourceWithMockedRemoteCall("rejection.json").doStartNextScroll("scroll", timeValueMillis(0), wrapAsListener(checkResponse));
        assertTrue(called.get());
    }

    public void testParseFailureWithStatus() throws Exception {
        // The rejection comes through in the handler because the mocked http response isn't marked as an error
        AtomicBoolean called = new AtomicBoolean();
        // Handling a scroll rejection is the same as handling a search rejection so we reuse the verification code
        Consumer<Response> checkResponse = r -> {
            assertFalse(r.isTimedOut());
            assertEquals(FAKE_SCROLL_ID, r.getScrollId());
            assertEquals(10000, r.getTotalHits());
            assertThat(r.getFailures(), hasSize(1));
            assertEquals(null, r.getFailures().get(0).getIndex());
            assertEquals(null, r.getFailures().get(0).getShardId());
            assertEquals(null, r.getFailures().get(0).getNodeId());
            assertThat(r.getFailures().get(0).getReason(), instanceOf(RuntimeException.class));
            assertEquals(
                "Unknown remote exception with reason=[SearchContextMissingException[No search context found for id [82]]]",
                r.getFailures().get(0).getReason().getMessage()
            );
            assertThat(r.getHits(), hasSize(1));
            assertEquals("test", r.getHits().get(0).getIndex());
            assertEquals("10000", r.getHits().get(0).getId());
            assertEquals("{\"test\":\"test10000\"}", r.getHits().get(0).getSource().utf8ToString());
            called.set(true);
        };
        sourceWithMockedRemoteCall("failure_with_status.json").doStart(wrapAsListener(checkResponse));
        assertTrue(called.get());
        called.set(false);
        sourceWithMockedRemoteCall("failure_with_status.json").doStartNextScroll(
            "scroll",
            timeValueMillis(0),
            wrapAsListener(checkResponse)
        );
        assertTrue(called.get());
    }

    /**
     * When constructed with a non-null initial remote version, doStart skips the version lookup and issues
     * the initial search directly. Only one HTTP request is made (the search), not two (version + search).
     */
    public void testDoStartSkipsVersionLookupWhenInitialRemoteVersionSet() throws Exception {
        AtomicBoolean called = new AtomicBoolean();
        RemoteScrollablePaginatedHitSource hitSource = sourceWithInitialRemoteVersion(Version.CURRENT, "start_ok.json");
        hitSource.doStart(wrapAsListener(r -> {
            assertFalse(r.isTimedOut());
            assertEquals(FAKE_SCROLL_ID, r.getScrollId());
            assertEquals(4, r.getTotalHits());
            assertThat(r.getHits(), hasSize(1));
            called.set(true);
        }));
        assertTrue(called.get());
    }

    public void testParseRequestFailure() throws Exception {
        AtomicBoolean called = new AtomicBoolean();
        Consumer<Response> checkResponse = r -> {
            assertFalse(r.isTimedOut());
            assertNull(r.getScrollId());
            assertEquals(0, r.getTotalHits());
            assertThat(r.getFailures(), hasSize(1));
            assertThat(r.getFailures().get(0).getReason(), instanceOf(ParsingException.class));
            ParsingException failure = (ParsingException) r.getFailures().get(0).getReason();
            assertEquals("Unknown key for a VALUE_STRING in [invalid].", failure.getMessage());
            assertEquals(2, failure.getLineNumber());
            assertEquals(14, failure.getColumnNumber());
            called.set(true);
        };
        sourceWithMockedRemoteCall("request_failure.json").doStart(wrapAsListener(checkResponse));
        assertTrue(called.get());
        called.set(false);
        sourceWithMockedRemoteCall("request_failure.json").doStartNextScroll("scroll", timeValueMillis(0), wrapAsListener(checkResponse));
        assertTrue(called.get());
    }

    public void testRetryAndSucceed() throws Exception {
        retriesAllowed = between(1, Integer.MAX_VALUE);
        sourceWithMockedRemoteCall("fail:rejection.json", "start_ok.json", "fail:rejection.json", "scroll_ok.json").start();
        PaginatedHitSource.AsyncResponse response = responseQueue.poll();
        assertNotNull(response);
        assertThat(response.response().getFailures(), empty());
        assertTrue(responseQueue.isEmpty());
        assertEquals(1, retries);
        retries = 0;
        response.done(timeValueMillis(0));
        response = responseQueue.poll();
        assertNotNull(response);
        assertThat(response.response().getFailures(), empty());
        assertTrue(responseQueue.isEmpty());
        assertEquals(1, retries);
    }

    public void testRetryUntilYouRunOutOfTries() throws Exception {
        retriesAllowed = between(0, 10);
        String[] paths = new String[retriesAllowed + 2];
        for (int i = 0; i < retriesAllowed + 2; i++) {
            paths[i] = "fail:rejection.json";
        }
        sourceWithMockedRemoteCall(paths).start();
        assertNotNull(failureQueue.poll());
        assertTrue(responseQueue.isEmpty());
        assertEquals(retriesAllowed, retries);
        retries = 0;
        String[] searchOKPaths = Stream.concat(Stream.of("start_ok.json"), Stream.of(paths)).toArray(String[]::new);
        sourceWithMockedRemoteCall(searchOKPaths).start();
        PaginatedHitSource.AsyncResponse response = responseQueue.poll();
        assertNotNull(response);
        assertThat(response.response().getFailures(), empty());
        assertTrue(responseQueue.isEmpty());

        response.done(timeValueMillis(0));
        assertNotNull(failureQueue.poll());
        assertTrue(responseQueue.isEmpty());
        assertEquals(retriesAllowed, retries);
    }

    public void testThreadContextRestored() throws Exception {
        String header = randomAlphaOfLength(5);
        threadPool.getThreadContext().putHeader("test", header);
        AtomicBoolean called = new AtomicBoolean();
        sourceWithMockedRemoteCall("start_ok.json").doStart(wrapAsListener(r -> {
            assertEquals(header, threadPool.getThreadContext().getHeader("test"));
            called.set(true);
        }));
        assertTrue(called.get());
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testTooLargeResponse() throws Exception {
        ContentTooLongException tooLong = new ContentTooLongException("too long!");
        CloseableHttpAsyncClient httpClient = mock(CloseableHttpAsyncClient.class);
        when(
            httpClient.<HttpResponse>execute(
                any(HttpAsyncRequestProducer.class),
                any(HttpAsyncResponseConsumer.class),
                any(HttpClientContext.class),
                any(FutureCallback.class)
            )
        ).then(new Answer<Future<HttpResponse>>() {
            @Override
            public Future<HttpResponse> answer(InvocationOnMock invocationOnMock) throws Throwable {
                HeapBufferedAsyncResponseConsumer consumer = (HeapBufferedAsyncResponseConsumer) invocationOnMock.getArguments()[1];
                FutureCallback callback = (FutureCallback) invocationOnMock.getArguments()[3];
                assertEquals(ByteSizeValue.of(100, ByteSizeUnit.MB).bytesAsInt(), consumer.getBufferLimit());
                callback.failed(tooLong);
                return null;
            }
        });
        RemoteScrollablePaginatedHitSource source = sourceWithMockedClient(true, httpClient);

        source.start();
        Throwable e = failureQueue.poll();
        // This next exception is what the user sees
        assertEquals("Remote responded with a chunk that was too large. Use a smaller batch size.", e.getMessage());
        // And that exception is reported as being caused by the underlying exception returned by the client
        assertSame(tooLong, e.getCause());
        assertTrue(responseQueue.isEmpty());
    }

    public void testInvalidJsonThinksRemoteIsNotES() throws Exception {
        sourceWithMockedRemoteCall("some_text.txt").start();
        Throwable e = failureQueue.poll();
        assertEquals("Error parsing the response, remote is likely not an Elasticsearch instance", e.getMessage());
    }

    public void testUnexpectedJsonThinksRemoteIsNotES() throws Exception {
        // Use the response from a main action instead of a proper start response to generate a parse error
        sourceWithMockedRemoteCall("main/2_3_3.json").start();
        Throwable e = failureQueue.poll();
        assertEquals("Error parsing the response, remote is likely not an Elasticsearch instance", e.getMessage());
    }

    public void testCleanupSuccessful() throws Exception {
        AtomicBoolean cleanupCallbackCalled = new AtomicBoolean();
        RestClient client = mock(RestClient.class);
        RemoteInfo remoteInfo = remoteInfo();
        TestRemoteScrollablePaginatedHitSource paginatedHitSource = new TestRemoteScrollablePaginatedHitSource(client, remoteInfo);
        paginatedHitSource.cleanup(() -> cleanupCallbackCalled.set(true));
        verify(client).close();
        assertTrue(cleanupCallbackCalled.get());
    }

    public void testCleanupFailure() throws Exception {
        AtomicBoolean cleanupCallbackCalled = new AtomicBoolean();
        RestClient client = mock(RestClient.class);
        doThrow(new RuntimeException("test")).when(client).close();
        RemoteInfo remoteInfo = remoteInfo();
        TestRemoteScrollablePaginatedHitSource paginatedHitSource = new TestRemoteScrollablePaginatedHitSource(client, remoteInfo);
        paginatedHitSource.cleanup(() -> cleanupCallbackCalled.set(true));
        verify(client).close();
        assertTrue(cleanupCallbackCalled.get());
    }

    private RemoteScrollablePaginatedHitSource sourceWithMockedRemoteCall(String... paths) throws Exception {
        return sourceWithMockedRemoteCall(true, ContentType.APPLICATION_JSON, paths);
    }

    /**
     * Creates a hit source that doesn't make the remote request and instead returns data from some files. Also requests are always returned
     * synchronously rather than asynchronously.
     */
    @SuppressWarnings("unchecked")
    private RemoteScrollablePaginatedHitSource sourceWithMockedRemoteCall(
        boolean mockRemoteVersion,
        ContentType contentType,
        String... paths
    ) {
        URL[] resources = new URL[paths.length];
        for (int i = 0; i < paths.length; i++) {
            resources[i] = Thread.currentThread().getContextClassLoader().getResource("responses/" + paths[i].replace("fail:", ""));
            if (resources[i] == null) {
                throw new IllegalArgumentException("Couldn't find [" + paths[i] + "]");
            }
        }

        CloseableHttpAsyncClient httpClient = mock(CloseableHttpAsyncClient.class);
        when(
            httpClient.<HttpResponse>execute(
                any(HttpAsyncRequestProducer.class),
                any(HttpAsyncResponseConsumer.class),
                any(HttpClientContext.class),
                any(FutureCallback.class)
            )
        ).thenAnswer(new Answer<Future<HttpResponse>>() {

            int responseCount = 0;

            @Override
            public Future<HttpResponse> answer(InvocationOnMock invocationOnMock) throws Throwable {
                // Throw away the current thread context to simulate running async httpclient's thread pool
                threadPool.getThreadContext().stashContext();
                HttpAsyncRequestProducer requestProducer = (HttpAsyncRequestProducer) invocationOnMock.getArguments()[0];
                FutureCallback<HttpResponse> futureCallback = (FutureCallback<HttpResponse>) invocationOnMock.getArguments()[3];
                HttpEntityEnclosingRequest request = (HttpEntityEnclosingRequest) requestProducer.generateRequest();
                URL resource = resources[responseCount];
                String path = paths[responseCount++];
                ProtocolVersion protocolVersion = new ProtocolVersion("http", 1, 1);
                if (path.startsWith("fail:")) {
                    String body = Streams.copyToString(new InputStreamReader(request.getEntity().getContent(), StandardCharsets.UTF_8));
                    if (path.equals("fail:rejection.json")) {
                        StatusLine statusLine = new BasicStatusLine(protocolVersion, RestStatus.TOO_MANY_REQUESTS.getStatus(), "");
                        BasicHttpResponse httpResponse = new BasicHttpResponse(statusLine);
                        futureCallback.completed(httpResponse);
                    } else {
                        futureCallback.failed(new RuntimeException(body));
                    }
                } else {
                    StatusLine statusLine = new BasicStatusLine(protocolVersion, 200, "");
                    HttpResponse httpResponse = new BasicHttpResponse(statusLine);
                    httpResponse.setEntity(new InputStreamEntity(FileSystemUtils.openFileURLStream(resource), contentType));
                    futureCallback.completed(httpResponse);
                }
                return null;
            }
        });
        return sourceWithMockedClient(mockRemoteVersion, httpClient);
    }

    private RemoteScrollablePaginatedHitSource sourceWithMockedClient(boolean mockRemoteVersion, CloseableHttpAsyncClient httpClient) {
        HttpAsyncClientBuilder clientBuilder = mock(HttpAsyncClientBuilder.class);
        when(clientBuilder.build()).thenReturn(httpClient);

        RestClient restClient = RestClient.builder(new HttpHost("localhost", 9200))
            .setHttpClientConfigCallback(httpClientBuilder -> clientBuilder)
            .build();

        RemoteInfo remoteInfo = remoteInfo();
        TestRemoteScrollablePaginatedHitSource paginatedHitSource = new TestRemoteScrollablePaginatedHitSource(restClient, remoteInfo) {
            @Override
            protected void doStart(RejectAwareActionListener<Response> searchListener) {
                // Short‑circuit version lookup by setting it to current
                if (mockRemoteVersion) {
                    remoteVersion = Version.CURRENT;
                    execute(
                        RemoteRequestBuilders.initialSearch(searchRequest, remoteInfo.getQuery(), remoteVersion),
                        RESPONSE_PARSER,
                        RejectAwareActionListener.withResponseHandler(searchListener, r -> onStartResponse(searchListener, r)),
                        threadPool,
                        restClient
                    );
                } else {
                    super.doStart(searchListener);
                }
            }
        };
        if (mockRemoteVersion) {
            paginatedHitSource.remoteVersion = Version.CURRENT;
        }
        return paginatedHitSource;
    }

    /**
     * Creates a RemoteScrollablePaginatedHitSource with a pre-resolved initial remote version so that doStart skips the version lookup.
     * The mock client serves only the given response paths (one request = one path when using initial version).
     */
    private RemoteScrollablePaginatedHitSource sourceWithInitialRemoteVersion(Version initialRemoteVersion, String... paths)
        throws Exception {
        return sourceWithInitialRemoteVersion(initialRemoteVersion, ContentType.APPLICATION_JSON, paths);
    }

    @SuppressWarnings("unchecked")
    private RemoteScrollablePaginatedHitSource sourceWithInitialRemoteVersion(
        Version initialRemoteVersion,
        ContentType contentType,
        String... paths
    ) throws Exception {
        URL[] resources = new URL[paths.length];
        for (int i = 0; i < paths.length; i++) {
            resources[i] = Thread.currentThread().getContextClassLoader().getResource("responses/" + paths[i].replace("fail:", ""));
            if (resources[i] == null) {
                throw new IllegalArgumentException("Couldn't find [" + paths[i] + "]");
            }
        }

        CloseableHttpAsyncClient httpClient = mock(CloseableHttpAsyncClient.class);
        when(
            httpClient.<HttpResponse>execute(
                any(HttpAsyncRequestProducer.class),
                any(HttpAsyncResponseConsumer.class),
                any(HttpClientContext.class),
                any(FutureCallback.class)
            )
        ).thenAnswer(new Answer<Future<HttpResponse>>() {
            int responseCount = 0;

            @Override
            public Future<HttpResponse> answer(InvocationOnMock invocationOnMock) throws Throwable {
                threadPool.getThreadContext().stashContext();
                FutureCallback<HttpResponse> futureCallback = (FutureCallback<HttpResponse>) invocationOnMock.getArguments()[3];
                HttpAsyncRequestProducer requestProducer = (HttpAsyncRequestProducer) invocationOnMock.getArguments()[0];
                HttpEntityEnclosingRequest request = (HttpEntityEnclosingRequest) requestProducer.generateRequest();
                URL resource = resources[responseCount];
                String path = paths[responseCount++];
                ProtocolVersion protocolVersion = new ProtocolVersion("http", 1, 1);
                if (path.startsWith("fail:")) {
                    String body = Streams.copyToString(new InputStreamReader(request.getEntity().getContent(), StandardCharsets.UTF_8));
                    if (path.equals("fail:rejection.json")) {
                        StatusLine statusLine = new BasicStatusLine(protocolVersion, RestStatus.TOO_MANY_REQUESTS.getStatus(), "");
                        futureCallback.completed(new BasicHttpResponse(statusLine));
                    } else {
                        futureCallback.failed(new RuntimeException(body));
                    }
                } else {
                    StatusLine statusLine = new BasicStatusLine(protocolVersion, 200, "");
                    HttpResponse httpResponse = new BasicHttpResponse(statusLine);
                    httpResponse.setEntity(new InputStreamEntity(FileSystemUtils.openFileURLStream(resource), contentType));
                    futureCallback.completed(httpResponse);
                }
                return null;
            }
        });

        HttpAsyncClientBuilder clientBuilder = mock(HttpAsyncClientBuilder.class);
        when(clientBuilder.build()).thenReturn(httpClient);
        RestClient restClient = RestClient.builder(new HttpHost("localhost", 9200))
            .setHttpClientConfigCallback(httpClientBuilder -> clientBuilder)
            .build();

        return new RemoteScrollablePaginatedHitSource(
            logger,
            backoff(),
            threadPool,
            this::countRetry,
            responseQueue::add,
            failureQueue::add,
            restClient,
            remoteInfo(),
            searchRequest,
            initialRemoteVersion
        );
    }

    private RemoteInfo remoteInfo() {
        return new RemoteInfo(
            "http",
            randomAlphaOfLength(8),
            randomIntBetween(4000, 9000),
            null,
            new BytesArray("{}"),
            null,
            null,
            Map.of(),
            TimeValue.timeValueSeconds(randomIntBetween(5, 30)),
            TimeValue.timeValueSeconds(randomIntBetween(5, 30))
        );
    }

    private BackoffPolicy backoff() {
        return BackoffPolicy.constantBackoff(timeValueMillis(0), retriesAllowed);
    }

    private void countRetry() {
        retries += 1;
    }

    private class TestRemoteScrollablePaginatedHitSource extends RemoteScrollablePaginatedHitSource {
        TestRemoteScrollablePaginatedHitSource(RestClient client, RemoteInfo remoteInfo) {
            super(
                RemoteScrollablePaginatedHitSourceTests.this.logger,
                backoff(),
                RemoteScrollablePaginatedHitSourceTests.this.threadPool,
                RemoteScrollablePaginatedHitSourceTests.this::countRetry,
                responseQueue::add,
                failureQueue::add,
                client,
                remoteInfo,
                RemoteScrollablePaginatedHitSourceTests.this.searchRequest,
                randomBoolean() ? Version.CURRENT : null
            );
        }
    }

    private <T> RejectAwareActionListener<T> wrapAsListener(Consumer<T> consumer) {
        return RejectAwareActionListener.wrap(consumer::accept, ESTestCase::fail, ESTestCase::fail);
    }
}
