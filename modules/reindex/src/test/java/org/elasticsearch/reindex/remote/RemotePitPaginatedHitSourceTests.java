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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.reindex.BulkByScrollTask;
import org.elasticsearch.index.reindex.RejectAwareActionListener;
import org.elasticsearch.index.reindex.RemoteInfo;
import org.elasticsearch.index.reindex.ResumeInfo;
import org.elasticsearch.reindex.PaginatedHitSource;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.mockito.stubbing.Answer;

import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.lang.Math.abs;
import static java.util.stream.Collectors.toList;
import static org.apache.lucene.tests.util.TestUtil.randomSimpleString;
import static org.elasticsearch.core.TimeValue.timeValueMillis;
import static org.elasticsearch.core.TimeValue.timeValueMinutes;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link RemotePitPaginatedHitSource}.
 */
public class RemotePitPaginatedHitSourceTests extends ESTestCase {

    private static final BytesReference PIT_ID = new BytesArray("pit-id".getBytes(StandardCharsets.UTF_8));
    private static final TimeValue KEEP_ALIVE = timeValueMinutes(5);

    private int retries;
    private ThreadPool threadPool;
    private SearchRequest searchRequest;
    private int retriesAllowed;

    private final Queue<PaginatedHitSource.AsyncResponse> responseQueue = new LinkedBlockingQueue<>();
    private final Queue<Throwable> failureQueue = new LinkedBlockingQueue<>();

    private static BulkByScrollTask.Status randomStatusWithoutException() {
        if (randomBoolean()) {
            return randomWorkingStatus(null);
        }
        boolean canHaveNullStatues = randomBoolean();
        List<BulkByScrollTask.StatusOrException> statuses = IntStream.range(0, between(0, 10)).mapToObj(i -> {
            if (canHaveNullStatues && rarely()) {
                return null;
            }
            return new BulkByScrollTask.StatusOrException(randomWorkingStatus(i));
        }).collect(toList());
        return new BulkByScrollTask.Status(statuses, randomBoolean() ? "test" : null);
    }

    private static BulkByScrollTask.Status randomWorkingStatus(Integer sliceId) {
        int total = between(0, 10000000);
        int updated = between(0, total);
        int created = between(0, total - updated);
        int deleted = between(0, total - updated - created);
        int noops = total - updated - created - deleted;
        int batches = between(0, 10000);
        long versionConflicts = between(0, total);
        long bulkRetries = between(0, 10000000);
        long searchRetries = between(0, 100000);
        TimeUnit[] timeUnits = { TimeUnit.MILLISECONDS, TimeUnit.SECONDS, TimeUnit.MINUTES, TimeUnit.HOURS, TimeUnit.DAYS };
        TimeValue throttled = new TimeValue(randomIntBetween(0, 1000), randomFrom(timeUnits));
        TimeValue throttledUntil = new TimeValue(randomIntBetween(0, 1000), randomFrom(timeUnits));
        return new BulkByScrollTask.Status(
            sliceId,
            total,
            updated,
            created,
            deleted,
            batches,
            versionConflicts,
            noops,
            bulkRetries,
            searchRetries,
            throttled,
            abs(randomFloat()),
            randomBoolean() ? null : randomSimpleString(random()),
            throttledUntil
        );
    }

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
            public ScheduledCancellable schedule(Runnable command, TimeValue delay, java.util.concurrent.Executor name) {
                command.run();
                return null;
            }
        };
        retries = 0;
        searchRequest = new SearchRequest();
        searchRequest.source(
            new SearchSourceBuilder().size(10)
                .version(true)
                .sort("_shard_doc")
                .pointInTimeBuilder(new PointInTimeBuilder(PIT_ID).setKeepAlive(KEEP_ALIVE))
        );
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

    /** Verifies doFirstSearch parses a PIT search response. */
    public void testParsePitOk() {
        AtomicBoolean called = new AtomicBoolean();
        sourceWithMockedRemoteCall("pit_ok.json").doFirstSearch(wrapAsListener(r -> {
            assertFalse(r.isTimedOut());
            assertNull(r.getScrollId());
            assertNotNull(r.getPitId());
            assertEquals(4, r.getTotalHits());
            assertThat(r.getFailures(), empty());
            assertThat(r.getHits(), hasSize(1));
            assertEquals("test", r.getHits().getFirst().getIndex());
            assertEquals("AVToMiC250DjIiBO3yJ_", r.getHits().getFirst().getId());
            assertEquals("{\"test\":\"test2\"}", r.getHits().getFirst().getSource().utf8ToString());
            assertNotNull(r.getSearchAfterValues());
            assertEquals(2, r.getSearchAfterValues().length);
            assertEquals(12345, r.getSearchAfterValues()[0]);
            assertEquals("sort-key", r.getSearchAfterValues()[1]);
            called.set(true);
        }));
        assertTrue(called.get());
    }

    /** Verifies doFirstSearch parses sort values that exceed int range as Long. */
    public void testParsePitOkWithLongSortValue() {
        AtomicBoolean called = new AtomicBoolean();
        sourceWithMockedRemoteCall("pit_ok_with_long_search_after_value.json").doFirstSearch(wrapAsListener(r -> {
            assertNotNull(r.getSearchAfterValues());
            assertEquals(2, r.getSearchAfterValues().length);
            assertEquals(4294967396L, ((Number) r.getSearchAfterValues()[0]).longValue());
            assertEquals("sort-key", r.getSearchAfterValues()[1]);
            called.set(true);
        }));
        assertTrue(called.get());
    }

    /** Verifies doNextSearch with search_after parses a PIT search response. */
    public void testParsePitNextOk() {
        AtomicBoolean called = new AtomicBoolean();
        Object[] searchAfter = new Object[] { 12345L, "sort-key" };
        sourceWithMockedRemoteCall("pit_ok.json").doNextPitSearch(searchAfter, timeValueMillis(0), wrapAsListener(r -> {
            assertFalse(r.isTimedOut());
            assertNull(r.getScrollId());
            assertNotNull(r.getPitId());
            assertEquals(4, r.getTotalHits());
            assertThat(r.getFailures(), empty());
            assertThat(r.getHits(), hasSize(1));
            assertEquals("test", r.getHits().getFirst().getIndex());
            called.set(true);
        }));
        assertTrue(called.get());
    }

    /** Verifies that constructor rejects SearchRequest without pointInTimeBuilder. */
    public void testConstructorRejectsMissingPointInTime() {
        SearchRequest nullSource = new SearchRequest();
        SearchRequest sourceWithoutPit = new SearchRequest().source(new SearchSourceBuilder());
        for (SearchRequest request : new SearchRequest[] { nullSource, sourceWithoutPit }) {
            expectThrows(
                IllegalArgumentException.class,
                () -> new RemotePitPaginatedHitSource(
                    logger,
                    BackoffPolicy.constantBackoff(TimeValue.ZERO, 0),
                    threadPool,
                    () -> {},
                    r -> {},
                    e -> {},
                    mock(RestClient.class),
                    remoteInfo(),
                    request,
                    Version.CURRENT
                )
            );
        }
    }

    /** Verifies that restoreState resumes from PitWorkerResumeInfo and fetches next batch. */
    public void testRestoreStateResumesFromPitWorkerResumeInfo() {
        Object[] searchAfterValues = new Object[] { 100L, "sort-key" };
        ResumeInfo.PitWorkerResumeInfo resumeInfo = new ResumeInfo.PitWorkerResumeInfo(
            PIT_ID,
            searchAfterValues,
            System.currentTimeMillis(),
            randomStatusWithoutException(),
            null
        );

        RemotePitPaginatedHitSource hitSource = sourceWithMockedRemoteCall("pit_ok.json");
        hitSource.resume(resumeInfo);

        PaginatedHitSource.AsyncResponse asyncResponse = responseQueue.poll();
        assertNotNull(asyncResponse);
        assertThat(asyncResponse.response().getHits(), hasSize(1));
        assertEquals("AVToMiC250DjIiBO3yJ_", asyncResponse.response().getHits().getFirst().getId());
    }

    /** Verifies retries on rejection succeed within the retry limit. */
    public void testRetryAndSucceed() {
        retriesAllowed = between(1, Integer.MAX_VALUE);
        sourceWithMockedRemoteCall("fail:rejection.json", "pit_ok.json", "fail:rejection.json", "pit_ok.json").start();
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

    /** Verifies retries on rejection fail when exceeding the retry limit. */
    public void testRetryUntilYouRunOutOfTries() {
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
        String[] pitOkPaths = Stream.concat(Stream.of("pit_ok.json"), Stream.of(paths)).toArray(String[]::new);
        sourceWithMockedRemoteCall(pitOkPaths).start();
        PaginatedHitSource.AsyncResponse response = responseQueue.poll();
        assertNotNull(response);
        assertThat(response.response().getFailures(), empty());
        assertTrue(responseQueue.isEmpty());

        response.done(timeValueMillis(0));
        assertNotNull(failureQueue.poll());
        assertTrue(responseQueue.isEmpty());
        assertEquals(retriesAllowed, retries);
    }

    /** Verifies thread context is restored after async execution. */
    public void testThreadContextRestored() {
        String header = randomAlphaOfLength(5);
        threadPool.getThreadContext().putHeader("test", header);
        AtomicBoolean called = new AtomicBoolean();
        sourceWithMockedRemoteCall("pit_ok.json").doFirstSearch(wrapAsListener(r -> {
            assertEquals(header, threadPool.getThreadContext().getHeader("test"));
            called.set(true);
        }));
        assertTrue(called.get());
    }

    /** Verifies cleanup closes the RestClient and RemoteInfo. */
    public void testCleanupSuccessful() throws Exception {
        AtomicBoolean cleanupCallbackCalled = new AtomicBoolean();
        RestClient client = mock(RestClient.class);
        RemoteInfo remoteInfo = remoteInfo();
        RemotePitPaginatedHitSource hitSource = new RemotePitPaginatedHitSource(
            logger,
            BackoffPolicy.constantBackoff(TimeValue.ZERO, 0),
            threadPool,
            Assert::fail,
            r -> fail(),
            e -> fail(),
            client,
            remoteInfo,
            searchRequest,
            Version.CURRENT
        );
        hitSource.cleanup(() -> cleanupCallbackCalled.set(true));
        verify(client).close();
        assertTrue(cleanupCallbackCalled.get());
    }

    /** Verifies cleanup callback is invoked even when client.close() throws. */
    public void testCleanupFailure() throws Exception {
        AtomicBoolean cleanupCallbackCalled = new AtomicBoolean();
        RestClient client = mock(RestClient.class);
        doThrow(new RuntimeException("test")).when(client).close();
        RemoteInfo remoteInfo = remoteInfo();
        RemotePitPaginatedHitSource hitSource = new RemotePitPaginatedHitSource(
            logger,
            BackoffPolicy.constantBackoff(TimeValue.ZERO, 0),
            threadPool,
            Assert::fail,
            r -> fail(),
            e -> fail(),
            client,
            remoteInfo,
            searchRequest,
            Version.CURRENT
        );
        hitSource.cleanup(() -> cleanupCallbackCalled.set(true));
        verify(client).close();
        assertTrue(cleanupCallbackCalled.get());
    }

    /** Verifies getPitId returns the PIT ID from the response after doFirstSearch. */
    public void testGetPitIdReturnsValueFromResponse() {
        // pit_ok_with_long_search_after_value.json has pit_id "bG9uZy1waXQtaWQ" (base64 for "long-pit-id")
        RemotePitPaginatedHitSource hitSource = sourceWithMockedRemoteCall("pit_ok_with_long_search_after_value.json");
        hitSource.doFirstSearch(wrapAsListener(r -> {}));
        BytesReference expectedPitId = new BytesArray("long-pit-id".getBytes(StandardCharsets.UTF_8));
        assertThat(hitSource.getPitId(), equalTo(expectedPitId));
    }

    /** Verifies hasMoreBatches reflects search_after state. */
    public void testHasMoreBatches() {
        RemotePitPaginatedHitSource paginatedHitSource = sourceWithMockedRemoteCall("pit_ok.json");

        // Initially: no search_after -> false
        assertFalse(paginatedHitSource.hasMoreBatches());

        // Non-null search_after -> true
        paginatedHitSource.setSearchAfterValues(new Object[] { 1L, "sort" });
        assertTrue(paginatedHitSource.hasMoreBatches());

        paginatedHitSource.setSearchAfterValues(null);
        assertFalse(paginatedHitSource.hasMoreBatches());
    }

    /** Verifies remoteVersion returns the configured version. */
    public void testRemoteVersion() {
        RemotePitPaginatedHitSource hitSource = sourceWithMockedRemoteCall("pit_ok.json");
        assertTrue(hitSource.remoteVersion().isPresent());
        assertEquals(Version.CURRENT, hitSource.remoteVersion().get());
    }

    /** Verifies parsing rejection (shard failure) response with PIT. */
    public void testParseRejection() {
        AtomicBoolean called = new AtomicBoolean();
        sourceWithMockedRemoteCall("rejection.json").doFirstSearch(wrapAsListener(r -> {
            assertFalse(r.isTimedOut());
            assertEquals(4, r.getTotalHits());
            assertThat(r.getFailures(), hasSize(1));
            assertThat(r.getFailures().getFirst().getReason(), instanceOf(EsRejectedExecutionException.class));
            assertThat(r.getHits(), hasSize(1));
            assertEquals("test", r.getHits().getFirst().getIndex());
            assertEquals("AVToMiC250DjIiBO3yJ_", r.getHits().getFirst().getId());
            called.set(true);
        }));
        assertTrue(called.get());
    }

    /** Verifies parsing shard failure with SearchContextMissingException. */
    public void testParseFailureWithStatus() {
        AtomicBoolean called = new AtomicBoolean();
        Consumer<PaginatedHitSource.Response> checkResponse = r -> {
            assertFalse(r.isTimedOut());
            assertEquals(10000, r.getTotalHits());
            assertThat(r.getFailures(), hasSize(1));
            assertNull(r.getFailures().getFirst().getIndex());
            assertNull(r.getFailures().getFirst().getShardId());
            assertNull(r.getFailures().getFirst().getNodeId());
            assertThat(r.getFailures().getFirst().getReason(), instanceOf(RuntimeException.class));
            assertEquals(
                "Unknown remote exception with reason=[SearchContextMissingException[No search context found for id [82]]]",
                r.getFailures().getFirst().getReason().getMessage()
            );
            assertThat(r.getHits(), hasSize(1));
            assertEquals("test", r.getHits().getFirst().getIndex());
            assertEquals("10000", r.getHits().getFirst().getId());
            assertEquals("{\"test\":\"test10000\"}", r.getHits().getFirst().getSource().utf8ToString());
            called.set(true);
        };
        sourceWithMockedRemoteCall("failure_with_status.json").doFirstSearch(wrapAsListener(checkResponse));
        assertTrue(called.get());
        called.set(false);
        sourceWithMockedRemoteCall("failure_with_status.json").doNextPitSearch(
            new Object[] { 12345L, "sort-key" },
            timeValueMillis(0),
            wrapAsListener(checkResponse)
        );
        assertTrue(called.get());
    }

    /** Verifies parsing request failure with ParsingException. */
    public void testParseRequestFailure() {
        AtomicBoolean called = new AtomicBoolean();
        Consumer<PaginatedHitSource.Response> checkResponse = r -> {
            assertFalse(r.isTimedOut());
            assertNull(r.getScrollId());
            assertEquals(0, r.getTotalHits());
            assertThat(r.getFailures(), hasSize(1));
            assertThat(r.getFailures().getFirst().getReason(), instanceOf(ParsingException.class));
            ParsingException failure = (ParsingException) r.getFailures().getFirst().getReason();
            assertEquals("Unknown key for a VALUE_STRING in [invalid].", failure.getMessage());
            assertEquals(2, failure.getLineNumber());
            assertEquals(14, failure.getColumnNumber());
            called.set(true);
        };
        sourceWithMockedRemoteCall("request_failure.json").doFirstSearch(wrapAsListener(checkResponse));
        assertTrue(called.get());
        called.set(false);
        sourceWithMockedRemoteCall("request_failure.json").doNextPitSearch(
            new Object[] { 12345L, "sort-key" },
            timeValueMillis(0),
            wrapAsListener(checkResponse)
        );
        assertTrue(called.get());
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testTooLargeResponse() {
        ContentTooLongException tooLong = new ContentTooLongException("too long!");
        CloseableHttpAsyncClient httpClient = mock(CloseableHttpAsyncClient.class);
        when(
            httpClient.<HttpResponse>execute(
                any(HttpAsyncRequestProducer.class),
                any(HttpAsyncResponseConsumer.class),
                any(HttpClientContext.class),
                any(FutureCallback.class)
            )
        ).then((Answer<Future<HttpResponse>>) invocationOnMock -> {
            HeapBufferedAsyncResponseConsumer consumer = (HeapBufferedAsyncResponseConsumer) invocationOnMock.getArguments()[1];
            FutureCallback callback = (FutureCallback) invocationOnMock.getArguments()[3];
            assertEquals(ByteSizeValue.of(100, ByteSizeUnit.MB).bytesAsInt(), consumer.getBufferLimit());
            callback.failed(tooLong);
            return null;
        });
        RemotePitPaginatedHitSource source = sourceWithMockedClient(httpClient);

        source.start();
        Throwable e = failureQueue.poll();
        assertEquals("Remote responded with a chunk that was too large. Use a smaller batch size.", e.getMessage());
        assertSame(tooLong, e.getCause());
        assertTrue(responseQueue.isEmpty());
    }

    public void testInvalidJsonThinksRemoteIsNotES() {
        sourceWithMockedRemoteCall("some_text.txt").start();
        Throwable e = failureQueue.poll();
        assertEquals("Error parsing the response, remote is likely not an Elasticsearch instance", e.getMessage());
    }

    public void testUnexpectedJsonThinksRemoteIsNotES() {
        sourceWithMockedRemoteCall("unexpected_json.json").start();
        Throwable e = failureQueue.poll();
        assertNotNull("Expected a parse failure to be reported", e);
        assertEquals("Error parsing the response, remote is likely not an Elasticsearch instance", e.getMessage());
    }

    private RemotePitPaginatedHitSource sourceWithMockedClient(CloseableHttpAsyncClient httpClient) {
        HttpAsyncClientBuilder clientBuilder = mock(HttpAsyncClientBuilder.class);
        when(clientBuilder.build()).thenReturn(httpClient);
        RestClient restClient = RestClient.builder(new HttpHost("localhost", 9200))
            .setHttpClientConfigCallback(httpClientBuilder -> clientBuilder)
            .build();
        return new RemotePitPaginatedHitSource(
            logger,
            backoff(),
            threadPool,
            this::countRetry,
            responseQueue::add,
            failureQueue::add,
            restClient,
            remoteInfo(),
            searchRequest,
            Version.CURRENT
        );
    }

    @SuppressWarnings("unchecked")
    private RemotePitPaginatedHitSource sourceWithMockedRemoteCall(String... paths) {
        URL[] resources = new URL[paths.length];
        for (int i = 0; i < paths.length; i++) {
            resources[i] = Thread.currentThread().getContextClassLoader().getResource("responses/" + paths[i].replace("fail:", ""));
            if (resources[i] == null) {
                throw new IllegalArgumentException("Couldn't find [" + paths[i] + "]");
            }
        }

        CloseableHttpAsyncClient httpClient = mock(CloseableHttpAsyncClient.class);
        final int[] responseCount = { 0 };
        when(
            httpClient.<HttpResponse>execute(
                any(HttpAsyncRequestProducer.class),
                any(HttpAsyncResponseConsumer.class),
                any(HttpClientContext.class),
                any(FutureCallback.class)
            )
        ).thenAnswer((Answer<Future<HttpResponse>>) invocation -> {
            threadPool.getThreadContext().stashContext();
            HttpAsyncRequestProducer requestProducer = invocation.getArgument(0);
            FutureCallback<HttpResponse> futureCallback = invocation.getArgument(3);
            HttpEntityEnclosingRequest request = (HttpEntityEnclosingRequest) requestProducer.generateRequest();
            int idx = Math.min(responseCount[0]++, paths.length - 1);
            String path = paths[idx];
            URL resource = resources[idx];
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
                httpResponse.setEntity(new InputStreamEntity(FileSystemUtils.openFileURLStream(resource), ContentType.APPLICATION_JSON));
                futureCallback.completed(httpResponse);
            }
            return null;
        });

        HttpAsyncClientBuilder clientBuilder = mock(HttpAsyncClientBuilder.class);
        when(clientBuilder.build()).thenReturn(httpClient);
        RestClient restClient = RestClient.builder(new HttpHost("localhost", 9200))
            .setHttpClientConfigCallback(httpClientBuilder -> clientBuilder)
            .build();

        return new RemotePitPaginatedHitSource(
            logger,
            backoff(),
            threadPool,
            this::countRetry,
            responseQueue::add,
            failureQueue::add,
            restClient,
            remoteInfo(),
            searchRequest,
            Version.CURRENT
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

    private <T> RejectAwareActionListener<T> wrapAsListener(Consumer<T> consumer) {
        return RejectAwareActionListener.wrap(consumer::accept, ESTestCase::fail, ESTestCase::fail);
    }
}
