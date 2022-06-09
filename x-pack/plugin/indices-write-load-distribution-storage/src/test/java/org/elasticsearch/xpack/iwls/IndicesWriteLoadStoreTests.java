/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.iwls;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.iwls.IndicesWriteLoadStore.INDICES_WRITE_LOAD_DATA_STREAM;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

@SuppressWarnings("unchecked")
public class IndicesWriteLoadStoreTests extends ESTestCase {
    private ThreadPool threadPool;
    private ClusterSettings clusterSettings;

    @Before
    public void beforeTest() {
        threadPool = new TestThreadPool(this.getClass().getName());
        clusterSettings = new ClusterSettings(Settings.EMPTY, Set.of(IndicesWriteLoadStore.ENABLED_SETTING));
    }

    @After
    public void afterTest() throws Exception {
        threadPool.shutdownNow();
    }

    public void testRequestAreNotSentWhenDisabled() throws Exception {
        final var settings = Settings.builder()
            .put(IndicesWriteLoadStore.MAX_DOCUMENTS_PER_BULK_SETTING.getKey(), 1)
            .put(IndicesWriteLoadStore.ENABLED_SETTING.getKey(), false)
            .build();
        try (
            var client = new MockClient(threadPool);
            var indicesWriteLoadStore = new IndicesWriteLoadStore(threadPool, client, clusterSettings, settings)
        ) {
            client.setHandler((action, request, listener) -> { assert false : "unexpected call"; });

            indexRandomSamples(indicesWriteLoadStore, randomIntBetween(1, 100));
        }
    }

    public void testIndicesWriteLoadSamplesAreStored() throws Exception {
        final int numberOfSamples = randomIntBetween(1, 1000);

        final var settings = Settings.builder().put(IndicesWriteLoadStore.MAX_DOCUMENTS_PER_BULK_SETTING.getKey(), numberOfSamples).build();
        try (
            var client = new MockClient(threadPool);
            var indicesWriteLoadStore = new IndicesWriteLoadStore(threadPool, client, clusterSettings, settings)
        ) {
            final var receivedBulkRequestCount = new AtomicInteger();
            client.setHandler((action, request, listener) -> {
                receivedBulkRequestCount.incrementAndGet();
                BulkRequest bulkRequest = (BulkRequest) request;
                assertNotNull(listener);
                assertThat(action, is(equalTo(BulkAction.INSTANCE)));

                for (DocWriteRequest<?> docWriteRequest : bulkRequest.requests()) {
                    assertThat(docWriteRequest.index(), is(equalTo(INDICES_WRITE_LOAD_DATA_STREAM)));
                    assertThat(docWriteRequest.opType(), is(equalTo(DocWriteRequest.OpType.CREATE)));
                }

                int indexRequestCount = bulkRequest.numberOfActions();
                assertThat(indexRequestCount, is(equalTo(numberOfSamples)));
                ((ActionListener<BulkResponse>) listener).onResponse(getSuccessfulResponse(indexRequestCount));
            });

            indexRandomSamples(indicesWriteLoadStore, numberOfSamples);

            assertBusy(() -> assertThat(receivedBulkRequestCount.get(), equalTo(1)));
        }
    }

    public void testNonRetryableErrorsAreNotRetried() throws Exception {
        final var numberOfSamples = randomIntBetween(1, 100);

        final var settings = Settings.builder().put(IndicesWriteLoadStore.MAX_DOCUMENTS_PER_BULK_SETTING.getKey(), numberOfSamples).build();
        try (
            var client = new MockClient(threadPool);
            var indicesWriteLoadStore = new IndicesWriteLoadStore(threadPool, client, clusterSettings, settings)
        ) {
            final var calledFuture = PlainActionFuture.newFuture();
            final var calledTimes = new AtomicInteger();
            client.setHandler((action, request, listener) -> {
                listener.onFailure(new IllegalArgumentException("Non retryable failure!"));
                calledTimes.incrementAndGet();
                calledFuture.onResponse(null);
            });

            final var indicesWriteLoadStoreLogger = LogManager.getLogger(IndicesWriteLoadStore.class);
            final var mockLogAppender = new MockLogAppender();
            mockLogAppender.start();
            try {
                mockLogAppender.addExpectation(
                    new MockLogAppender.SeenEventExpectation(
                        "expected message",
                        IndicesWriteLoadStore.class.getCanonicalName(),
                        Level.WARN,
                        "Failed to index * items into indices write load index"
                    )
                );
                Loggers.addAppender(indicesWriteLoadStoreLogger, mockLogAppender);

                indexRandomSamples(indicesWriteLoadStore, numberOfSamples);

                calledFuture.get();
                assertThat(calledTimes.get(), is(equalTo(1)));
                mockLogAppender.assertAllExpectationsMatched();
            } finally {
                Loggers.removeAppender(indicesWriteLoadStoreLogger, mockLogAppender);
                mockLogAppender.stop();
            }
        }
    }

    public void testIndexingIsRetriedWhenErrorIsRetryable() throws Exception {
        final var numberOfSamples = randomIntBetween(1, 100);
        final var maxRetries = randomIntBetween(1, 3);
        final var settings = Settings.builder()
            .put(IndicesWriteLoadStore.MAX_DOCUMENTS_PER_BULK_SETTING.getKey(), numberOfSamples)
            .put(IndicesWriteLoadStore.MAX_RETRIES_SETTING.getKey(), maxRetries)
            .build();
        try (
            var client = new MockClient(threadPool);
            var indicesWriteLoadStore = new IndicesWriteLoadStore(threadPool, client, clusterSettings, settings)
        ) {
            final var receivedBulkRequestCount = new AtomicInteger();
            final var successfulWriteResponseFuture = PlainActionFuture.newFuture();
            client.setHandler((action, request, listener) -> {
                if (receivedBulkRequestCount.incrementAndGet() < maxRetries + 1) {
                    listener.onFailure(new CircuitBreakingException("I'll be back", CircuitBreaker.Durability.TRANSIENT));
                    return;
                }
                BulkRequest bulkRequest = (BulkRequest) request;
                assertNotNull(listener);

                for (DocWriteRequest<?> docWriteRequest : bulkRequest.requests()) {
                    assertThat(docWriteRequest.index(), is(equalTo(INDICES_WRITE_LOAD_DATA_STREAM)));
                    assertThat(docWriteRequest.opType(), is(equalTo(DocWriteRequest.OpType.CREATE)));
                }

                assertThat(bulkRequest.numberOfActions(), is(equalTo(numberOfSamples)));

                int responses = bulkRequest.numberOfActions();
                ((ActionListener<BulkResponse>) listener).onResponse(getSuccessfulResponse(responses));
                successfulWriteResponseFuture.onResponse(null);
            });

            indexRandomSamples(indicesWriteLoadStore, numberOfSamples);

            successfulWriteResponseFuture.get();
            assertBusy(() -> assertThat(receivedBulkRequestCount.get(), equalTo(maxRetries + 1)));
        }
    }

    public void testMaxConcurrentRequestIsConfigurable() throws Exception {
        final int numberOfSamples = randomIntBetween(1, 100);
        final int maxConcurrentRequests = randomIntBetween(0, 10);
        final var settings = Settings.builder()
            .put(IndicesWriteLoadStore.MAX_DOCUMENTS_PER_BULK_SETTING.getKey(), numberOfSamples)
            .put(IndicesWriteLoadStore.MAX_CONCURRENT_REQUESTS_SETTING.getKey(), maxConcurrentRequests)
            .build();
        try (
            var client = new MockClient(threadPool);
            var indicesWriteLoadStore = new IndicesWriteLoadStore(threadPool, client, clusterSettings, settings)
        ) {
            final var receivedBulkRequestCount = new AtomicInteger();
            final Queue<ActionListener<BulkResponse>> pendingListeners = new ConcurrentLinkedQueue<>();
            client.setHandler((action, request, listener) -> {
                receivedBulkRequestCount.incrementAndGet();

                pendingListeners.add((ActionListener<BulkResponse>) listener);
            });

            for (int i = 1; i <= maxConcurrentRequests; i++) {
                final int currentRequest = i;
                indexRandomSamples(indicesWriteLoadStore, numberOfSamples);
                assertBusy(() -> assertThat(receivedBulkRequestCount.get(), equalTo(currentRequest)));
                assertThat(pendingListeners.size(), is(equalTo(currentRequest)));
            }

            assertThat(receivedBulkRequestCount.get(), equalTo(maxConcurrentRequests));
            assertThat(pendingListeners.size(), equalTo(maxConcurrentRequests));

            // Since there are maxConcurrentRequest sent the call to indexRandomSamples would block
            // the thread until some outstanding request is done, therefore we need to spin up a new
            // thread to avoid blocking the test thread.
            var thread = new Thread(() -> indexRandomSamples(indicesWriteLoadStore, numberOfSamples));
            thread.start();
            assertBusy(() -> assertThat(receivedBulkRequestCount.get(), equalTo(maxConcurrentRequests)));

            for (int i = 0; i < maxConcurrentRequests; i++) {
                final var listener = pendingListeners.poll();
                assertThat(listener, is(notNullValue()));
                listener.onResponse(getSuccessfulResponse(numberOfSamples));
            }

            assertBusy(() -> assertThat(receivedBulkRequestCount.get(), equalTo(maxConcurrentRequests + 1)));
            assertThat(pendingListeners.size(), is(equalTo(1)));

            final var listener = pendingListeners.poll();
            assertThat(listener, is(notNullValue()));
            listener.onResponse(getSuccessfulResponse(100));

            thread.join(TimeValue.timeValueSeconds(5).getMillis());
        }
    }

    public void testPartialFailuresAreLogged() throws Exception {
        final var numberOfSamples = randomIntBetween(2, 100);
        final var settings = Settings.builder().put(IndicesWriteLoadStore.MAX_DOCUMENTS_PER_BULK_SETTING.getKey(), numberOfSamples).build();
        try (
            var client = new MockClient(threadPool);
            var indicesWriteLoadStore = new IndicesWriteLoadStore(threadPool, client, clusterSettings, settings)
        ) {
            final var latch = new CountDownLatch(1);
            client.setHandler((action, request, listener) -> {
                BulkRequest bulkRequest = (BulkRequest) request;
                assertNotNull(listener);

                final var shardId = new ShardId("index", "uuid", 0);
                final var bulkResponse = new BulkResponse(
                    Stream.concat(
                        IntStream.range(0, 1)
                            .mapToObj(
                                i -> BulkItemResponse.success(
                                    i,
                                    DocWriteRequest.OpType.INDEX,
                                    new IndexResponse(shardId, randomAlphaOfLength(10), 1, 1, 1, true)
                                )
                            ),
                        IntStream.range(1, bulkRequest.numberOfActions())
                            .mapToObj(
                                i -> BulkItemResponse.failure(
                                    i,
                                    DocWriteRequest.OpType.INDEX,
                                    new BulkItemResponse.Failure("index", Integer.toString(i), new RuntimeException("Failure"))
                                )
                            )
                    ).toArray(BulkItemResponse[]::new),
                    randomLongBetween(100, 1000)
                );

                ((ActionListener<BulkResponse>) listener).onResponse(bulkResponse);

                latch.countDown();
            });

            final var indicesWriteLoadStoreLogger = LogManager.getLogger(IndicesWriteLoadStore.class);
            final var mockLogAppender = new MockLogAppender();
            mockLogAppender.start();
            try {
                mockLogAppender.addExpectation(
                    new MockLogAppender.SeenEventExpectation(
                        "expected message",
                        IndicesWriteLoadStore.class.getCanonicalName(),
                        Level.WARN,
                        "Failed to index some indices write load distributions: *"
                    )
                );
                Loggers.addAppender(indicesWriteLoadStoreLogger, mockLogAppender);

                indexRandomSamples(indicesWriteLoadStore, numberOfSamples);

                latch.await();

                mockLogAppender.assertAllExpectationsMatched();
            } finally {
                Loggers.removeAppender(indicesWriteLoadStoreLogger, mockLogAppender);
                mockLogAppender.stop();
            }
        }
    }

    public void testPendingSamplesAreFlushedOnClose() throws Exception {
        final var numberOfSamples = randomIntBetween(2, 100);
        final var settings = Settings.builder()
            .put(IndicesWriteLoadStore.MAX_DOCUMENTS_PER_BULK_SETTING.getKey(), numberOfSamples + 1)
            .build();
        final var receivedBulkRequestCount = new AtomicInteger();
        try (
            var client = new MockClient(threadPool);
            var indicesWriteLoadStore = new IndicesWriteLoadStore(threadPool, client, clusterSettings, settings)
        ) {
            client.setHandler((action, request, listener) -> {
                receivedBulkRequestCount.incrementAndGet();
                BulkRequest bulkRequest = (BulkRequest) request;

                int indexRequestCount = bulkRequest.numberOfActions();
                ((ActionListener<BulkResponse>) listener).onResponse(getSuccessfulResponse(indexRequestCount));
            });

            indexRandomSamples(indicesWriteLoadStore, numberOfSamples);

            assertThat(receivedBulkRequestCount.get(), is(equalTo(0)));
        }

        assertBusy(() -> assertThat(receivedBulkRequestCount.get(), is(equalTo(1))));
    }

    public void testMaxBulkSizeIsConfigurable() throws Exception {
        final var maxBulkSize = ByteSizeValue.ofKb(randomIntBetween(10, 100));
        final var averageDocumentSizeInBytes = getWriteLoadDistributionSerializedSize();
        final var maxDocumentsPerBulk = (int) Math.ceil((double) maxBulkSize.getBytes() / averageDocumentSizeInBytes);

        final var settings = Settings.builder().put(IndicesWriteLoadStore.MAX_BULK_SIZE_SETTING.getKey(), maxBulkSize).build();
        final var receivedBulkRequestCount = new AtomicInteger();
        try (
            var client = new MockClient(threadPool);
            var indicesWriteLoadStore = new IndicesWriteLoadStore(threadPool, client, clusterSettings, settings)
        ) {
            client.setHandler((action, request, listener) -> {
                receivedBulkRequestCount.incrementAndGet();
                BulkRequest bulkRequest = (BulkRequest) request;

                int indexRequestCount = bulkRequest.numberOfActions();

                assertThat(indexRequestCount, is(lessThanOrEqualTo(maxDocumentsPerBulk)));
                ((ActionListener<BulkResponse>) listener).onResponse(getSuccessfulResponse(indexRequestCount));
            });

            indexRandomSamples(indicesWriteLoadStore, maxDocumentsPerBulk / 2);

            // We're still below the max bulk size threshold
            assertThat(receivedBulkRequestCount.get(), is(equalTo(0)));

            indexRandomSamples(indicesWriteLoadStore, maxDocumentsPerBulk);

            assertThat(receivedBulkRequestCount.get(), is(equalTo(1)));
        }

        // Ensure that the remaining docs are flushed after closing the store
        assertThat(receivedBulkRequestCount.get(), is(equalTo(2)));
    }

    private void indexRandomSamples(IndicesWriteLoadStore indicesWriteLoadStore, int numberOfSamples) {
        int remainingSamples = numberOfSamples;
        while (remainingSamples > 0) {
            final int batchSamples = randomIntBetween(1, remainingSamples);
            final var shardWriteLoadDistributions = new ArrayList<ShardWriteLoadDistribution>(batchSamples);
            for (int i = 0; i < batchSamples; i++) {
                shardWriteLoadDistributions.add(getRandomWriteLoadDistribution());
            }
            indicesWriteLoadStore.putAsync(shardWriteLoadDistributions);
            remainingSamples -= batchSamples;
        }
    }

    private long getWriteLoadDistributionSerializedSize() throws IOException {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            getRandomWriteLoadDistribution().toXContent(builder, ToXContent.EMPTY_PARAMS);
            return BytesReference.bytes(builder).length();
        }
    }

    private ShardWriteLoadDistribution getRandomWriteLoadDistribution() {
        return new ShardWriteLoadDistribution(
            System.currentTimeMillis(),
            "parent",
            new ShardId("index", "uuid", 0),
            randomBoolean(),
            randomLoadDistribution(),
            randomLoadDistribution(),
            randomLoadDistribution()
        );
    }

    private LoadDistribution randomLoadDistribution() {
        return new LoadDistribution(
            randomDoubleBetween(0.0, 128.0, true),
            randomDoubleBetween(0.0, 128.0, true),
            randomDoubleBetween(0.0, 128.0, true),
            randomDoubleBetween(0.0, 128.0, true),
            randomDoubleBetween(0.0, 128.0, true)
        );
    }

    private BulkResponse getSuccessfulResponse(int numberOps) {
        final var shardId = new ShardId("index", "uuid", 0);
        return new BulkResponse(
            IntStream.range(0, numberOps)
                .mapToObj(
                    i -> BulkItemResponse.success(
                        i,
                        DocWriteRequest.OpType.INDEX,
                        new IndexResponse(shardId, randomAlphaOfLength(10), 1, 1, 1, true)
                    )
                )
                .toArray(BulkItemResponse[]::new),
            randomLongBetween(100, 1000)
        );
    }

    static class MockClient extends NoOpClient {

        private volatile TriConsumer<ActionType<?>, ActionRequest, ActionListener<?>> handler = (a, r, l) -> fail("handler not set");

        MockClient(ThreadPool threadPool) {
            super(threadPool);
        }

        @Override
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            try {
                handler.apply(action, request, listener);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }

        void setHandler(TriConsumer<ActionType<?>, ActionRequest, ActionListener<?>> handler) {
            this.handler = handler;
        }
    }
}
