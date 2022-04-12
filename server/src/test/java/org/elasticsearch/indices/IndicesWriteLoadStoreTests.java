/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

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
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.elasticsearch.indices.IndicesWriteLoadStore.INDICES_WRITE_LOAD_DATA_STREAM;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@SuppressWarnings("unchecked")
public class IndicesWriteLoadStoreTests extends ESTestCase {
    private static final Settings testDefaultSettings = Settings.builder()
        .put(IndicesWriteLoadStore.FLUSH_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(1))
        .build();

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
        final var settings = Settings.builder().put(testDefaultSettings).put(IndicesWriteLoadStore.ENABLED_SETTING.getKey(), false).build();
        final var latch = new CountDownLatch(1);
        try (
            var client = new MockClient(threadPool);
            var indicesWriteLoadStore = new IndicesWriteLoadStore(threadPool, client, clusterSettings, settings, () -> {
                latch.countDown();
                return true;
            })
        ) {
            client.setHandler((action, request, listener) -> { assert false : "unexpected call"; });

            indexRandomSamples(indicesWriteLoadStore, randomIntBetween(1, 100));

            assertFalse(latch.await(IndicesWriteLoadStore.FLUSH_INTERVAL_SETTING.get(settings).seconds(), TimeUnit.SECONDS));
        }
    }

    public void testIndicesWriteLoadSamplesAreStored() throws Exception {
        try (
            var client = new MockClient(threadPool);
            var indicesWriteLoadStore = new IndicesWriteLoadStore(threadPool, client, clusterSettings, testDefaultSettings)
        ) {
            final int numberOfSamples = randomIntBetween(1, 1000);
            final var calledTimes = new AtomicInteger();
            client.setHandler((action, request, listener) -> {
                calledTimes.incrementAndGet();
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

            assertBusy(() -> assertThat(calledTimes.get(), equalTo(1)));
        }
    }

    public void testNonRetryableErrorsAreNotRetried() throws Exception {
        try (
            var client = new MockClient(threadPool);
            var indicesWriteLoadStore = new IndicesWriteLoadStore(threadPool, client, clusterSettings, testDefaultSettings)
        ) {
            final var numberOfSamples = randomIntBetween(1, 100);
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
        final var maxRetries = randomIntBetween(1, 3);
        final var settings = Settings.builder()
            .put(testDefaultSettings)
            .put(IndicesWriteLoadStore.MAX_RETRIES_SETTING.getKey(), maxRetries)
            .build();
        try (
            var client = new MockClient(threadPool);
            var indicesWriteLoadStore = new IndicesWriteLoadStore(threadPool, client, clusterSettings, settings)
        ) {
            final var numberOfSamples = randomIntBetween(1, 100);
            final var calledTimes = new AtomicInteger();
            final var successfulWriteResponseFuture = PlainActionFuture.newFuture();
            client.setHandler((action, request, listener) -> {
                if (calledTimes.incrementAndGet() < maxRetries + 1) {
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
            assertBusy(() -> assertThat(calledTimes.get(), equalTo(maxRetries + 1)));
        }
    }

    public void testOnlyOneConcurrentRequestIsSent() throws Exception {
        final var flushWritesLatch = new CountDownLatch(2);
        try (
            var client = new MockClient(threadPool);
            var indicesWriteLoadStore = new IndicesWriteLoadStore(threadPool, client, clusterSettings, testDefaultSettings, () -> {
                flushWritesLatch.countDown();
                return true;
            })
        ) {

            final var calledTimes = new AtomicInteger();
            final Queue<ActionListener<BulkResponse>> pendingListeners = new ConcurrentLinkedQueue<>();
            client.setHandler((action, request, listener) -> {
                calledTimes.incrementAndGet();

                pendingListeners.add((ActionListener<BulkResponse>) listener);
            });

            {
                final int numberOfSamples = randomIntBetween(1, 100);
                indexRandomSamples(indicesWriteLoadStore, numberOfSamples);
                assertBusy(() -> assertThat(calledTimes.get(), equalTo(1)));
                assertThat(pendingListeners.size(), is(equalTo(1)));
            }

            {
                final int numberOfSamples = randomIntBetween(1, 100);
                indexRandomSamples(indicesWriteLoadStore, numberOfSamples);
                assertBusy(() -> assertThat(calledTimes.get(), equalTo(1)));
            }

            flushWritesLatch.await();

            {
                final var listener = pendingListeners.poll();
                assertThat(listener, is(notNullValue()));
                listener.onResponse(getSuccessfulResponse(100));
            }

            assertBusy(() -> assertThat(calledTimes.get(), equalTo(2)));

            {
                final var listener = pendingListeners.poll();
                assertThat(listener, is(notNullValue()));
                listener.onResponse(getSuccessfulResponse(100));
            }
        }
    }

    public void testPartialFailuresAreLogged() throws Exception {
        try (
            var client = new MockClient(threadPool);
            var indicesWriteLoadStore = new IndicesWriteLoadStore(threadPool, client, clusterSettings, testDefaultSettings)
        ) {
            final var numberOfSamples = randomIntBetween(2, 100);
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

    private void indexRandomSamples(IndicesWriteLoadStore indicesWriteLoadStore, int numberOfSamples) {
        int remainingSamples = numberOfSamples;
        while (remainingSamples > 0) {
            final int batchSamples = randomIntBetween(1, remainingSamples);
            final var shardWriteLoadDistributions = new ArrayList<ShardWriteLoadDistribution>(batchSamples);
            for (int i = 0; i < batchSamples; i++) {
                shardWriteLoadDistributions.add(
                    new ShardWriteLoadDistribution(
                        System.currentTimeMillis(),
                        "parent",
                        new ShardId("index", "uuid", 0),
                        randomBoolean(),
                        randomDouble(),
                        randomDouble(),
                        randomDouble(),
                        randomDouble(),
                        randomDouble(),
                        randomDouble(),
                        randomDouble(),
                        randomDouble(),
                        randomDouble()
                    )
                );
            }
            indicesWriteLoadStore.putAsync(shardWriteLoadDistributions);
            remainingSamples -= batchSamples;
        }
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
