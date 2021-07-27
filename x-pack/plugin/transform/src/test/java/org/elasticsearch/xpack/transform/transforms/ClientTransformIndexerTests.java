/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.ClosePointInTimeResponse;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.Client;
import org.elasticsearch.search.SearchContextMissingException;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.profile.SearchProfileShardResults;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ActionNotFoundTransportException;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.scheduler.SchedulerEngine;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpoint;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfigTests;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerPosition;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerStats;
import org.elasticsearch.xpack.core.transform.transforms.TransformProgress;
import org.elasticsearch.xpack.core.transform.transforms.persistence.TransformInternalIndexConstants;
import org.elasticsearch.xpack.transform.TransformServices;
import org.elasticsearch.xpack.transform.checkpoint.CheckpointProvider;
import org.elasticsearch.xpack.transform.checkpoint.TransformCheckpointService;
import org.elasticsearch.xpack.transform.notifications.TransformAuditor;
import org.elasticsearch.xpack.transform.persistence.IndexBasedTransformConfigManager;
import org.elasticsearch.xpack.transform.persistence.SeqNoPrimaryTermAndIndex;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ClientTransformIndexerTests extends ESTestCase {

    public void testAudiOnFinishFrequency() {
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.executor("generic")).thenReturn(mock(ExecutorService.class));

        ClientTransformIndexer indexer = new ClientTransformIndexer(
            mock(ThreadPool.class),
            new TransformServices(
                mock(IndexBasedTransformConfigManager.class),
                mock(TransformCheckpointService.class),
                mock(TransformAuditor.class),
                mock(SchedulerEngine.class)
            ),
            mock(CheckpointProvider.class),
            new AtomicReference<>(IndexerState.STOPPED),
            null,
            mock(Client.class),
            mock(TransformIndexerStats.class),
            mock(TransformConfig.class),
            null,
            new TransformCheckpoint("transform", Instant.now().toEpochMilli(), 0L, Collections.emptyMap(), Instant.now().toEpochMilli()),
            new TransformCheckpoint("transform", Instant.now().toEpochMilli(), 2L, Collections.emptyMap(), Instant.now().toEpochMilli()),
            new SeqNoPrimaryTermAndIndex(1, 1, TransformInternalIndexConstants.LATEST_INDEX_NAME),
            mock(TransformContext.class),
            false
        );

        List<Boolean> shouldAudit = IntStream.range(0, 100_000).boxed().map(indexer::shouldAuditOnFinish).collect(Collectors.toList());

        // Audit every checkpoint for the first 10
        assertTrue(shouldAudit.get(0));
        assertTrue(shouldAudit.get(1));
        assertTrue(shouldAudit.get(10));

        // Then audit every 10 while < 100
        assertFalse(shouldAudit.get(11));
        assertTrue(shouldAudit.get(20));
        assertFalse(shouldAudit.get(29));
        assertTrue(shouldAudit.get(30));
        assertFalse(shouldAudit.get(99));

        // Then audit every 100 < 1000
        assertTrue(shouldAudit.get(100));
        assertFalse(shouldAudit.get(109));
        assertFalse(shouldAudit.get(110));
        assertFalse(shouldAudit.get(199));

        // Then audit every 1000 for the rest of time
        assertFalse(shouldAudit.get(1999));
        assertFalse(shouldAudit.get(2199));
        assertTrue(shouldAudit.get(3000));
        assertTrue(shouldAudit.get(10_000));
        assertFalse(shouldAudit.get(10_999));
        assertTrue(shouldAudit.get(11_000));
        assertFalse(shouldAudit.get(11_001));
        assertFalse(shouldAudit.get(11_999));
    }

    public void testPitInjection() throws InterruptedException {
        TransformConfig config = TransformConfigTests.randomTransformConfig();

        try (PitMockClient client = new PitMockClient(getTestName(), true)) {
            MockClientTransformIndexer indexer = new MockClientTransformIndexer(
                mock(ThreadPool.class),
                new TransformServices(
                    mock(IndexBasedTransformConfigManager.class),
                    mock(TransformCheckpointService.class),
                    mock(TransformAuditor.class),
                    mock(SchedulerEngine.class)
                ),
                mock(CheckpointProvider.class),
                new AtomicReference<>(IndexerState.STOPPED),
                null,
                client,
                mock(TransformIndexerStats.class),
                config,
                Collections.emptyMap(),
                null,
                new TransformCheckpoint(
                    "transform",
                    Instant.now().toEpochMilli(),
                    0L,
                    Collections.emptyMap(),
                    Instant.now().toEpochMilli()
                ),
                new TransformCheckpoint(
                    "transform",
                    Instant.now().toEpochMilli(),
                    2L,
                    Collections.emptyMap(),
                    Instant.now().toEpochMilli()
                ),
                new SeqNoPrimaryTermAndIndex(1, 1, TransformInternalIndexConstants.LATEST_INDEX_NAME),
                mock(TransformContext.class),
                false
            );

            this.<SearchResponse>assertAsync(
                listener -> indexer.doNextSearch(0, listener),
                response -> { assertEquals("the_pit_id+", response.pointInTimeId()); }
            );

            assertEquals(1L, client.getPitContextCounter());

            indexer.afterFinishOrFailure();
            assertEquals(0L, client.getPitContextCounter());

            // check its not called again
            indexer.onStop();
            assertEquals(0L, client.getPitContextCounter());

            this.<SearchResponse>assertAsync(
                listener -> indexer.doNextSearch(0, listener),
                response -> { assertEquals("the_pit_id+", response.pointInTimeId()); }
            );

            this.<SearchResponse>assertAsync(
                listener -> indexer.doNextSearch(0, listener),
                response -> { assertEquals("the_pit_id++", response.pointInTimeId()); }
            );

            this.<SearchResponse>assertAsync(
                listener -> indexer.doNextSearch(0, listener),
                response -> { assertEquals("the_pit_id+++", response.pointInTimeId()); }
            );

            assertEquals(1L, client.getPitContextCounter());

            indexer.onStop();
            assertEquals(0L, client.getPitContextCounter());

            this.<SearchResponse>assertAsync(
                listener -> indexer.doNextSearch(0, listener),
                response -> { assertEquals("the_pit_id+", response.pointInTimeId()); }
            );

            this.<SearchResponse>assertAsync(
                listener -> indexer.doNextSearch(0, listener),
                response -> { assertEquals("the_pit_id++", response.pointInTimeId()); }
            );

            this.<SearchResponse>assertAsync(
                listener -> indexer.doNextSearch(0, listener),
                response -> { assertEquals("the_pit_id+++", response.pointInTimeId()); }
            );

            assertEquals(1L, client.getPitContextCounter());

            // throws search context missing:
            this.<SearchResponse>assertAsync(
                listener -> indexer.doNextSearch(0, listener),
                response -> { assertNull(response.pointInTimeId()); }
            );
        }
    }

    public void testPitInjectionIfPitNotSupported() throws InterruptedException {
        TransformConfig config = TransformConfigTests.randomTransformConfig();

        try (PitMockClient client = new PitMockClient(getTestName(), false)) {
            MockClientTransformIndexer indexer = new MockClientTransformIndexer(
                mock(ThreadPool.class),
                new TransformServices(
                    mock(IndexBasedTransformConfigManager.class),
                    mock(TransformCheckpointService.class),
                    mock(TransformAuditor.class),
                    mock(SchedulerEngine.class)
                ),
                mock(CheckpointProvider.class),
                new AtomicReference<>(IndexerState.STOPPED),
                null,
                client,
                mock(TransformIndexerStats.class),
                config,
                Collections.emptyMap(),
                null,
                new TransformCheckpoint(
                    "transform",
                    Instant.now().toEpochMilli(),
                    0L,
                    Collections.emptyMap(),
                    Instant.now().toEpochMilli()
                ),
                new TransformCheckpoint(
                    "transform",
                    Instant.now().toEpochMilli(),
                    2L,
                    Collections.emptyMap(),
                    Instant.now().toEpochMilli()
                ),
                new SeqNoPrimaryTermAndIndex(1, 1, TransformInternalIndexConstants.LATEST_INDEX_NAME),
                mock(TransformContext.class),
                false
            );

            this.<SearchResponse>assertAsync(
                listener -> indexer.doNextSearch(0, listener),
                response -> { assertNull(response.pointInTimeId()); }
            );

            assertEquals(0L, client.getPitContextCounter());

            indexer.afterFinishOrFailure();
            assertEquals(0L, client.getPitContextCounter());

            // check its not called again
            indexer.onStop();
            assertEquals(0L, client.getPitContextCounter());

            this.<SearchResponse>assertAsync(
                listener -> indexer.doNextSearch(0, listener),
                response -> { assertNull(response.pointInTimeId()); }
            );

            this.<SearchResponse>assertAsync(
                listener -> indexer.doNextSearch(0, listener),
                response -> { assertNull(response.pointInTimeId()); }
            );

            this.<SearchResponse>assertAsync(
                listener -> indexer.doNextSearch(0, listener),
                response -> { assertNull(response.pointInTimeId()); }
            );

            assertEquals(0L, client.getPitContextCounter());

            indexer.onStop();
            assertEquals(0L, client.getPitContextCounter());
        }
    }

    private static class MockClientTransformIndexer extends ClientTransformIndexer {

        MockClientTransformIndexer(
            ThreadPool threadPool,
            TransformServices transformServices,
            CheckpointProvider checkpointProvider,
            AtomicReference<IndexerState> initialState,
            TransformIndexerPosition initialPosition,
            Client client,
            TransformIndexerStats initialStats,
            TransformConfig transformConfig,
            Map<String, String> fieldMappings,
            TransformProgress transformProgress,
            TransformCheckpoint lastCheckpoint,
            TransformCheckpoint nextCheckpoint,
            SeqNoPrimaryTermAndIndex seqNoPrimaryTermAndIndex,
            TransformContext context,
            boolean shouldStopAtCheckpoint
        ) {
            super(
                threadPool,
                transformServices,
                checkpointProvider,
                initialState,
                initialPosition,
                client,
                initialStats,
                transformConfig,
                transformProgress,
                lastCheckpoint,
                nextCheckpoint,
                seqNoPrimaryTermAndIndex,
                context,
                shouldStopAtCheckpoint
            );
        }

        @Override
        protected SearchRequest buildSearchRequest() {
            return new SearchRequest().source(new SearchSourceBuilder());
        }
    }

    private static class PitMockClient extends NoOpClient {
        private final boolean pitSupported;
        private AtomicLong pitContextCounter = new AtomicLong();

        PitMockClient(String testName, boolean pitSupported) {
            super(testName);
            this.pitSupported = pitSupported;
        }

        public long getPitContextCounter() {
            return pitContextCounter.get();
        }

        @SuppressWarnings("unchecked")
        @Override
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            if (request instanceof OpenPointInTimeRequest) {
                if (pitSupported) {
                    pitContextCounter.incrementAndGet();
                    OpenPointInTimeResponse response = new OpenPointInTimeResponse("the_pit_id");
                    listener.onResponse((Response) response);
                } else {
                    listener.onFailure(new ActionNotFoundTransportException("_pit"));
                }
                return;
            } else if (request instanceof ClosePointInTimeRequest) {
                ClosePointInTimeResponse response = new ClosePointInTimeResponse(true, 1);
                assert pitContextCounter.get() > 0;
                pitContextCounter.decrementAndGet();
                listener.onResponse((Response) response);
                return;
            } else if (request instanceof SearchRequest) {
                SearchRequest searchRequest = (SearchRequest) request;

                // throw search context missing for the 4th run
                if (searchRequest.pointInTimeBuilder() != null
                    && "the_pit_id+++".equals(searchRequest.pointInTimeBuilder().getEncodedId())) {
                    listener.onFailure(new SearchContextMissingException(new ShardSearchContextId("sc_missing", 42)));
                } else {
                    SearchResponse response = new SearchResponse(
                        new InternalSearchResponse(
                            new SearchHits(new SearchHit[] { new SearchHit(1) }, new TotalHits(1L, TotalHits.Relation.EQUAL_TO), 1.0f),
                            // Simulate completely null aggs
                            null,
                            new Suggest(Collections.emptyList()),
                            new SearchProfileShardResults(Collections.emptyMap()),
                            false,
                            false,
                            1
                        ),
                        null,
                        1,
                        1,
                        0,
                        0,
                        ShardSearchFailure.EMPTY_ARRAY,
                        SearchResponse.Clusters.EMPTY,
                        // copy the pit from the request
                        searchRequest.pointInTimeBuilder() != null ? searchRequest.pointInTimeBuilder().getEncodedId() + "+" : null
                    );
                    listener.onResponse((Response) response);

                }
                return;
            }

            super.doExecute(action, request, listener);
        }
    }

    private <T> void assertAsync(Consumer<ActionListener<T>> function, Consumer<T> furtherTests) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean listenerCalled = new AtomicBoolean(false);

        LatchedActionListener<T> listener = new LatchedActionListener<>(ActionListener.wrap(r -> {
            assertTrue("listener called more than once", listenerCalled.compareAndSet(false, true));
            furtherTests.accept(r);
        }, e -> {
            assertTrue("listener called more than once", listenerCalled.compareAndSet(false, true));
            fail("got unexpected exception: " + e);
        }), latch);

        function.accept(listener);
        assertTrue("timed out after 5s", latch.await(5, TimeUnit.SECONDS));
    }
}
