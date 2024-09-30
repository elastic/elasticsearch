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
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.search.SearchContextMissingException;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.profile.SearchProfileResults;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ActionNotFoundTransportException;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.transform.transforms.SettingsConfig;
import org.elasticsearch.xpack.core.transform.transforms.SourceConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpoint;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfigTests;
import org.elasticsearch.xpack.core.transform.transforms.TransformEffectiveSettings;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerPosition;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerStats;
import org.elasticsearch.xpack.core.transform.transforms.TransformProgress;
import org.elasticsearch.xpack.core.transform.transforms.persistence.TransformInternalIndexConstants;
import org.elasticsearch.xpack.transform.TransformExtension;
import org.elasticsearch.xpack.transform.TransformNode;
import org.elasticsearch.xpack.transform.TransformServices;
import org.elasticsearch.xpack.transform.checkpoint.CheckpointProvider;
import org.elasticsearch.xpack.transform.checkpoint.TransformCheckpointService;
import org.elasticsearch.xpack.transform.notifications.TransformAuditor;
import org.elasticsearch.xpack.transform.persistence.IndexBasedTransformConfigManager;
import org.elasticsearch.xpack.transform.persistence.SeqNoPrimaryTermAndIndex;
import org.elasticsearch.xpack.transform.transforms.scheduling.TransformScheduler;

import java.time.Clock;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ClientTransformIndexerTests extends ESTestCase {

    public void testAuditOnFinishFrequency() {
        ClientTransformIndexer indexer = createTestIndexer();
        List<Boolean> shouldAudit = IntStream.range(0, 100_000).boxed().map(indexer::shouldAuditOnFinish).toList();

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

    public void testDoSearchGivenNoIndices() {
        ClientTransformIndexer indexer = createTestIndexer();
        SearchRequest searchRequest = new SearchRequest(new String[0]);
        Tuple<String, SearchRequest> namedSearchRequest = new Tuple<>("test", searchRequest);
        indexer.doSearch(
            namedSearchRequest,
            // A search of zero indices should return null rather than attempt to search all indices
            ActionTestUtils.assertNoFailureListener(ESTestCase::assertNull)
        );
    }

    public void testPitInjection() throws InterruptedException {
        // pit must be enabled, otherwise take a random config
        TransformConfig config = new TransformConfig.Builder(TransformConfigTests.randomTransformConfig()).setSettings(
            new SettingsConfig.Builder().setUsePit(true).build()
        ).build();

        try (var threadPool = createThreadPool()) {
            final var client = new PitMockClient(threadPool, true);
            MockClientTransformIndexer indexer = new MockClientTransformIndexer(
                mock(ThreadPool.class),
                mock(ClusterService.class),
                mock(IndexNameExpressionResolver.class),
                mock(TransformExtension.class),
                new TransformServices(
                    mock(IndexBasedTransformConfigManager.class),
                    mock(TransformCheckpointService.class),
                    mock(TransformAuditor.class),
                    new TransformScheduler(Clock.systemUTC(), mock(ThreadPool.class), Settings.EMPTY, TimeValue.ZERO),
                    mock(TransformNode.class)
                ),
                mock(CheckpointProvider.class),
                new AtomicReference<>(IndexerState.STOPPED),
                null,
                new ParentTaskAssigningClient(client, new TaskId("dummy-node:123456")),
                mock(TransformIndexerStats.class),
                config,
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

            this.<SearchResponse>assertAsync(listener -> indexer.doNextSearch(0, listener), response -> {
                assertEquals(new BytesArray("the_pit_id+"), response.pointInTimeId());
            });

            assertEquals(1L, client.getPitContextCounter());

            indexer.afterFinishOrFailure();
            assertEquals(0L, client.getPitContextCounter());

            // check its not called again
            indexer.onStop();
            assertEquals(0L, client.getPitContextCounter());

            this.<SearchResponse>assertAsync(listener -> indexer.doNextSearch(0, listener), response -> {
                assertEquals(new BytesArray("the_pit_id+"), response.pointInTimeId());
            });

            this.<SearchResponse>assertAsync(listener -> indexer.doNextSearch(0, listener), response -> {
                assertEquals(new BytesArray("the_pit_id++"), response.pointInTimeId());
            });

            this.<SearchResponse>assertAsync(listener -> indexer.doNextSearch(0, listener), response -> {
                assertEquals(new BytesArray("the_pit_id+++"), response.pointInTimeId());
            });

            assertEquals(1L, client.getPitContextCounter());

            indexer.onStop();
            assertEquals(0L, client.getPitContextCounter());

            this.<SearchResponse>assertAsync(listener -> indexer.doNextSearch(0, listener), response -> {
                assertEquals(new BytesArray("the_pit_id+"), response.pointInTimeId());
            });

            this.<SearchResponse>assertAsync(listener -> indexer.doNextSearch(0, listener), response -> {
                assertEquals(new BytesArray("the_pit_id++"), response.pointInTimeId());
            });

            this.<SearchResponse>assertAsync(listener -> indexer.doNextSearch(0, listener), response -> {
                assertEquals(new BytesArray("the_pit_id+++"), response.pointInTimeId());
            });

            assertEquals(1L, client.getPitContextCounter());

            // throws search context missing:
            this.<SearchResponse>assertAsync(
                listener -> indexer.doNextSearch(0, listener),
                response -> { assertNull(response.pointInTimeId()); }
            );
        }
    }

    public void testPitInjectionIfPitNotSupported() throws InterruptedException {
        // pit must be enabled, otherwise take a random config
        TransformConfig config = new TransformConfig.Builder(TransformConfigTests.randomTransformConfig()).setSettings(
            new SettingsConfig.Builder().setUsePit(true).build()
        ).build();

        try (var threadPool = createThreadPool()) {
            final var client = new PitMockClient(threadPool, false);
            MockClientTransformIndexer indexer = new MockClientTransformIndexer(
                mock(ThreadPool.class),
                mock(ClusterService.class),
                mock(IndexNameExpressionResolver.class),
                mock(TransformExtension.class),
                new TransformServices(
                    mock(IndexBasedTransformConfigManager.class),
                    mock(TransformCheckpointService.class),
                    mock(TransformAuditor.class),
                    new TransformScheduler(Clock.systemUTC(), mock(ThreadPool.class), Settings.EMPTY, TimeValue.ZERO),
                    mock(TransformNode.class)
                ),
                mock(CheckpointProvider.class),
                new AtomicReference<>(IndexerState.STOPPED),
                null,
                new ParentTaskAssigningClient(client, new TaskId("dummy-node:123456")),
                mock(TransformIndexerStats.class),
                config,
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

    public void testDisablePit() throws InterruptedException {
        TransformConfig.Builder configBuilder = new TransformConfig.Builder(TransformConfigTests.randomTransformConfig());
        TransformConfig config = configBuilder.build();

        boolean pitEnabled = TransformEffectiveSettings.isPitDisabled(config.getSettings()) == false;

        try (var threadPool = createThreadPool()) {
            final var client = new PitMockClient(threadPool, true);
            MockClientTransformIndexer indexer = new MockClientTransformIndexer(
                mock(ThreadPool.class),
                mock(ClusterService.class),
                mock(IndexNameExpressionResolver.class),
                mock(TransformExtension.class),
                new TransformServices(
                    mock(IndexBasedTransformConfigManager.class),
                    mock(TransformCheckpointService.class),
                    mock(TransformAuditor.class),
                    new TransformScheduler(Clock.systemUTC(), mock(ThreadPool.class), Settings.EMPTY, TimeValue.ZERO),
                    mock(TransformNode.class)
                ),
                mock(CheckpointProvider.class),
                new AtomicReference<>(IndexerState.STOPPED),
                null,
                new ParentTaskAssigningClient(client, new TaskId("dummy-node:123456")),
                mock(TransformIndexerStats.class),
                config,
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

            this.<SearchResponse>assertAsync(listener -> indexer.doNextSearch(0, listener), response -> {
                if (pitEnabled) {
                    assertEquals(new BytesArray("the_pit_id+"), response.pointInTimeId());
                } else {
                    assertNull(response.pointInTimeId());
                }
            });

            // reverse the setting
            indexer.applyNewSettings(new SettingsConfig.Builder().setUsePit(pitEnabled == false).build());

            this.<SearchResponse>assertAsync(listener -> indexer.doNextSearch(0, listener), response -> {
                if (pitEnabled) {
                    assertNull(response.pointInTimeId());
                } else {
                    assertEquals(new BytesArray("the_pit_id+"), response.pointInTimeId());
                }
            });
        }
    }

    public void testDisablePitWhenThereIsRemoteIndexInSource() throws InterruptedException {
        TransformConfig config = new TransformConfig.Builder(TransformConfigTests.randomTransformConfig())
            // Remote index is configured within source
            .setSource(new SourceConfig("remote-cluster:remote-index"))
            .build();
        boolean pitEnabled = TransformEffectiveSettings.isPitDisabled(config.getSettings()) == false;

        try (var threadPool = createThreadPool()) {
            final var client = new PitMockClient(threadPool, true);
            MockClientTransformIndexer indexer = new MockClientTransformIndexer(
                mock(ThreadPool.class),
                mock(ClusterService.class),
                mock(IndexNameExpressionResolver.class),
                mock(TransformExtension.class),
                new TransformServices(
                    mock(IndexBasedTransformConfigManager.class),
                    mock(TransformCheckpointService.class),
                    mock(TransformAuditor.class),
                    new TransformScheduler(Clock.systemUTC(), mock(ThreadPool.class), Settings.EMPTY, TimeValue.ZERO),
                    mock(TransformNode.class)
                ),
                mock(CheckpointProvider.class),
                new AtomicReference<>(IndexerState.STOPPED),
                null,
                new ParentTaskAssigningClient(client, new TaskId("dummy-node:123456")),
                mock(TransformIndexerStats.class),
                config,
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

            // Because remote index is configured within source, we expect PIT *not* being used regardless the transform settings
            this.<SearchResponse>assertAsync(
                listener -> indexer.doNextSearch(0, listener),
                response -> assertNull(response.pointInTimeId())
            );

            // reverse the setting
            indexer.applyNewSettings(new SettingsConfig.Builder().setUsePit(pitEnabled == false).build());

            // Because remote index is configured within source, we expect PIT *not* being used regardless the transform settings
            this.<SearchResponse>assertAsync(
                listener -> indexer.doNextSearch(0, listener),
                response -> assertNull(response.pointInTimeId())
            );
        }
    }

    public void testHandlePitIndexNotFound() throws InterruptedException {
        // simulate a deleted index due to ILM
        try (var threadPool = createThreadPool()) {
            final var client = new PitMockClient(threadPool, true);
            ClientTransformIndexer indexer = createTestIndexer(new ParentTaskAssigningClient(client, new TaskId("dummy-node:123456")));
            SearchRequest searchRequest = new SearchRequest("deleted-index").source(
                new SearchSourceBuilder().pointInTimeBuilder(new PointInTimeBuilder(new BytesArray("the_pit_id_on_deleted_index")))
            );
            Tuple<String, SearchRequest> namedSearchRequest = new Tuple<>("test-handle-pit-index-not-found", searchRequest);
            this.<SearchResponse>assertAsync(listener -> indexer.doSearch(namedSearchRequest, listener), response -> {
                // if the pit got deleted, we know it retried
                assertNull(response.pointInTimeId());
            });
        }

        // simulate a deleted index that is essential, search must fail (after a retry without pit)
        try (var threadPool = createThreadPool()) {
            final var client = new PitMockClient(threadPool, true);
            ClientTransformIndexer indexer = createTestIndexer(new ParentTaskAssigningClient(client, new TaskId("dummy-node:123456")));
            SearchRequest searchRequest = new SearchRequest("essential-deleted-index").source(
                new SearchSourceBuilder().pointInTimeBuilder(new PointInTimeBuilder(new BytesArray("the_pit_id_essential-deleted-index")))
            );
            Tuple<String, SearchRequest> namedSearchRequest = new Tuple<>("test-handle-pit-index-not-found", searchRequest);
            indexer.doSearch(namedSearchRequest, ActionListener.wrap(r -> fail("expected a failure, got response"), e -> {
                assertTrue(e instanceof IndexNotFoundException);
                assertEquals("no such index [essential-deleted-index]", e.getMessage());
            }));
        }
    }

    private static class MockClientTransformIndexer extends ClientTransformIndexer {

        MockClientTransformIndexer(
            ThreadPool threadPool,
            ClusterService clusterService,
            IndexNameExpressionResolver indexNameExpressionResolver,
            TransformExtension transformExtension,
            TransformServices transformServices,
            CheckpointProvider checkpointProvider,
            AtomicReference<IndexerState> initialState,
            TransformIndexerPosition initialPosition,
            ParentTaskAssigningClient client,
            TransformIndexerStats initialStats,
            TransformConfig transformConfig,
            TransformProgress transformProgress,
            TransformCheckpoint lastCheckpoint,
            TransformCheckpoint nextCheckpoint,
            SeqNoPrimaryTermAndIndex seqNoPrimaryTermAndIndex,
            TransformContext context,
            boolean shouldStopAtCheckpoint
        ) {
            super(
                threadPool,
                clusterService,
                indexNameExpressionResolver,
                transformExtension,
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
        protected Tuple<String, SearchRequest> buildSearchRequest() {
            return new Tuple<>("mock", new SearchRequest("source_index").source(new SearchSourceBuilder()));
        }
    }

    private static class PitMockClient extends NoOpClient {
        private final boolean pitSupported;
        private AtomicLong pitContextCounter = new AtomicLong();

        PitMockClient(ThreadPool threadPool, boolean pitSupported) {
            super(threadPool);
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
                    OpenPointInTimeResponse response = new OpenPointInTimeResponse(new BytesArray("the_pit_id"), 1, 1, 0, 0);
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
            } else if (request instanceof SearchRequest searchRequest) {
                // if pit is used and deleted-index is given throw index not found
                if (searchRequest.pointInTimeBuilder() != null
                    && searchRequest.pointInTimeBuilder().getEncodedId().equals(new BytesArray("the_pit_id_on_deleted_index"))) {
                    listener.onFailure(new IndexNotFoundException("deleted-index"));
                    return;
                }

                if ((searchRequest.pointInTimeBuilder() != null
                    && searchRequest.pointInTimeBuilder().getEncodedId().equals(new BytesArray("the_pit_id_essential-deleted-index")))
                    || (searchRequest.indices().length > 0 && searchRequest.indices()[0].equals("essential-deleted-index"))) {
                    listener.onFailure(new IndexNotFoundException("essential-deleted-index"));
                    return;
                }

                // throw search context missing for the 4th run
                if (searchRequest.pointInTimeBuilder() != null
                    && new BytesArray("the_pit_id+++").equals(searchRequest.pointInTimeBuilder().getEncodedId())) {
                    listener.onFailure(new SearchContextMissingException(new ShardSearchContextId("sc_missing", 42)));
                } else {
                    ActionListener.respondAndRelease(
                        listener,
                        (Response) new SearchResponse(
                            SearchHits.unpooled(
                                new SearchHit[] { SearchHit.unpooled(1) },
                                new TotalHits(1L, TotalHits.Relation.EQUAL_TO),
                                1.0f
                            ),
                            // Simulate completely null aggs
                            null,
                            new Suggest(Collections.emptyList()),
                            false,
                            false,
                            new SearchProfileResults(Collections.emptyMap()),
                            1,
                            null,
                            1,
                            1,
                            0,
                            0,
                            ShardSearchFailure.EMPTY_ARRAY,
                            SearchResponse.Clusters.EMPTY,
                            // copy the pit from the request
                            searchRequest.pointInTimeBuilder() != null
                                ? CompositeBytesReference.of(searchRequest.pointInTimeBuilder().getEncodedId(), new BytesArray("+"))
                                : null
                        )
                    );

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

    private ClientTransformIndexer createTestIndexer() {
        return createTestIndexer(null);
    }

    private ClientTransformIndexer createTestIndexer(ParentTaskAssigningClient client) {
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.executor("generic")).thenReturn(mock(ExecutorService.class));

        return new ClientTransformIndexer(
            mock(ThreadPool.class),
            mock(ClusterService.class),
            mock(IndexNameExpressionResolver.class),
            mock(TransformExtension.class),
            new TransformServices(
                mock(IndexBasedTransformConfigManager.class),
                mock(TransformCheckpointService.class),
                mock(TransformAuditor.class),
                new TransformScheduler(Clock.systemUTC(), mock(ThreadPool.class), Settings.EMPTY, TimeValue.ZERO),
                mock(TransformNode.class)
            ),
            mock(CheckpointProvider.class),
            new AtomicReference<>(IndexerState.STOPPED),
            null,
            client == null ? mock(ParentTaskAssigningClient.class) : client,
            mock(TransformIndexerStats.class),
            TransformConfigTests.randomTransformConfig(),
            null,
            new TransformCheckpoint("transform", Instant.now().toEpochMilli(), 0L, Collections.emptyMap(), Instant.now().toEpochMilli()),
            new TransformCheckpoint("transform", Instant.now().toEpochMilli(), 2L, Collections.emptyMap(), Instant.now().toEpochMilli()),
            new SeqNoPrimaryTermAndIndex(1, 1, TransformInternalIndexConstants.LATEST_INDEX_NAME),
            mock(TransformContext.class),
            false
        );
    }
}
