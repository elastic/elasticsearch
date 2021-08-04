/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.Client;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.BulkByScrollTask;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.profile.SearchProfileShardResults;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.indexing.IterationResult;
import org.elasticsearch.xpack.core.scheduler.SchedulerEngine;
import org.elasticsearch.xpack.core.transform.transforms.TimeRetentionPolicyConfigTests;
import org.elasticsearch.xpack.core.transform.transforms.TimeSyncConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpoint;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerPosition;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerStats;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskState;
import org.elasticsearch.xpack.transform.TransformServices;
import org.elasticsearch.xpack.transform.checkpoint.CheckpointProvider;
import org.elasticsearch.xpack.transform.checkpoint.MockTimebasedCheckpointProvider;
import org.elasticsearch.xpack.transform.checkpoint.TransformCheckpointService;
import org.elasticsearch.xpack.transform.notifications.MockTransformAuditor;
import org.elasticsearch.xpack.transform.notifications.TransformAuditor;
import org.elasticsearch.xpack.transform.persistence.InMemoryTransformConfigManager;
import org.elasticsearch.xpack.transform.persistence.TransformConfigManager;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.core.transform.transforms.DestConfigTests.randomDestConfig;
import static org.elasticsearch.xpack.core.transform.transforms.SourceConfigTests.randomSourceConfig;
import static org.elasticsearch.xpack.core.transform.transforms.pivot.PivotConfigTests.randomPivotConfig;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.oneOf;
import static org.mockito.Mockito.mock;

public class TransformIndexerTests extends ESTestCase {

    private static final SearchResponse ONE_HIT_SEARCH_RESPONSE = new SearchResponse(
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
        "",
        1,
        1,
        0,
        0,
        ShardSearchFailure.EMPTY_ARRAY,
        SearchResponse.Clusters.EMPTY
    );

    private Client client;
    private ThreadPool threadPool;
    private TransformAuditor auditor;
    private TransformConfigManager transformConfigManager;

    class MockedTransformIndexer extends TransformIndexer {

        private final ThreadPool threadPool;

        private int deleteByQueryCallCount = 0;
        // used for synchronizing with the test
        private CountDownLatch searchLatch;
        private CountDownLatch doProcessLatch;
        private CountDownLatch doSaveStateLatch;

        private AtomicBoolean saveStateInProgress = new AtomicBoolean(false);

        // how many loops to execute until reporting done
        private int numberOfLoops;

        MockedTransformIndexer(
            int numberOfLoops,
            ThreadPool threadPool,
            TransformServices transformServices,
            CheckpointProvider checkpointProvider,
            TransformConfig transformConfig,
            AtomicReference<IndexerState> initialState,
            TransformIndexerPosition initialPosition,
            TransformIndexerStats jobStats,
            TransformContext context
        ) {
            super(
                threadPool,
                transformServices,
                checkpointProvider,
                transformConfig,
                initialState,
                initialPosition,
                jobStats,
                /* TransformProgress */ null,
                TransformCheckpoint.EMPTY,
                TransformCheckpoint.EMPTY,
                context
            );
            this.threadPool = threadPool;
            this.numberOfLoops = numberOfLoops;
        }

        public void initialize() {
            this.initializeFunction();
        }

        public CountDownLatch createAwaitForSearchLatch(int count) {
            return searchLatch = new CountDownLatch(count);
        }

        public CountDownLatch createCountDownOnResponseLatch(int count) {
            return doProcessLatch = new CountDownLatch(count);
        }

        public CountDownLatch createAwaitForDoSaveStateLatch(int count) {
            return doSaveStateLatch = new CountDownLatch(count);
        }

        @Override
        void doGetInitialProgress(SearchRequest request, ActionListener<SearchResponse> responseListener) {
            responseListener.onResponse(ONE_HIT_SEARCH_RESPONSE);
        }

        @Override
        void doDeleteByQuery(DeleteByQueryRequest deleteByQueryRequest, ActionListener<BulkByScrollResponse> responseListener) {
            deleteByQueryCallCount++;
            try {
                // yes, I know, a sleep, how dare you, this is to test stats collection and this requires a resolution of a millisecond
                Thread.sleep(1);
            } catch (InterruptedException e) {
                fail("unexpected exception during sleep: " + e);
            }
            responseListener.onResponse(
                new BulkByScrollResponse(
                    TimeValue.ZERO,
                    new BulkByScrollTask.Status(
                        0,
                        0L,
                        0L,
                        0L,
                        /*deleted*/ 42L,
                        0,
                        0L,
                        0L,
                        0L,
                        0L,
                        TimeValue.ZERO,
                        0.0f,
                        null,
                        TimeValue.ZERO
                    ),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    false
                )
            );
        }

        @Override
        void refreshDestinationIndex(ActionListener<RefreshResponse> responseListener) {
            responseListener.onResponse(new RefreshResponse(1, 1, 0, Collections.emptyList()));
        }

        @Override
        protected void doNextSearch(long waitTimeInNanos, ActionListener<SearchResponse> nextPhase) {
            if (searchLatch != null) {
                try {
                    searchLatch.await();
                } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
            }
            threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> nextPhase.onResponse(ONE_HIT_SEARCH_RESPONSE));
        }

        @Override
        protected void doNextBulk(BulkRequest request, ActionListener<BulkResponse> nextPhase) {
            if (doProcessLatch != null) {
                doProcessLatch.countDown();
            }
            threadPool.executor(ThreadPool.Names.GENERIC)
                .execute(() -> nextPhase.onResponse(new BulkResponse(new BulkItemResponse[0], 100)));
        }

        @Override
        protected void doSaveState(IndexerState state, TransformIndexerPosition position, Runnable next) {
            // assert that the indexer does not call doSaveState again, while it is still saving state
            // this is only useful together with the doSaveStateLatch
            assertTrue("doSaveState called again while still in progress", saveStateInProgress.compareAndSet(false, true));
            if (doSaveStateLatch != null) {
                try {
                    doSaveStateLatch.await();

                } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
            }

            assert state == IndexerState.STARTED || state == IndexerState.INDEXING || state == IndexerState.STOPPED;

            assertTrue(saveStateInProgress.compareAndSet(true, false));
            next.run();
        }

        @Override
        protected IterationResult<TransformIndexerPosition> doProcess(SearchResponse searchResponse) {
            assert numberOfLoops > 0;
            --numberOfLoops;
            // pretend that we processed 10k documents for each call
            getStats().incrementNumDocuments(10_000);
            return new IterationResult<>(
                Stream.of(new IndexRequest()),
                new TransformIndexerPosition(null, null),
                numberOfLoops == 0
            );
        }

        @Override
        void doGetFieldMappings(ActionListener<Map<String, String>> fieldMappingsListener) {
            fieldMappingsListener.onResponse(Collections.emptyMap());
        }

        public boolean waitingForNextSearch() {
            return super.getScheduledNextSearch() != null;
        }

        public int getDeleteByQueryCallCount() {
            return deleteByQueryCallCount;
        }
    }

    @Before
    public void setUpMocks() {
        auditor = MockTransformAuditor.createMockAuditor();
        transformConfigManager = new InMemoryTransformConfigManager();
        client = new NoOpClient(getTestName());
        threadPool = new TestThreadPool(ThreadPool.Names.GENERIC);
    }

    @After
    public void tearDownClient() {
        client.close();
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
    }

    public void testRetentionPolicyExecution() throws Exception {
        TransformConfig config = new TransformConfig(
            randomAlphaOfLength(10),
            randomSourceConfig(),
            randomDestConfig(),
            null,
            new TimeSyncConfig("timestamp", TimeValue.timeValueSeconds(1)),
            null,
            randomPivotConfig(),
            null,
            randomBoolean() ? null : randomAlphaOfLengthBetween(1, 1000),
            null,
            TimeRetentionPolicyConfigTests.randomTimeRetentionPolicyConfig(),
            null,
            null
        );
        AtomicReference<IndexerState> state = new AtomicReference<>(IndexerState.STARTED);
        {
            TransformContext context = new TransformContext(TransformTaskState.STARTED, "", 0, mock(TransformContext.Listener.class));
            final MockedTransformIndexer indexer = createMockIndexer(
                10,
                config,
                state,
                null,
                threadPool,
                auditor,
                new TransformIndexerStats(),
                context
            );

            indexer.start();
            assertTrue(indexer.maybeTriggerAsyncJob(System.currentTimeMillis()));
            assertThat(indexer.getState(), oneOf(IndexerState.INDEXING, IndexerState.STARTED));

            assertBusy(() -> assertEquals(1L, indexer.getLastCheckpoint().getCheckpoint()), 5, TimeUnit.SECONDS);

            // delete by query has been executed
            assertEquals(1, indexer.getDeleteByQueryCallCount());
            assertEquals(42L, indexer.getStats().getNumDeletedDocuments());
            assertThat(indexer.getStats().getDeleteTime(), greaterThan(0L));
        }

        // test without retention
        config = new TransformConfig(
            randomAlphaOfLength(10),
            randomSourceConfig(),
            randomDestConfig(),
            null,
            new TimeSyncConfig("timestamp", TimeValue.timeValueSeconds(1)),
            null,
            randomPivotConfig(),
            null,
            randomBoolean() ? null : randomAlphaOfLengthBetween(1, 1000),
            null,
            null,
            null,
            null
        );

        state = new AtomicReference<>(IndexerState.STARTED);
        {
            TransformContext context = new TransformContext(TransformTaskState.STARTED, "", 0, mock(TransformContext.Listener.class));
            final MockedTransformIndexer indexer = createMockIndexer(
                10,
                config,
                state,
                null,
                threadPool,
                auditor,
                new TransformIndexerStats(),
                context
            );

            indexer.start();
            assertTrue(indexer.maybeTriggerAsyncJob(System.currentTimeMillis()));
            assertThat(indexer.getState(), oneOf(IndexerState.INDEXING, IndexerState.STARTED));

            assertBusy(() -> assertEquals(1L, indexer.getLastCheckpoint().getCheckpoint()), 5, TimeUnit.SECONDS);

            // delete by query has _not_ been executed
            assertEquals(0, indexer.getDeleteByQueryCallCount());
            assertEquals(0L, indexer.getStats().getNumDeletedDocuments());
            assertEquals(0L, indexer.getStats().getDeleteTime());
        }
    }

    /**
     * This test ensures correct handling of async behavior during indexer shutdown
     *
     * Indexer shutdown is not atomic: 1st the state is set back to e.g. STARTED, afterwards state is stored.
     * State is stored async and is IO based, therefore it can take time until this is done.
     *
     * Between setting the state and storing it, some race condition occurred, this test acts
     * as regression test.
     */
    public void testInterActionWhileIndexerShutsdown() throws Exception {
        TransformConfig config = new TransformConfig(
            randomAlphaOfLength(10),
            randomSourceConfig(),
            randomDestConfig(),
            null,
            new TimeSyncConfig("timestamp", TimeValue.timeValueSeconds(1)),
            null,
            randomPivotConfig(),
            null,
            randomBoolean() ? null : randomAlphaOfLengthBetween(1, 1000),
            null,
            null,
            null,
            null
        );
        AtomicReference<IndexerState> state = new AtomicReference<>(IndexerState.STARTED);

        TransformContext context = new TransformContext(TransformTaskState.STARTED, "", 0, mock(TransformContext.Listener.class));
        final MockedTransformIndexer indexer = createMockIndexer(
            5,
            config,
            state,
            null,
            threadPool,
            auditor,
            new TransformIndexerStats(),
            context
        );

        // add a latch at doSaveState
        CountDownLatch saveStateLatch = indexer.createAwaitForDoSaveStateLatch(1);

        indexer.start();
        assertTrue(indexer.maybeTriggerAsyncJob(System.currentTimeMillis()));
        assertEquals(indexer.getState(), IndexerState.INDEXING);

        assertBusy(() -> assertEquals(IndexerState.STARTED, indexer.getState()), 5, TimeUnit.SECONDS);

        // the indexer thread is shutting down, the trigger should be ignored
        assertFalse(indexer.maybeTriggerAsyncJob(System.currentTimeMillis()));
        this.<Void>assertAsync(listener -> setStopAtCheckpoint(indexer, true, listener), v -> {});
        saveStateLatch.countDown();

        // after the indexer has shutdown, it should check for stop at checkpoint and shutdown
        assertBusy(() -> assertEquals(IndexerState.STOPPED, indexer.getState()), 5, TimeUnit.SECONDS);
    }

    private MockedTransformIndexer createMockIndexer(
        int numberOfLoops,
        TransformConfig config,
        AtomicReference<IndexerState> state,
        Consumer<String> failureConsumer,
        ThreadPool threadPool,
        TransformAuditor auditor,
        TransformIndexerStats jobStats,
        TransformContext context
    ) {
        CheckpointProvider checkpointProvider = new MockTimebasedCheckpointProvider(config);
        transformConfigManager.putTransformConfiguration(config, ActionListener.wrap(r -> {}, e -> {}));
        TransformServices transformServices = new TransformServices(
            transformConfigManager,
            mock(TransformCheckpointService.class),
            auditor,
            mock(SchedulerEngine.class)
        );

        MockedTransformIndexer indexer = new MockedTransformIndexer(
            numberOfLoops,
            threadPool,
            transformServices,
            checkpointProvider,
            config,
            state,
            null,
            jobStats,
            context
        );

        indexer.initialize();
        return indexer;
    }

    private void setStopAtCheckpoint(
        TransformIndexer indexer,
        boolean shouldStopAtCheckpoint,
        ActionListener<Void> shouldStopAtCheckpointListener
    ) {
        // we need to simulate that this is called from the task, which offloads it to the generic threadpool
        CountDownLatch latch = new CountDownLatch(1);
        threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
            indexer.setStopAtCheckpoint(shouldStopAtCheckpoint, shouldStopAtCheckpointListener);
            latch.countDown();
        });
        try {
            assertTrue("timed out after 5s", latch.await(5, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            fail("timed out after 5s");
        }
    }

    private <T> void assertAsync(Consumer<ActionListener<T>> function, Consumer<T> furtherTests) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean listenerCalled = new AtomicBoolean(false);

        LatchedActionListener<T> listener = new LatchedActionListener<>(ActionListener.wrap(r -> {
            assertTrue("listener called more than once", listenerCalled.compareAndSet(false, true));
            furtherTests.accept(r);
        }, e -> { fail("got unexpected exception: " + e); }), latch);

        function.accept(listener);
        assertTrue("timed out after 5s", latch.await(5, TimeUnit.SECONDS));
    }

}
