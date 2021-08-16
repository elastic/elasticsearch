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
import org.elasticsearch.xpack.core.transform.transforms.SettingsConfig;
import org.elasticsearch.xpack.core.transform.transforms.TimeSyncConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpoint;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerPosition;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerStats;
import org.elasticsearch.xpack.core.transform.transforms.TransformState;
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.core.transform.transforms.DestConfigTests.randomDestConfig;
import static org.elasticsearch.xpack.core.transform.transforms.SourceConfigTests.randomSourceConfig;
import static org.elasticsearch.xpack.core.transform.transforms.pivot.PivotConfigTests.randomPivotConfig;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.Mockito.mock;

public class TransformIndexerStateTests extends ESTestCase {

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

        private TransformState persistedState;
        private int saveStateListenerCallCount = 0;
        // used for synchronizing with the test
        private CountDownLatch searchLatch;
        private CountDownLatch doProcessLatch;

        MockedTransformIndexer(
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

            persistedState = new TransformState(
                context.getTaskState(),
                initialState.get(),
                initialPosition,
                context.getCheckpoint(),
                context.getStateReason(),
                getProgress(),
                null,
                context.shouldStopAtCheckpoint()
            );
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

        @Override
        void doGetInitialProgress(SearchRequest request, ActionListener<SearchResponse> responseListener) {
            responseListener.onResponse(ONE_HIT_SEARCH_RESPONSE);
        }

        @Override
        void doDeleteByQuery(DeleteByQueryRequest deleteByQueryRequest, ActionListener<BulkByScrollResponse> responseListener) {
            responseListener.onResponse(
                new BulkByScrollResponse(
                    TimeValue.ZERO,
                    new BulkByScrollTask.Status(Collections.emptyList(), null),
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
            persistedState = new TransformState(
                context.getTaskState(),
                state,
                position,
                context.getCheckpoint(),
                context.getStateReason(),
                getProgress(),
                null,
                context.shouldStopAtCheckpoint()
            );

            Collection<ActionListener<Void>> saveStateListenersAtTheMomentOfCalling = saveStateListeners.getAndSet(null);
            try {
                if (saveStateListenersAtTheMomentOfCalling != null) {
                    saveStateListenerCallCount += saveStateListenersAtTheMomentOfCalling.size();
                    ActionListener.onResponse(saveStateListenersAtTheMomentOfCalling, null);
                }
            } catch (Exception onResponseException) {
                fail("failed to call save state listeners");
            } finally {
                next.run();
            }
        }

        @Override
        protected IterationResult<TransformIndexerPosition> doProcess(SearchResponse searchResponse) {
            // pretend that we processed 10k documents for each call
            getStats().incrementNumDocuments(10_000);
            return new IterationResult<>(
                Stream.of(new IndexRequest()),
                new TransformIndexerPosition(null, null),
                false
            );
        }

        public boolean waitingForNextSearch() {
            return super.getScheduledNextSearch() != null;
        }

        public int getSaveStateListenerCallCount() {
            return saveStateListenerCallCount;
        }

        public TransformState getPersistedState() {
            return persistedState;
        }

        @Override
        void doGetFieldMappings(ActionListener<Map<String, String>> fieldMappingsListener) {
            fieldMappingsListener.onResponse(Collections.emptyMap());
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

    public void testStopAtCheckpoint() throws Exception {
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

        for (IndexerState state : IndexerState.values()) {
            // skip indexing case, tested below
            if (IndexerState.INDEXING.equals(state)) {
                continue;
            }
            AtomicReference<IndexerState> stateRef = new AtomicReference<>(state);
            TransformContext context = new TransformContext(TransformTaskState.STARTED, "", 0, mock(TransformContext.Listener.class));
            final MockedTransformIndexer indexer = createMockIndexer(
                config,
                stateRef,
                null,
                threadPool,
                auditor,
                new TransformIndexerStats(),
                context
            );
            assertResponse(listener -> setStopAtCheckpoint(indexer, true, listener));
            assertEquals(0, indexer.getSaveStateListenerCallCount());
            if (IndexerState.STARTED.equals(state)) {
                assertTrue(context.shouldStopAtCheckpoint());
                assertTrue(indexer.getPersistedState().shouldStopAtNextCheckpoint());
            } else {
                // shouldStopAtCheckpoint should not be set, because the indexer is already stopped, stopping or aborting
                assertFalse(context.shouldStopAtCheckpoint());
                assertFalse(indexer.getPersistedState().shouldStopAtNextCheckpoint());
            }
        }

        // lets test a running indexer
        AtomicReference<IndexerState> state = new AtomicReference<>(IndexerState.STARTED);
        {
            TransformContext context = new TransformContext(TransformTaskState.STARTED, "", 0, mock(TransformContext.Listener.class));
            final MockedTransformIndexer indexer = createMockIndexer(
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
            assertEquals(indexer.getState(), IndexerState.INDEXING);

            assertResponse(listener -> setStopAtCheckpoint(indexer, true, listener));

            indexer.stop();
            assertBusy(() -> assertThat(indexer.getState(), equalTo(IndexerState.STOPPED)), 5, TimeUnit.SECONDS);

            // listener must have been called by the indexing thread
            assertEquals(1, indexer.getSaveStateListenerCallCount());

            // as the state is stopped it should go back to directly
            assertResponse(listener -> setStopAtCheckpoint(indexer, true, listener));
            assertEquals(1, indexer.getSaveStateListenerCallCount());
        }

        // do another round
        {
            TransformContext context = new TransformContext(TransformTaskState.STARTED, "", 0, mock(TransformContext.Listener.class));
            final MockedTransformIndexer indexer = createMockIndexer(
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
            assertEquals(indexer.getState(), IndexerState.INDEXING);

            // this time call it 3 times
            assertResponse(listener -> setStopAtCheckpoint(indexer, true, listener));
            assertResponse(listener -> setStopAtCheckpoint(indexer, true, listener));
            assertResponse(listener -> setStopAtCheckpoint(indexer, true, listener));

            indexer.stop();
            assertBusy(() -> assertThat(indexer.getState(), equalTo(IndexerState.STOPPED)), 5, TimeUnit.SECONDS);

            // listener must have been called by the indexing thread between 1 and 3 times
            assertThat(indexer.getSaveStateListenerCallCount(), greaterThanOrEqualTo(1));
            assertThat(indexer.getSaveStateListenerCallCount(), lessThanOrEqualTo(3));
        }

        // 3rd round with some back and forth
        {
            TransformContext context = new TransformContext(TransformTaskState.STARTED, "", 0, mock(TransformContext.Listener.class));
            final MockedTransformIndexer indexer = createMockIndexer(
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
            assertEquals(indexer.getState(), IndexerState.INDEXING);

            // slow down the indexer
            CountDownLatch searchLatch = indexer.createAwaitForSearchLatch(1);

            // this time call 5 times and change stopAtCheckpoint every time
            List<CountDownLatch> responseLatches = new ArrayList<>();
            for (int i = 0; i < 5; ++i) {
                CountDownLatch latch = new CountDownLatch(1);
                boolean stopAtCheckpoint = i % 2 == 0;
                countResponse(listener -> setStopAtCheckpoint(indexer, stopAtCheckpoint, listener), latch);
                responseLatches.add(latch);
            }

            // now let the indexer run again
            searchLatch.countDown();

            indexer.stop();
            assertBusy(() -> assertThat(indexer.getState(), equalTo(IndexerState.STOPPED)), 5, TimeUnit.SECONDS);

            // wait for all listeners
            for (CountDownLatch l : responseLatches) {
                assertTrue("timed out after 5s", l.await(5, TimeUnit.SECONDS));
            }

            // listener must have been called 5 times, because the value changed every time and we slowed down the indexer
            assertThat(indexer.getSaveStateListenerCallCount(), equalTo(5));
        }

        // 4th round: go wild
        {
            TransformContext context = new TransformContext(TransformTaskState.STARTED, "", 0, mock(TransformContext.Listener.class));
            final MockedTransformIndexer indexer = createMockIndexer(
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
            assertEquals(indexer.getState(), IndexerState.INDEXING);

            // slow down the indexer
            CountDownLatch searchLatch = indexer.createAwaitForSearchLatch(1);

            List<CountDownLatch> responseLatches = new ArrayList<>();
            int timesStopAtCheckpointChanged = 0;
            // default stopAtCheckpoint is false
            boolean previousStopAtCheckpoint = false;

            for (int i = 0; i < 3; ++i) {
                CountDownLatch latch = new CountDownLatch(1);
                boolean stopAtCheckpoint = randomBoolean();
                timesStopAtCheckpointChanged += (stopAtCheckpoint == previousStopAtCheckpoint ? 0 : 1);
                previousStopAtCheckpoint = stopAtCheckpoint;
                countResponse(listener -> setStopAtCheckpoint(indexer, stopAtCheckpoint, listener), latch);
                responseLatches.add(latch);
            }

            // now let the indexer run again
            searchLatch.countDown();

            // call it 3 times again
            for (int i = 0; i < 3; ++i) {
                boolean stopAtCheckpoint = randomBoolean();
                timesStopAtCheckpointChanged += (stopAtCheckpoint == previousStopAtCheckpoint ? 0 : 1);
                previousStopAtCheckpoint = stopAtCheckpoint;
                assertResponse(listener -> setStopAtCheckpoint(indexer, stopAtCheckpoint, listener));
            }

            indexer.stop();
            assertBusy(() -> assertThat(indexer.getState(), equalTo(IndexerState.STOPPED)), 5, TimeUnit.SECONDS);

            // wait for all listeners
            for (CountDownLatch l : responseLatches) {
                assertTrue("timed out after 5s", l.await(5, TimeUnit.SECONDS));
            }

            // listener must have been called by the indexing thread between timesStopAtCheckpointChanged and 6 times
            // this is not exact, because we do not know _when_ the other thread persisted the flag
            assertThat(indexer.getSaveStateListenerCallCount(), greaterThanOrEqualTo(timesStopAtCheckpointChanged));
            assertThat(indexer.getSaveStateListenerCallCount(), lessThanOrEqualTo(6));
        }
    }

    public void testStopAtCheckpointForThrottledTransform() throws Exception {
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
            new SettingsConfig(null, Float.valueOf(1.0f), (Boolean) null, (Boolean) null),
            null,
            null,
            null
        );
        AtomicReference<IndexerState> state = new AtomicReference<>(IndexerState.STARTED);

        TransformContext context = new TransformContext(TransformTaskState.STARTED, "", 0, mock(TransformContext.Listener.class));
        final MockedTransformIndexer indexer = createMockIndexer(
            config,
            state,
            null,
            threadPool,
            auditor,
            new TransformIndexerStats(),
            context
        );

        // create a latch to wait until data has been processed once
        CountDownLatch onResponseLatch = indexer.createCountDownOnResponseLatch(1);

        indexer.start();
        assertTrue(indexer.maybeTriggerAsyncJob(System.currentTimeMillis()));
        assertEquals(indexer.getState(), IndexerState.INDEXING);

        // wait until we processed something, the indexer should throttle
        onResponseLatch.await();
        // re-create the latch for the next use (before setStop, otherwise the other thread might overtake)
        onResponseLatch = indexer.createCountDownOnResponseLatch(1);

        // the calls are likely executed _before_ the next search is even scheduled
        assertResponse(listener -> setStopAtCheckpoint(indexer, true, listener));
        assertTrue(indexer.getPersistedState().shouldStopAtNextCheckpoint());
        assertResponse(listener -> setStopAtCheckpoint(indexer, false, listener));
        assertFalse(indexer.getPersistedState().shouldStopAtNextCheckpoint());
        assertResponse(listener -> setStopAtCheckpoint(indexer, true, listener));
        assertTrue(indexer.getPersistedState().shouldStopAtNextCheckpoint());
        assertResponse(listener -> setStopAtCheckpoint(indexer, true, listener));
        assertResponse(listener -> setStopAtCheckpoint(indexer, false, listener));
        assertFalse(indexer.getPersistedState().shouldStopAtNextCheckpoint());

        onResponseLatch.await();
        onResponseLatch = indexer.createCountDownOnResponseLatch(1);

        // wait until a search is scheduled
        assertBusy(() -> assertTrue(indexer.waitingForNextSearch()), 5, TimeUnit.SECONDS);
        assertResponse(listener -> setStopAtCheckpoint(indexer, true, listener));
        assertTrue(indexer.getPersistedState().shouldStopAtNextCheckpoint());

        onResponseLatch.await();
        onResponseLatch = indexer.createCountDownOnResponseLatch(1);

        assertBusy(() -> assertTrue(indexer.waitingForNextSearch()), 5, TimeUnit.SECONDS);
        assertResponse(listener -> setStopAtCheckpoint(indexer, false, listener));
        assertFalse(indexer.getPersistedState().shouldStopAtNextCheckpoint());

        onResponseLatch.await();
        assertBusy(() -> assertTrue(indexer.waitingForNextSearch()), 5, TimeUnit.SECONDS);
        assertResponse(listener -> setStopAtCheckpoint(indexer, true, listener));
        assertTrue(indexer.getPersistedState().shouldStopAtNextCheckpoint());
        assertResponse(listener -> setStopAtCheckpoint(indexer, true, listener));

        indexer.stop();
        assertBusy(() -> assertThat(indexer.getState(), equalTo(IndexerState.STOPPED)), 5, TimeUnit.SECONDS);
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

    private void assertResponse(Consumer<ActionListener<Void>> function) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        countResponse(function, latch);
        assertTrue("timed out after 5s", latch.await(5, TimeUnit.SECONDS));
    }

    private void countResponse(Consumer<ActionListener<Void>> function, CountDownLatch latch) throws InterruptedException {
        LatchedActionListener<Void> listener = new LatchedActionListener<>(
            ActionListener.wrap(
                r -> { assertEquals("listener called more than once", 1, latch.getCount()); },
                e -> { fail("got unexpected exception: " + e.getMessage()); }
            ),
            latch
        );
        function.accept(listener);
    }

    private MockedTransformIndexer createMockIndexer(
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
}
