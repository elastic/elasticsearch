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
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.BulkByScrollTask;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.test.junit.annotations.TestIssueLogging;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.indexing.IterationResult;
import org.elasticsearch.xpack.core.transform.action.ValidateTransformAction;
import org.elasticsearch.xpack.core.transform.transforms.SettingsConfig;
import org.elasticsearch.xpack.core.transform.transforms.TimeSyncConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpoint;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerPosition;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerStats;
import org.elasticsearch.xpack.core.transform.transforms.TransformState;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskState;
import org.elasticsearch.xpack.transform.TransformNode;
import org.elasticsearch.xpack.transform.TransformServices;
import org.elasticsearch.xpack.transform.checkpoint.CheckpointProvider;
import org.elasticsearch.xpack.transform.checkpoint.MockTimebasedCheckpointProvider;
import org.elasticsearch.xpack.transform.checkpoint.TransformCheckpointService;
import org.elasticsearch.xpack.transform.notifications.MockTransformAuditor;
import org.elasticsearch.xpack.transform.notifications.TransformAuditor;
import org.elasticsearch.xpack.transform.persistence.InMemoryTransformConfigManager;
import org.elasticsearch.xpack.transform.persistence.TransformConfigManager;
import org.elasticsearch.xpack.transform.transforms.scheduling.TransformScheduler;
import org.junit.After;
import org.junit.Before;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.core.transform.transforms.DestConfigTests.randomDestConfig;
import static org.elasticsearch.xpack.core.transform.transforms.SourceConfigTests.randomSourceConfig;
import static org.elasticsearch.xpack.core.transform.transforms.pivot.PivotConfigTests.randomPivotConfig;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class TransformIndexerStateTests extends ESTestCase {

    private static final SearchResponse ONE_HIT_SEARCH_RESPONSE = SearchResponseUtils.successfulResponse(
        SearchHits.unpooled(new SearchHit[] { SearchHit.unpooled(1) }, new TotalHits(1L, TotalHits.Relation.EQUAL_TO), 1.0f)
    );

    private Client client;
    private ThreadPool threadPool;
    private TransformAuditor auditor;
    private TransformConfigManager transformConfigManager;

    class MockedTransformIndexer extends TransformIndexer {

        private final ThreadPool threadPool;

        private TransformState persistedState;
        private AtomicInteger saveStateListenerCallCount = new AtomicInteger(0);
        private SearchResponse searchResponse = ONE_HIT_SEARCH_RESPONSE;
        // used for synchronizing with the test
        private CountDownLatch startLatch;
        private CountDownLatch searchLatch;
        private CountDownLatch doProcessLatch;
        private CountDownLatch finishLatch = new CountDownLatch(1);
        private CountDownLatch afterFinishLatch;

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
                context.shouldStopAtCheckpoint(),
                null
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

        public CountDownLatch createAwaitForStartLatch(int count) {
            return startLatch = new CountDownLatch(count);
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
        void refreshDestinationIndex(ActionListener<Void> responseListener) {
            responseListener.onResponse(null);
        }

        @Override
        protected void doNextSearch(long waitTimeInNanos, ActionListener<SearchResponse> nextPhase) {
            maybeWaitOnLatch(searchLatch);
            threadPool.generic().execute(() -> nextPhase.onResponse(searchResponse));
        }

        private static void maybeWaitOnLatch(CountDownLatch countDownLatch) {
            if (countDownLatch != null) {
                try {
                    countDownLatch.await();
                } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
            }
        }

        @Override
        protected void onStart(long now, ActionListener<Boolean> listener) {
            maybeWaitOnLatch(startLatch);
            super.onStart(now, listener);
        }

        @Override
        protected void doNextBulk(BulkRequest request, ActionListener<BulkResponse> nextPhase) {
            if (doProcessLatch != null) {
                doProcessLatch.countDown();
            }
            threadPool.generic().execute(() -> nextPhase.onResponse(new BulkResponse(new BulkItemResponse[0], 100)));
        }

        @Override
        protected void doSaveState(IndexerState state, TransformIndexerPosition position, Runnable next) {
            var saveStateListenersAtTheMomentOfCalling = saveStateListeners.get();
            if (saveStateListenersAtTheMomentOfCalling != null) {
                saveStateListenerCallCount.updateAndGet(count -> count + saveStateListenersAtTheMomentOfCalling.size());
            }
            super.doSaveState(state, position, next);
        }

        @Override
        protected IterationResult<TransformIndexerPosition> doProcess(SearchResponse searchResponse) {
            // pretend that we processed 10k documents for each call
            getStats().incrementNumDocuments(10_000);
            return new IterationResult<>(Stream.of(new IndexRequest()), new TransformIndexerPosition(null, null), false);
        }

        public boolean waitingForNextSearch() {
            return super.getScheduledNextSearch() != null;
        }

        public int getSaveStateListenerCallCount() {
            return saveStateListenerCallCount.get();
        }

        public int getSaveStateListenerCount() {
            Collection<ActionListener<Void>> saveStateListenersAtTheMomentOfCalling = saveStateListeners.get();
            return (saveStateListenersAtTheMomentOfCalling != null) ? saveStateListenersAtTheMomentOfCalling.size() : 0;
        }

        public TransformState getPersistedState() {
            return persistedState;
        }

        @Override
        void doGetFieldMappings(ActionListener<Map<String, String>> fieldMappingsListener) {
            fieldMappingsListener.onResponse(Collections.emptyMap());
        }

        @Override
        void doMaybeCreateDestIndex(Map<String, String> deducedDestIndexMappings, ActionListener<Boolean> listener) {
            listener.onResponse(null);
        }

        @Override
        void persistState(TransformState state, ActionListener<Void> listener) {
            persistedState = state;
            listener.onResponse(null);
        }

        @Override
        void validate(ActionListener<ValidateTransformAction.Response> listener) {
            listener.onResponse(null);
        }

        @Override
        protected void onFinish(ActionListener<Void> listener) {
            try {
                super.onFinish(listener);
            } finally {
                finishLatch.countDown();
            }
        }

        public void waitUntilFinished() throws InterruptedException {
            assertTrue(
                Strings.format(
                    "Timed out waiting for the Indexer to complete onFinish().  Indexer state and stats: [{}] [{}]",
                    getState().value(),
                    getStats()
                ),
                finishLatch.await(5, TimeUnit.SECONDS)
            );
        }

        void finishCheckpoint() {
            searchResponse = null;
        }

        @Override
        protected void afterFinishOrFailure() {
            maybeWaitOnLatch(afterFinishLatch);
            super.afterFinishOrFailure();
        }

        public CountDownLatch createAfterFinishLatch(int count) {
            return afterFinishLatch = new CountDownLatch(count);
        }
    }

    class MockedTransformIndexerForStatePersistenceTesting extends TransformIndexer {

        private long timeNanos = 0;

        MockedTransformIndexerForStatePersistenceTesting(
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
        }

        public void setTimeMillis(long millis) {
            this.timeNanos = TimeUnit.MILLISECONDS.toNanos(millis);
        }

        @Override
        protected long getTimeNanos() {
            return timeNanos;
        }

        @Override
        protected void doNextSearch(long waitTimeInNanos, ActionListener<SearchResponse> nextPhase) {
            threadPool.generic().execute(() -> nextPhase.onResponse(ONE_HIT_SEARCH_RESPONSE));
        }

        @Override
        protected void doNextBulk(BulkRequest request, ActionListener<BulkResponse> nextPhase) {
            threadPool.generic().execute(() -> nextPhase.onResponse(new BulkResponse(new BulkItemResponse[0], 100)));
        }

        @Override
        void doGetInitialProgress(SearchRequest request, ActionListener<SearchResponse> responseListener) {
            responseListener.onResponse(ONE_HIT_SEARCH_RESPONSE);
        }

        @Override
        void doGetFieldMappings(ActionListener<Map<String, String>> fieldMappingsListener) {
            fieldMappingsListener.onResponse(Collections.emptyMap());
        }

        @Override
        void doMaybeCreateDestIndex(Map<String, String> deducedDestIndexMappings, ActionListener<Boolean> listener) {
            listener.onResponse(null);
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
        void refreshDestinationIndex(ActionListener<Void> responseListener) {
            responseListener.onResponse(null);
        }

        @Override
        void persistState(TransformState state, ActionListener<Void> listener) {
            listener.onResponse(null);
        }

        @Override
        void validate(ActionListener<ValidateTransformAction.Response> listener) {
            listener.onResponse(null);
        }

        public void initialize() {
            this.initializeFunction();
        }
    }

    @Before
    public void setUpMocks() {
        auditor = MockTransformAuditor.createMockAuditor();
        transformConfigManager = new InMemoryTransformConfigManager();
        threadPool = new TestThreadPool(ThreadPool.Names.GENERIC);
        client = new NoOpClient(threadPool);
    }

    @After
    public void tearDownClient() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
    }

    public void testTriggerStatePersistence() {
        TransformConfig config = createTransformConfig();
        AtomicReference<IndexerState> state = new AtomicReference<>(IndexerState.INDEXING);

        TransformContext context = new TransformContext(TransformTaskState.STARTED, "", 0, mock(TransformContext.Listener.class));
        final MockedTransformIndexerForStatePersistenceTesting indexer = createMockIndexerForStatePersistenceTesting(
            config,
            state,
            null,
            threadPool,
            auditor,
            null,
            new TransformIndexerStats(),
            context
        );

        assertFalse(indexer.triggerSaveState());
        // simple: advancing the time should trigger state persistence
        indexer.setTimeMillis(80_000);
        assertTrue(indexer.triggerSaveState());
        // still true as state persistence has not been executed
        assertTrue(indexer.triggerSaveState());
        indexer.doSaveState(IndexerState.INDEXING, null, () -> {});
        // after state persistence, the trigger should return false
        assertFalse(indexer.triggerSaveState());
        // advance time twice, but don't persist
        indexer.setTimeMillis(81_000);
        assertFalse(indexer.triggerSaveState());
        indexer.setTimeMillis(140_000);
        assertFalse(indexer.triggerSaveState());
        // now trigger should return false as last persistence was 80_000
        indexer.setTimeMillis(140_001);
        assertTrue(indexer.triggerSaveState());
        // persist and check trigger
        indexer.doSaveState(IndexerState.INDEXING, null, () -> {});
        assertFalse(indexer.triggerSaveState());
        // check trigger but persist later
        indexer.setTimeMillis(200_001);
        assertFalse(indexer.triggerSaveState());
        indexer.setTimeMillis(240_000);
        indexer.doSaveState(IndexerState.INDEXING, null, () -> {});
        assertFalse(indexer.triggerSaveState());
        // last persistence should be 240_000, so don't trigger
        indexer.setTimeMillis(270_000);
        assertFalse(indexer.triggerSaveState());
        indexer.setTimeMillis(300_001);
        assertTrue(indexer.triggerSaveState());
        indexer.doSaveState(IndexerState.INDEXING, null, () -> {});
        assertFalse(indexer.triggerSaveState());
        // advance again, it shouldn't trigger
        indexer.setTimeMillis(310_000);
        assertFalse(indexer.triggerSaveState());

        // set stop at checkpoint, which must trigger state persistence
        setStopAtCheckpoint(indexer, true, ActionListener.noop());
        assertTrue(indexer.triggerSaveState());
        indexer.setTimeMillis(311_000);
        // after state persistence, trigger should return false
        indexer.doSaveState(IndexerState.INDEXING, null, () -> {});
        indexer.setTimeMillis(310_200);
        assertFalse(indexer.triggerSaveState());

        // after time has passed, trigger should work again
        indexer.setTimeMillis(371_001);
    }

    public void testStopAtCheckpoint() throws Exception {
        TransformConfig config = createTransformConfig();

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
                new TransformIndexerPosition(Collections.singletonMap("afterkey", "value"), Collections.emptyMap()),
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

        // test the case that the indexer is at a checkpoint already
        {
            AtomicReference<IndexerState> stateRef = new AtomicReference<>(IndexerState.STARTED);
            TransformContext context = new TransformContext(TransformTaskState.STARTED, "", 0, mock(TransformContext.Listener.class));
            final MockedTransformIndexer indexer = createMockIndexer(
                config,
                stateRef,
                null,
                threadPool,
                auditor,
                null,
                new TransformIndexerStats(),
                context
            );
            assertResponse(listener -> setStopAtCheckpoint(indexer, true, listener));
            assertEquals(0, indexer.getSaveStateListenerCallCount());
            // shouldStopAtCheckpoint should not be set, the indexer was started, however at a checkpoint
            assertFalse(context.shouldStopAtCheckpoint());
            assertFalse(indexer.getPersistedState().shouldStopAtNextCheckpoint());
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
                null,
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
                null,
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
                null,
                new TransformIndexerStats(),
                context
            );

            // stop the indexer before it dispatches a search thread so we can load the listeners first
            CountDownLatch searchLatch = indexer.createAwaitForSearchLatch(1);
            indexer.start();
            assertTrue(indexer.maybeTriggerAsyncJob(System.currentTimeMillis()));
            assertEquals(indexer.getState(), IndexerState.INDEXING);

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
                null,
                new TransformIndexerStats(),
                context
            );
            indexer.start();
            assertTrue(indexer.maybeTriggerAsyncJob(System.currentTimeMillis()));
            assertEquals(indexer.getState(), IndexerState.INDEXING);

            // slow down the indexer
            CountDownLatch searchLatch = indexer.createAwaitForSearchLatch(1);

            List<CountDownLatch> responseLatches = new ArrayList<>();
            // default stopAtCheckpoint is false
            boolean previousStopAtCheckpoint = false;

            for (int i = 0; i < 3; ++i) {
                CountDownLatch latch = new CountDownLatch(1);
                boolean stopAtCheckpoint = randomBoolean();
                previousStopAtCheckpoint = stopAtCheckpoint;
                countResponse(listener -> setStopAtCheckpoint(indexer, stopAtCheckpoint, listener), latch);
                responseLatches.add(latch);
            }

            // now let the indexer run again
            searchLatch.countDown();

            // call it 3 times again
            for (int i = 0; i < 3; ++i) {
                boolean stopAtCheckpoint = randomBoolean();
                previousStopAtCheckpoint = stopAtCheckpoint;
                assertResponse(listener -> setStopAtCheckpoint(indexer, stopAtCheckpoint, listener));
            }

            indexer.stop();
            assertBusy(() -> assertThat(indexer.getState(), equalTo(IndexerState.STOPPED)), 5, TimeUnit.SECONDS);

            // wait for all listeners
            for (CountDownLatch l : responseLatches) {
                assertTrue("timed out after 5s", l.await(5, TimeUnit.SECONDS));
            }

            // there should be no listeners waiting
            assertEquals(0, indexer.getSaveStateListenerCount());

            // listener must have been called by the indexing thread between timesStopAtCheckpointChanged and 6 times
            // this is not exact, because we do not know _when_ the other thread persisted the flag
            assertThat(indexer.getSaveStateListenerCallCount(), lessThanOrEqualTo(6));
        }
    }

    /**
     * Given a started transform
     * And the indexer thread has not started yet
     * When a user calls _stop?force=false
     * Then the indexer thread should exit early
     */
    public void testStopBeforeIndexingThreadStarts() throws Exception {
        var indexer = createMockIndexer(
            createTransformConfig(),
            new AtomicReference<>(IndexerState.STARTED),
            null,
            threadPool,
            auditor,
            null,
            new TransformIndexerStats(),
            new TransformContext(TransformTaskState.STARTED, "", 0, mock(TransformContext.Listener.class))
        );

        // stop the indexer thread once it kicks off
        var startLatch = indexer.createAwaitForStartLatch(1);
        assertEquals(IndexerState.STARTED, indexer.start());
        assertTrue(indexer.maybeTriggerAsyncJob(System.currentTimeMillis()));
        assertEquals(IndexerState.INDEXING, indexer.getState());

        // stop the indexer, equivalent to _stop?force=false
        assertEquals(IndexerState.STOPPING, indexer.stop());
        assertEquals(IndexerState.STOPPING, indexer.getState());

        // now let the indexer thread run
        startLatch.countDown();
        indexer.waitUntilFinished();
        assertThat(indexer.getState(), equalTo(IndexerState.STOPPED));
        assertThat(indexer.getLastCheckpoint().getCheckpoint(), equalTo(-1L));
    }

    /**
     * Given a started transform
     * And the indexer thread has not started yet
     * When a user calls _stop?force=true
     * Then the indexer thread should exit early
     */
    public void testForceStopBeforeIndexingThreadStarts() throws Exception {
        var indexer = createMockIndexer(
            createTransformConfig(),
            new AtomicReference<>(IndexerState.STARTED),
            null,
            threadPool,
            auditor,
            null,
            new TransformIndexerStats(),
            new TransformContext(TransformTaskState.STARTED, "", 0, mock(TransformContext.Listener.class))
        );

        // stop the indexer thread once it kicks off
        var startLatch = indexer.createAwaitForStartLatch(1);
        assertEquals(IndexerState.STARTED, indexer.start());
        assertTrue(indexer.maybeTriggerAsyncJob(System.currentTimeMillis()));
        assertEquals(IndexerState.INDEXING, indexer.getState());

        // stop the indexer, equivalent to _stop?force=true
        assertFalse("Transform Indexer thread should still be running", indexer.abort());
        assertEquals(IndexerState.ABORTING, indexer.getState());

        // now let the indexer thread run
        startLatch.countDown();
        indexer.waitUntilFinished();

        assertThat(indexer.getState(), equalTo(IndexerState.ABORTING));
        assertThat(indexer.getLastCheckpoint().getCheckpoint(), equalTo(-1L));
    }

    /**
     * Given a started transform
     * And the indexer thread has not started yet
     * When a user calls _stop?wait_for_checkpoint=true
     * Then the indexer thread should not exit early
     */
    public void testStopWaitForCheckpointBeforeIndexingThreadStarts() throws Exception {
        var context = new TransformContext(TransformTaskState.STARTED, "", 0, mock(TransformContext.Listener.class));
        var indexer = createMockIndexer(
            createTransformConfig(),
            new AtomicReference<>(IndexerState.STARTED),
            null,
            threadPool,
            auditor,
            null,
            new TransformIndexerStats(),
            context
        );

        // stop the indexer thread once it kicks off
        var startLatch = indexer.createAwaitForStartLatch(1);
        assertEquals(IndexerState.STARTED, indexer.start());
        assertTrue(indexer.maybeTriggerAsyncJob(System.currentTimeMillis()));
        assertEquals(IndexerState.INDEXING, indexer.getState());

        // stop the indexer, equivalent to _stop?wait_for_checkpoint=true
        context.setShouldStopAtCheckpoint(true);
        CountDownLatch stopLatch = new CountDownLatch(1);
        countResponse(listener -> setStopAtCheckpoint(indexer, true, listener), stopLatch);

        // now let the indexer thread run
        indexer.finishCheckpoint();
        startLatch.countDown();

        // wait for all listeners
        assertTrue("timed out after 5s", stopLatch.await(5, TimeUnit.SECONDS));

        // there should be no listeners waiting
        assertEquals(0, indexer.getSaveStateListenerCount());

        // listener must have been called by the indexing thread between timesStopAtCheckpointChanged and 6 times
        // this is not exact, because we do not know _when_ the other thread persisted the flag
        assertThat(indexer.getSaveStateListenerCallCount(), lessThanOrEqualTo(1));

        assertBusy(() -> {
            assertThat(indexer.getState(), equalTo(IndexerState.STOPPED));
            assertThat(indexer.getLastCheckpoint().getCheckpoint(), equalTo(1L));
        });
    }

    /**
     * Given the indexer thread is reloading the transform's Config
     * When a user calls DELETE _transform/id
     * Then the indexer thread should exit early without failing the transform
     */
    public void testDeleteTransformBeforeConfigReload() throws Exception {
        var contextListener = mock(TransformContext.Listener.class);
        var context = new TransformContext(TransformTaskState.STARTED, "", 0, contextListener);
        var config = createTransformConfig();

        var configManager = spy(transformConfigManager);

        var indexer = new MockedTransformIndexer(
            threadPool,
            new TransformServices(
                configManager,
                mock(TransformCheckpointService.class),
                auditor,
                new TransformScheduler(Clock.systemUTC(), threadPool, Settings.EMPTY, TimeValue.ZERO),
                mock(TransformNode.class)
            ),
            new MockTimebasedCheckpointProvider(config),
            config,
            new AtomicReference<>(IndexerState.STARTED),
            null,
            new TransformIndexerStats(),
            context
        );

        indexer.initialize();

        // stop the indexer thread once it kicks off
        var startLatch = indexer.createAwaitForStartLatch(1);
        assertEquals(IndexerState.STARTED, indexer.start());
        assertTrue(indexer.maybeTriggerAsyncJob(System.currentTimeMillis()));
        assertEquals(IndexerState.INDEXING, indexer.getState());

        // delete the transform, equivalent to DELETE _transform/id
        doAnswer(ans -> {
            indexer.abort();
            return ans.callRealMethod();
        }).when(configManager).getTransformConfiguration(eq(config.getId()), any());

        // now let the indexer thread run
        startLatch.countDown();
        indexer.waitUntilFinished();

        assertThat(indexer.getState(), equalTo(IndexerState.ABORTING));
        assertThat(indexer.getLastCheckpoint().getCheckpoint(), equalTo(-1L));
        verify(contextListener, never()).fail(any(), any(), any());
        verify(contextListener).shutdown();
    }

    @TestIssueLogging(
        value = "org.elasticsearch.xpack.transform.transforms:DEBUG",
        issueUrl = "https://github.com/elastic/elasticsearch/issues/92069"
    )
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
            new SettingsConfig.Builder().setRequestsPerSecond(1.0f).build(),
            null,
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
            null,
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

    /**
     * Given one indexer thread is finishing its run
     * And that thread is after finishAndSetState() but before afterFinishOrFailure()
     * When another thread calls maybeTriggerAsyncJob
     * Then that other thread should not start another indexer run
     */
    public void testRunOneJobAtATime() throws Exception {
        var indexer = createMockIndexer(
            createTransformConfig(),
            new AtomicReference<>(IndexerState.STARTED),
            null,
            threadPool,
            auditor,
            null,
            new TransformIndexerStats(),
            new TransformContext(TransformTaskState.STARTED, "", 0, mock(TransformContext.Listener.class))
        );

        // stop the indexer thread once it kicks off
        var startLatch = indexer.createAwaitForStartLatch(1);
        // stop the indexer thread before afterFinishOrFailure
        var afterFinishLatch = indexer.createAfterFinishLatch(1);

        // flip IndexerState to INDEXING
        assertEquals(IndexerState.STARTED, indexer.start());
        assertTrue(indexer.maybeTriggerAsyncJob(System.currentTimeMillis()));
        assertEquals(IndexerState.INDEXING, indexer.getState());

        // now let the indexer thread run
        indexer.finishCheckpoint();
        startLatch.countDown();

        // wait until the IndexerState flips back to STARTED
        assertBusy(() -> assertThat(indexer.getState(), equalTo(IndexerState.STARTED)), 5, TimeUnit.SECONDS);

        assertFalse(
            "Indexer state is STARTED, but the Indexer is not finished cleaning up from the previous run.",
            indexer.maybeTriggerAsyncJob(System.currentTimeMillis())
        );

        // let the first job finish
        afterFinishLatch.countDown();
        indexer.waitUntilFinished();

        // we should now (eventually) be able to schedule the next job
        assertBusy(() -> assertTrue(indexer.maybeTriggerAsyncJob(System.currentTimeMillis())), 5, TimeUnit.SECONDS);

        // stop the indexer, equivalent to _stop?force=true
        assertFalse("Transform Indexer thread should still be running", indexer.abort());
        assertEquals(IndexerState.ABORTING, indexer.getState());
    }

    private void setStopAtCheckpoint(
        TransformIndexer indexer,
        boolean shouldStopAtCheckpoint,
        ActionListener<Void> shouldStopAtCheckpointListener
    ) {
        // we need to simulate that this is called from the task, which offloads it to the generic threadpool
        CountDownLatch latch = new CountDownLatch(1);
        threadPool.generic().execute(() -> {
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
            ActionTestUtils.assertNoFailureListener(r -> assertEquals("listener called more than once", 1, latch.getCount())),
            latch
        );
        function.accept(listener);
    }

    private MockedTransformIndexer createMockIndexer(
        TransformConfig config,
        AtomicReference<IndexerState> state,
        Consumer<String> failureConsumer,
        ThreadPool threadPool,
        TransformAuditor transformAuditor,
        TransformIndexerPosition initialPosition,
        TransformIndexerStats jobStats,
        TransformContext context
    ) {
        CheckpointProvider checkpointProvider = new MockTimebasedCheckpointProvider(config);
        transformConfigManager.putTransformConfiguration(config, ActionListener.noop());
        TransformServices transformServices = new TransformServices(
            transformConfigManager,
            mock(TransformCheckpointService.class),
            transformAuditor,
            new TransformScheduler(Clock.systemUTC(), threadPool, Settings.EMPTY, TimeValue.ZERO),
            mock(TransformNode.class)
        );

        MockedTransformIndexer indexer = new MockedTransformIndexer(
            threadPool,
            transformServices,
            checkpointProvider,
            config,
            state,
            initialPosition,
            jobStats,
            context
        );

        indexer.initialize();
        return indexer;
    }

    private MockedTransformIndexerForStatePersistenceTesting createMockIndexerForStatePersistenceTesting(
        TransformConfig config,
        AtomicReference<IndexerState> state,
        Consumer<String> failureConsumer,
        ThreadPool threadPool,
        TransformAuditor transformAuditor,
        TransformIndexerPosition initialPosition,
        TransformIndexerStats jobStats,
        TransformContext context
    ) {
        CheckpointProvider checkpointProvider = new MockTimebasedCheckpointProvider(config);
        transformConfigManager.putTransformConfiguration(config, ActionListener.noop());
        TransformServices transformServices = new TransformServices(
            transformConfigManager,
            mock(TransformCheckpointService.class),
            transformAuditor,
            new TransformScheduler(Clock.systemUTC(), threadPool, Settings.EMPTY, TimeValue.ZERO),
            mock(TransformNode.class)
        );

        MockedTransformIndexerForStatePersistenceTesting indexer = new MockedTransformIndexerForStatePersistenceTesting(
            threadPool,
            transformServices,
            checkpointProvider,
            config,
            state,
            initialPosition,
            jobStats,
            context
        );

        indexer.initialize();
        return indexer;
    }

    private static TransformConfig createTransformConfig() {
        return new TransformConfig(
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
            null,
            null
        );
    }
}
